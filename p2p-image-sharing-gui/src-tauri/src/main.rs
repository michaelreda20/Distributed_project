// Tauri Backend for P2P Image Sharing Application
// This integrates with the cloud_p2p_project library

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use tauri::State;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex as TokioMutex};
use tokio::sync::mpsc;

// Import from your main project
use cloud_p2p_project::directory_service::{
    DirectoryMessage, ImageInfo, UserStatus,
    send_directory_message,
};
use cloud_p2p_project::p2p_protocol::{
    ImageMetadata, PeerImageStore, P2PMessage, send_p2p_message,
    list_peer_images, request_image_from_peer, request_thumbnail_from_peer, start_p2p_server,
};
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions, get_local_ip};
use image::imageops;

// ============================================================================
// APP STATE
// ============================================================================

pub struct AppState {
    pub username: Mutex<Option<String>>,
    pub p2p_port: Mutex<Option<u16>>,
    pub is_online: Mutex<bool>,
    pub directory_servers: Mutex<Vec<String>>,
    pub images_directory: Mutex<Option<PathBuf>>,
    pub local_images: Mutex<Vec<LocalImage>>,
    pub received_images: Mutex<Vec<ReceivedImage>>,
    pub image_store: Arc<RwLock<PeerImageStore>>,
    pub p2p_address: Mutex<Option<String>>,
    pub heartbeat_failures: Mutex<u32>,  // Track consecutive heartbeat failures
    pub heartbeat_shutdown: TokioMutex<Option<mpsc::Sender<()>>>,  // Channel to stop heartbeat task (using Tokio's async Mutex)
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            username: Mutex::new(None),
            p2p_port: Mutex::new(None),
            is_online: Mutex::new(false),
            directory_servers: Mutex::new(vec![
                "10.7.57.239:9000".to_string(),
                "10.7.57.240:9000".to_string(),
                "10.7.57.99:9000".to_string(),
            ]),
            images_directory: Mutex::new(None),
            local_images: Mutex::new(Vec::new()),
            received_images: Mutex::new(Vec::new()),
            image_store: Arc::new(RwLock::new(PeerImageStore::new())),
            p2p_address: Mutex::new(None),
            heartbeat_failures: Mutex::new(0),
            heartbeat_shutdown: TokioMutex::new(None),
        }
    }
}

// ============================================================================
// RESPONSE TYPES FOR FRONTEND
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalImage {
    pub image_id: String,
    pub file_path: String,
    pub file_name: String,
    pub file_size_kb: u64,
    pub is_encrypted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedImage {
    pub image_id: String,
    pub from_owner: String,
    pub file_path: String,
    pub file_name: String,
    pub views_remaining: u32,
    pub received_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    pub data: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub username: String,
    pub p2p_address: String,
    pub status: String,
    pub shared_images: Vec<ImageInfoJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageInfoJson {
    pub image_id: String,
    pub image_name: String,
    pub thumbnail_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestInfo {
    pub request_id: String,
    pub from_user: String,
    pub to_user: String,
    pub image_id: String,
    pub requested_views: u32,
    pub timestamp: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationInfo {
    pub request_id: String,
    pub to_user: String,
    pub image_id: String,
    pub requested_views: u32,
    pub status: String,
    pub timestamp: String,
}

// ============================================================================
// NETWORK HELPERS
// ============================================================================

/// Create a blurred thumbnail from an image
fn create_blurred_thumbnail(img_path: &PathBuf, blur_sigma: f32) -> Result<String> {
    // Load the image
    let img = image::open(img_path)?;

    // Resize to thumbnail size (200x200) for faster loading
    let thumbnail = img.resize(200, 200, imageops::FilterType::Lanczos3);

    // Apply Gaussian blur
    let blurred = imageops::blur(&thumbnail, blur_sigma);

    // Create a temp file path for the thumbnail
    let file_name = img_path.file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("thumbnail");
    let temp_dir = std::env::temp_dir();
    let thumbnail_path = temp_dir.join(format!("{}_blurred.png", file_name));

    // Save the blurred thumbnail
    blurred.save(&thumbnail_path)?;

    Ok(thumbnail_path.to_string_lossy().to_string())
}

async fn send_directory_message_async(addr: &str, message: DirectoryMessage) -> Result<DirectoryMessage> {
    send_directory_message(addr, message).await
}

async fn multicast_directory_message(servers: &[String], message: DirectoryMessage) -> Result<DirectoryMessage> {
    for server in servers {
        match send_directory_message_async(server, message.clone()).await {
            Ok(response) => return Ok(response),
            Err(e) => {
                eprintln!("Server {} failed: {}", server, e);
                continue;
            }
        }
    }
    bail!("All directory servers failed to respond")
}

// ============================================================================
// TAURI COMMANDS
// ============================================================================

#[tauri::command]
async fn set_directory_servers(
    state: State<'_, AppState>,
    servers: Vec<String>,
) -> Result<ApiResponse<()>, String> {
    let mut dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?;
    *dir_servers = servers.clone();
    
    Ok(ApiResponse {
        success: true,
        message: format!("Set {} directory servers", servers.len()),
        data: None,
    })
}

#[tauri::command]
async fn get_directory_servers(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<String>>, String> {
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?;
    
    Ok(ApiResponse {
        success: true,
        message: "Directory servers retrieved".to_string(),
        data: Some(dir_servers.clone()),
    })
}

#[tauri::command]
async fn go_online(
    state: State<'_, AppState>,
    username: String,
    port: u16,
    images_dir: String,
) -> Result<ApiResponse<Vec<LocalImage>>, String> {
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    if dir_servers.is_empty() {
        return Ok(ApiResponse {
            success: false,
            message: "No directory servers configured".to_string(),
            data: None,
        });
    }
    
    // Setup directory structure
    let images_path = PathBuf::from(&images_dir);
    let encrypted_dir = images_path.join("encrypted");
    let received_dir = images_path.join("received");

    // Create subdirectories if they don't exist
    let _ = fs::create_dir_all(&encrypted_dir);
    let _ = fs::create_dir_all(&received_dir);

    let mut shared_images: Vec<ImageInfo> = Vec::new();
    let mut local_images_list: Vec<LocalImage> = Vec::new();

    // Get access to the image store
    let image_store = state.image_store.clone();

    // Scan ONLY the encrypted folder for images to share with peers
    if encrypted_dir.exists() && encrypted_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&encrypted_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let image_id = file_name.clone();
                            let file_size = fs::metadata(&path)
                                .map(|m| m.len() / 1024)
                                .unwrap_or(0);

                            // These are encrypted images - share them with peers (NO thumbnail)
                            shared_images.push(ImageInfo {
                                image_id: image_id.clone(),
                                image_name: file_name.clone(),
                                thumbnail_path: None, // No thumbnail for encrypted images
                            });

                            // Add to image store
                            let metadata = ImageMetadata {
                                image_id: image_id.clone(),
                                image_name: file_name.clone(),
                                owner: username.clone(),
                                description: Some(format!("Encrypted image from {}", username)),
                                file_size_kb: file_size,
                            };

                            image_store.write().await.add_image(
                                image_id,
                                path.clone(),
                                metadata,
                            );
                        }
                    }
                }
            }
        }
    }

    // Scan the main directory for ALL images (for local display only, not shared)
    if images_path.exists() && images_path.is_dir() {
        if let Ok(entries) = fs::read_dir(&images_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let image_id = file_name.clone();
                            let file_size = fs::metadata(&path)
                                .map(|m| m.len() / 1024)
                                .unwrap_or(0);

                            // Check if encrypted
                            let is_encrypted = if let Ok(data) = fs::read(&path) {
                                if let Ok(img) = image::load_from_memory(&data) {
                                    lsb::decode(&img).ok().flatten().is_some()
                                } else {
                                    false
                                }
                            } else {
                                false
                            };

                            local_images_list.push(LocalImage {
                                image_id: image_id.clone(),
                                file_path: path.to_string_lossy().to_string(),
                                file_name: file_name.clone(),
                                file_size_kb: file_size,
                                is_encrypted,
                            });
                        }
                    }
                }
            }
        }
    }

    // NOTE: We only show images from the main directory the user entered
    // Encrypted images (in the /encrypted subfolder) are NOT shown in local images
    // They are only used for sharing with peers
    
    // Get local IP address dynamically
    let local_ip = match get_local_ip() {
        Ok(ip) => {
            eprintln!("Detected local IP: {}", ip);
            ip
        }
        Err(e) => {
            eprintln!("Failed to detect local IP: {}, falling back to 0.0.0.0", e);
            return Ok(ApiResponse {
                success: false,
                message: format!("Failed to detect local IP address: {}. Please check your network connection.", e),
                data: None,
            });
        }
    };
    let p2p_address = format!("{}:{}", local_ip, port);
    
    // Register with directory service
    let register_msg = DirectoryMessage::Register {
        username: username.clone(),
        p2p_address: p2p_address.clone(),
        shared_images,
    };
    
    match multicast_directory_message(&dir_servers, register_msg).await {
        Ok(DirectoryMessage::RegisterResponse { success, message }) => {
            if success {
                // Update state
                *state.username.lock().map_err(|e| e.to_string())? = Some(username.clone());
                *state.p2p_port.lock().map_err(|e| e.to_string())? = Some(port);
                *state.is_online.lock().map_err(|e| e.to_string())? = true;
                *state.images_directory.lock().map_err(|e| e.to_string())? = Some(images_path.clone());
                *state.local_images.lock().map_err(|e| e.to_string())? = local_images_list.clone();
                *state.p2p_address.lock().map_err(|e| e.to_string())? = Some(p2p_address.clone());
                
                // Set received images directory in the image store to the received/ subfolder
                {
                    let mut store = state.image_store.write().await;
                    store.set_received_images_dir(received_dir.clone());
                }
                
                // Start P2P server in background
                let store_clone = state.image_store.clone();
                let user_clone = username.clone();
                tokio::spawn(async move {
                    if let Err(e) = start_p2p_server(port, user_clone, store_clone).await {
                        eprintln!("P2P server error: {}", e);
                    }
                });
                
                // Start heartbeat task with shutdown channel
                let heartbeat_username = username.clone();
                let heartbeat_servers = dir_servers.clone();
                let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

                // Store the shutdown sender in state so we can cancel the heartbeat task
                *state.heartbeat_shutdown.lock().await = Some(shutdown_tx);

                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                                let heartbeat_msg = DirectoryMessage::Heartbeat {
                                    username: heartbeat_username.clone(),
                                };

                                if let Err(e) = multicast_directory_message(&heartbeat_servers, heartbeat_msg).await {
                                    eprintln!("Heartbeat failed: {}", e);
                                }
                            }
                            _ = shutdown_rx.recv() => {
                                eprintln!("Heartbeat task shutting down");
                                break;
                            }
                        }
                    }
                });
                
                Ok(ApiResponse {
                    success: true,
                    message: format!("Connected as {} on port {}", username, port),
                    data: Some(local_images_list),
                })
            } else {
                Ok(ApiResponse {
                    success: false,
                    message,
                    data: None,
                })
            }
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response from directory service".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to connect: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn go_offline(
    state: State<'_, AppState>,
) -> Result<ApiResponse<()>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone();
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();

    // CRITICAL FIX: Stop the heartbeat task FIRST before unregistering
    // This prevents the heartbeat from re-registering the user after we unregister
    if let Some(sender) = state.heartbeat_shutdown.lock().await.take() {
        // Send shutdown signal - this will stop the heartbeat loop
        let _ = sender.send(()).await;
        eprintln!("Sent shutdown signal to heartbeat task");
    }

    if let Some(user) = username {
        let unregister_msg = DirectoryMessage::Unregister {
            username: user,
        };

        let _ = multicast_directory_message(&dir_servers, unregister_msg).await;
    }

    *state.is_online.lock().map_err(|e| e.to_string())? = false;
    *state.username.lock().map_err(|e| e.to_string())? = None;
    *state.p2p_port.lock().map_err(|e| e.to_string())? = None;

    Ok(ApiResponse {
        success: true,
        message: "Went offline successfully".to_string(),
        data: None,
    })
}

#[tauri::command]
async fn get_connection_status(
    state: State<'_, AppState>,
) -> Result<ApiResponse<serde_json::Value>, String> {
    let is_online = *state.is_online.lock().map_err(|e| e.to_string())?;
    let username = state.username.lock().map_err(|e| e.to_string())?.clone();
    let port = state.p2p_port.lock().map_err(|e| e.to_string())?.clone();
    
    Ok(ApiResponse {
        success: true,
        message: "Status retrieved".to_string(),
        data: Some(serde_json::json!({
            "is_online": is_online,
            "username": username,
            "port": port,
        })),
    })
}

#[tauri::command]
async fn discover_peers(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<PeerInfo>>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();

    // Use QueryAllPeers to get both online and offline users
    let query_msg = DirectoryMessage::QueryAllPeers {
        requesting_user: username,
    };

    match multicast_directory_message(&dir_servers, query_msg).await {
        Ok(DirectoryMessage::QueryAllPeersResponse { peers }) => {
            let peer_infos: Vec<PeerInfo> = peers.iter().map(|p| PeerInfo {
                username: p.username.clone(),
                p2p_address: p.p2p_address.clone(),
                status: format!("{:?}", p.status),
                shared_images: p.shared_images.iter().map(|img| ImageInfoJson {
                    image_id: img.image_id.clone(),
                    image_name: img.image_name.clone(),
                    thumbnail_path: img.thumbnail_path.clone(),
                }).collect(),
            }).collect();

            Ok(ApiResponse {
                success: true,
                message: format!("Found {} peers", peer_infos.len()),
                data: Some(peer_infos),
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to discover peers: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn request_image(
    state: State<'_, AppState>,
    peer_username: String,
    image_id: String,
    views: u32,
) -> Result<ApiResponse<String>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    let leave_request_msg = DirectoryMessage::LeaveRequest {
        from_user: username,
        to_user: peer_username.clone(),
        image_id: image_id.clone(),
        requested_views: views,
    };
    
    match multicast_directory_message(&dir_servers, leave_request_msg).await {
        Ok(DirectoryMessage::LeaveRequestResponse { success, request_id, message }) => {
            Ok(ApiResponse {
                success,
                message,
                data: if success { Some(request_id) } else { None },
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to request image: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn get_pending_requests(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<RequestInfo>>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    let msg = DirectoryMessage::GetPendingRequests {
        username,
    };
    
    match multicast_directory_message(&dir_servers, msg).await {
        Ok(DirectoryMessage::GetPendingRequestsResponse { requests }) => {
            let request_infos: Vec<RequestInfo> = requests.iter().map(|r| {
                let timestamp_str = r.timestamp.duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| {
                        let secs = d.as_secs();
                        let now_secs = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|n| n.as_secs())
                            .unwrap_or(0);
                        let diff = now_secs.saturating_sub(secs);
                        let mins = diff / 60;
                        let hours = mins / 60;
                        if hours > 0 {
                            format!("{} hours ago", hours)
                        } else if mins > 0 {
                            format!("{} mins ago", mins)
                        } else {
                            "Just now".to_string()
                        }
                    })
                    .unwrap_or_else(|_| "Unknown".to_string());
                
                RequestInfo {
                    request_id: r.request_id.clone(),
                    from_user: r.from_user.clone(),
                    to_user: r.to_user.clone(),
                    image_id: r.image_id.clone(),
                    requested_views: r.requested_views,
                    timestamp: timestamp_str,
                    status: format!("{:?}", r.status),
                }
            }).collect();
            
            Ok(ApiResponse {
                success: true,
                message: format!("Found {} pending requests", request_infos.len()),
                data: Some(request_infos),
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to get requests: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn respond_to_request(
    state: State<'_, AppState>,
    request_id: String,
    accept: bool,
) -> Result<ApiResponse<()>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    let p2p_address = state.p2p_address.lock().map_err(|e| e.to_string())?.clone();
    
    let msg = DirectoryMessage::RespondToRequest {
        request_id: request_id.clone(),
        owner: username.clone(),
        accept,
    };
    
    match multicast_directory_message(&dir_servers, msg).await {
        Ok(DirectoryMessage::RespondToRequestResponse { success, message, request }) => {
            if success && accept {
                // If accepted, grant permissions and deliver image
                if let Some(req) = request {
                    if let Some(own_addr) = p2p_address {
                        // Fetch the image from our P2P server with the REQUESTING user's name
                        // so the quota gets embedded for them, not the owner
                        match request_image_from_peer(&own_addr, &req.from_user, &req.image_id, req.requested_views).await {
                            Ok(encrypted_image) => {
                                // Try to deliver to the requester
                                let query_msg = DirectoryMessage::QueryUser {
                                    username: req.from_user.clone(),
                                };
                                
                                if let Ok(DirectoryMessage::QueryUserResponse { user: Some(target) }) = 
                                    multicast_directory_message(&dir_servers, query_msg).await {
                                    if target.status == UserStatus::Online {
                                        let deliver_msg = P2PMessage::DeliverImage {
                                            from_owner: username.clone(),
                                            image_id: req.image_id.clone(),
                                            requested_views: req.requested_views,
                                            encrypted_image: encrypted_image.clone(),
                                        };
                                        
                                        let _ = send_p2p_message(&target.p2p_address, deliver_msg).await;
                                    } else {
                                        // Store for later delivery
                                        let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
                                            from_owner: username.clone(),
                                            target_user: req.from_user.clone(),
                                            image_id: req.image_id.clone(),
                                            new_quota: req.requested_views,
                                            embedded_image: Some(encrypted_image),
                                        };
                                        let _ = multicast_directory_message(&dir_servers, pending_msg).await;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to fetch image for delivery: {}", e);
                            }
                        }
                    }
                }
            }
            
            Ok(ApiResponse {
                success,
                message,
                data: None,
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to respond: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn get_notifications(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<NotificationInfo>>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    let msg = DirectoryMessage::GetNotifications {
        username,
    };
    
    match multicast_directory_message(&dir_servers, msg).await {
        Ok(DirectoryMessage::GetNotificationsResponse { notifications }) => {
            let notif_infos: Vec<NotificationInfo> = notifications.iter().map(|n| {
                let timestamp_str = n.timestamp.duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| {
                        let secs = d.as_secs();
                        let now_secs = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|ns| ns.as_secs())
                            .unwrap_or(0);
                        let diff = now_secs.saturating_sub(secs);
                        let mins = diff / 60;
                        let hours = mins / 60;
                        if hours > 0 {
                            format!("{} hours ago", hours)
                        } else if mins > 0 {
                            format!("{} mins ago", mins)
                        } else {
                            "Just now".to_string()
                        }
                    })
                    .unwrap_or_else(|_| "Unknown".to_string());
                
                NotificationInfo {
                    request_id: n.request_id.clone(),
                    to_user: n.to_user.clone(),
                    image_id: n.image_id.clone(),
                    requested_views: n.requested_views,
                    status: format!("{:?}", n.status),
                    timestamp: timestamp_str,
                }
            }).collect();
            
            Ok(ApiResponse {
                success: true,
                message: format!("Found {} notifications", notif_infos.len()),
                data: Some(notif_infos),
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to get notifications: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn update_permissions(
    state: State<'_, AppState>,
    target_user: String,
    image_id: String,
    new_quota: u32,
) -> Result<ApiResponse<()>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Images directory not configured")?;
    
    // Find the encrypted image file
    let encrypted_dir = images_directory.join("encrypted");
    let image_path = encrypted_dir.join(&image_id);
    
    if !image_path.exists() {
        return Ok(ApiResponse {
            success: false,
            message: format!("Encrypted image '{}' not found in {}", image_id, encrypted_dir.display()),
            data: None,
        });
    }
    
    // Read and update the image permissions locally
    let img_data = fs::read(&image_path).map_err(|e| format!("Failed to read image: {}", e))?;
    let carrier_img = image::load_from_memory(&img_data).map_err(|e| format!("Failed to load image: {}", e))?;
    
    let payload = lsb::decode(&carrier_img)
        .map_err(|e| format!("Failed to decode: {}", e))?
        .ok_or("No hidden metadata found in image")?;
    
    let mut combined_data: CombinedPayload = bincode::deserialize(&payload)
        .map_err(|e| format!("Failed to deserialize: {}", e))?;
    
    // Verify ownership
    if combined_data.permissions.owner != username {
        return Ok(ApiResponse {
            success: false,
            message: format!("You are not the owner of this image. Owner is: {}", combined_data.permissions.owner),
            data: None,
        });
    }
    
    // Update the quota for target user
    combined_data.permissions.quotas.insert(target_user.clone(), new_quota);
    
    // Re-encode and save the updated image
    let updated_payload = bincode::serialize(&combined_data)
        .map_err(|e| format!("Failed to serialize: {}", e))?;
    let updated_carrier = lsb::encode(&carrier_img, &updated_payload)
        .map_err(|e| format!("Failed to encode: {}", e))?;
    updated_carrier.save(&image_path)
        .map_err(|e| format!("Failed to save: {}", e))?;
    
    eprintln!("✓ Updated local image permissions: {} now has {} views for {}", target_user, new_quota, image_id);
    
    // Now create a copy of the image with the target user's quota embedded for delivery
    // Read the freshly saved image to get the updated version
    let updated_img_data = fs::read(&image_path).map_err(|e| format!("Failed to read updated image: {}", e))?;
    
    // Check if target user is online and deliver/store the update
    let query_msg = DirectoryMessage::QueryUser {
        username: target_user.clone(),
    };
    
    match multicast_directory_message(&dir_servers, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(target) }) => {
            if target.status == UserStatus::Online {
                eprintln!("📤 Target user {} is online, delivering updated image...", target_user);
                // Deliver directly via P2P
                let deliver_msg = P2PMessage::DeliverImage {
                    from_owner: username.clone(),
                    image_id: image_id.clone(),
                    requested_views: new_quota,
                    encrypted_image: updated_img_data.clone(),
                };
                match send_p2p_message(&target.p2p_address, deliver_msg).await {
                    Ok(P2PMessage::DeliverImageResponse { success: true, message }) => {
                        eprintln!("✓ Image delivered: {}", message);
                    }
                    Ok(P2PMessage::DeliverImageResponse { success: false, message }) => {
                        eprintln!("⚠ Delivery failed: {}, storing for later", message);
                        // Fall back to storing
                        let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
                            from_owner: username.clone(),
                            target_user: target_user.clone(),
                            image_id: image_id.clone(),
                            new_quota,
                            embedded_image: Some(updated_img_data.clone()),
                        };
                        let _ = multicast_directory_message(&dir_servers, pending_msg).await;
                    }
                    Err(e) => {
                        eprintln!("⚠ Delivery error: {}, storing for later", e);
                        let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
                            from_owner: username.clone(),
                            target_user: target_user.clone(),
                            image_id: image_id.clone(),
                            new_quota,
                            embedded_image: Some(updated_img_data.clone()),
                        };
                        let _ = multicast_directory_message(&dir_servers, pending_msg).await;
                    }
                    _ => {}
                }
            } else {
                eprintln!("📥 Target user {} is offline, storing update for later delivery...", target_user);
                let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
                    from_owner: username.clone(),
                    target_user: target_user.clone(),
                    image_id: image_id.clone(),
                    new_quota,
                    embedded_image: Some(updated_img_data),
                };
                let _ = multicast_directory_message(&dir_servers, pending_msg).await;
            }
        }
        _ => {
            eprintln!("📥 Target user {} not found, storing update for later delivery...", target_user);
            let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
                from_owner: username.clone(),
                target_user: target_user.clone(),
                image_id: image_id.clone(),
                new_quota,
                embedded_image: Some(updated_img_data),
            };
            let _ = multicast_directory_message(&dir_servers, pending_msg).await;
        }
    }
    
    let action = if new_quota == 0 { "revoked" } else { "updated" };
    Ok(ApiResponse {
        success: true,
        message: format!("Permissions {} for {}. They now have {} views.", action, target_user, new_quota),
        data: None,
    })
}

#[tauri::command]
async fn get_local_images(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<LocalImage>>, String> {
    let images = state.local_images.lock().map_err(|e| e.to_string())?.clone();

    Ok(ApiResponse {
        success: true,
        message: format!("Found {} local images", images.len()),
        data: Some(images),
    })
}

#[tauri::command]
async fn get_encrypted_images(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<LocalImage>>, String> {
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();

    let mut encrypted_list: Vec<LocalImage> = Vec::new();

    // Get the encrypted directory from the user's images directory
    let encrypted_dir = match images_directory {
        Some(images_path) => images_path.join("encrypted"),
        None => {
            return Ok(ApiResponse {
                success: true,
                message: "Not connected - no images directory configured".to_string(),
                data: Some(encrypted_list),
            });
        }
    };

    eprintln!("Scanning encrypted directory: {:?}", encrypted_dir);

    if encrypted_dir.exists() && encrypted_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&encrypted_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let image_id = file_name.clone();
                            let file_size = fs::metadata(&path)
                                .map(|m| m.len() / 1024)
                                .unwrap_or(0);

                            encrypted_list.push(LocalImage {
                                image_id: image_id.clone(),
                                file_path: path.to_string_lossy().to_string(),
                                file_name: file_name.clone(),
                                file_size_kb: file_size,
                                is_encrypted: true,
                            });
                        }
                    }
                }
            }
        }
    }

    eprintln!("Total encrypted images found: {}", encrypted_list.len());

    Ok(ApiResponse {
        success: true,
        message: format!("Found {} encrypted images", encrypted_list.len()),
        data: Some(encrypted_list),
    })
}

#[tauri::command]
async fn get_received_images(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<ReceivedImage>>, String> {
    // Scan the received images directory for ALL images
    let username = state.username.lock().map_err(|e| e.to_string())?.clone();
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();

    let mut received_list: Vec<ReceivedImage> = Vec::new();

    // Get the received directory from the user's images directory (entered in the GUI)
    let received_dir = match images_directory {
        Some(images_path) => images_path.join("received"),
        None => {
            // Fallback: user not connected yet, return empty list
            return Ok(ApiResponse {
                success: true,
                message: "Not connected - no images directory configured".to_string(),
                data: Some(received_list),
            });
        }
    };
    
    eprintln!("Scanning directory: {:?}", received_dir);
    eprintln!("Directory exists: {}", received_dir.exists());
    
    if received_dir.exists() && received_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&received_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                eprintln!("Found file: {:?}", path);
                
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();

                            // Skip the viewable_image.png temp file
                            if file_name == "viewable_image.png" {
                                continue;
                            }

                            // Try to extract owner and views from encrypted data, use defaults if not available
                            let mut from_owner = "Unknown".to_string();
                            let mut views_remaining: u32 = 0;

                            // Try to read encrypted metadata if available
                            if let Ok(data) = fs::read(&path) {
                                if let Ok(img) = image::load_from_memory(&data) {
                                    if let Ok(Some(payload_bytes)) = lsb::decode(&img) {
                                        if let Ok(combined_data) = bincode::deserialize::<CombinedPayload>(&payload_bytes) {
                                            let permissions = combined_data.permissions;
                                            from_owner = permissions.owner.clone();
                                            if let Some(user) = &username {
                                                views_remaining = permissions.quotas.get(user).copied().unwrap_or(0);
                                            }
                                        }
                                    }
                                }
                            }

                            // Get timestamp from file metadata
                            let received_at = match fs::metadata(&path).and_then(|m| m.modified()) {
                                Ok(modified_time) => {
                                    match modified_time.duration_since(SystemTime::UNIX_EPOCH) {
                                        Ok(d) => {
                                            let secs = d.as_secs();
                                            let now_secs = SystemTime::now()
                                                .duration_since(SystemTime::UNIX_EPOCH)
                                                .map(|n| n.as_secs())
                                                .unwrap_or(0);
                                            let diff = now_secs.saturating_sub(secs);
                                            let mins = diff / 60;
                                            let hours = mins / 60;
                                            let days = hours / 24;
                                            if days > 0 {
                                                format!("{} days ago", days)
                                            } else if hours > 0 {
                                                format!("{} hours ago", hours)
                                            } else if mins > 0 {
                                                format!("{} mins ago", mins)
                                            } else {
                                                "Just now".to_string()
                                            }
                                        }
                                        Err(_) => "Unknown".to_string()
                                    }
                                }
                                Err(_) => "Unknown".to_string()
                            };

                            eprintln!("Adding image: {} from {}", file_name, from_owner);
                            
                            received_list.push(ReceivedImage {
                                image_id: file_name.clone(),
                                from_owner,
                                file_path: path.to_string_lossy().to_string(),
                                file_name,
                                views_remaining,
                                received_at,
                            });
                        }
                    }
                }
            }
        }
    }

    eprintln!("Total received images found: {}", received_list.len());

    // Update state
    *state.received_images.lock().map_err(|e| e.to_string())? = received_list.clone();

    Ok(ApiResponse {
        success: true,
        message: format!("Found {} received images", received_list.len()),
        data: Some(received_list),
    })
}

#[tauri::command]
async fn refresh_images(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<LocalImage>>, String> {
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();
    let username = state.username.lock().map_err(|e| e.to_string())?.clone();
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    let is_online = *state.is_online.lock().map_err(|e| e.to_string())?;

    let images_path = match images_directory {
        Some(path) => path,
        None => {
            return Ok(ApiResponse {
                success: false,
                message: "No images directory configured. Please go online first.".to_string(),
                data: None,
            });
        }
    };

    let user = username.clone().unwrap_or_else(|| "unknown".to_string());
    let image_store = state.image_store.clone();

    let encrypted_dir = images_path.join("encrypted");
    let received_dir = images_path.join("received");

    // Ensure subdirectories exist
    let _ = fs::create_dir_all(&encrypted_dir);
    let _ = fs::create_dir_all(&received_dir);

    let mut local_images_list: Vec<LocalImage> = Vec::new();
    let mut shared_images: Vec<ImageInfo> = Vec::new();

    // Scan ONLY the encrypted folder for images to share with peers
    if encrypted_dir.exists() && encrypted_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&encrypted_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let image_id = file_name.clone();
                            let file_size = fs::metadata(&path)
                                .map(|m| m.len() / 1024)
                                .unwrap_or(0);

                            // Add encrypted image to shared list (NO thumbnail)
                            shared_images.push(ImageInfo {
                                image_id: image_id.clone(),
                                image_name: file_name.clone(),
                                thumbnail_path: None,
                            });

                            // Add to image store
                            let metadata = ImageMetadata {
                                image_id: image_id.clone(),
                                image_name: file_name.clone(),
                                owner: user.clone(),
                                description: Some(format!("Encrypted image from {}", user)),
                                file_size_kb: file_size,
                            };

                            image_store.write().await.add_image(
                                image_id,
                                path.clone(),
                                metadata,
                            );
                        }
                    }
                }
            }
        }
    }

    // Scan main directory for original images (for local display only)
    if images_path.exists() && images_path.is_dir() {
        if let Ok(entries) = fs::read_dir(&images_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            let file_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let image_id = file_name.clone();
                            let file_size = fs::metadata(&path)
                                .map(|m| m.len() / 1024)
                                .unwrap_or(0);

                            local_images_list.push(LocalImage {
                                image_id: image_id.clone(),
                                file_path: path.to_string_lossy().to_string(),
                                file_name: file_name.clone(),
                                file_size_kb: file_size,
                                is_encrypted: false,
                            });
                        }
                    }
                }
            }
        }
    }

    // NOTE: We only show images from the main directory the user entered
    // Encrypted images (in the /encrypted subfolder) are NOT shown in local images

    // Update the local images in state
    *state.local_images.lock().map_err(|e| e.to_string())? = local_images_list.clone();

    // ALSO refresh received images
    let mut received_list: Vec<ReceivedImage> = Vec::new();
    if received_dir.exists() && received_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&received_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_str().unwrap_or("").to_lowercase();
                        if ext_str == "png" || ext_str == "jpg" || ext_str == "jpeg" {
                            // Try to read the image and check if it's encrypted
                            if let Ok(data) = fs::read(&path) {
                                if let Ok(img) = image::load_from_memory(&data) {
                                    if let Ok(Some(payload_bytes)) = lsb::decode(&img) {
                                        // This is an encrypted image, decode the metadata
                                        if let Ok(combined_data) = bincode::deserialize::<CombinedPayload>(&payload_bytes) {
                                            let permissions = combined_data.permissions;
                                            let file_name = path.file_name()
                                                .and_then(|n| n.to_str())
                                                .unwrap_or("unknown")
                                                .to_string();

                                            // DEBUG: Log the permissions for troubleshooting
                                            println!("[DEBUG] Received image: {}", file_name);
                                            println!("[DEBUG] Owner: {}", permissions.owner);
                                            println!("[DEBUG] Quotas: {:?}", permissions.quotas);
                                            println!("[DEBUG] Current user: {:?}", username);

                                            // Get views remaining for current user
                                            let views_remaining = if let Some(current_user) = &username {
                                                let views = permissions.quotas.get(current_user).copied().unwrap_or(0);
                                                println!("[DEBUG] Views for '{}': {}", current_user, views);
                                                views
                                            } else {
                                                0
                                            };

                                            // Try to extract timestamp from file metadata
                                            let received_at = match fs::metadata(&path)
                                                .and_then(|m| m.modified())
                                            {
                                                Ok(modified_time) => {
                                                    match modified_time.duration_since(SystemTime::UNIX_EPOCH) {
                                                        Ok(d) => {
                                                            let secs = d.as_secs();
                                                            let now_secs = SystemTime::now()
                                                                .duration_since(SystemTime::UNIX_EPOCH)
                                                                .map(|n| n.as_secs())
                                                                .unwrap_or(0);
                                                            let diff = now_secs.saturating_sub(secs);
                                                            let mins = diff / 60;
                                                            let hours = mins / 60;
                                                            let days = hours / 24;
                                                            if days > 0 {
                                                                format!("{} days ago", days)
                                                            } else if hours > 0 {
                                                                format!("{} hours ago", hours)
                                                            } else if mins > 0 {
                                                                format!("{} mins ago", mins)
                                                            } else {
                                                                "Just now".to_string()
                                                            }
                                                        }
                                                        Err(_) => "Unknown".to_string()
                                                    }
                                                }
                                                Err(_) => "Unknown".to_string()
                                            };

                                            received_list.push(ReceivedImage {
                                                image_id: file_name.clone(),
                                                from_owner: permissions.owner,
                                                file_path: path.to_string_lossy().to_string(),
                                                file_name,
                                                views_remaining,
                                                received_at,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Update received images in state
    *state.received_images.lock().map_err(|e| e.to_string())? = received_list.clone();

    // IMPORTANT: Update the directory service with the new shared images list
    // This ensures other peers see the updated list when they query
    if is_online && username.is_some() {
        let update_msg = DirectoryMessage::UpdateSharedImages {
            username: user.clone(),
            shared_images,
        };

        // Try to update the directory service
        match multicast_directory_message(&dir_servers, update_msg).await {
            Ok(DirectoryMessage::UpdateResponse { success, message }) => {
                eprintln!("Directory service update: {} - {}", success, message);
            }
            Ok(_) => {
                eprintln!("Unexpected response from directory service");
            }
            Err(e) => {
                eprintln!("Failed to update directory service: {}", e);
            }
        }
    }

    Ok(ApiResponse {
        success: true,
        message: format!("Refreshed: Found {} local images and {} received images", local_images_list.len(), received_list.len()),
        data: Some(local_images_list),
    })
}

#[tauri::command]
async fn encrypt_image(
    state: State<'_, AppState>,
    image_path: String,
) -> Result<ApiResponse<String>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    
    // Read the image file
    let img_data = fs::read(&image_path).map_err(|e| e.to_string())?;
    
    // Create permissions metadata
    let permissions = ImagePermissions {
        owner: username.clone(),
        quotas: HashMap::new(),
    };
    let meta_bytes = bincode::serialize(&permissions).map_err(|e| e.to_string())?;
    
    // Load servers.conf from the main project directory
    let servers_content = fs::read_to_string("/home/michael12@auc.egy/Documents/Distributed_project/servers.conf")
        .unwrap_or_else(|_| "10.7.57.239:8080\n10.7.57.240:8081\n10.7.57.99:8082".to_string());
    
    let servers: Vec<String> = servers_content
        .lines()
        .filter(|s| !s.trim().is_empty() && !s.starts_with('#'))
        .map(|s| s.trim().to_string())
        .collect();
    
    // Get the images directory and encrypted subfolder
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();
    let encrypted_dir = images_directory
        .ok_or("Not online. Please go online first.")?
        .join("encrypted");

    // Ensure encrypted directory exists
    fs::create_dir_all(&encrypted_dir).map_err(|e| e.to_string())?;

    // Try each server
    for server in &servers {
        match send_encryption_request(server, &meta_bytes, &img_data) {
            Ok(encrypted_data) => {
                // Save encrypted image to the encrypted/ folder
                let original_path = PathBuf::from(&image_path);
                let file_name = original_path.file_name().unwrap_or_default().to_string_lossy();
                let output_path = encrypted_dir.join(format!("encrypted_{}", file_name));

                fs::write(&output_path, &encrypted_data).map_err(|e| e.to_string())?;
                
                let file_name = output_path.file_name().unwrap_or_default().to_string_lossy().to_string();
                let image_id = file_name.clone();
                let file_size_kb = encrypted_data.len() as u64 / 1024;
                
                // Update local images list (scope the lock to drop it before await)
                {
                    let mut local_images = state.local_images.lock().map_err(|e| e.to_string())?;
                    local_images.push(LocalImage {
                        image_id: image_id.clone(),
                        file_path: output_path.to_string_lossy().to_string(),
                        file_name: file_name.clone(),
                        file_size_kb,
                        is_encrypted: true,
                    });
                } // Lock dropped here
                
                // IMPORTANT: Also add to the P2P image store so it's immediately shareable!
                let metadata = ImageMetadata {
                    image_id: image_id.clone(),
                    image_name: file_name.clone(),
                    owner: username.clone(),
                    description: Some(format!("Encrypted image from {}", username)),
                    file_size_kb,
                };
                
                state.image_store.write().await.add_image(
                    image_id.clone(),
                    output_path.clone(),
                    metadata,
                );
                
                eprintln!("✓ Added '{}' to P2P image store - now shareable with peers!", image_id);
                
                return Ok(ApiResponse {
                    success: true,
                    message: "Image encrypted and added to shareable images".to_string(),
                    data: Some(output_path.to_string_lossy().to_string()),
                });
            }
            Err(e) => {
                eprintln!("Server {} failed: {}", server, e);
                continue;
            }
        }
    }
    
    Ok(ApiResponse {
        success: false,
        message: "All encryption servers failed".to_string(),
        data: None,
    })
}

fn send_encryption_request(addr: &str, meta_bytes: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(
        &addr.parse()?,
        Duration::from_secs(10),
    )?;
    
    stream.set_read_timeout(Some(Duration::from_secs(120)))?;
    stream.set_write_timeout(Some(Duration::from_secs(120)))?;
    
    // Send metadata size and data
    let meta_size = meta_bytes.len() as u64;
    stream.write_all(&meta_size.to_be_bytes())?;
    stream.write_all(meta_bytes)?;
    
    // Send image size and data
    let img_size = img_buf.len() as u64;
    stream.write_all(&img_size.to_be_bytes())?;
    stream.write_all(img_buf)?;
    stream.flush()?;
    
    // Read response
    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes)?;
    let response_size = u64::from_be_bytes(size_bytes);
    
    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf)?;
    
    // Check for error responses
    if let Ok(msg) = std::str::from_utf8(&response_buf) {
        if msg.starts_with("NOT_LEADER") || msg.starts_with("NO_LEADER") {
            bail!("{}", msg);
        }
    }
    
    Ok(response_buf)
}

#[tauri::command]
async fn view_image(
    state: State<'_, AppState>,
    image_path: String,
) -> Result<ApiResponse<String>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    
    // Read and decode the image
    let img_data = fs::read(&image_path).map_err(|e| e.to_string())?;
    let carrier_img = image::load_from_memory(&img_data).map_err(|e| e.to_string())?;
    
    let payload = lsb::decode(&carrier_img)
        .map_err(|e| e.to_string())?
        .ok_or("No hidden metadata found")?;
    
    let combined_data: CombinedPayload = bincode::deserialize(&payload)
        .map_err(|e| e.to_string())?;
    
    let mut permissions = combined_data.permissions;
    let client_image_bytes = combined_data.unified_image;
    
    let is_owner = username == permissions.owner;
    
    let has_access = if is_owner {
        true
    } else {
        match permissions.quotas.get_mut(&username) {
            Some(views_left) if *views_left > 0 => {
                *views_left -= 1;
                true
            }
            _ => false,
        }
    };
    
    if has_access {
        // Save viewable image
        let view_path = PathBuf::from(&image_path)
            .parent()
            .map(|p| p.join("viewable_image.png"))
            .unwrap_or_else(|| PathBuf::from("viewable_image.png"));
        
        fs::write(&view_path, &client_image_bytes).map_err(|e| e.to_string())?;
        
        // Update metadata if not owner
        if !is_owner {
            let updated_combined = CombinedPayload {
                permissions,
                unified_image: client_image_bytes,
            };
            let updated_payload = bincode::serialize(&updated_combined).map_err(|e| e.to_string())?;
            let updated_carrier = lsb::encode(&carrier_img, &updated_payload).map_err(|e| e.to_string())?;
            updated_carrier.save(&image_path).map_err(|e| e.to_string())?;
        }
        
        Ok(ApiResponse {
            success: true,
            message: "Image decoded successfully".to_string(),
            data: Some(view_path.to_string_lossy().to_string()),
        })
    } else {
        Ok(ApiResponse {
            success: false,
            message: "Access denied - no remaining views or not authorized".to_string(),
            data: None,
        })
    }
}

#[tauri::command]
async fn send_heartbeat(
    state: State<'_, AppState>,
) -> Result<ApiResponse<serde_json::Value>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone();
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    let is_online = *state.is_online.lock().map_err(|e| e.to_string())?;
    
    if !is_online || username.is_none() {
        return Ok(ApiResponse {
            success: false,
            message: "Not online".to_string(),
            data: None,
        });
    }
    
    let heartbeat_msg = DirectoryMessage::Heartbeat {
        username: username.unwrap(),
    };
    
    const MAX_FAILURES: u32 = 3; // Disconnect after 3 consecutive failures
    
    match multicast_directory_message(&dir_servers, heartbeat_msg).await {
        Ok(DirectoryMessage::HeartbeatResponse { success }) => {
            if success {
                // Reset failure counter on success
                *state.heartbeat_failures.lock().map_err(|e| e.to_string())? = 0;
            }
            Ok(ApiResponse {
                success,
                message: if success { "Heartbeat sent" } else { "Heartbeat failed" }.to_string(),
                data: Some(serde_json::json!({
                    "connected": true,
                    "failures": 0
                })),
            })
        }
        Ok(_) => {
            let mut failures = state.heartbeat_failures.lock().map_err(|e| e.to_string())?;
            *failures += 1;
            let should_disconnect = *failures >= MAX_FAILURES;
            
            if should_disconnect {
                // Auto-disconnect
                *state.is_online.lock().map_err(|e| e.to_string())? = false;
                *state.heartbeat_failures.lock().map_err(|e| e.to_string())? = 0;
            }
            
            Ok(ApiResponse {
                success: false,
                message: format!("Unexpected response (failures: {})", *failures),
                data: Some(serde_json::json!({
                    "connected": !should_disconnect,
                    "failures": *failures,
                    "disconnected": should_disconnect
                })),
            })
        }
        Err(e) => {
            let mut failures = state.heartbeat_failures.lock().map_err(|e| e.to_string())?;
            *failures += 1;
            let current_failures = *failures;
            let should_disconnect = current_failures >= MAX_FAILURES;
            drop(failures); // Release lock before acquiring another
            
            if should_disconnect {
                // Auto-disconnect - all servers are down
                *state.is_online.lock().map_err(|e| e.to_string())? = false;
                *state.heartbeat_failures.lock().map_err(|e| e.to_string())? = 0;
                eprintln!("All directory servers unreachable. Auto-disconnecting.");
            }
            
            Ok(ApiResponse {
                success: false,
                message: format!("Heartbeat failed: {} (failures: {}/{})", e, current_failures, MAX_FAILURES),
                data: Some(serde_json::json!({
                    "connected": !should_disconnect,
                    "failures": current_failures,
                    "disconnected": should_disconnect,
                    "reason": "All directory servers unreachable"
                })),
            })
        }
    }
}

#[tauri::command]
async fn list_peer_images_cmd(
    state: State<'_, AppState>,
    peer_username: String,
) -> Result<ApiResponse<Vec<ImageMetadata>>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    // Query directory to get peer's P2P address
    let query_msg = DirectoryMessage::QueryUser {
        username: peer_username.clone(),
    };
    
    match multicast_directory_message(&dir_servers, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(peer) }) => {
            match list_peer_images(&peer.p2p_address, &username).await {
                Ok(images) => {
                    Ok(ApiResponse {
                        success: true,
                        message: format!("Found {} images", images.len()),
                        data: Some(images),
                    })
                }
                Err(e) => Ok(ApiResponse {
                    success: false,
                    message: format!("Failed to list images: {}", e),
                    data: None,
                }),
            }
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            Ok(ApiResponse {
                success: false,
                message: format!("Peer {} not found or offline", peer_username),
                data: None,
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to query peer: {}", e),
            data: None,
        }),
    }
}

#[tauri::command]
async fn get_image_thumbnail(
    state: State<'_, AppState>,
    peer_username: String,
    image_id: String,
) -> Result<ApiResponse<String>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    
    // Query directory to get peer's P2P address
    let query_msg = DirectoryMessage::QueryUser {
        username: peer_username.clone(),
    };
    
    match multicast_directory_message(&dir_servers, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(peer) }) => {
            if peer.status != UserStatus::Online {
                return Ok(ApiResponse {
                    success: false,
                    message: format!("Peer {} is not online", peer_username),
                    data: None,
                });
            }
            
            // Request thumbnail from peer
            match request_thumbnail_from_peer(&peer.p2p_address, &username, &image_id).await {
                Ok(thumbnail_bytes) => {
                    // Convert to base64 for easy transfer to frontend
                    use base64::{Engine as _, engine::general_purpose::STANDARD};
                    let base64_thumbnail = STANDARD.encode(&thumbnail_bytes);
                    let data_url = format!("data:image/png;base64,{}", base64_thumbnail);
                    
                    Ok(ApiResponse {
                        success: true,
                        message: "Thumbnail retrieved".to_string(),
                        data: Some(data_url),
                    })
                }
                Err(e) => Ok(ApiResponse {
                    success: false,
                    message: format!("Failed to get thumbnail: {}", e),
                    data: None,
                }),
            }
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            Ok(ApiResponse {
                success: false,
                message: format!("Peer {} not found", peer_username),
                data: None,
            })
        }
        Ok(_) => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to query peer: {}", e),
            data: None,
        }),
    }
}

// ============================================================================
// PENDING PERMISSION UPDATES
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionUpdateInfo {
    pub from_owner: String,
    pub image_id: String,
    pub new_quota: u32,
    pub message: String,
}

#[tauri::command]
async fn check_pending_permission_updates(
    state: State<'_, AppState>,
) -> Result<ApiResponse<Vec<PermissionUpdateInfo>>, String> {
    let username = state.username.lock().map_err(|e| e.to_string())?.clone()
        .ok_or("Not logged in")?;
    let dir_servers = state.directory_servers.lock().map_err(|e| e.to_string())?.clone();
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();
    
    let received_dir = match images_directory {
        Some(path) => path.join("received"),
        None => return Ok(ApiResponse {
            success: false,
            message: "Images directory not configured".to_string(),
            data: None,
        }),
    };
    
    // Ensure received directory exists
    let _ = fs::create_dir_all(&received_dir);
    
    let pending_msg = DirectoryMessage::GetPendingPermissionUpdates {
        username: username.clone(),
    };
    
    match multicast_directory_message(&dir_servers, pending_msg).await {
        Ok(DirectoryMessage::GetPendingPermissionUpdatesResponse { updates }) => {
            let mut processed_updates: Vec<PermissionUpdateInfo> = Vec::new();
            
            for update in updates {
                let mut info = PermissionUpdateInfo {
                    from_owner: update.from_owner.clone(),
                    image_id: update.image_id.clone(),
                    new_quota: update.new_quota,
                    message: String::new(),
                };
                
                // If there's an embedded image, save it
                if let Some(embedded_image) = update.embedded_image {
                    let save_name = format!("from_{}_{}", update.from_owner, update.image_id);
                    let save_path = received_dir.join(&save_name);
                    
                    match fs::write(&save_path, &embedded_image) {
                        Ok(_) => {
                            if update.new_quota == 0 {
                                info.message = format!(
                                    "{} has REVOKED your access to image '{}'",
                                    update.from_owner, update.image_id
                                );
                            } else {
                                info.message = format!(
                                    "{} has updated your permissions for image '{}' to {} views",
                                    update.from_owner, update.image_id, update.new_quota
                                );
                            }
                        }
                        Err(e) => {
                            info.message = format!("Failed to save image: {}", e);
                        }
                    }
                } else {
                    info.message = format!(
                        "{} updated permissions for image '{}' to {} views (no image delivered)",
                        update.from_owner, update.image_id, update.new_quota
                    );
                }
                
                processed_updates.push(info);
            }
            
            Ok(ApiResponse {
                success: true,
                message: format!("Processed {} pending updates", processed_updates.len()),
                data: Some(processed_updates),
            })
        }
        Err(e) => Ok(ApiResponse {
            success: false,
            message: format!("Failed to check updates: {}", e),
            data: None,
        }),
        _ => Ok(ApiResponse {
            success: false,
            message: "Unexpected response".to_string(),
            data: None,
        }),
    }
}

#[tauri::command]
async fn delete_image(
    state: State<'_, AppState>,
    file_path: String,
) -> Result<ApiResponse<()>, String> {
    let path = PathBuf::from(&file_path);
    
    // Verify the file exists
    if !path.exists() {
        return Ok(ApiResponse {
            success: false,
            message: format!("File not found: {}", file_path),
            data: None,
        });
    }
    
    // Get images directory to make sure we're only deleting files within allowed directories
    let images_directory = state.images_directory.lock().map_err(|e| e.to_string())?.clone();
    
    let allowed = match &images_directory {
        Some(base_dir) => {
            // Allow deletion from: main dir, encrypted/, or received/
            let encrypted_dir = base_dir.join("encrypted");
            let received_dir = base_dir.join("received");
            
            path.starts_with(base_dir) || 
            path.starts_with(&encrypted_dir) || 
            path.starts_with(&received_dir)
        }
        None => false,
    };
    
    if !allowed {
        return Ok(ApiResponse {
            success: false,
            message: "Cannot delete files outside of your images directory".to_string(),
            data: None,
        });
    }
    
    // Delete the file
    match fs::remove_file(&path) {
        Ok(_) => {
            let file_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            
            eprintln!("✓ Deleted image: {}", file_path);
            
            // Also remove from local_images state if it exists there
            if let Ok(mut local_images) = state.local_images.lock() {
                local_images.retain(|img| img.file_path != file_path);
            }
            
            // Remove from image_store if it's an encrypted image
            let image_store = state.image_store.clone();
            let image_id = file_name.to_string();
            {
                let mut store = image_store.write().await;
                store.remove_image(&image_id);
            }
            
            Ok(ApiResponse {
                success: true,
                message: format!("Image '{}' deleted successfully", file_name),
                data: None,
            })
        }
        Err(e) => {
            eprintln!("✗ Failed to delete image: {}", e);
            Ok(ApiResponse {
                success: false,
                message: format!("Failed to delete image: {}", e),
                data: None,
            })
        }
    }
}

// ============================================================================
// MAIN
// ============================================================================

fn main() {
    tauri::Builder::default()
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            set_directory_servers,
            get_directory_servers,
            go_online,
            go_offline,
            get_connection_status,
            discover_peers,
            request_image,
            get_pending_requests,
            respond_to_request,
            get_notifications,
            update_permissions,
            get_local_images,
            get_encrypted_images,
            get_received_images,
            refresh_images,
            encrypt_image,
            view_image,
            send_heartbeat,
            list_peer_images_cmd,
            get_image_thumbnail,
            check_pending_permission_updates,
            delete_image,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
