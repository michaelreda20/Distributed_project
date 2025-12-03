use anyhow::{bail, Result};
use bincode;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// =============================================================================
// P2P MESSAGE PROTOCOL
// =============================================================================

/// P2P messages exchanged between clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum P2PMessage {
    /// Request to view an image with a specific number of views
    ImageRequest {
        requesting_user: String,
        image_id: String,
        requested_views: u32,
    },
    
    /// Response with the encrypted image or rejection
    ImageResponse {
        success: bool,
        message: String,
        encrypted_image: Option<Vec<u8>>, // The encrypted image with embedded permissions
    },
    
    /// Query available images from a peer
    ListImages {
        requesting_user: String,
    },
    
    /// Response with list of available images
    ListImagesResponse {
        images: Vec<ImageMetadata>,
    },
    
    /// Request to update permissions for an already-shared image
    UpdatePermissions {
        owner: String,
        image_id: String,
        username: String,
        new_quota: u32,
    },
    
    /// Response to permission update request
    UpdatePermissionsResponse {
        success: bool,
        message: String,
    },
}

/// Metadata about an available image
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image_id: String,
    pub image_name: String,
    pub owner: String,
    pub description: Option<String>,
    pub file_size_kb: u64,
}

// =============================================================================
// P2P REQUEST HANDLER
// =============================================================================

/// Information about images that this peer owns
pub struct PeerImageStore {
    /// Map of image_id -> (file_path, metadata)
    images: HashMap<String, (PathBuf, ImageMetadata)>,
}

impl PeerImageStore {
    pub fn new() -> Self {
        Self {
            images: HashMap::new(),
        }
    }
    
    /// Add an image to the store
    pub fn add_image(
        &mut self,
        image_id: String,
        file_path: PathBuf,
        metadata: ImageMetadata,
    ) {
        self.images.insert(image_id, (file_path, metadata));
    }
    
    /// Get image file path
    pub fn get_image_path(&self, image_id: &str) -> Option<&PathBuf> {
        self.images.get(image_id).map(|(path, _)| path)
    }
    
    /// Get all image metadata
    pub fn get_all_metadata(&self) -> Vec<ImageMetadata> {
        self.images
            .values()
            .map(|(_, metadata)| metadata.clone())
            .collect()
    }
}

// =============================================================================
// P2P SERVER
// =============================================================================

/// Start a P2P server to handle incoming requests from other peers
pub async fn start_p2p_server(
    port: u16,
    username: String,
    image_store: std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("P2P server for user '{}' listening on {}", username, bind_addr);
    
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("P2P connection from {}", addr);
                let username_clone = username.clone();
                let store_clone = image_store.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_p2p_request(stream, username_clone, store_clone).await {
                        error!("Error handling P2P request from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting P2P connection: {}", e);
            }
        }
    }
}

/// Handle a single P2P request
async fn handle_p2p_request(
    mut stream: TcpStream,
    owner_username: String,
    image_store: std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> Result<()> {
    // Read message
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: P2PMessage = serde_json::from_slice(&msg_buf)?;
    
    // Process message
    let response = match message {
        P2PMessage::ImageRequest {
            requesting_user,
            image_id,
            requested_views,
        } => {
            info!(
                "Received image request from {} for image {} with {} views",
                requesting_user, image_id, requested_views
            );
            
            handle_image_request(
                &owner_username,
                &requesting_user,
                &image_id,
                requested_views,
                &image_store,
            )
            .await
        }
        
        P2PMessage::ListImages { requesting_user } => {
            info!("Received list images request from {}", requesting_user);
            
            let store = image_store.read().await;
            let images = store.get_all_metadata();
            
            P2PMessage::ListImagesResponse { images }
        }
        
        P2PMessage::UpdatePermissions {
            owner,
            image_id,
            username,
            new_quota,
        } => {
            info!(
                "Received update permissions request from {} for user {} on image {} -> {} views",
                owner, username, image_id, new_quota
            );
            
            // Verify the requester is the owner
            if owner != owner_username {
                P2PMessage::UpdatePermissionsResponse {
                    success: false,
                    message: "Only the owner can update permissions".to_string(),
                }
            } else {
                handle_update_permissions(&image_id, &username, new_quota, &image_store).await
            }
        }
        
        _ => {
            bail!("Unexpected P2P message type");
        }
    };
    
    // Send response
    let response_json = serde_json::to_string(&response)?;
    let response_bytes = response_json.as_bytes();
    
    stream.write_u32(response_bytes.len() as u32).await?;
    stream.write_all(response_bytes).await?;
    stream.flush().await?;
    
    Ok(())
}

/// Handle an image request - grant access by modifying the encrypted image
async fn handle_image_request(
    _owner: &str,
    requesting_user: &str,
    image_id: &str,
    requested_views: u32,
    image_store: &std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> P2PMessage {
    // Get the image path
    let image_path = {
        let store = image_store.read().await;
        match store.get_image_path(image_id) {
            Some(path) => path.clone(),
            None => {
                return P2PMessage::ImageResponse {
                    success: false,
                    message: format!("Image {} not found", image_id),
                    encrypted_image: None,
                };
            }
        }
    };
    
    // Read the encrypted image
    let encrypted_data = match fs::read(&image_path) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to read image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Load and decode the image to extract permissions
    let carrier_img = match image::load_from_memory(&encrypted_data) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to load image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Decode embedded payload
    use crate::lsb;
    use crate::CombinedPayload;
    
    let payload = match lsb::decode(&carrier_img) {
        Ok(Some(data)) => data,
        Ok(None) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: "No embedded data found in image".to_string(),
                encrypted_image: None,
            };
        }
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to decode image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Deserialize the combined payload
    let mut combined_data: CombinedPayload = match bincode::deserialize(&payload) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to deserialize payload: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Update permissions - add the requesting user with the specified quota
    combined_data
        .permissions
        .quotas
        .insert(requesting_user.to_string(), requested_views);
    
    info!(
        "Granted {} views to {} for image {}",
        requested_views, requesting_user, image_id
    );
    
    // Re-serialize and re-encode
    let updated_payload = match bincode::serialize(&combined_data) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to serialize updated payload: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    let updated_carrier = match lsb::encode(&carrier_img, &updated_payload) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to encode updated image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Convert to PNG bytes
    use image::ImageOutputFormat;
    use std::io::Cursor;
    
    let mut out_buf = Vec::new();
    if let Err(e) = updated_carrier.write_to(&mut Cursor::new(&mut out_buf), ImageOutputFormat::Png)
    {
        return P2PMessage::ImageResponse {
            success: false,
            message: format!("Failed to write image: {}", e),
            encrypted_image: None,
        };
    }
    
    P2PMessage::ImageResponse {
        success: true,
        message: format!(
            "Access granted: {} views for user {}",
            requested_views, requesting_user
        ),
        encrypted_image: Some(out_buf),
    }
}

/// Handle updating permissions for an existing user
async fn handle_update_permissions(
    image_id: &str,
    username: &str,
    new_quota: u32,
    image_store: &std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> P2PMessage {
    // Similar to handle_image_request but updates existing user quota
    let image_path = {
        let store = image_store.read().await;
        match store.get_image_path(image_id) {
            Some(path) => path.clone(),
            None => {
                return P2PMessage::UpdatePermissionsResponse {
                    success: false,
                    message: format!("Image {} not found", image_id),
                };
            }
        }
    };
    
    // Read, decode, update, encode, write back
    let encrypted_data = match fs::read(&image_path) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to read image: {}", e),
            };
        }
    };
    
    let carrier_img = match image::load_from_memory(&encrypted_data) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to load image: {}", e),
            };
        }
    };
    
    use crate::lsb;
    use crate::CombinedPayload;
    
    let payload = match lsb::decode(&carrier_img) {
        Ok(Some(data)) => data,
        Ok(None) | Err(_) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: "Failed to decode image".to_string(),
            };
        }
    };
    
    let mut combined_data: CombinedPayload = match bincode::deserialize(&payload) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to deserialize: {}", e),
            };
        }
    };
    
    // Update the quota
    combined_data
        .permissions
        .quotas
        .insert(username.to_string(), new_quota);
    
    // Re-encode and save
    let updated_payload = match bincode::serialize(&combined_data) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to serialize: {}", e),
            };
        }
    };
    
    let updated_carrier = match lsb::encode(&carrier_img, &updated_payload) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to encode: {}", e),
            };
        }
    };
    
    // Save back to the same file
    if let Err(e) = updated_carrier.save(&image_path) {
        return P2PMessage::UpdatePermissionsResponse {
            success: false,
            message: format!("Failed to save updated image: {}", e),
        };
    }
    
    P2PMessage::UpdatePermissionsResponse {
        success: true,
        message: format!("Updated {} to {} views", username, new_quota),
    }
}

// =============================================================================
// P2P CLIENT HELPERS
// =============================================================================

/// Send a P2P message and receive response
pub async fn send_p2p_message(peer_addr: &str, message: P2PMessage) -> Result<P2PMessage> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    
    // Send message
    let msg_json = serde_json::to_string(&message)?;
    let msg_bytes = msg_json.as_bytes();
    
    stream.write_u32(msg_bytes.len() as u32).await?;
    stream.write_all(msg_bytes).await?;
    stream.flush().await?;
    
    // Read response
    let response_len = stream.read_u32().await?;
    let mut response_buf = vec![0u8; response_len as usize];
    stream.read_exact(&mut response_buf).await?;
    
    let response: P2PMessage = serde_json::from_slice(&response_buf)?;
    Ok(response)
}

/// Request an image from a peer
pub async fn request_image_from_peer(
    peer_addr: &str,
    requesting_user: &str,
    image_id: &str,
    requested_views: u32,
) -> Result<Vec<u8>> {
    let message = P2PMessage::ImageRequest {
        requesting_user: requesting_user.to_string(),
        image_id: image_id.to_string(),
        requested_views,
    };
    
    let response = send_p2p_message(peer_addr, message).await?;
    
    match response {
        P2PMessage::ImageResponse {
            success: true,
            encrypted_image: Some(data),
            ..
        } => Ok(data),
        P2PMessage::ImageResponse {
            success: false,
            message,
            ..
        } => bail!("Request failed: {}", message),
        _ => bail!("Unexpected response type"),
    }
}

/// List available images from a peer
pub async fn list_peer_images(peer_addr: &str, requesting_user: &str) -> Result<Vec<ImageMetadata>> {
    let message = P2PMessage::ListImages {
        requesting_user: requesting_user.to_string(),
    };
    
    let response = send_p2p_message(peer_addr, message).await?;
    
    match response {
        P2PMessage::ListImagesResponse { images } => Ok(images),
        _ => bail!("Unexpected response type"),
    }
}