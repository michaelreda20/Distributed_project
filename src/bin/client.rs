use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::directory_service::{DirectoryMessage, ImageInfo, send_directory_message};
use cloud_p2p_project::p2p_protocol::{
    ImageMetadata, PeerImageStore,
    list_peer_images, start_p2p_server,
};
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions, get_local_ip};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::sync::RwLock;

const ENCRYPTED_OUTPUT_IMAGE: &str = "encrypted_lsb_image.png";
const VIEWABLE_OUTPUT_IMAGE: &str = "viewable_image.png";
const SERVER_CONFIG_FILE: &str = "servers.conf";

// List of all directory servers for multicast
const DIRECTORY_SERVERS: &[&str] = &[
    "10.7.57.239:9000",
    "10.7.57.240:9000",
    "10.7.57.99:9000",
];

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Encrypt an image by multicasting to all servers
    Encrypt {
        /// The input image file to encrypt
        #[arg(short, long)]
        input: PathBuf,

        /// The user who owns this image
        #[arg(short, long)]
        owner: String,
    },
    
    /// View a protected image (local viewing)
    View {
        /// The protected image file to view
        #[arg(short, long)]
        input: PathBuf,

        /// The user who is trying to view the image
        #[arg(short, long)]
        user: String,
    },
    
    /// Start as a P2P peer (register with directory service and listen for requests)
    StartPeer {
        /// Your username
        #[arg(short, long)]
        username: String,
        
        /// P2P listening port
        #[arg(short, long)]
        port: u16,
        
        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },
    
    /// Discover online peers
    DiscoverPeers {
        /// Your username
        #[arg(short, long)]
        username: String,
        
        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },
    
    /// Request an image from a peer
    RequestImage {
        /// Your username
        #[arg(short, long)]
        username: String,
        
        /// Peer username to request from
        #[arg(short, long)]
        peer: String,
        
        /// Image ID to request
        #[arg(short, long)]
        image_id: String,
        
        /// Number of views requested
        #[arg(short, long)]
        views: u32,
        
        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },
    
    /// List available images from a peer
    ListPeerImages {
        /// Your username
        #[arg(short, long)]
        username: String,

        /// Peer username to query
        #[arg(short, long)]
        peer: String,

        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },

    /// Check pending image requests (for owners)
    CheckRequests {
        /// Your username
        #[arg(short, long)]
        username: String,

        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },

    /// Respond to a pending request (accept or reject)
    RespondRequest {
        /// Your username (must be the owner)
        #[arg(short, long)]
        owner: String,

        /// Request ID to respond to
        #[arg(short, long)]
        request_id: String,

        /// Accept the request (use --accept to accept, omit to reject)
        #[arg(long, default_value_t = false)]
        accept: bool,

        /// Reject the request (use --reject to reject, omit to accept)
        #[arg(long, default_value_t = false)]
        reject: bool,

        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },

    /// Check notifications (for requesters)
    CheckNotifications {
        /// Your username
        #[arg(short, long)]
        username: String,

        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },

    /// Remotely update permissions on an image you've already shared
    RemoteUpdatePermissions {
        /// Your username (the owner of the image)
        #[arg(short, long)]
        owner: String,

        /// The user whose permissions you want to update
        #[arg(short, long)]
        target_user: String,

        /// The image ID
        #[arg(short, long)]
        image_id: String,

        /// New quota (number of views)
        #[arg(short, long)]
        new_quota: u32,

        /// Directory service address (optional, will multicast if not specified)
        #[arg(short, long)]
        directory: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let cli = Cli::parse();
    match &cli.command {
        Commands::Encrypt { ref input, ref owner } => {
            handle_encrypt(input, owner)?;
        }
        Commands::View { ref input, ref user } => {
            handle_view(input, user)?;
        }
        Commands::StartPeer {
            username,
            port,
            directory,
        } => {
            handle_start_peer(username, *port, directory.as_deref()).await?;
        }
        Commands::DiscoverPeers { username, directory } => {
            handle_discover_peers(username, directory.as_deref()).await?;
        }
        Commands::RequestImage {
            username,
            peer,
            image_id,
            views,
            directory,
        } => {
            handle_request_image(username, peer, image_id, *views, directory.as_deref()).await?;
        }
        Commands::ListPeerImages {
            username,
            peer,
            directory,
        } => {
            handle_list_peer_images(username, peer, directory.as_deref()).await?;
        }
        Commands::CheckRequests { username, directory } => {
            handle_check_requests(username, directory.as_deref()).await?;
        }
        Commands::RespondRequest {
            owner,
            request_id,
            accept,
            reject,
            directory,
        } => {
            // Validate that exactly one of accept/reject is specified
            if *accept && *reject {
                bail!("Cannot specify both --accept and --reject");
            }
            if !*accept && !*reject {
                bail!("Must specify either --accept or --reject");
            }

            handle_respond_request(owner, request_id, *accept, directory.as_deref()).await?;
        }
        Commands::CheckNotifications { username, directory } => {
            handle_check_notifications(username, directory.as_deref()).await?;
        }
        Commands::RemoteUpdatePermissions {
            owner,
            target_user,
            image_id,
            new_quota,
            directory,
        } => {
            handle_remote_update_permissions(owner, target_user, image_id, *new_quota, directory.as_deref()).await?;
        }
    }

    Ok(())
}

// =============================================================================
// MULTICAST DIRECTORY SERVICE SUPPORT
// =============================================================================

/// Multicast a directory message to all directory servers
/// Returns the first successful response
async fn multicast_directory_message(
    message: DirectoryMessage,
) -> Result<DirectoryMessage> {
    println!("📡 Multicasting to {} directory servers...", DIRECTORY_SERVERS.len());
    
    let responses: Arc<Mutex<Vec<Result<DirectoryMessage>>>> = 
        Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];
    
    for &server_addr in DIRECTORY_SERVERS {
        let msg = message.clone();
        let responses_clone = Arc::clone(&responses);
        let addr = server_addr.to_string();
        
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            rt.block_on(async {
                println!("  [{}] Connecting...", addr);
                let result = send_directory_message(&addr, msg).await;
                
                match &result {
                    Ok(_) => println!("  [{}] ✓ SUCCESS", addr),
                    Err(e) => println!("  [{}] ✗ Failed: {}", addr, e),
                }
                
                let mut responses_lock = responses_clone.lock().unwrap();
                responses_lock.push(result);
            });
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }
    
    // Return first successful response
    let responses_lock = responses.lock().unwrap();
    for response in responses_lock.iter() {
        if let Ok(msg) = response {
            return Ok(msg.clone());
        }
    }
    
    bail!("❌ All directory servers failed to respond")
}

/// Send directory message (with optional multicast fallback)
async fn send_directory_or_multicast(
    specific_addr: Option<&str>,
    message: DirectoryMessage,
) -> Result<DirectoryMessage> {
    if let Some(addr) = specific_addr {
        // Use specific address if provided
        send_directory_message(addr, message).await
    } else {
        // Otherwise multicast to all servers
        multicast_directory_message(message).await
    }
}

// =============================================================================
// PHASE 1 COMMANDS (ENCRYPTION AND VIEWING)
// =============================================================================

#[derive(Debug, Clone)]
enum ServerResponse {
    Success(Vec<u8>),
    NotLeader(String),
    NoLeader,
    ConnectionFailed(String),
}

fn configure_tcp_socket(stream: &TcpStream) -> Result<()> {
    let raw_fd = stream.as_raw_fd();
    
    unsafe {
        use std::mem;
        let size: libc::c_int = 8 * 1024 * 1024;
        
        libc::setsockopt(
            raw_fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void,
            mem::size_of_val(&size) as libc::socklen_t,
        );
        
        libc::setsockopt(
            raw_fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &size as *const _ as *const libc::c_void,
            mem::size_of_val(&size) as libc::socklen_t,
        );
    }
    
    stream.set_nodelay(true)?;
    Ok(())
}

fn load_servers() -> Result<Vec<String>> {
    let content = fs::read_to_string(SERVER_CONFIG_FILE)?;
    let servers: Vec<String> = content
        .lines()
        .filter(|&s| !s.trim().is_empty() && !s.trim().starts_with('#'))
        .map(|s| s.trim().to_string())
        .collect();
    if servers.is_empty() {
        bail!("No servers found in '{}'", SERVER_CONFIG_FILE);
    }
    Ok(servers)
}

fn handle_encrypt(input_path: &PathBuf, owner: &String) -> Result<()> {
    println!("=== Encryptor Mode (Multicast with Fault Tolerance) ===");

    let servers = load_servers()?;
    println!("Loaded {} servers from '{}'", servers.len(), SERVER_CONFIG_FILE);

    let img_buf = fs::read(input_path)?;
    println!("Read '{}' ({} bytes = {:.2} MB)",
             input_path.display(),
             img_buf.len(),
             img_buf.len() as f64 / 1_048_576.0);

    // Create empty quotas - owner doesn't need a quota (unlimited access)
    // Other users can be granted access via P2P requests
    let quotas = HashMap::new();

    let permissions = ImagePermissions {
        owner: owner.clone(),
        quotas,
    };
    let meta_bytes = bincode::serialize(&permissions)?;

    println!("\n=== MULTICASTING to all {} servers ===", servers.len());
    
    let max_attempts = 5;
    let mut attempt = 0;
    
    while attempt < max_attempts {
        attempt += 1;
        
        if attempt > 1 {
            println!("\n=== ATTEMPT {} of {} ===", attempt, max_attempts);
            println!("Waiting 2 seconds before retry...");
            thread::sleep(Duration::from_secs(2));
        } else {
            println!("\n=== ATTEMPT {} of {} ===", attempt, max_attempts);
        }

        let responses = multicast_to_servers(&servers, &meta_bytes, &img_buf);
        
        let mut success_response = None;
        let mut not_leader_count = 0;
        let mut no_leader_count = 0;
        let mut connection_failed_count = 0;

        for (server_addr, response) in &responses {
            match response {
                ServerResponse::Success(image_data) => {
                    println!("  ✓ SUCCESS from {}", server_addr);
                    success_response = Some(image_data.clone());
                    break;
                }
                ServerResponse::NotLeader(hint) => {
                    println!("  ✗ {} is NOT_LEADER (hint: {})", server_addr, hint);
                    not_leader_count += 1;
                }
                ServerResponse::NoLeader => {
                    println!("  ✗ {} says NO_LEADER (election in progress)", server_addr);
                    no_leader_count += 1;
                }
                ServerResponse::ConnectionFailed(reason) => {
                    println!("  ✗ {} connection failed: {}", server_addr, reason);
                    connection_failed_count += 1;
                }
            }
        }

        if let Some(encrypted_image) = success_response {
            println!("\n=== ✓ ENCRYPTION SUCCESSFUL ===");
            println!("Received encrypted image ({} bytes = {:.2} MB)", 
                     encrypted_image.len(),
                     encrypted_image.len() as f64 / 1_048_576.0);
            
            fs::write(ENCRYPTED_OUTPUT_IMAGE, &encrypted_image)?;
            println!("Saved encrypted image to '{}'", ENCRYPTED_OUTPUT_IMAGE);
            
            println!("\n💡 NOTE: If you're running a P2P server (online mode), you need to");
            println!("   restart it for this new image to be shareable with peers.");
            println!("   Press Ctrl+C and run: cargo run --bin client -- online -u {} -p <port>", owner);
            
            return Ok(());
        }

        println!("\n--- Response Summary ---");
        println!("  NOT_LEADER responses: {}", not_leader_count);
        println!("  NO_LEADER responses: {}", no_leader_count);
        println!("  Connection failures: {}", connection_failed_count);
    }

    bail!("Failed to encrypt image after {} attempts", max_attempts)
}

fn multicast_to_servers(
    servers: &[String],
    meta_bytes: &[u8],
    img_buf: &[u8],
) -> Vec<(String, ServerResponse)> {
    use std::sync::{Arc, Mutex};
    
    println!("Multicasting to all servers simultaneously...");
    
    let responses: Arc<Mutex<Vec<(String, ServerResponse)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut thread_handles = vec![];

    for server_addr in servers {
        let meta_clone = meta_bytes.to_vec();
        let img_clone = img_buf.to_vec();
        let responses_clone = Arc::clone(&responses);
        let addr_clone = server_addr.clone();

        let handle = thread::spawn(move || {
            println!("  [Thread-{}] Connecting...", addr_clone);
            
            let response = match send_multicast_request(&addr_clone, &meta_clone, &img_clone) {
                Ok(image_data) => {
                    println!("  [Thread-{}] ✓ Got encrypted image!", addr_clone);
                    ServerResponse::Success(image_data)
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.starts_with("NOT_LEADER:") {
                        let hint = err_msg.strip_prefix("NOT_LEADER:").unwrap_or("unknown");
                        ServerResponse::NotLeader(hint.to_string())
                    } else if err_msg.starts_with("NO_LEADER") {
                        ServerResponse::NoLeader
                    } else {
                        ServerResponse::ConnectionFailed(err_msg)
                    }
                }
            };

            let mut responses_lock = responses_clone.lock().unwrap();
            responses_lock.push((addr_clone.clone(), response));
        });

        thread_handles.push(handle);
    }

    for handle in thread_handles {
        let _ = handle.join();
    }

    let responses_lock = responses.lock().unwrap();
    responses_lock.clone()
}

fn send_multicast_request(addr: &str, meta_bytes: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(
        &addr.parse()?, 
        Duration::from_secs(10)
    )?;
    
    configure_tcp_socket(&stream)?;
    
    stream.set_read_timeout(Some(Duration::from_secs(120)))?;
    stream.set_write_timeout(Some(Duration::from_secs(120)))?;

    let meta_size = meta_bytes.len() as u64;
    stream.write_all(&meta_size.to_be_bytes())?;
    stream.write_all(meta_bytes)?;

    let img_size = img_buf.len() as u64;
    stream.write_all(&img_size.to_be_bytes())?;
    stream.write_all(img_buf)?;
    
    stream.flush()?;

    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes)?;
    let response_size = u64::from_be_bytes(size_bytes);

    let mut response_buf = vec![0; response_size as usize];
    stream.read_exact(&mut response_buf)?;

    if let Ok(msg) = std::str::from_utf8(&response_buf) {
        if msg.starts_with("NOT_LEADER") || msg.starts_with("NO_LEADER") {
            bail!("{}", msg);
        }
    }

    Ok(response_buf)
}

fn handle_view(input_path: &PathBuf, current_user: &String) -> Result<()> {
    println!("\n=== Viewing Protected Image ===");
    println!("Viewing user: {}", current_user);
    println!("Viewing image: {}", input_path.display());

    let img_data = fs::read(input_path)?;
    let carrier_img = image::load_from_memory(&img_data)?;

    let payload = lsb::decode(&carrier_img)?
        .ok_or_else(|| anyhow::anyhow!("No hidden metadata found!"))?;

    let combined_data: CombinedPayload = bincode::deserialize(&payload)?;

    let mut permissions = combined_data.permissions;
    let client_image_bytes = combined_data.unified_image;

    println!("Decoded metadata before view: {:#?}", permissions);

    // Check if current user is the owner
    let is_owner = current_user == &permissions.owner;

    let has_access = if is_owner {
        // Owner always has unlimited access
        println!("✓ You are the owner - unlimited access granted!");
        true
    } else {
        // Non-owner users need quota-based access
        match permissions.quotas.get_mut(current_user) {
            Some(views_left) if *views_left > 0 => {
                println!("✓ Access granted. You have {} views left.", *views_left);
                *views_left -= 1;
                true
            }
            Some(_) => {
                println!("✗ Access denied. No remaining views!");
                false
            }
            None => {
                println!("✗ Access denied. You are not authorized to view this image!");
                false
            }
        }
    };

    if has_access {
        fs::write(VIEWABLE_OUTPUT_IMAGE, &client_image_bytes)?;
        println!("Saved viewable image to '{}'", VIEWABLE_OUTPUT_IMAGE);

        if !is_owner {
            println!(
                "Updated views left: {}",
                permissions.quotas.get(current_user).unwrap_or(&0)
            );
        }

        // Only update metadata if non-owner (to save the decremented quota)
        // Owner doesn't need metadata updates since they have unlimited access
        if !is_owner {
            let updated_combined_payload = CombinedPayload {
                permissions,
                unified_image: client_image_bytes,
            };

            let updated_payload = bincode::serialize(&updated_combined_payload)?;
            let updated_carrier = lsb::encode(&carrier_img, &updated_payload)?;

            updated_carrier.save(input_path)?;

            println!("Re-embedded updated metadata back into '{}'", input_path.display());
        } else {
            println!("Owner access - no quota update needed");
        }
    } else {
        println!("Access denied - showing default image");
        carrier_img.save(VIEWABLE_OUTPUT_IMAGE)?;
        println!("Saved default image to '{}'", VIEWABLE_OUTPUT_IMAGE);
    }

    Ok(())
}

// =============================================================================
// PHASE 2 COMMANDS (P2P AND DIRECTORY SERVICE)
// =============================================================================

async fn handle_start_peer(
    username: &str,
    port: u16,
    directory_addr: Option<&str>,
) -> Result<()> {
    // Use current directory as images directory
    let images_dir = std::env::current_dir()?;
    
    println!("=== Starting P2P Peer ===");
    println!("Username: {}", username);
    println!("P2P Port: {}", port);
    println!("Images Directory: {}", images_dir.display());
    
    if let Some(addr) = directory_addr {
        println!("Directory Service: {} (specific)", addr);
    } else {
        println!("Directory Service: Multicast mode");
    }
    
    // Scan images directory and build image store
    let image_store = Arc::new(RwLock::new(PeerImageStore::new()));
    let mut shared_images = Vec::new();
    
    if images_dir.exists() && images_dir.is_dir() {
        for entry in fs::read_dir(&images_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "png" || ext == "jpg" || ext == "jpeg" {
                        let file_name = path.file_name().unwrap().to_str().unwrap();
                        let image_id = file_name.to_string();
                        
                        let metadata = ImageMetadata {
                            image_id: image_id.clone(),
                            image_name: file_name.to_string(),
                            owner: username.to_string(),
                            description: Some(format!("Image from {}", username)),
                            file_size_kb: fs::metadata(&path)?.len() / 1024,
                        };
                        
                        let image_info = ImageInfo {
                            image_id: image_id.clone(),
                            image_name: file_name.to_string(),
                            thumbnail_path: None,
                        };
                        
                        image_store.write().await.add_image(
                            image_id,
                            path.clone(),
                            metadata,
                        );
                        
                        shared_images.push(image_info);
                    }
                }
            }
        }
    }
    
    println!("Found {} images to share", shared_images.len());

    // Get local IP address dynamically
    let local_ip = match get_local_ip() {
        Ok(ip) => {
            println!("Detected local IP: {}", ip);
            ip
        }
        Err(e) => {
            bail!("Failed to detect local IP address: {}. Please check your network connection.", e);
        }
    };
    let p2p_address = format!("{}:{}", local_ip, port);
    let register_msg = DirectoryMessage::Register {
        username: username.to_string(),
        p2p_address: p2p_address.clone(),
        shared_images: shared_images.clone(),
    };
    
    match send_directory_or_multicast(directory_addr, register_msg).await {
        Ok(DirectoryMessage::RegisterResponse { success, message }) => {
            if success {
                println!("✓ Registered with directory service: {}", message);
            } else {
                bail!("Failed to register: {}", message);
            }
        }
        Err(e) => {
            bail!("Error connecting to directory service: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }

    // Check for pending requests (someone tried to contact this user while offline)
    println!("\n📬 Checking for pending requests...");
    let check_requests_msg = DirectoryMessage::GetPendingRequests {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, check_requests_msg).await {
        Ok(DirectoryMessage::GetPendingRequestsResponse { requests }) => {
            if !requests.is_empty() {
                println!("🔔 You have {} pending request(s)!", requests.len());
                for (idx, req) in requests.iter().enumerate() {
                    println!("\n  {}. From: {}", idx + 1, req.from_user);
                    println!("     Image: {}", req.image_id);
                    println!("     Requested views: {}", req.requested_views);
                }
                println!("\n💡 Use 'check-requests' command to view details and respond");
            } else {
                println!("✓ No pending requests");
            }
        }
        Err(e) => {
            eprintln!("⚠ Could not check pending requests: {}", e);
        }
        _ => {
            eprintln!("⚠ Unexpected response when checking requests");
        }
    }

    // Check for notifications (responses to requests this user made)
    println!("\n🔔 Checking for notifications...");
    let check_notifs_msg = DirectoryMessage::GetNotifications {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, check_notifs_msg).await {
        Ok(DirectoryMessage::GetNotificationsResponse { notifications }) => {
            if !notifications.is_empty() {
                println!("🔔 You have {} notification(s)!", notifications.len());
                for (idx, notif) in notifications.iter().enumerate() {
                    let status_icon = match notif.status {
                        cloud_p2p_project::directory_service::RequestStatus::Accepted => "✅",
                        cloud_p2p_project::directory_service::RequestStatus::Rejected => "❌",
                        _ => "⏳",
                    };
                    println!("\n  {} {}. Request to: {}", status_icon, idx + 1, notif.to_user);
                    println!("     Image: {}", notif.image_id);
                    println!("     Requested views: {}", notif.requested_views);
                    println!("     Status: {:?}", notif.status);
                    println!();
                }
                println!("\n💡 Use 'check-notifications' command to view details");
            } else {
                println!("✓ No new notifications");
            }
        }
        Err(e) => {
            eprintln!("⚠ Could not check notifications: {}", e);
        }
        _ => {
            eprintln!("⚠ Unexpected response when checking notifications");
        }
    }

    // -----------------------------------------------------------------
    // NEW: Fetch any pending permission updates stored while this user was offline
    // Apply them locally so permissions are enforced immediately on login
    // -----------------------------------------------------------------
    println!("\n🔁 Checking for pending permission updates...");
    let pending_updates_msg = DirectoryMessage::GetPendingPermissionUpdates {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, pending_updates_msg).await {
        Ok(DirectoryMessage::GetPendingPermissionUpdatesResponse { updates }) => {
            if updates.is_empty() {
                println!("✓ No pending permission updates");
            } else {
                println!("🔔 Processing {} pending permission update(s)...", updates.len());

                for upd in updates {
                    println!("  • Update from {} for image {} -> {} views",
                             upd.from_owner, upd.image_id, upd.new_quota);

                    // Check if we have an embedded image to save directly
                    if let Some(embedded_image) = upd.embedded_image {
                        // Save the image directly as from_{owner}_{username}.png
                        let save_path = format!("from_{}_{}.png", upd.from_owner, username);
                        match std::fs::write(&save_path, &embedded_image) {
                            Ok(()) => {
                                println!("    ✅ Saved delivered image as '{}'", save_path);
                                if upd.new_quota == 0 {
                                    println!("    ⚠ Note: Your access has been REVOKED (0 views)");
                                } else {
                                    println!("    ✓ You have {} views available", upd.new_quota);
                                }
                            }
                            Err(e) => {
                                eprintln!("    ❌ Failed to save delivered image: {}", e);
                            }
                        }
                    } else {
                        // No embedded image - try to apply update to local image (legacy behavior)
                        let maybe_path = {
                            let store = image_store.read().await;
                            store.get_image_path(&upd.image_id).cloned()
                        };

                        if let Some(path) = maybe_path {
                            // Read file, decode, modify quota for this user, re-encode and save atomically
                            match std::fs::read(&path) {
                                Ok(buf) => match image::load_from_memory(&buf) {
                                    Ok(img) => {
                                        match lsb::decode(&img) {
                                            Ok(Some(payload)) => {
                                                match bincode::deserialize::<CombinedPayload>(&payload) {
                                                    Ok(mut combined) => {
                                                        combined.permissions.quotas.insert(username.to_string(), upd.new_quota);

                                                        match bincode::serialize(&combined) {
                                                            Ok(new_payload) => match lsb::encode(&img, &new_payload) {
                                                                Ok(updated_carrier) => {
                                                                    // Atomic save: write to temp file then rename
                                                                    // Keep .png extension so image crate recognizes format
                                                                    let tmp = path.with_file_name(format!(
                                                                        "{}.pending_update_tmp.png",
                                                                        path.file_stem().unwrap_or_default().to_string_lossy()
                                                                    ));
                                                                    if let Err(e) = updated_carrier.save(&tmp) {
                                                                        eprintln!("Failed to save temp updated image for {}: {}", path.display(), e);
                                                                        let _ = std::fs::remove_file(&tmp);
                                                                        continue;
                                                                    }
                                                                    if let Err(e) = std::fs::rename(&tmp, &path) {
                                                                        eprintln!("Failed to rename temp updated image into place for {}: {}", path.display(), e);
                                                                        let _ = std::fs::remove_file(&tmp);
                                                                        continue;
                                                                    }

                                                                    println!("    ✓ Applied update to {} (now {} views)", upd.image_id, upd.new_quota);
                                                                }
                                                                Err(e) => {
                                                                    eprintln!("Failed to encode updated payload for {}: {}", upd.image_id, e);
                                                                }
                                                            },
                                                            Err(e) => {
                                                                eprintln!("Failed to serialize updated payload for {}: {}", upd.image_id, e);
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        eprintln!("Failed to deserialize payload for {}: {}", upd.image_id, e);
                                                    }
                                                }
                                            }
                                            Ok(None) => {
                                                eprintln!("No embedded payload found in {} to apply update", path.display());
                                            }
                                            Err(e) => {
                                                eprintln!("Failed to decode LSB payload in {}: {}", path.display(), e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to load image {}: {}", path.display(), e);
                                    }
                                },
                                Err(e) => {
                                    eprintln!("Failed to read local image {}: {}", path.display(), e);
                                }
                            }
                        } else {
                            println!("    ℹ Local copy of image {} not found and no embedded image provided", upd.image_id);
                        }
                    }
                }

                println!("🔔 Pending permission updates processed");
            }
        }
        Err(e) => {
            eprintln!("⚠ Failed to fetch pending permission updates: {}", e);
        }
        _ => {
            eprintln!("⚠ Unexpected response when fetching pending permission updates");
        }
    }

    // Start heartbeat task
    let heartbeat_username = username.to_string();
    let heartbeat_addr_opt = directory_addr.map(|s| s.to_string());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            
            let heartbeat_msg = DirectoryMessage::Heartbeat {
                username: heartbeat_username.clone(),
            };
            
            let result = if let Some(ref addr) = heartbeat_addr_opt {
                send_directory_message(addr, heartbeat_msg).await
            } else {
                multicast_directory_message(heartbeat_msg).await
            };
            
            if let Err(e) = result {
                eprintln!("Heartbeat failed: {}", e);
            }
        }
    });
    
    // Start background task to periodically scan for new images
    let rescan_store = image_store.clone();
    let rescan_username = username.to_string();
    let rescan_dir = images_dir.clone();
    tokio::spawn(async move {
        loop {
            // Scan every 5 seconds for new images
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            if let Ok(entries) = fs::read_dir(&rescan_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(ext) = path.extension() {
                            if ext == "png" || ext == "jpg" || ext == "jpeg" {
                                let file_name = path.file_name().unwrap().to_str().unwrap();
                                let image_id = file_name.to_string();
                                
                                // Check if already in store
                                let already_exists = {
                                    let store = rescan_store.read().await;
                                    store.get_image_path(&image_id).is_some()
                                };
                                
                                if !already_exists {
                                    // New image found - add to store!
                                    let file_size_kb = fs::metadata(&path)
                                        .map(|m| m.len() / 1024)
                                        .unwrap_or(0);
                                    
                                    let metadata = ImageMetadata {
                                        image_id: image_id.clone(),
                                        image_name: file_name.to_string(),
                                        owner: rescan_username.clone(),
                                        description: Some(format!("Image from {}", rescan_username)),
                                        file_size_kb,
                                    };
                                    
                                    rescan_store.write().await.add_image(
                                        image_id.clone(),
                                        path.clone(),
                                        metadata,
                                    );
                                    
                                    println!("\n📷 [AUTO-DETECT] New image found: '{}'", image_id);
                                    println!("   ✓ Added to shareable images automatically!");
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    
    // Start P2P server
    println!("✓ Starting P2P server on port {}...", port);
    println!("📷 Auto-scanning for new images in: {}", images_dir.display());
    println!("Press Ctrl+C to stop");
    
    start_p2p_server(port, username.to_string(), image_store).await?;
    
    Ok(())
}

async fn handle_discover_peers(username: &str, directory_addr: Option<&str>) -> Result<()> {
    println!("=== Discovering Online Peers ===");
    println!("Your username: {}", username);
    
    if let Some(addr) = directory_addr {
        println!("Directory service: {} (specific)", addr);
    } else {
        println!("Directory service: Multicast mode");
    }
    
    let query_msg = DirectoryMessage::QueryPeers {
        requesting_user: username.to_string(),
    };
    
    match send_directory_or_multicast(directory_addr, query_msg).await {
        Ok(DirectoryMessage::QueryPeersResponse { peers }) => {
            println!("\n✓ Found {} online peers:", peers.len());
            
            if peers.is_empty() {
                println!("  No other peers online");
            } else {
                for peer in peers {
                    println!("\n  Username: {}", peer.username);
                    println!("  Address:  {}", peer.p2p_address);
                    println!("  Status:   {:?}", peer.status);
                    println!("  Shared Images: {}", peer.shared_images.len());
                    
                    for img in &peer.shared_images {
                        println!("    - {} (ID: {})", img.image_name, img.image_id);
                    }
                }
            }
            
            Ok(())
        }
        Err(e) => {
            bail!("Error querying peers: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }
}

async fn handle_request_image(
    username: &str,
    peer_username: &str,
    image_id: &str,
    views: u32,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Requesting Image from Peer ===");
    println!("Your username: {}", username);
    println!("Peer: {}", peer_username);
    println!("Image ID: {}", image_id);
    println!("Requested views: {}", views);

    // First, verify that the requesting user (yourself) is online
    println!("\nVerifying you are connected to directory service...");
    let self_query_msg = DirectoryMessage::QueryUser {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, self_query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user_entry) }) => {
            // Check if user is actually ONLINE (not just registered)
            use cloud_p2p_project::directory_service::UserStatus;
            if user_entry.status == UserStatus::Online {
                println!("✓ You are online and connected to directory service");
            } else {
                bail!(
                    "❌ You must be online to request images!\n\
                    \n\
                    Your account exists but your P2P peer is offline.\n\
                    You need to start your P2P peer:\n\
                      cargo run --bin client -- start-peer --username {} --port <PORT>\n\
                    \n\
                    This will mark you as online and allow you to request images.",
                    username
                );
            }
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!(
                "❌ You must be online to request images!\n\
                \n\
                You need to start your P2P peer first:\n\
                  cargo run --bin client -- start-peer --username {} --port <PORT>\n\
                \n\
                This will register you with the directory service and allow you to request images.",
                username
            );
        }
        Err(e) => {
            bail!("Error connecting to directory service: {}\n\nMake sure the directory service is running.", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }

    // Query directory service for peer address
    println!("\nLooking up peer '{}'...", peer_username);
    let query_msg = DirectoryMessage::QueryUser {
        username: peer_username.to_string(),
    };
    
    match send_directory_or_multicast(directory_addr, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user) }) => {
            use cloud_p2p_project::directory_service::UserStatus;
            if user.status == UserStatus::Online {
                println!("✓ Owner '{}' is online", peer_username);
            } else {
                println!("ℹ Owner '{}' is currently offline", peer_username);
            }
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            println!("ℹ Owner '{}' is not registered yet", peer_username);
        }
        Err(e) => {
            bail!("Error querying directory service: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    };

    // Always leave a request for the owner to approve (whether online or offline)
    println!("\n📝 Submitting request to owner for approval...");
    let leave_request_msg = DirectoryMessage::LeaveRequest {
        from_user: username.to_string(),
        to_user: peer_username.to_string(),
        image_id: image_id.to_string(),
        requested_views: views,
    };

    match send_directory_or_multicast(directory_addr, leave_request_msg).await {
        Ok(DirectoryMessage::LeaveRequestResponse { success: true, request_id, message }) => {
            println!("✓ Request submitted successfully!");
            println!("\n📋 Request details:");
            println!("   Request ID: {}", request_id);
            println!("   To: {}", peer_username);
            println!("   Image: {}", image_id);
            println!("   Requested views: {}", views);
            println!("\n⏳ Waiting for owner approval...");
            println!("   The owner must accept your request before you can view the image.");
            println!("   If the owner is online, they will see your request immediately.");
            println!("   If offline, they will see it when they come online.");
            println!("\n💡 Check for owner's response with:");
            println!("   cargo run --bin client -- check-notifications --username {}", username);
            println!("\n   Once accepted, the image will be automatically delivered to you!");
            Ok(())
        }
        Ok(DirectoryMessage::LeaveRequestResponse { success: false, message, .. }) => {
            bail!("Failed to leave request: {}", message);
        }
        Err(e) => {
            bail!("Error leaving request: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }
}

async fn handle_list_peer_images(
    username: &str,
    peer_username: &str,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Listing Peer's Images ===");
    println!("Your username: {}", username);
    println!("Peer: {}", peer_username);

    // First, verify that the requesting user (yourself) is online
    println!("\nVerifying you are connected to directory service...");
    let self_query_msg = DirectoryMessage::QueryUser {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, self_query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user_entry) }) => {
            // Check if user is actually ONLINE (not just registered)
            use cloud_p2p_project::directory_service::UserStatus;
            if user_entry.status == UserStatus::Online {
                println!("✓ You are online and connected to directory service");
            } else {
                bail!(
                    "❌ You must be online to list peer images!\n\
                    \n\
                    Your account exists but your P2P peer is offline.\n\
                    You need to start your P2P peer:\n\
                      cargo run --bin client -- start-peer --username {} --port <PORT>",
                    username
                );
            }
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!(
                "❌ You must be online to list peer images!\n\
                \n\
                You need to start your P2P peer first:\n\
                  cargo run --bin client -- start-peer --username {} --port <PORT>",
                username
            );
        }
        Err(e) => {
            bail!("Error connecting to directory service: {}\n\nMake sure the directory service is running.", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }

    // Query directory service for peer address
    println!("\nLooking up peer '{}'...", peer_username);
    let query_msg = DirectoryMessage::QueryUser {
        username: peer_username.to_string(),
    };
    
    let peer_addr = match send_directory_or_multicast(directory_addr, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user) }) => {
            println!("✓ Found peer at: {}", user.p2p_address);
            user.p2p_address
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!("Peer '{}' not found or offline", peer_username);
        }
        Err(e) => {
            bail!("Error querying directory service: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    };
    
    // List images from peer
    println!("Querying peer for available images...");
    match list_peer_images(&peer_addr, username).await {
        Ok(images) => {
            println!("\n✓ Peer has {} images available:", images.len());
            
            if images.is_empty() {
                println!("  No images shared by this peer");
            } else {
                for img in images {
                    println!("\n  Image ID: {}", img.image_id);
                    println!("  Name:     {}", img.image_name);
                    println!("  Owner:    {}", img.owner);
                    println!("  Size:     {} KB", img.file_size_kb);
                    
                    if let Some(desc) = img.description {
                        println!("  Description: {}", desc);
                    }
                }
            }
            
            Ok(())
        }
        Err(e) => {
            bail!("Failed to list images from peer: {}", e);
        }
    }
}

/// Helper function to store a pending permission update with embedded image
async fn store_pending_update_with_image(
    directory_addr: Option<&str>,
    owner: &str,
    target_user: &str,
    image_id: &str,
    new_quota: u32,
    encrypted_image: Vec<u8>,
) {
    let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
        from_owner: owner.to_string(),
        target_user: target_user.to_string(),
        image_id: image_id.to_string(),
        new_quota,
        embedded_image: Some(encrypted_image),
    };

    match send_directory_or_multicast(directory_addr, pending_msg).await {
        Ok(DirectoryMessage::StorePendingPermissionUpdateResponse { success: true, message, .. }) => {
            println!("✅ {}", message);
            println!("   Image will be delivered as from_{}_{}.png when {} comes online", owner, target_user, target_user);
        }
        Ok(DirectoryMessage::StorePendingPermissionUpdateResponse { success: false, message, .. }) => {
            eprintln!("⚠ Failed to store pending update: {}", message);
        }
        Err(e) => {
            eprintln!("⚠ Failed to store pending update: {}", e);
        }
        _ => {
            eprintln!("⚠ Unexpected response when storing pending update");
        }
    }
}

async fn handle_update_permissions(
    owner: &str,
    image_id: &str,
    username: &str,
    new_quota: u32,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Updating Permissions ===");
    println!("Owner: {}", owner);
    println!("Image ID: {}", image_id);
    println!("User: {}", username);
    println!("New quota: {} views", new_quota);

    if new_quota == 0 {
        println!("⚠ This will REVOKE access for user '{}'", username);
    }

    // The owner needs to connect to their OWN P2P server to update the image
    // Query directory service for own address
    let query_msg = DirectoryMessage::QueryUser {
        username: owner.to_string(),
    };

    let own_addr = match send_directory_or_multicast(directory_addr, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user) }) => {
            println!("✓ Found own P2P server at: {}", user.p2p_address);
            user.p2p_address
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!("You must be running your P2P server to update permissions.\nStart with: cargo run --bin client -- start-peer --username {} --port <port>", owner);
        }
        Err(e) => {
            bail!("Error querying directory service: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    };

    // Send update permissions request to own P2P server
    use cloud_p2p_project::p2p_protocol::{P2PMessage, send_p2p_message, request_image_from_peer};

    let update_msg = P2PMessage::UpdatePermissions {
        owner: owner.to_string(),
        image_id: image_id.to_string(),
        username: username.to_string(),
        new_quota,
    };

    println!("Sending permission update request...");
    match send_p2p_message(&own_addr, update_msg).await {
        Ok(P2PMessage::UpdatePermissionsResponse { success: true, message }) => {
            println!("✓ {}", message);
            if new_quota == 0 {
                println!("✓ User '{}' can no longer view this image", username);
            } else {
                println!("✓ User '{}' now has {} views", username, new_quota);
            }

            // Now check if the target user is online and send them the updated image
            println!("\n📤 Checking if {} is online to send updated image...", username);
            
            let target_query_msg = DirectoryMessage::QueryUser {
                username: username.to_string(),
            };

            match send_directory_or_multicast(directory_addr, target_query_msg).await {
                Ok(DirectoryMessage::QueryUserResponse { user: Some(target_user) }) => {
                    use cloud_p2p_project::directory_service::UserStatus;
                    if target_user.status == UserStatus::Online {
                        println!("✓ {} is online at {}", username, target_user.p2p_address);
                        println!("🚀 Fetching updated image to send to {}...", username);

                        // Fetch the updated image from our own P2P server (as owner)
                        match request_image_from_peer(
                            &own_addr,
                            owner,  // Request as owner
                            image_id,
                            new_quota,
                        ).await {
                            Ok(encrypted_image) => {
                                println!("✓ Image fetched, now delivering to {}...", username);

                                // Clone the image data in case we need to store it for later
                                let image_for_fallback = encrypted_image.clone();

                                // Deliver the updated image to the target user
                                let deliver_msg = P2PMessage::DeliverImage {
                                    from_owner: owner.to_string(),
                                    image_id: image_id.to_string(),
                                    requested_views: new_quota,
                                    encrypted_image,
                                };

                                match send_p2p_message(&target_user.p2p_address, deliver_msg).await {
                                    Ok(P2PMessage::DeliverImageResponse { success: true, message }) => {
                                        println!("\n✅ Updated image delivered successfully to {}!", username);
                                        println!("   {}", message);
                                    }
                                    Ok(P2PMessage::DeliverImageResponse { success: false, message }) => {
                                        eprintln!("\n⚠ Failed to deliver updated image: {}", message);
                                        // Fall back to storing pending update
                                        println!("📝 Storing update for later delivery...");
                                        store_pending_update_with_image(directory_addr, owner, username, image_id, new_quota, image_for_fallback).await;
                                    }
                                    Err(e) => {
                                        eprintln!("\n⚠ Could not deliver updated image to {} (may be offline): {}", username, e);
                                        // Fall back to storing pending update
                                        println!("📝 Storing update for later delivery...");
                                        store_pending_update_with_image(directory_addr, owner, username, image_id, new_quota, image_for_fallback).await;
                                    }
                                    _ => {
                                        eprintln!("\n⚠ Unexpected response when delivering image");
                                        // Fall back to storing pending update
                                        println!("📝 Storing update for later delivery...");
                                        store_pending_update_with_image(directory_addr, owner, username, image_id, new_quota, image_for_fallback).await;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("\n⚠ Failed to fetch image for delivery: {}", e);
                            }
                        }
                    } else {
                        println!("ℹ {} is offline. Storing update with image for delivery when they come online...", username);
                        
                        // Fetch the updated image to store for later delivery
                        match request_image_from_peer(
                            &own_addr,
                            owner,  // Request as owner
                            image_id,
                            new_quota,
                        ).await {
                            Ok(encrypted_image) => {
                                println!("✓ Image fetched, storing for later delivery...");
                                store_pending_update_with_image(directory_addr, owner, username, image_id, new_quota, encrypted_image).await;
                            }
                            Err(e) => {
                                eprintln!("⚠ Failed to fetch image for storage: {}", e);
                            }
                        }
                    }
                }
                Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
                    println!("ℹ {} is not registered. Storing update with image for delivery when they register...", username);
                    
                    // Fetch the updated image to store for later delivery
                    match request_image_from_peer(
                        &own_addr,
                        owner,  // Request as owner
                        image_id,
                        new_quota,
                    ).await {
                        Ok(encrypted_image) => {
                            println!("✓ Image fetched, storing for later delivery...");
                            store_pending_update_with_image(directory_addr, owner, username, image_id, new_quota, encrypted_image).await;
                        }
                        Err(e) => {
                            eprintln!("⚠ Failed to fetch image for storage: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("⚠ Could not check if {} is online: {}", username, e);
                }
                _ => {}
            }

            Ok(())
        }
        Ok(P2PMessage::UpdatePermissionsResponse { success: false, message }) => {
            bail!("Failed to update permissions: {}", message);
        }
        Err(e) => {
            bail!("Failed to communicate with P2P server: {}", e);
        }
        _ => {
            bail!("Unexpected response from P2P server");
        }
    }
}

async fn handle_check_requests(
    username: &str,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Checking Pending Requests ===");
    println!("Username: {}", username);

    // Verify the user is online first
    println!("\n🔍 Verifying you are online...");
    let self_query = DirectoryMessage::QueryUser {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, self_query).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user_entry) }) => {
            use cloud_p2p_project::directory_service::UserStatus;
            if user_entry.status != UserStatus::Online {
                bail!(
                    "❌ You must be online to check requests!\n\
                    \n\
                    Start your P2P server first:\n\
                      cargo run --bin client -- start-peer --username {} --port <PORT>\n\
                    \n\
                    Your pending requests will be shown automatically when you come online.",
                    username
                );
            }
            println!("✓ You are online\n");
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!(
                "❌ You must be online to check requests!\n\
                \n\
                Start your P2P server first:\n\
                  cargo run --bin client -- start-peer --username {} --port <PORT>",
                username
            );
        }
        Err(e) => {
            bail!("Error checking online status: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }

    let msg = DirectoryMessage::GetPendingRequests {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, msg).await {
        Ok(DirectoryMessage::GetPendingRequestsResponse { requests }) => {
            if requests.is_empty() {
                println!("✓ No pending requests");
            } else {
                println!("\n📬 You have {} pending request(s):\n", requests.len());

                for (idx, req) in requests.iter().enumerate() {
                    println!("{}. Request ID: {}", idx + 1, req.request_id);
                    println!("   From: {}", req.from_user);
                    println!("   Image: {}", req.image_id);
                    println!("   Requested views: {}", req.requested_views);

                    if let Ok(duration) = req.timestamp.elapsed() {
                        let secs = duration.as_secs();
                        if secs < 60 {
                            println!("   Time: {} seconds ago", secs);
                        } else if secs < 3600 {
                            println!("   Time: {} minutes ago", secs / 60);
                        } else {
                            println!("   Time: {} hours ago", secs / 3600);
                        }
                    }

                    println!();
                }

                println!("To respond to a request, use:");
                println!("  cargo run --bin client -- respond-request --owner {} --request-id <ID> --accept <true/false>", username);
            }

            Ok(())
        }
        Err(e) => {
            bail!("Error checking requests: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }
}

async fn handle_respond_request(
    owner: &str,
    request_id: &str,
    accept: bool,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Responding to Request ===");
    println!("Request ID: {}", request_id);
    println!("Action: {}", if accept { "ACCEPT" } else { "REJECT" });

    // If accepting, verify the owner is online first
    if accept {
        println!("\n🔍 Verifying you are online...");
        let self_query = DirectoryMessage::QueryUser {
            username: owner.to_string(),
        };

        match send_directory_or_multicast(directory_addr, self_query).await {
            Ok(DirectoryMessage::QueryUserResponse { user: Some(user_entry) }) => {
                use cloud_p2p_project::directory_service::UserStatus;
                if user_entry.status != UserStatus::Online {
                    bail!(
                        "❌ You must be online to accept requests!\n\
                        \n\
                        To accept this request, you need to start your P2P server:\n\
                          cargo run --bin client -- start-peer --username {} --port <PORT>\n\
                        \n\
                        Then run the respond-request command again.",
                        owner
                    );
                }
                println!("✓ You are online");
            }
            Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
                bail!(
                    "❌ You must be online to accept requests!\n\
                    \n\
                    Start your P2P server first:\n\
                    cargo run --bin client -- start-peer --username {} --port <PORT>",
                    owner
                );
            }
            Err(e) => {
                bail!("Error checking online status: {}", e);
            }
            _ => {
                bail!("Unexpected response from directory service");
            }
        }
    }

    let msg = DirectoryMessage::RespondToRequest {
        request_id: request_id.to_string(),
        owner: owner.to_string(),
        accept,
    };

    match send_directory_or_multicast(directory_addr, msg).await {
        Ok(DirectoryMessage::RespondToRequestResponse { success: true, message, request: Some(req) }) => {
            println!("✓ {}", message);

            if accept {
                // Automatically grant permissions by updating the image
                println!("\n🔄 Automatically granting permissions...");
                println!("   User: {}", req.from_user);
                println!("   Image: {}", req.image_id);
                println!("   Views: {}", req.requested_views);

                // Call update_permissions automatically
                match handle_update_permissions(
                    owner,
                    &req.image_id,
                    &req.from_user,
                    req.requested_views,
                    directory_addr,
                )
                .await
                {
                    Ok(()) => {
                        println!("\n✅ Permissions granted successfully!");

                        // Now check if requester is online and deliver the image automatically
                        println!("\n📤 Checking if {} is online to deliver the image...", req.from_user);

                        let query_msg = DirectoryMessage::QueryUser {
                            username: req.from_user.clone(),
                        };

                        // First, fetch the image from our own P2P server (with updated permissions)
                        use cloud_p2p_project::p2p_protocol::{P2PMessage, send_p2p_message, request_image_from_peer};

                        // Query directory to get our own P2P address
                        let self_query = DirectoryMessage::QueryUser {
                            username: owner.to_string(),
                        };

                        let encrypted_image = match send_directory_or_multicast(directory_addr, self_query).await {
                            Ok(DirectoryMessage::QueryUserResponse { user: Some(self_user) }) => {
                                // Fetch the image from our own P2P server WITH THE REQUESTING USER'S NAME
                                // so the quota gets embedded for them, not the owner
                                match request_image_from_peer(
                                    &self_user.p2p_address,
                                    &req.from_user,  // Request as the requester (Alice), not as owner (Bob)
                                    &req.image_id,
                                    req.requested_views,
                                )
                                .await
                                {
                                    Ok(img) => {
                                        println!("✓ Image fetched successfully");
                                        Some(img)
                                    }
                                    Err(e) => {
                                        eprintln!("\n⚠ Failed to fetch image: {}", e);
                                        None
                                    }
                                }
                            }
                            _ => {
                                eprintln!("\n⚠ Could not find own P2P server");
                                None
                            }
                        };

                        if encrypted_image.is_none() {
                            println!("💡 {} can manually request the image when ready", req.from_user);
                            return Ok(());
                        }

                        let encrypted_image = encrypted_image.unwrap();

                        // Now check if requester is online and try to deliver
                        match send_directory_or_multicast(directory_addr, query_msg).await {
                            Ok(DirectoryMessage::QueryUserResponse { user: Some(user) }) => {
                                use cloud_p2p_project::directory_service::UserStatus;
                                if user.status == UserStatus::Online {
                                    println!("✓ {} is online at {}", req.from_user, user.p2p_address);
                                    println!("🚀 Attempting to deliver image to {}...", req.from_user);

                                    // Clone image for fallback
                                    let image_for_fallback = encrypted_image.clone();

                                    // Try to deliver the image to the requester
                                    let deliver_msg = P2PMessage::DeliverImage {
                                        from_owner: owner.to_string(),
                                        image_id: req.image_id.clone(),
                                        requested_views: req.requested_views,
                                        encrypted_image,
                                    };

                                    match send_p2p_message(&user.p2p_address, deliver_msg).await {
                                        Ok(P2PMessage::DeliverImageResponse { success: true, message }) => {
                                            println!("\n✅ Image delivered successfully to {}!", req.from_user);
                                            println!("   {}", message);
                                        }
                                        Ok(P2PMessage::DeliverImageResponse { success: false, message }) => {
                                            eprintln!("\n⚠ Failed to deliver image: {}", message);
                                            println!("📝 Storing image for delivery when {} is fully online...", req.from_user);
                                            store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, image_for_fallback).await;
                                        }
                                        Err(e) => {
                                            eprintln!("\n⚠ Could not deliver image to {} (connection failed: {})", req.from_user, e);
                                            println!("📝 Storing image for delivery when {} is fully online...", req.from_user);
                                            store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, image_for_fallback).await;
                                        }
                                        _ => {
                                            eprintln!("\n⚠ Unexpected response when delivering image");
                                            println!("📝 Storing image for delivery when {} is fully online...", req.from_user);
                                            store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, image_for_fallback).await;
                                        }
                                    }
                                } else {
                                    println!("ℹ {} is offline. Storing image for delivery when they come online...", req.from_user);
                                    store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, encrypted_image).await;
                                }
                            }
                            Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
                                println!("ℹ {} is not online. Storing image for delivery when they register...", req.from_user);
                                store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, encrypted_image).await;
                            }
                            Err(e) => {
                                eprintln!("⚠ Could not check if {} is online: {}", req.from_user, e);
                                println!("📝 Storing image for delivery as fallback...");
                                store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, encrypted_image).await;
                            }
                            _ => {
                                println!("📝 Storing image for delivery as fallback...");
                                store_pending_update_with_image(directory_addr, owner, &req.from_user, &req.image_id, req.requested_views, encrypted_image).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("\n⚠ Warning: Request was accepted but failed to grant permissions:");
                        eprintln!("   {}", e);
                        eprintln!("\n💡 You can manually grant permissions with:");
                        eprintln!("   cargo run --bin client -- update-permissions --owner {} --image-id {} --username {} --new-quota {}",
                                 owner, req.image_id, req.from_user, req.requested_views);
                    }
                }
            } else {
                println!("\n✅ Request rejected successfully.");
            }

            Ok(())
        }
        Ok(DirectoryMessage::RespondToRequestResponse { success: true, message, request: None }) => {
            println!("✓ {}", message);
            eprintln!("⚠ Warning: No request details returned");
            Ok(())
        }
        Ok(DirectoryMessage::RespondToRequestResponse { success: false, message, .. }) => {
            bail!("Failed to respond: {}", message);
        }
        Err(e) => {
            bail!("Error responding to request: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }
}

async fn handle_check_notifications(
    username: &str,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Checking Notifications ===");
    println!("Username: {}", username);

    let msg = DirectoryMessage::GetNotifications {
        username: username.to_string(),
    };

    match send_directory_or_multicast(directory_addr, msg).await {
        Ok(DirectoryMessage::GetNotificationsResponse { notifications }) => {
            if notifications.is_empty() {
                println!("✓ No new notifications");
            } else {
                println!("\n🔔 You have {} notification(s):\n", notifications.len());

                for (idx, notif) in notifications.iter().enumerate() {
                    let status_icon = match notif.status {
                        cloud_p2p_project::directory_service::RequestStatus::Accepted => "✅",
                        cloud_p2p_project::directory_service::RequestStatus::Rejected => "❌",
                        _ => "⏳",
                    };

                    println!("{} {}. Request to: {}", status_icon, idx + 1, notif.to_user);
                    println!("   Image: {}", notif.image_id);
                    println!("   Requested views: {}", notif.requested_views);
                    println!("   Status: {:?}", notif.status);

                    if let Ok(duration) = notif.timestamp.elapsed() {
                        let secs = duration.as_secs();
                        if secs < 60 {
                            println!("   Time: {} seconds ago", secs);
                        } else if secs < 3600 {
                            println!("   Time: {} minutes ago", secs / 60);
                        } else {
                            println!("   Time: {} hours ago", secs / 3600);
                        }
                    }

                    if notif.status == cloud_p2p_project::directory_service::RequestStatus::Accepted {
                        println!("\n   💡 Your request was accepted! You can now request the image:");
                        println!("   cargo run --bin client -- request-image --username {} --peer {} --image-id {} --views {}",
                                 username, notif.to_user, notif.image_id, notif.requested_views);
                    }

                    println!();
                }
            }

            Ok(())
        }
        Err(e) => {
            bail!("Error checking notifications: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    }
}

async fn handle_remote_update_permissions(
    owner: &str,
    target_user: &str,
    image_id: &str,
    new_quota: u32,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Remote Permission Update ===");
    println!("Owner: {}", owner);
    println!("Target user: {}", target_user);
    println!("Image ID: {}", image_id);
    println!("New quota: {} views", new_quota);

    // First, verify the owner is online and P2P server is actually running
    println!("\n🔍 Verifying you are online...");
    let self_query = DirectoryMessage::QueryUser {
        username: owner.to_string(),
    };

    let owner_p2p_addr = match send_directory_or_multicast(directory_addr, self_query).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user_entry) }) => {
            use cloud_p2p_project::directory_service::UserStatus;
            if user_entry.status != UserStatus::Online {
                bail!(
                    "❌ You must be online to send remote permission updates!\n\
                    \n\
                    Start your P2P server first:\n\
                      cargo run --bin client -- start-peer --username {} --port <PORT>",
                    owner
                );
            }
            user_entry.p2p_address
        }
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!("❌ User '{}' not found in directory", owner);
        }
        Err(e) => {
            bail!("Failed to verify online status: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    };

    // Actually verify the P2P server is reachable by sending a list images request
    use cloud_p2p_project::p2p_protocol::{list_peer_images};
    use tokio::time::{timeout, Duration};

    match timeout(Duration::from_secs(2), list_peer_images(&owner_p2p_addr, owner)).await {
        Ok(Ok(_)) => {
            println!("✓ Your P2P server is running\n");
        }
        Ok(Err(e)) => {
            bail!(
                "❌ Your P2P server is not reachable at {}!\n\
                \n\
                Error: {}\n\
                \n\
                Start your P2P server first:\n\
                  cargo run --bin client -- start-peer --username {} --port <PORT>",
                owner_p2p_addr, e, owner
            );
        }
        Err(_) => {
            bail!(
                "❌ Connection timeout: Your P2P server is not responding at {}!\n\
                \n\
                Start your P2P server first:\n\
                  cargo run --bin client -- start-peer --username {} --port <PORT>",
                owner_p2p_addr, owner
            );
        }
    }

    // Query the directory service for the target user
    println!("🔍 Looking up target user '{}'...", target_user);
    let query_msg = DirectoryMessage::QueryUser {
        username: target_user.to_string(),
    };

    let target_user_info = match send_directory_or_multicast(directory_addr, query_msg).await {
        Ok(DirectoryMessage::QueryUserResponse { user: Some(user) }) => user,
        Ok(DirectoryMessage::QueryUserResponse { user: None }) => {
            bail!("❌ User '{}' not found in directory", target_user);
        }
        Err(e) => {
            bail!("Failed to query directory: {}", e);
        }
        _ => {
            bail!("Unexpected response from directory service");
        }
    };

    use cloud_p2p_project::directory_service::UserStatus;

    // Check if user is offline or unreachable - if so, queue the update
    let is_offline = target_user_info.status == UserStatus::Offline;
    let is_unreachable = if !is_offline {
        // Try to verify P2P server is actually reachable
        match timeout(Duration::from_secs(2), list_peer_images(&target_user_info.p2p_address, target_user)).await {
            Ok(Ok(_)) => false,
            _ => true,
        }
    } else {
        true
    };

    if is_offline || is_unreachable {
        // User is offline or unreachable - store pending update with the embedded image
        println!("⚠  Target user '{}' is currently offline or unreachable.", target_user);
        println!("📝 Fetching image to queue with permission update...");

        // First, fetch the image from our own P2P server with the updated permissions
        use cloud_p2p_project::p2p_protocol::request_image_from_peer;
        
        let embedded_image = match request_image_from_peer(
            &owner_p2p_addr,
            owner,  // Request as owner
            image_id,
            new_quota,
        ).await {
            Ok(image_data) => {
                println!("✓ Image fetched successfully");
                Some(image_data)
            }
            Err(e) => {
                eprintln!("⚠ Warning: Could not fetch image: {}", e);
                eprintln!("  The update will be stored without image data");
                None
            }
        };

        let pending_msg = DirectoryMessage::StorePendingPermissionUpdate {
            from_owner: owner.to_string(),
            target_user: target_user.to_string(),
            image_id: image_id.to_string(),
            new_quota,
            embedded_image,
        };

        match send_directory_or_multicast(directory_addr, pending_msg).await {
            Ok(DirectoryMessage::StorePendingPermissionUpdateResponse { success: true, message, .. }) => {
                println!("\n✅ Permission update queued successfully!");
                println!("   {}", message);
                println!("\n   When '{}' comes online:", target_user);
                println!("   • The image will be delivered as from_{}_{}.png", owner, target_user);
                if new_quota == 0 {
                    println!("   • Their access will be revoked (0 views)");
                } else {
                    println!("   • Their quota will be set to {} views", new_quota);
                }
                return Ok(());
            }
            Ok(DirectoryMessage::StorePendingPermissionUpdateResponse { success: false, message, .. }) => {
                bail!("Failed to queue permission update: {}", message);
            }
            Err(e) => {
                bail!("Failed to communicate with directory service: {}", e);
            }
            _ => {
                bail!("Unexpected response from directory service");
            }
        }
    }

    // User is online and reachable
    println!("✓ Target user '{}' is online at {}", target_user, target_user_info.p2p_address);

    // Send the remote update request to the target user's P2P server
    println!("\n📤 Sending permission update request to {}...", target_user);

    use cloud_p2p_project::p2p_protocol::{P2PMessage, send_p2p_message};

    let update_msg = P2PMessage::RemoteUpdatePermissions {
        from_owner: owner.to_string(),
        image_id: image_id.to_string(),
        for_user: target_user.to_string(),
        new_quota,
    };

    // NOTE: use the p2p address from the fetched target_user_info (was using undefined `target_p2p_addr`)
    match send_p2p_message(&target_user_info.p2p_address, update_msg).await {
        Ok(P2PMessage::RemoteUpdatePermissionsResponse { success: true, message }) => {
            println!("\n✅ Permission update successful!");
            println!("   {}", message);

            if new_quota == 0 {
                println!("\n⚠  User '{}' can no longer view this image.", target_user);
            } else {
                println!("\n✓ User '{}' now has {} views for image '{}'", target_user, new_quota, image_id);
            }

            Ok(())
        }
        Ok(P2PMessage::RemoteUpdatePermissionsResponse { success: false, message }) => {
            bail!("❌ Permission update failed: {}", message);
        }
        Err(e) => {
            bail!("Failed to send update request: {}", e);
        }
        _ => {
            bail!("Unexpected response from target user");
        }
    }
}