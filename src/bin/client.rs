use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::directory_service::{DirectoryMessage, ImageInfo, send_directory_message};
use cloud_p2p_project::p2p_protocol::{
    ImageMetadata, PeerImageStore, request_image_from_peer, 
    list_peer_images, start_p2p_server,
};
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions};
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
    "127.0.0.1:9000",
    "127.0.0.1:9001",
    "127.0.0.1:9002",
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
        
        /// Directory of images to share
        #[arg(short, long)]
        images_dir: PathBuf,
        
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
        
        /// Output file path
        #[arg(short, long)]
        output: PathBuf,
        
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
            images_dir,
            directory,
        } => {
            handle_start_peer(username, *port, images_dir, directory.as_deref()).await?;
        }
        Commands::DiscoverPeers { username, directory } => {
            handle_discover_peers(username, directory.as_deref()).await?;
        }
        Commands::RequestImage {
            username,
            peer,
            image_id,
            views,
            output,
            directory,
        } => {
            handle_request_image(username, peer, image_id, *views, output, directory.as_deref()).await?;
        }
        Commands::ListPeerImages {
            username,
            peer,
            directory,
        } => {
            handle_list_peer_images(username, peer, directory.as_deref()).await?;
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

    let mut quotas = HashMap::new();
    quotas.insert(owner.clone(), 3);

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

    let has_access = match permissions.quotas.get_mut(current_user) {
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
    };

    if has_access {
        fs::write(VIEWABLE_OUTPUT_IMAGE, &client_image_bytes)?;
        println!("Saved viewable image to '{}'", VIEWABLE_OUTPUT_IMAGE);

        println!(
            "Updated views left: {}",
            permissions.quotas.get(current_user).unwrap_or(&0)
        );

        let updated_combined_payload = CombinedPayload {
            permissions,
            unified_image: client_image_bytes,
        };

        let updated_payload = bincode::serialize(&updated_combined_payload)?;
        let updated_carrier = lsb::encode(&carrier_img, &updated_payload)?;
       
        updated_carrier.save(input_path)?;
       
        println!("Re-embedded updated metadata back into '{}'", input_path.display());
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
    images_dir: &PathBuf,
    directory_addr: Option<&str>,
) -> Result<()> {
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
        for entry in fs::read_dir(images_dir)? {
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
    
    // Register with directory service (with multicast support)
    let p2p_address = format!("0.0.0.0:{}", port);
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
    
    // Start P2P server
    println!("✓ Starting P2P server on port {}...", port);
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
    output: &PathBuf,
    directory_addr: Option<&str>,
) -> Result<()> {
    println!("=== Requesting Image from Peer ===");
    println!("Your username: {}", username);
    println!("Peer: {}", peer_username);
    println!("Image ID: {}", image_id);
    println!("Requested views: {}", views);
    
    // Query directory service for peer address
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
    
    // Request image from peer
    println!("Requesting image from peer...");
    match request_image_from_peer(&peer_addr, username, image_id, views).await {
        Ok(encrypted_image) => {
            fs::write(output, &encrypted_image)?;
            println!("✓ Image received and saved to '{}'", output.display());
            println!("You now have {} views for this image", views);
            Ok(())
        }
        Err(e) => {
            bail!("Failed to get image from peer: {}", e);
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
    
    // Query directory service for peer address
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