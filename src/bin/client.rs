use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

const ENCRYPTED_OUTPUT_IMAGE: &str = "encrypted_lsb_image.png";
const VIEWABLE_OUTPUT_IMAGE: &str = "viewable_image.png";
const SERVER_CONFIG_FILE: &str = "servers.conf";

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
    /// View a protected image, acting as a peer
    View {
        /// The protected image file to view
        #[arg(short, long)]
        input: PathBuf,

        /// The user who is trying to view the image
        #[arg(short, long)]
        user: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Encrypt { ref input, ref owner } => {
            handle_encrypt(input, owner)?;
        }
        Commands::View { ref input, ref user } => {
            handle_view(input, user)?;
        }
    }

    Ok(())
}

// -------------------------------------------------------------------
// --- ROLE 1: ENCRYPTOR with TRUE MULTICAST + FAULT TOLERANCE ---
// -------------------------------------------------------------------

/// Reads server addresses from the config file
fn load_servers() -> Result<Vec<String>> {
    let content = fs::read_to_string(SERVER_CONFIG_FILE)?;
    let servers: Vec<String> = content
        .lines()
        .filter(|&s| !s.is_empty())
        .map(String::from)
        .collect();
    if servers.is_empty() {
        bail!("No servers found in '{}'", SERVER_CONFIG_FILE);
    }
    Ok(servers)
}

#[derive(Debug, Clone)]
enum ServerResponse {
    Success(Vec<u8>),           // Got encrypted image
    NotLeader(String),          // Server is not leader, with leader hint
    NoLeader,                   // No leader elected yet
    ConnectionFailed(String),   // Network error or timeout
}

fn handle_encrypt(input_path: &PathBuf, owner: &String) -> Result<()> {
    println!("=== Encryptor Mode (Multicast with Fault Tolerance) ===");

    // 1. Load server list
    let servers = load_servers()?;
    println!("Loaded {} servers from '{}'", servers.len(), SERVER_CONFIG_FILE);

    // 2. Prepare metadata and image
    let img_buf = fs::read(input_path)?;
    println!("Read '{}' ({} bytes)", input_path.display(), img_buf.len());

    let mut quotas = HashMap::new();
    quotas.insert(owner.clone(), 3);
    quotas.insert("alice".to_string(), 2);
    quotas.insert("bob".to_string(), 1);

    let permissions = ImagePermissions {
        owner: owner.clone(),
        quotas,
    };
    let meta_bytes = bincode::serialize(&permissions)?;

    // 3. MULTICAST with retry logic for leader failures
    println!("\n=== MULTICASTING to all {} servers ===", servers.len());
    
    let max_attempts = 5;  // More attempts for fault tolerance
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

        // Perform multicast and collect responses
        let responses = multicast_to_servers(&servers, &meta_bytes, &img_buf);
        
        // Analyze responses
        let mut success_response = None;
        let mut not_leader_count = 0;
        let mut no_leader_count = 0;
        let mut connection_failed_count = 0;
        let mut leader_might_have_failed = false;

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

        // Check if we got success
        if let Some(encrypted_image) = success_response {
            println!("\n=== ✓ ENCRYPTION SUCCESSFUL ===");
            println!("Received encrypted image ({} bytes)", encrypted_image.len());
            
            fs::write(ENCRYPTED_OUTPUT_IMAGE, &encrypted_image)?;
            println!("Saved encrypted image to '{}'", ENCRYPTED_OUTPUT_IMAGE);
            
            return Ok(());
        }

        // Analyze failure reasons
        println!("\n--- Response Summary ---");
        println!("  NOT_LEADER responses: {}", not_leader_count);
        println!("  NO_LEADER responses: {}", no_leader_count);
        println!("  Connection failures: {}", connection_failed_count);

        // Detect if leader might have failed
        if not_leader_count > 0 && connection_failed_count > 0 {
            // Some servers said "not leader" and one failed to respond
            // The failed one might have been the leader
            leader_might_have_failed = true;
            println!("\n⚠ Detected possible LEADER FAILURE!");
            println!("  → Some servers identified a leader, but it didn't respond");
            println!("  → Raft should elect a new leader...");
        } else if no_leader_count == servers.len() {
            // All servers say no leader - election in progress
            println!("\n⚠ No leader currently elected");
            println!("  → Waiting for Raft election to complete...");
        } else if connection_failed_count == servers.len() {
            // All servers unreachable - network problem
            println!("\n⚠ All servers unreachable - network issue?");
            bail!("Cannot connect to any server");
        } else if not_leader_count == servers.len() {
            // All servers say they're not leader - inconsistent state
            println!("\n⚠ All servers claim to NOT be leader");
            println!("  → This shouldn't happen! Waiting for re-election...");
        }

        if attempt < max_attempts {
            if leader_might_have_failed {
                println!("  → Will retry after new leader election...");
            } else if no_leader_count > 0 {
                println!("  → Will retry once election completes...");
            } else {
                println!("  → Will retry multicast...");
            }
        }
    }

    bail!("Failed to encrypt image after {} attempts. Possible reasons: leader keeps failing, network issues, or cluster unstable", max_attempts)
}

/// Multicast request to all servers and collect responses
fn multicast_to_servers(
    servers: &[String],
    meta_bytes: &[u8],
    img_buf: &[u8],
) -> Vec<(String, ServerResponse)> {
    println!("Multicasting to all servers simultaneously...");
    
    // Shared storage for responses
    let responses: Arc<Mutex<Vec<(String, ServerResponse)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut thread_handles = vec![];

    // Spawn thread for each server
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
                        // Connection error, timeout, etc.
                        ServerResponse::ConnectionFailed(err_msg)
                    }
                }
            };

            // Store response
            let mut responses_lock = responses_clone.lock().unwrap();
            responses_lock.push((addr_clone.clone(), response));
        });

        thread_handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in thread_handles {
        let _ = handle.join();
    }

    // Return collected responses
    let responses_lock = responses.lock().unwrap();
    responses_lock.clone()
}

/// Send multicast request to a single server
fn send_multicast_request(addr: &str, meta_bytes: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
    // Connection timeout: 3 seconds
    let mut stream = TcpStream::connect_timeout(
        &addr.parse()?, 
        Duration::from_secs(3)
    )?;
    
    // Read timeout: 20 seconds (to account for 5 second processing + network delay)
    // If leader fails during processing, we'll timeout and detect it
    stream.set_read_timeout(Some(Duration::from_secs(20)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Send metadata size and data
    let meta_size = meta_bytes.len() as u64;
    stream.write_all(&meta_size.to_be_bytes())?;
    stream.write_all(meta_bytes)?;

    // Send image size and data
    let img_size = img_buf.len() as u64;
    stream.write_all(&img_size.to_be_bytes())?;
    stream.write_all(img_buf)?;

    // Receive response size
    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes)?;
    let response_size = u64::from_be_bytes(size_bytes);

    // Read response
    let mut response_buf = vec![0; response_size as usize];
    stream.read_exact(&mut response_buf)?;

    // Check if response is an error message
    if let Ok(msg) = std::str::from_utf8(&response_buf) {
        if msg.starts_with("NOT_LEADER") || 
           msg.starts_with("NO_LEADER") {
            bail!("{}", msg);
        }
    }

    // Otherwise it's the encrypted image
    Ok(response_buf)
}

// -------------------------------------------------------------------
// --- ROLE 2: P2P VIEWER (Unchanged) ---
// -------------------------------------------------------------------

fn handle_view(input_path: &PathBuf, current_user: &String) -> Result<()> {
    println!("\n=== Simulating P2P client-to-client view ===");
    println!("Viewing user: {}", current_user);
    println!("Viewing image: {}", input_path.display());

    // Load the encrypted image
    let img_data = fs::read(input_path)?;
    let encoded_img = image::load_from_memory(&img_data)?;

    // Decode embedded payload
    let payload = lsb::decode(&encoded_img)?
        .ok_or_else(|| anyhow::anyhow!("No hidden metadata found!"))?;

    // Deserialize the CombinedPayload
    let combined_data: CombinedPayload = bincode::deserialize(&payload)?;

    // Extract permissions and unified image
    let mut permissions = combined_data.permissions;
    let unified_image_bytes = combined_data.unified_image;

    println!("Decoded metadata before view: {:#?}", permissions);

    // Check if current user is authorized
    let has_access = match permissions.quotas.get_mut(current_user) {
        Some(views_left) if *views_left > 0 => {
            println!(" Access granted. You have {} views left.", *views_left);
            *views_left -= 1;
            true
        }
        Some(_) => {
            println!(" Access denied. No remaining views!");
            false
        }
        None => {
            println!(" Access denied. You are not authorized to view this image!");
            false
        }
    };

    if has_access {
        // Save the viewable image
        encoded_img.save(VIEWABLE_OUTPUT_IMAGE)?;
        println!("Saved viewable image to '{}'", VIEWABLE_OUTPUT_IMAGE);

        println!(
            "Updated views left (for next peer): {}",
            permissions.quotas.get(current_user).unwrap_or(&0)
        );

        // Re-create the CombinedPayload with updated permissions
        let updated_combined_payload = CombinedPayload {
            permissions,
            unified_image: unified_image_bytes,
        };

        // Re-encode back into the image
        let updated_payload = bincode::serialize(&updated_combined_payload)?;
        let updated_img = lsb::encode(&encoded_img, &updated_payload)?;
        updated_img.save(input_path)?;
        
        println!(
            "Re-embedded updated metadata back into -> '{}'",
            input_path.display()
        );
    } else {
        // Save the "Access Denied" image
        fs::write(VIEWABLE_OUTPUT_IMAGE, unified_image_bytes)?;
        println!(
            "Saved default 'Access Denied' image to '{}'",
            VIEWABLE_OUTPUT_IMAGE
        );
    }

    Ok(())
}