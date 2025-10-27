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
    /// Encrypt an image by sending it to the leader server
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
// --- ROLE 1: ENCRYPTOR with Leader Discovery ---
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

fn handle_encrypt(input_path: &PathBuf, owner: &String) -> Result<()> {
    println!("=== Encryptor Mode (Leader Discovery) ===");

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

    // 3. Try to find and connect to the leader
    println!("\nSearching for leader server...");
    
    let mut leader_found = false;
    let mut attempts = 0;
    let max_attempts = 10;

    while !leader_found && attempts < max_attempts {
        attempts += 1;
        println!("\nAttempt {} of {}", attempts, max_attempts);

        for server_addr in &servers {
            println!("  Trying {}...", server_addr);
            
            match send_request_to_server(server_addr, &meta_bytes, &img_buf) {
                Ok(received_image) => {
                    // Success! This was the leader
                    println!("Successfully connected to leader: {}", server_addr);
                    println!("Received encrypted image ({} bytes)", received_image.len());
                    
                    fs::write(ENCRYPTED_OUTPUT_IMAGE, &received_image)?;
                    println!("Saved encrypted image to '{}'", ENCRYPTED_OUTPUT_IMAGE);
                    
                    leader_found = true;
                    break;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("NOT_LEADER") || err_msg.contains("NO_LEADER") {
                        println!("{} is not the leader", server_addr);
                        if err_msg.contains("NOT_LEADER:") {
                            let leader_id = err_msg.split(':').nth(1).unwrap_or("");
                            println!("     Current leader ID: {}", leader_id);
                        } else {
                            println!("     No leader elected yet");
                        }
                    } else {
                        println!("  Connection failed: {}", e);
                    }
                }
            }
        }

        if !leader_found {
            println!("\nNo leader available. Waiting 2 seconds before retry...");
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    if !leader_found {
        bail!("Failed to connect to leader after {} attempts", max_attempts);
    }

    Ok(())
}

fn send_request_to_server(addr: &str, meta_bytes: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(&addr.parse()?, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    // Send metadata
    let meta_size = meta_bytes.len() as u64;
    stream.write_all(&meta_size.to_be_bytes())?;
    stream.write_all(meta_bytes)?;

    // Send image
    let img_size = img_buf.len() as u64;
    stream.write_all(&img_size.to_be_bytes())?;
    stream.write_all(img_buf)?;

    // Receive response
    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes)?;
    let response_size = u64::from_be_bytes(size_bytes);

    // Check if this is an error message
    let mut response_buf = vec![0; response_size as usize];
    stream.read_exact(&mut response_buf)?;

    // Check if response starts with "NOT_LEADER" or "NO_LEADER"
    if let Ok(msg) = std::str::from_utf8(&response_buf) {
        if msg.starts_with("NOT_LEADER") || msg.starts_with("NO_LEADER") {
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