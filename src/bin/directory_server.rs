use anyhow::{bail, Result};
use cloud_p2p_project::directory_service::start_directory_service;
use log::info;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        eprintln!("Usage: directory_server <port> <server_id> [peer1:port] [peer2:port] ...");
        eprintln!("\nExamples:");
        eprintln!("  Single server:");
        eprintln!("    directory_server 9000 dir1");
        eprintln!("\n  Replicated (3 servers):");
        eprintln!("    Server 1: directory_server 9000 dir1 10.40.7.2:9000 10.40.7.3:9000");
        eprintln!("    Server 2: directory_server 9000 dir2 10.40.7.1:9000 10.40.7.3:9000");
        eprintln!("    Server 3: directory_server 9000 dir3 10.40.7.1:9000 10.40.7.2:9000");
        bail!("Incorrect arguments");
    }
    
    let port: u16 = args[1].parse()?;
    let server_id = args[2].clone();
    let peer_servers: Vec<String> = args[3..].to_vec();
    
    // State file path
    let state_file = PathBuf::from(format!("directory_state_{}.json", server_id));
    
    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║   Directory Service with Replication + Persistence       ║");
    info!("╚══════════════════════════════════════════════════════════╝");
    info!("");
    info!("Server ID: {}", server_id);
    info!("Port: {}", port);
    info!("State file: {}", state_file.display());
    
    if peer_servers.is_empty() {
        info!("Mode: SINGLE SERVER (no replication)");
        info!("⚠ WARNING: Single point of failure for availability");
        info!("✓ BUT: State persists to disk, survives restarts!");
    } else {
        info!("Mode: REPLICATED ({} peers) + PERSISTENT", peer_servers.len());
        info!("Peer servers:");
        for (i, peer) in peer_servers.iter().enumerate() {
            info!("  {}. {}", i + 1, peer);
        }
        info!("");
        info!("✓ This directory service is FULLY FAULT TOLERANT:");
        info!("  • Survives server crashes (disk persistence)");
        info!("  • Survives individual failures (replication)");
        info!("  • Recovers from total failure (disk + peer sync)");
    }
    info!("");
    
    // Start the directory service
    start_directory_service(port, server_id, peer_servers, state_file).await?;
    
    Ok(())
}