use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::raft::{RaftConfig, RaftNode};
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions, RaftMessage};
use image::ImageOutputFormat;
use log::{error, info};
use std::env;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use log::debug;
const RAFT_PORT_OFFSET: u16 = 1000; // Raft runs on port + 1000

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();

    // Parse command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        bail!("Usage: server <port> <server_id> [peer1:port] [peer2:port] ...");
    }

    let port: u16 = args[1].parse()?;
    let server_id = args[2].clone();
    let peers: Vec<String> = args[3..].to_vec();

    info!("Starting server {} on port {}", server_id, port);
    info!("Peers: {:?}", peers);

    // Convert peer addresses to include Raft port
    let raft_peers: Vec<String> = peers
        .iter()
        .map(|p| {
            let parts: Vec<&str> = p.split(':').collect();
            let peer_port: u16 = parts[1].parse().unwrap();
            format!("{}:{}", parts[0], peer_port + RAFT_PORT_OFFSET)
        })
        .collect();

    // Create Raft configuration
    let raft_config = RaftConfig {
        server_id: server_id.clone(),
        peers: raft_peers,
        election_timeout_min: 5000,
        election_timeout_max: 8000,
        heartbeat_interval: 1000,
    };

    // Create and start Raft node
    let raft_node = Arc::new(RaftNode::new(raft_config));
    let raft_clone = Arc::clone(&raft_node);
    raft_clone.start().await;

    // Start Raft message listener on separate port
    let raft_port = port + RAFT_PORT_OFFSET;
    let raft_listener_node = Arc::clone(&raft_node);
    tokio::spawn(async move {
        if let Err(e) = start_raft_listener(raft_port, raft_listener_node).await {
            error!("Raft listener error: {}", e);
        }
    });

    // Start main application server
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Application server listening on {}", bind_addr);
    info!("Raft consensus running on port {}", raft_port);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Client connected from {}", addr);
                let raft_ref = Arc::clone(&raft_node);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, raft_ref).await {
                        error!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

/// Start the Raft message listener
async fn start_raft_listener(port: u16, raft_node: Arc<RaftNode>) -> Result<()> {
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Raft listener started on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let raft_ref = Arc::clone(&raft_node);
                tokio::spawn(async move {
                    if let Err(e) = handle_raft_message(stream, raft_ref).await {
                        error!("Error handling Raft message: {}", e);
                    }
                });
            }
            Err(e) => error!("Failed to accept Raft connection: {}", e),
        }
    }
}

/// Handle Raft protocol messages
async fn handle_raft_message(mut stream: TcpStream, raft_node: Arc<RaftNode>) -> Result<()> {
    // Read message
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: RaftMessage = serde_json::from_slice(&msg_buf)?;
    
    // Handle message and get response
    if let Some(response) = raft_node.handle_raft_message(message).await {
        let response_json = serde_json::to_string(&response)?;
        let response_bytes = response_json.as_bytes();
        stream.write_u32(response_bytes.len() as u32).await?;
        stream.write_all(response_bytes).await?;
        stream.flush().await?;
    }

    Ok(())
}

/// Handle client image encryption requests
async fn handle_client(mut stream: TcpStream, raft_node: Arc<RaftNode>) -> Result<()> {
    // Check if this server is the leader
    if !raft_node.is_leader().await {
        // Not the leader, inform client
        let leader_id = raft_node.get_leader_id().await;
        let error_msg = match &leader_id {
            Some(id) => format!("NOT_LEADER:{}", id),
            None => "NO_LEADER".to_string(),
        };
        
        let error_bytes = error_msg.as_bytes();
        stream.write_u64(error_bytes.len() as u64).await?;
        stream.write_all(error_bytes).await?;
        stream.flush().await?;
        
        info!("Rejected client - not leader. Current leader: {:?}", leader_id);
        return Ok(());
    }

    info!("Processing request as LEADER");

    // Receive metadata
    let meta_size = stream.read_u64().await?;
    let mut meta_buf = vec![0; meta_size as usize];
    stream.read_exact(&mut meta_buf).await?;
    let permissions: ImagePermissions = bincode::deserialize(&meta_buf)?;
    info!("Received metadata: {:?}", permissions);

    // Receive image
    let img_size = stream.read_u64().await?;
    let mut img_buf = vec![0; img_size as usize];
    stream.read_exact(&mut img_buf).await?;
    info!("Received image ({} bytes)", img_size);
    let img = image::load_from_memory(&img_buf)?;

    // Embed unified image with metadata
    let unified_image_bytes = match fs::read("unified_image.png") {
        Ok(bytes) => {
            info!("Loaded unified image ({} bytes)", bytes.len());
            debug!("Successfully loaded unified_image.png");
            bytes
        }
        Err(e) => {
            error!("FATAL: Could not load 'unified_image.png': {}", e);
            bail!("Could not load unified_image.png");
        }
    };

    let combined_payload = CombinedPayload {
        permissions,
        unified_image: unified_image_bytes,
    };
    let final_payload = bincode::serialize(&combined_payload)?;
    let encoded_img = lsb::encode(&img, &final_payload)?;
    info!("Embedded combined payload ({} bytes) via LSB", final_payload.len());

    // Simulate processing time
    info!("Simulating 5 seconds of work...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    info!("Work finished, sending image back");

    // Send back encrypted image
    let mut out_buf = Vec::new();
    encoded_img.write_to(&mut Cursor::new(&mut out_buf), ImageOutputFormat::Png)?;
    let out_size = out_buf.len() as u64;
    stream.write_u64(out_size).await?;
    stream.write_all(&out_buf).await?;
    stream.flush().await?;
    info!("Sent back encrypted image ({} bytes)", out_size);

    Ok(())
}
