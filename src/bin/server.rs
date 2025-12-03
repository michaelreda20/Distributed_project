// use anyhow::{bail, Result};
// use bincode;
// use cloud_p2p_project::raft::{RaftConfig, RaftNode};
// use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions, LoadBalancingMessage, RaftMessage, ServerMetrics};
// use image::{ImageOutputFormat, GenericImageView};
// use log::{error, info};
// use std::env;
// use std::fs;
// use std::io::Cursor;
// use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
// use std::sync::Arc;
// use std::time::Instant;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::{TcpListener, TcpStream};
// use std::os::unix::io::AsRawFd;

// /// Configure TCP socket for large file transfers
// fn configure_large_transfer_socket(stream: &TcpStream) -> Result<()> {
//     #[cfg(unix)]
//     {
//         use std::mem;
        
//         unsafe {
//             let fd = stream.as_raw_fd();
//             let size: libc::c_int = 8 * 1024 * 1024; // 8MB buffer
            
//             // Set send buffer
//             libc::setsockopt(
//                 fd,
//                 libc::SOL_SOCKET,
//                 libc::SO_SNDBUF,
//                 &size as *const _ as *const libc::c_void,
//                 mem::size_of_val(&size) as libc::socklen_t,
//             );
            
//             // Set receive buffer
//             libc::setsockopt(
//                 fd,
//                 libc::SOL_SOCKET,
//                 libc::SO_RCVBUF,
//                 &size as *const _ as *const libc::c_void,
//                 mem::size_of_val(&size) as libc::socklen_t,
//             );
//         }
//     }
//     Ok(())
// }

// const RAFT_PORT_OFFSET: u16 = 1000;    // Raft runs on port + 1000
// const METRICS_PORT_OFFSET: u16 = 2000; // Metrics server on port + 2000
// const WORK_PORT_OFFSET: u16 = 3000;    // Work receiver on port + 3000

// // =============================================================================
// // LOAD BALANCING STATE
// // =============================================================================

// pub struct LoadBalancingState {
//     pub active_connections: AtomicU32,
//     pub total_requests: AtomicU64,
//     pub total_response_time_ms: AtomicU64,
// }

// impl LoadBalancingState {
//     pub fn new() -> Self {
//         Self {
//             active_connections: AtomicU32::new(0),
//             total_requests: AtomicU64::new(0),
//             total_response_time_ms: AtomicU64::new(0),
//         }
//     }
    
//     /// Get current metrics for this server
//     pub fn get_metrics(&self, server_id: String) -> ServerMetrics {
//         let total_requests = self.total_requests.load(Ordering::Relaxed);
//         let avg_response = if total_requests > 0 {
//             self.total_response_time_ms.load(Ordering::Relaxed) / total_requests
//         } else {
//             0
//         };
        
//         let active_conns = self.active_connections.load(Ordering::Relaxed);
        
//         ServerMetrics {
//             server_id,
//             cpu_load: Self::estimate_cpu_load(active_conns),
//             active_connections: active_conns,
//             avg_response_time_ms: avg_response,
//             total_requests,
//             timestamp: std::time::SystemTime::now(),
//         }
//     }
    
//     /// Estimate CPU load based on active connections
//     /// In production, use sysinfo crate for real CPU metrics
//     fn estimate_cpu_load(connections: u32) -> f32 {
//         // Simple estimation: each connection adds ~10% CPU load
//         (connections as f32 * 10.0).min(100.0)
//     }
    
//     pub fn increment_connections(&self) {
//         self.active_connections.fetch_add(1, Ordering::Relaxed);
//     }
    
//     pub fn decrement_connections(&self) {
//         self.active_connections.fetch_sub(1, Ordering::Relaxed);
//     }
    
//     pub fn record_request(&self, response_time_ms: u64) {
//         self.total_requests.fetch_add(1, Ordering::Relaxed);
//         self.total_response_time_ms.fetch_add(response_time_ms, Ordering::Relaxed);
//     }
// }

// // =============================================================================
// // MAIN
// // =============================================================================

// #[tokio::main]
// async fn main() -> Result<()> {
//     // Initialize logger
//     env_logger::init();

//     // Parse command-line arguments
//     let args: Vec<String> = env::args().collect();
//     if args.len() < 3 {
//         bail!("Usage: server <port> <server_id> [peer1:port] [peer2:port] ...");
//     }

//     let port: u16 = args[1].parse()?;
//     let server_id = args[2].clone();
//     let peers: Vec<String> = args[3..].to_vec();

//     info!("Starting server {} on port {}", server_id, port);
//     info!("Peers: {:?}", peers);

//     // Create load balancing state
//     let lb_state = Arc::new(LoadBalancingState::new());

//     // Convert peer addresses to include Raft port
//     let raft_peers: Vec<String> = peers
//         .iter()
//         .map(|p| {
//             let parts: Vec<&str> = p.split(':').collect();
//             let peer_port: u16 = parts[1].parse().unwrap();
//             format!("{}:{}", parts[0], peer_port + RAFT_PORT_OFFSET)
//         })
//         .collect();

//     // Create Raft configuration
//     let raft_config = RaftConfig {
//         server_id: server_id.clone(),
//         peers: raft_peers,
//         election_timeout_min: 4000,
//         election_timeout_max: 10000,
//         heartbeat_interval: 2000,
//     };

//     // Create and start Raft node
//     let raft_node = Arc::new(RaftNode::new(raft_config));
//     let raft_clone = Arc::clone(&raft_node);
//     raft_clone.start().await;

//     // Start Raft message listener on separate port
//     let raft_port = port + RAFT_PORT_OFFSET;
//     let raft_listener_node = Arc::clone(&raft_node);
//     tokio::spawn(async move {
//         if let Err(e) = start_raft_listener(raft_port, raft_listener_node).await {
//             error!("Raft listener error: {}", e);
//         }
//     });

//     // Start metrics server (for load balancing)
//     let metrics_port = port + METRICS_PORT_OFFSET;
//     let metrics_lb_state = Arc::clone(&lb_state);
//     let metrics_server_id = server_id.clone();
//     tokio::spawn(async move {
//         if let Err(e) = start_metrics_server(metrics_port, metrics_lb_state, metrics_server_id).await {
//             error!("Metrics server error: {}", e);
//         }
//     });

//     // Start work receiver (for forwarded work from leader)
//     let work_port = port + WORK_PORT_OFFSET;
//     let work_lb_state = Arc::clone(&lb_state);
//     tokio::spawn(async move {
//         if let Err(e) = start_work_receiver(work_port, work_lb_state).await {
//             error!("Work receiver error: {}", e);
//         }
//     });

//     // Start main application server
//     let bind_addr = format!("0.0.0.0:{}", port);
//     let listener = TcpListener::bind(&bind_addr).await?;
//     info!("Application server listening on {}", bind_addr);
//     info!("Raft consensus running on port {}", raft_port);
//     info!("Metrics server running on port {}", metrics_port);
//     info!("Work receiver running on port {}", work_port);

//     loop {
//         match listener.accept().await {
//             Ok((stream, addr)) => {
//                 info!("Client connected from {}", addr);
//                 let raft_ref = Arc::clone(&raft_node);
//                 let lb_ref = Arc::clone(&lb_state);
//                 let peers_clone = peers.clone();
//                 tokio::spawn(async move {
//                     if let Err(e) = handle_client_with_load_balancing(
//                         stream,
//                         raft_ref,
//                         lb_ref,
//                         peers_clone,
//                     ).await {
//                         error!("Error handling client: {}", e);
//                     }
//                 });
//             }
//             Err(e) => error!("Failed to accept connection: {}", e),
//         }
//     }
// }

// // =============================================================================
// // RAFT LISTENER
// // =============================================================================

// async fn start_raft_listener(port: u16, raft_node: Arc<RaftNode>) -> Result<()> {
//     let bind_addr = format!("0.0.0.0:{}", port);
//     let listener = TcpListener::bind(&bind_addr).await?;
//     info!("Raft listener started on {}", bind_addr);

//     loop {
//         match listener.accept().await {
//             Ok((stream, _)) => {
//                 let raft_ref = Arc::clone(&raft_node);
//                 tokio::spawn(async move {
//                     if let Err(e) = handle_raft_message(stream, raft_ref).await {
//                         error!("Error handling Raft message: {}", e);
//                     }
//                 });
//             }
//             Err(e) => error!("Failed to accept Raft connection: {}", e),
//         }
//     }
// }

// async fn handle_raft_message(mut stream: TcpStream, raft_node: Arc<RaftNode>) -> Result<()> {
//     // Read message
//     let msg_len = stream.read_u32().await?;
//     let mut msg_buf = vec![0u8; msg_len as usize];
//     stream.read_exact(&mut msg_buf).await?;
    
//     let message: RaftMessage = serde_json::from_slice(&msg_buf)?;
    
//     // Handle message and get response
//     if let Some(response) = raft_node.handle_raft_message(message).await {
//         let response_json = serde_json::to_string(&response)?;
//         let response_bytes = response_json.as_bytes();
//         stream.write_u32(response_bytes.len() as u32).await?;
//         stream.write_all(response_bytes).await?;
//         stream.flush().await?;
//     }

//     Ok(())
// }

// // =============================================================================
// // METRICS SERVER (for Load Balancing)
// // =============================================================================

// async fn start_metrics_server(
//     port: u16,
//     lb_state: Arc<LoadBalancingState>,
//     server_id: String,
// ) -> Result<()> {
//     let bind_addr = format!("0.0.0.0:{}", port);
//     let listener = TcpListener::bind(&bind_addr).await?;
//     info!("Metrics server listening on {}", bind_addr);

//     loop {
//         match listener.accept().await {
//             Ok((mut stream, _)) => {
//                 let lb_clone = Arc::clone(&lb_state);
//                 let id_clone = server_id.clone();
                
//                 tokio::spawn(async move {
//                     if let Err(e) = handle_metrics_request(&mut stream, lb_clone, id_clone).await {
//                         error!("Error handling metrics request: {}", e);
//                     }
//                 });
//             }
//             Err(e) => error!("Metrics server accept error: {}", e),
//         }
//     }
// }

// async fn handle_metrics_request(
//     stream: &mut TcpStream,
//     lb_state: Arc<LoadBalancingState>,
//     server_id: String,
// ) -> Result<()> {
//     // Read request
//     let msg_len = stream.read_u32().await?;
//     let mut msg_buf = vec![0u8; msg_len as usize];
//     stream.read_exact(&mut msg_buf).await?;
    
//     let message: LoadBalancingMessage = serde_json::from_slice(&msg_buf)?;
    
//     match message {
//         LoadBalancingMessage::MetricsRequest => {
//             // Get current metrics
//             let metrics = lb_state.get_metrics(server_id);
            
//             // Send response
//             let response = LoadBalancingMessage::MetricsResponse { metrics };
//             let response_json = serde_json::to_string(&response)?;
//             let response_bytes = response_json.as_bytes();
            
//             stream.write_u32(response_bytes.len() as u32).await?;
//             stream.write_all(response_bytes).await?;
//             stream.flush().await?;
//         }
//         _ => {
//             error!("Unexpected message type in metrics server");
//         }
//     }
    
//     Ok(())
// }

// // =============================================================================
// // WORK RECEIVER (for Forwarded Work)
// // =============================================================================

// async fn start_work_receiver(
//     port: u16,
//     lb_state: Arc<LoadBalancingState>,
// ) -> Result<()> {
//     let bind_addr = format!("0.0.0.0:{}", port);
//     let listener = TcpListener::bind(&bind_addr).await?;
//     info!("Work receiver listening on {}", bind_addr);

//     loop {
//         match listener.accept().await {
//             Ok((mut stream, _)) => {
//                 let lb_clone = Arc::clone(&lb_state);
                
//                 tokio::spawn(async move {
//                     if let Err(e) = handle_forwarded_work(&mut stream, lb_clone).await {
//                         error!("Error handling forwarded work: {}", e);
//                     }
//                 });
//             }
//             Err(e) => error!("Work receiver accept error: {}", e),
//         }
//     }
// }

// async fn handle_forwarded_work(
//     stream: &mut TcpStream,
//     lb_state: Arc<LoadBalancingState>,
// ) -> Result<()> {
//     let start_time = Instant::now();
    
//     info!("Received forwarded work from leader");
//     lb_state.increment_connections();

//     // Read the forwarded work message
//     let msg_len = stream.read_u32().await?;
//     let mut msg_buf = vec![0u8; msg_len as usize];
//     stream.read_exact(&mut msg_buf).await?;
    
//     let message: LoadBalancingMessage = serde_json::from_slice(&msg_buf)?;
    
//     match message {
//         LoadBalancingMessage::ForwardWork { metadata, image_data } => {
//             info!("Processing forwarded encryption work...");
            
//             // Process the encryption
//             let result = process_encryption_work(&metadata, &image_data).await?;
            
//             // Send result back
//             let response = LoadBalancingMessage::WorkResult {
//                 encrypted_image: result,
//             };
//             let response_json = serde_json::to_string(&response)?;
//             let response_bytes = response_json.as_bytes();
            
//             stream.write_u32(response_bytes.len() as u32).await?;
//             stream.write_all(response_bytes).await?;
//             stream.flush().await?;
            
//             let elapsed = start_time.elapsed().as_millis() as u64;
//             lb_state.record_request(elapsed);
//             lb_state.decrement_connections();
            
//             info!("Forwarded work completed in {}ms", elapsed);
//         }
//         _ => {
//             bail!("Unexpected message type in work receiver");
//         }
//     }
    
//     Ok(())
// }

// // =============================================================================
// // CLIENT HANDLER WITH LOAD BALANCING
// // =============================================================================

// async fn handle_client_with_load_balancing(
//     mut stream: TcpStream,
//     raft_node: Arc<RaftNode>,
//     lb_state: Arc<LoadBalancingState>,
//     peers: Vec<String>,
// ) -> Result<()> {
//     let start_time = Instant::now();

//     // Configure TCP buffers for large transfers
//     configure_large_transfer_socket(&stream)?;

//     // Check if this server is the leader
//     if !raft_node.is_leader().await {
//         // Not the leader, inform client
//         let leader_id = raft_node.get_leader_id().await;
//         let error_msg = match &leader_id {
//             Some(id) => format!("NOT_LEADER:{}", id),
//             None => "NO_LEADER".to_string(),
//         };
        
//         let error_bytes = error_msg.as_bytes();
//         stream.write_u64(error_bytes.len() as u64).await?;
//         stream.write_all(error_bytes).await?;
//         stream.flush().await?;
        
//         info!("Rejected client - not leader. Current leader: {:?}", leader_id);
//         return Ok(());
//     }

//     info!("=== LEADER: Performing load balancing ===");

//     // Read client request
//     let meta_size = stream.read_u64().await?;
//     let mut meta_buf = vec![0; meta_size as usize];
//     stream.read_exact(&mut meta_buf).await?;

//     let img_size = stream.read_u64().await?;
//     let mut img_buf = vec![0; img_size as usize];
//     stream.read_exact(&mut img_buf).await?;
    
//     info!("Received client request (meta: {} bytes, image: {} bytes)", meta_size, img_size);

//     // === LOAD BALANCING: Collect metrics from all servers ===
//     // Store both metrics and their corresponding addresses
//     let mut server_info: Vec<(ServerMetrics, Option<String>)> = vec![];
    
//     // Get own metrics (no address needed for self)
//     let my_metrics = lb_state.get_metrics(raft_node.config.server_id.clone());
//     info!("My metrics: connections={}, load={:.1}%, score={:.3}",
//           my_metrics.active_connections, my_metrics.cpu_load, my_metrics.calculate_load_score());
//     server_info.push((my_metrics, None));
    
//     // Get metrics from peers and store their addresses
//     for peer_addr in &peers {
//         match request_metrics_from_peer(peer_addr).await {
//             Ok(metrics) => {
//                 info!("Peer {} metrics: connections={}, load={:.1}%, score={:.3}",
//                       metrics.server_id, metrics.active_connections, 
//                       metrics.cpu_load, metrics.calculate_load_score());
//                 server_info.push((metrics, Some(peer_addr.clone())));
//             }
//             Err(e) => {
//                 info!("Could not get metrics from {}: {}", peer_addr, e);
//             }
//         }
//     }
    
//     // Select best server based on load scores
//     let (best_server, best_addr) = server_info
//         .iter()
//         .min_by(|(a, _), (b, _)| {
//             a.calculate_load_score()
//                 .partial_cmp(&b.calculate_load_score())
//                 .unwrap_or(std::cmp::Ordering::Equal)
//         })
//         .expect("At least one server should be available");
    
//     info!("=== LOAD BALANCING DECISION ===");
//     info!("Selected server: {} (score: {:.3})", 
//           best_server.server_id, best_server.calculate_load_score());
    
//     // Decide: process locally or forward
//     let result = if best_server.server_id == raft_node.config.server_id {
//         info!("Processing LOCALLY (I am the best choice)");
//         lb_state.increment_connections();
        
//         let encrypted = process_encryption_work(&meta_buf, &img_buf).await?;
        
//         lb_state.decrement_connections();
//         let elapsed = start_time.elapsed().as_millis() as u64;
//         lb_state.record_request(elapsed);
        
//         info!("Local processing completed in {}ms", elapsed);
//         encrypted
//     } else {
//         // Forward to the selected server using its stored address
//         let target_address = best_addr
//             .as_ref()
//             .ok_or_else(|| anyhow::anyhow!("No address found for server {}", best_server.server_id))?;
        
//         info!("Forwarding to server {} at {}", best_server.server_id, target_address);
        
//         let encrypted = forward_work_to_address(
//             target_address,
//             &meta_buf,
//             &img_buf,
//         ).await?;
        
//         info!("Forwarded work completed");
//         encrypted
//     };

//     // Send result back to client
//     stream.write_u64(result.len() as u64).await?;
//     stream.write_all(&result).await?;
//     stream.flush().await?;
    
//     info!("Sent result to client ({} bytes)", result.len());
//     Ok(())
// }

// // =============================================================================
// // HELPER FUNCTIONS
// // =============================================================================

// /// Request metrics from a peer server
// async fn request_metrics_from_peer(peer_addr: &str) -> Result<ServerMetrics> {
//     let parts: Vec<&str> = peer_addr.split(':').collect();
//     let base_port: u16 = parts[1].parse()?;
//     let metrics_port = base_port + METRICS_PORT_OFFSET;
//     let metrics_addr = format!("{}:{}", parts[0], metrics_port);
    
//     let mut stream = TcpStream::connect(&metrics_addr).await?;
    
//     // Send metrics request
//     let request = LoadBalancingMessage::MetricsRequest;
//     let json = serde_json::to_string(&request)?;
//     let bytes = json.as_bytes();
    
//     stream.write_u32(bytes.len() as u32).await?;
//     stream.write_all(bytes).await?;
//     stream.flush().await?;
    
//     // Read response
//     let response_len = stream.read_u32().await?;
//     let mut response_buf = vec![0u8; response_len as usize];
//     stream.read_exact(&mut response_buf).await?;
    
//     let response: LoadBalancingMessage = serde_json::from_slice(&response_buf)?;
    
//     match response {
//         LoadBalancingMessage::MetricsResponse { metrics } => Ok(metrics),
//         _ => bail!("Unexpected response type from metrics server"),
//     }
// }

// /// Forward work to another server using its direct address
// async fn forward_work_to_address(
//     target_addr: &str,
//     meta_buf: &[u8],
//     img_buf: &[u8],
// ) -> Result<Vec<u8>> {
//     let parts: Vec<&str> = target_addr.split(':').collect();
//     let base_port: u16 = parts[1].parse()?;
//     let work_port = base_port + WORK_PORT_OFFSET;
//     let work_addr = format!("{}:{}", parts[0], work_port);
    
//     info!("Connecting to work receiver at {}", work_addr);
//     let mut stream = TcpStream::connect(&work_addr).await?;
    
//     // Send forwarded work
//     let message = LoadBalancingMessage::ForwardWork {
//         metadata: meta_buf.to_vec(),
//         image_data: img_buf.to_vec(),
//     };
//     let json = serde_json::to_string(&message)?;
//     let bytes = json.as_bytes();
    
//     stream.write_u32(bytes.len() as u32).await?;
//     stream.write_all(bytes).await?;
//     stream.flush().await?;
    
//     info!("Work forwarded, waiting for result...");
    
//     // Receive result
//     let result_len = stream.read_u32().await?;
//     let mut result_buf = vec![0u8; result_len as usize];
//     stream.read_exact(&mut result_buf).await?;
    
//     let response: LoadBalancingMessage = serde_json::from_slice(&result_buf)?;
    
//     match response {
//         LoadBalancingMessage::WorkResult { encrypted_image } => {
//             info!("Received encrypted result ({} bytes)", encrypted_image.len());
//             Ok(encrypted_image)
//         },
//         _ => bail!("Unexpected response type from work receiver"),
//     }
// }

// // server.rs - REVERSED STEGANOGRAPHY: Embed client image + metadata inside server's default image
// async fn process_encryption_work(meta_buf: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
//     let meta_buf = meta_buf.to_vec();
//     let img_buf = img_buf.to_vec();
   
//     // Run CPU/IO intensive work on blocking thread pool
//     tokio::task::spawn_blocking(move || {
//         // 1. Deserialize the permissions metadata
//         let permissions: ImagePermissions = bincode::deserialize(&meta_buf)?;
       
//         // 2. Load the CLIENT'S image (this will be embedded)
//         let client_img = image::load_from_memory(&img_buf)?;
       
//         // 3. Convert client's image to bytes for embedding
//         let mut client_img_bytes = Vec::new();
//         client_img.write_to(
//             &mut Cursor::new(&mut client_img_bytes),
//             ImageOutputFormat::Png
//         )?;

//         // ✅ SAVE THE LENGTH BEFORE MOVING
//         let client_img_size = client_img_bytes.len();

//         // 4. Load the SERVER'S DEFAULT image (this will be the CARRIER)
//         let default_image_path = "unified_image.png";
//         let default_img = image::open(default_image_path)?;
       
//         // ✅ Log using saved size
//         info!("Embedding client image ({} bytes) + metadata into server's default image",
//               client_img_size);

//         // 5. Create combined payload: metadata + client's image
//         let combined_payload = CombinedPayload {
//             permissions,
//             unified_image: client_img_bytes,  // ✅ Move happens here
//         };
       
//         // 6. Serialize the combined payload
//         let final_payload = bincode::serialize(&combined_payload)?;
       
//         info!("Total payload size to embed: {} bytes ({:.2} KB)",
//               final_payload.len(),
//               final_payload.len() as f64 / 1024.0);
       
//         // ============================================================
//         // CAPACITY CHECK BEFORE EMBEDDING
//         // ============================================================
//         let (width, height) = default_img.dimensions();
//         let available_capacity = (width * height * 3) as usize; // 3 bits per pixel (RGB)
//         let required_capacity = final_payload.len() * 8; // 8 bits per byte
       
//         if required_capacity > available_capacity {
//             let min_pixels = required_capacity / 3;
//             let min_dimension = (min_pixels as f64).sqrt().ceil() as u32;
           
//             bail!(
//                 "Default image too small! Required: {} bits ({:.2} KB payload), \
//                  Available: {} bits. \
//                  Minimum default image size needed: {}x{} pixels (current: {}x{})",
//                 required_capacity,
//                 final_payload.len() as f64 / 1024.0,
//                 available_capacity,
//                 min_dimension,
//                 min_dimension,
//                 width,
//                 height
//             );
//         }
       
//         info!("Capacity check passed: {} bits needed, {} bits available",
//               required_capacity, available_capacity);
       
//         // 7. Embed payload into SERVER'S DEFAULT IMAGE (the carrier)
//         let encoded_img = lsb::encode(&default_img, &final_payload)?;
       
//         // 8. Convert to PNG bytes for transmission
//         let mut out_buf = Vec::new();
//         encoded_img.write_to(&mut Cursor::new(&mut out_buf), ImageOutputFormat::Png)?;
       
//         info!("Encrypted image created: {} bytes ({:.2} KB)",
//               out_buf.len(),
//               out_buf.len() as f64 / 1024.0);
       
//         Ok::<Vec<u8>, anyhow::Error>(out_buf)
//     })
//     .await?
// }







use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::raft::{RaftConfig, RaftNode};
use cloud_p2p_project::{lsb, CombinedPayload, ImagePermissions, LoadBalancingMessage, RaftMessage, ServerMetrics};
use image::{ImageOutputFormat, GenericImageView};
use log::{error, info};
use std::env;
use std::fs;
use std::io::Cursor;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::time::Duration;

/// Configure TCP socket for large file transfers
fn configure_large_transfer_socket(stream: &TcpStream) -> Result<()> {
    #[cfg(unix)]
    {
        use std::mem;
        
        unsafe {
            let fd = stream.as_raw_fd();
            let size: libc::c_int = 8 * 1024 * 1024; // 8MB buffer
            
            // Set send buffer
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &size as *const _ as *const libc::c_void,
                mem::size_of_val(&size) as libc::socklen_t,
            );
            
            // Set receive buffer
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &size as *const _ as *const libc::c_void,
                mem::size_of_val(&size) as libc::socklen_t,
            );
        }
    }
    Ok(())
}

const RAFT_PORT_OFFSET: u16 = 1000;    // Raft runs on port + 1000
const METRICS_PORT_OFFSET: u16 = 2000; // Metrics server on port + 2000
const WORK_PORT_OFFSET: u16 = 3000;    // Work receiver on port + 3000

// =============================================================================
// LOAD BALANCING STATE
// =============================================================================

pub struct LoadBalancingState {
    pub active_connections: AtomicU32,
    pub total_requests: AtomicU64,
    pub total_response_time_ms: AtomicU64,
}

impl LoadBalancingState {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicU32::new(0),
            total_requests: AtomicU64::new(0),
            total_response_time_ms: AtomicU64::new(0),
        }
    }
    
    /// Get current metrics for this server
    pub fn get_metrics(&self, server_id: String) -> ServerMetrics {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let avg_response = if total_requests > 0 {
            self.total_response_time_ms.load(Ordering::Relaxed) / total_requests
        } else {
            0
        };
        
        let active_conns = self.active_connections.load(Ordering::Relaxed);
        
        ServerMetrics {
            server_id,
            cpu_load: Self::estimate_cpu_load(active_conns),
            active_connections: active_conns,
            avg_response_time_ms: avg_response,
            total_requests,
            timestamp: std::time::SystemTime::now(),
        }
    }
    
    /// Estimate CPU load based on active connections
    /// In production, use sysinfo crate for real CPU metrics
    fn estimate_cpu_load(connections: u32) -> f32 {
        // Simple estimation: each connection adds ~10% CPU load
        (connections as f32 * 10.0).min(100.0)
    }
    
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub fn record_request(&self, response_time_ms: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_ms.fetch_add(response_time_ms, Ordering::Relaxed);
    }
}

// =============================================================================
// MAIN
// =============================================================================

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

    // Create load balancing state
    let lb_state = Arc::new(LoadBalancingState::new());

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
        election_timeout_min: 4000,  // 4 seconds (conservative for network reliability)
        election_timeout_max: 10000, // 10 seconds
        heartbeat_interval: 2000,    // 2 seconds
    };

    // Create and start Raft node
    // Raft will handle elections - if peers are unreachable, this server will win
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

    // Start metrics server (for load balancing)
    let metrics_port = port + METRICS_PORT_OFFSET;
    let metrics_lb_state = Arc::clone(&lb_state);
    let metrics_server_id = server_id.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_port, metrics_lb_state, metrics_server_id).await {
            error!("Metrics server error: {}", e);
        }
    });

    // Start work receiver (for forwarded work from leader)
    let work_port = port + WORK_PORT_OFFSET;
    let work_lb_state = Arc::clone(&lb_state);
    tokio::spawn(async move {
        if let Err(e) = start_work_receiver(work_port, work_lb_state).await {
            error!("Work receiver error: {}", e);
        }
    });

    // Start main application server
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Application server listening on {}", bind_addr);
    info!("Raft consensus running on port {}", raft_port);
    info!("Metrics server running on port {}", metrics_port);
    info!("Work receiver running on port {}", work_port);

    // Wait a bit for initial Raft election if peers are configured
    if !peers.is_empty() {
        info!("Waiting for initial Raft election/stabilization...");
        
        // Wait and check leadership multiple times to handle dynamic elections
        // With 4-10s election timeouts: 3 elections × ~7s ≈ 21s
        // Check every 5s to provide status updates
        for attempt in 1..=5 {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            if raft_node.is_leader().await {
                info!("✓ This server is now the LEADER (stabilized after {}s)", attempt * 5);
                break;
            } else {
                let leader_id = raft_node.get_leader_id().await;
                if let Some(leader) = &leader_id {
                    info!("This server is a FOLLOWER - current leader: {} (check {}/5)", leader, attempt);
                } else {
                    info!("No leader elected yet (check {}/5) - elections continuing...", attempt);
                }
                
                // On last attempt, give final status
                if attempt == 5 {
                    if leader_id.is_some() {
                        info!("Election stabilized - this server is a FOLLOWER");
                    } else {
                        info!("⚠ No stable leader after 25s - cluster may be partitioned");
                    }
                }
            }
        }
    } else {
        info!("No peers configured - this server will become leader immediately");
    }

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Client connected from {}", addr);
                let raft_ref = Arc::clone(&raft_node);
                let lb_ref = Arc::clone(&lb_state);
                let peers_clone = peers.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client_with_load_balancing(
                        stream,
                        raft_ref,
                        lb_ref,
                        peers_clone,
                    ).await {
                        error!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

// =============================================================================
// RAFT LISTENER
// =============================================================================

async fn start_raft_listener(port: u16, raft_node: Arc<RaftNode>) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
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

// =============================================================================
// METRICS SERVER (for Load Balancing)
// =============================================================================

async fn start_metrics_server(
    port: u16,
    lb_state: Arc<LoadBalancingState>,
    server_id: String,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Metrics server listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let lb_clone = Arc::clone(&lb_state);
                let id_clone = server_id.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_metrics_request(&mut stream, lb_clone, id_clone).await {
                        error!("Error handling metrics request: {}", e);
                    }
                });
            }
            Err(e) => error!("Metrics server accept error: {}", e),
        }
    }
}

async fn handle_metrics_request(
    stream: &mut TcpStream,
    lb_state: Arc<LoadBalancingState>,
    server_id: String,
) -> Result<()> {
    // Read request
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: LoadBalancingMessage = serde_json::from_slice(&msg_buf)?;
    
    match message {
        LoadBalancingMessage::MetricsRequest => {
            // Get current metrics
            let metrics = lb_state.get_metrics(server_id);
            
            // Send response
            let response = LoadBalancingMessage::MetricsResponse { metrics };
            let response_json = serde_json::to_string(&response)?;
            let response_bytes = response_json.as_bytes();
            
            stream.write_u32(response_bytes.len() as u32).await?;
            stream.write_all(response_bytes).await?;
            stream.flush().await?;
        }
        _ => {
            error!("Unexpected message type in metrics server");
        }
    }
    
    Ok(())
}

// =============================================================================
// WORK RECEIVER (for Forwarded Work)
// =============================================================================

async fn start_work_receiver(
    port: u16,
    lb_state: Arc<LoadBalancingState>,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Work receiver listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let lb_clone = Arc::clone(&lb_state);
                
                tokio::spawn(async move {
                    if let Err(e) = handle_forwarded_work(&mut stream, lb_clone).await {
                        error!("Error handling forwarded work: {}", e);
                    }
                });
            }
            Err(e) => error!("Work receiver accept error: {}", e),
        }
    }
}

async fn handle_forwarded_work(
    stream: &mut TcpStream,
    lb_state: Arc<LoadBalancingState>,
) -> Result<()> {
    let start_time = Instant::now();
    
    info!("Received forwarded work from leader");
    lb_state.increment_connections();

    // Read the forwarded work message
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: LoadBalancingMessage = serde_json::from_slice(&msg_buf)?;
    
    match message {
        LoadBalancingMessage::ForwardWork { metadata, image_data } => {
            info!("Processing forwarded encryption work...");
            
            // Process the encryption
            let result = process_encryption_work(&metadata, &image_data).await?;
            
            // Send result back
            let response = LoadBalancingMessage::WorkResult {
                encrypted_image: result,
            };
            let response_json = serde_json::to_string(&response)?;
            let response_bytes = response_json.as_bytes();
            
            stream.write_u32(response_bytes.len() as u32).await?;
            stream.write_all(response_bytes).await?;
            stream.flush().await?;
            
            let elapsed = start_time.elapsed().as_millis() as u64;
            lb_state.record_request(elapsed);
            lb_state.decrement_connections();
            
            info!("Forwarded work completed in {}ms", elapsed);
        }
        _ => {
            bail!("Unexpected message type in work receiver");
        }
    }
    
    Ok(())
}

// =============================================================================
// CLIENT HANDLER WITH LOAD BALANCING
// =============================================================================

async fn handle_client_with_load_balancing(
    mut stream: TcpStream,
    raft_node: Arc<RaftNode>,
    lb_state: Arc<LoadBalancingState>,
    peers: Vec<String>,
) -> Result<()> {
    let start_time = Instant::now();

    // Configure TCP buffers for large transfers
    configure_large_transfer_socket(&stream)?;

    // Check if this server is the leader (with retry for leader election)
    // In single-node scenarios, Raft will auto-elect this server as leader
    // During leader transitions, wait for election to complete
    // Elections take ~7s each, 3 elections = ~21s max
    // Check every 3s for quicker response once leader is elected
    let max_leader_wait_attempts = 8;  // 8 attempts × 3s = 24s max wait
    let mut is_leader = false;
    
    for attempt in 1..=max_leader_wait_attempts {
        if raft_node.is_leader().await {
            is_leader = true;
            break;
        }
        
        // Check if there's a known leader
        let leader_id = raft_node.get_leader_id().await;
        
        if attempt < max_leader_wait_attempts {
            // No leader yet - might be in election, wait a bit
            if leader_id.is_none() {
                info!("No leader elected yet (attempt {}/{}), waiting for election...", 
                      attempt, max_leader_wait_attempts);
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            } else {
                // There is a leader, but it's not us - reject immediately
                info!("Rejected client - not leader. Current leader: {:?}", leader_id);
                let error_msg = format!("NOT_LEADER:{}", leader_id.unwrap());
                let error_bytes = error_msg.as_bytes();
                stream.write_u64(error_bytes.len() as u64).await?;
                stream.write_all(error_bytes).await?;
                stream.flush().await?;
                return Ok(());
            }
        } else {
            // Final attempt failed
            let error_msg = match &leader_id {
                Some(id) => {
                    info!("Rejected client - not leader. Current leader: {:?}", leader_id);
                    format!("NOT_LEADER:{}", id)
                }
                None => {
                    info!("Rejected client - no leader elected after {} seconds", max_leader_wait_attempts * 8);
                    "NO_LEADER".to_string()
                }
            };
            
            let error_bytes = error_msg.as_bytes();
            stream.write_u64(error_bytes.len() as u64).await?;
            stream.write_all(error_bytes).await?;
            stream.flush().await?;
            return Ok(());
        }
    }

    info!("=== LEADER: Performing load balancing ===");

    // Read client request
    let meta_size = stream.read_u64().await?;
    let mut meta_buf = vec![0; meta_size as usize];
    stream.read_exact(&mut meta_buf).await?;

    let img_size = stream.read_u64().await?;
    let mut img_buf = vec![0; img_size as usize];
    stream.read_exact(&mut img_buf).await?;
    
    info!("Received client request (meta: {} bytes, image: {} bytes)", meta_size, img_size);

    // === LOAD BALANCING: Collect metrics from all servers ===
    // Store both metrics and their corresponding addresses
    let mut server_info: Vec<(ServerMetrics, Option<String>)> = vec![];
    
    // Get own metrics (no address needed for self)
    let my_metrics = lb_state.get_metrics(raft_node.config.server_id.clone());
    info!("My metrics: connections={}, load={:.1}%, score={:.3}",
          my_metrics.active_connections, my_metrics.cpu_load, my_metrics.calculate_load_score());
    server_info.push((my_metrics, None));
    
    // Get metrics from peers and store their addresses
    for peer_addr in &peers {
        match request_metrics_from_peer(peer_addr).await {
            Ok(metrics) => {
                info!("Peer {} metrics: connections={}, load={:.1}%, score={:.3}",
                      metrics.server_id, metrics.active_connections, 
                      metrics.cpu_load, metrics.calculate_load_score());
                server_info.push((metrics, Some(peer_addr.clone())));
            }
            Err(e) => {
                info!("Could not get metrics from {}: {}", peer_addr, e);
            }
        }
    }
    
    // Select best server based on load scores
    let (best_server, best_addr) = server_info
        .iter()
        .min_by(|(a, _), (b, _)| {
            a.calculate_load_score()
                .partial_cmp(&b.calculate_load_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("At least one server should be available");
    
    info!("=== LOAD BALANCING DECISION ===");
    info!("Selected server: {} (score: {:.3})", 
          best_server.server_id, best_server.calculate_load_score());
    
    // Decide: process locally or forward
    let result = if best_server.server_id == raft_node.config.server_id {
        info!("Processing LOCALLY (I am the best choice)");
        lb_state.increment_connections();
        
        let encrypted = process_encryption_work(&meta_buf, &img_buf).await?;
        
        lb_state.decrement_connections();
        let elapsed = start_time.elapsed().as_millis() as u64;
        lb_state.record_request(elapsed);
        
        info!("Local processing completed in {}ms", elapsed);
        encrypted
    } else {
        // Forward to the selected server using its stored address
        let target_address = best_addr
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No address found for server {}", best_server.server_id))?;
        
        info!("Forwarding to server {} at {}", best_server.server_id, target_address);
        
        let encrypted = forward_work_to_address(
            target_address,
            &meta_buf,
            &img_buf,
        ).await?;
        
        info!("Forwarded work completed");
        encrypted
    };

    // Send result back to client
    stream.write_u64(result.len() as u64).await?;
    stream.write_all(&result).await?;
    stream.flush().await?;
    
    info!("Sent result to client ({} bytes)", result.len());
    Ok(())
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Request metrics from a peer server
async fn request_metrics_from_peer(peer_addr: &str) -> Result<ServerMetrics> {
    let parts: Vec<&str> = peer_addr.split(':').collect();
    let base_port: u16 = parts[1].parse()?;
    let metrics_port = base_port + METRICS_PORT_OFFSET;
    let metrics_addr = format!("{}:{}", parts[0], metrics_port);
    
    // Add timeout to prevent hanging on dead peers
    let connect_future = TcpStream::connect(&metrics_addr);
    let mut stream = tokio::time::timeout(Duration::from_secs(2), connect_future)
        .await
        .map_err(|_| anyhow::anyhow!("Connection timeout to {}", metrics_addr))??;
    
    // Send metrics request
    let request = LoadBalancingMessage::MetricsRequest;
    let json = serde_json::to_string(&request)?;
    let bytes = json.as_bytes();
    
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    
    // Read response with timeout
    let read_future = async {
        let response_len = stream.read_u32().await?;
        let mut response_buf = vec![0u8; response_len as usize];
        stream.read_exact(&mut response_buf).await?;
        Ok::<Vec<u8>, anyhow::Error>(response_buf)
    };
    
    let response_buf = tokio::time::timeout(Duration::from_secs(2), read_future)
        .await
        .map_err(|_| anyhow::anyhow!("Read timeout from {}", metrics_addr))??;
    
    let response: LoadBalancingMessage = serde_json::from_slice(&response_buf)?;
    
    match response {
        LoadBalancingMessage::MetricsResponse { metrics } => Ok(metrics),
        _ => bail!("Unexpected response type from metrics server"),
    }
}

/// Forward work to another server using its direct address
async fn forward_work_to_address(
    target_addr: &str,
    meta_buf: &[u8],
    img_buf: &[u8],
) -> Result<Vec<u8>> {
    let parts: Vec<&str> = target_addr.split(':').collect();
    let base_port: u16 = parts[1].parse()?;
    let work_port = base_port + WORK_PORT_OFFSET;
    let work_addr = format!("{}:{}", parts[0], work_port);
    
    info!("Connecting to work receiver at {}", work_addr);
    
    // Add timeout to prevent hanging on dead peers
    let connect_future = TcpStream::connect(&work_addr);
    let mut stream = tokio::time::timeout(Duration::from_secs(5), connect_future)
        .await
        .map_err(|_| anyhow::anyhow!("Connection timeout to {}", work_addr))??;
    
    // Send forwarded work
    let message = LoadBalancingMessage::ForwardWork {
        metadata: meta_buf.to_vec(),
        image_data: img_buf.to_vec(),
    };
    let json = serde_json::to_string(&message)?;
    let bytes = json.as_bytes();
    
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await?;
    stream.flush().await?;
    
    info!("Work forwarded, waiting for result...");
    
    // Receive result with timeout
    let read_future = async {
        let result_len = stream.read_u32().await?;
        let mut result_buf = vec![0u8; result_len as usize];
        stream.read_exact(&mut result_buf).await?;
        Ok::<Vec<u8>, anyhow::Error>(result_buf)
    };
    
    let result_buf = tokio::time::timeout(Duration::from_secs(30), read_future)
        .await
        .map_err(|_| anyhow::anyhow!("Read timeout from {}", work_addr))??;
    
    let response: LoadBalancingMessage = serde_json::from_slice(&result_buf)?;
    
    match response {
        LoadBalancingMessage::WorkResult { encrypted_image } => {
            info!("Received encrypted result ({} bytes)", encrypted_image.len());
            Ok(encrypted_image)
        },
        _ => bail!("Unexpected response type from work receiver"),
    }
}

// server.rs - REVERSED STEGANOGRAPHY: Embed client image + metadata inside server's default image
async fn process_encryption_work(meta_buf: &[u8], img_buf: &[u8]) -> Result<Vec<u8>> {
    let meta_buf = meta_buf.to_vec();
    let img_buf = img_buf.to_vec();
   
    // Run CPU/IO intensive work on blocking thread pool
    tokio::task::spawn_blocking(move || {
        // 1. Deserialize the permissions metadata
        let permissions: ImagePermissions = bincode::deserialize(&meta_buf)?;
       
        // 2. Load the CLIENT'S image (this will be embedded)
        let client_img = image::load_from_memory(&img_buf)?;
       
        // 3. Convert client's image to bytes for embedding
        let mut client_img_bytes = Vec::new();
        client_img.write_to(
            &mut Cursor::new(&mut client_img_bytes),
            ImageOutputFormat::Png
        )?;

        // ✅ SAVE THE LENGTH BEFORE MOVING
        let client_img_size = client_img_bytes.len();

        // 4. Load the SERVER'S DEFAULT image (this will be the CARRIER)
        let default_image_path = "src/unified_image.png";
        let default_img = image::open(default_image_path)?;
       
        // ✅ Log using saved size
        info!("Embedding client image ({} bytes) + metadata into server's default image",
              client_img_size);

        // 5. Create combined payload: metadata + client's image
        let combined_payload = CombinedPayload {
            permissions,
            unified_image: client_img_bytes,  // ✅ Move happens here
        };
       
        // 6. Serialize the combined payload
        let final_payload = bincode::serialize(&combined_payload)?;
       
        info!("Total payload size to embed: {} bytes ({:.2} KB)",
              final_payload.len(),
              final_payload.len() as f64 / 1024.0);
       
        // ============================================================
        // CAPACITY CHECK BEFORE EMBEDDING
        // ============================================================
        let (width, height) = default_img.dimensions();
        let available_capacity = (width * height * 3) as usize; // 3 bits per pixel (RGB)
        let required_capacity = final_payload.len() * 8; // 8 bits per byte
       
        if required_capacity > available_capacity {
            let min_pixels = required_capacity / 3;
            let min_dimension = (min_pixels as f64).sqrt().ceil() as u32;
           
            bail!(
                "Default image too small! Required: {} bits ({:.2} KB payload), \
                 Available: {} bits. \
                 Minimum default image size needed: {}x{} pixels (current: {}x{})",
                required_capacity,
                final_payload.len() as f64 / 1024.0,
                available_capacity,
                min_dimension,
                min_dimension,
                width,
                height
            );
        }
       
        info!("Capacity check passed: {} bits needed, {} bits available",
              required_capacity, available_capacity);
       
        // 7. Embed payload into SERVER'S DEFAULT IMAGE (the carrier)
        let encoded_img = lsb::encode(&default_img, &final_payload)?;
       
        // 8. Convert to PNG bytes for transmission
        let mut out_buf = Vec::new();
        encoded_img.write_to(&mut Cursor::new(&mut out_buf), ImageOutputFormat::Png)?;
       
        info!("Encrypted image created: {} bytes ({:.2} KB)",
              out_buf.len(),
              out_buf.len() as f64 / 1024.0);
       
        Ok::<Vec<u8>, anyhow::Error>(out_buf)
    })
    .await?
}
