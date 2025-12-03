use anyhow::{bail, Result};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::sleep;

// =============================================================================
// DIRECTORY SERVICE DATA STRUCTURES
// =============================================================================

/// Represents a user registered in the directory service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEntry {
    pub username: String,
    pub p2p_address: String,
    pub last_heartbeat: SystemTime,
    pub status: UserStatus,
    pub shared_images: Vec<ImageInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UserStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageInfo {
    pub image_id: String,
    pub image_name: String,
    pub thumbnail_path: Option<String>,
}

/// Directory service messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectoryMessage {
    Register {
        username: String,
        p2p_address: String,
        shared_images: Vec<ImageInfo>,
    },
    RegisterResponse {
        success: bool,
        message: String,
    },
    Heartbeat {
        username: String,
    },
    HeartbeatResponse {
        success: bool,
    },
    Unregister {
        username: String,
    },
    UnregisterResponse {
        success: bool,
    },
    QueryPeers {
        requesting_user: String,
    },
    QueryPeersResponse {
        peers: Vec<UserEntry>,
    },
    UpdateSharedImages {
        username: String,
        shared_images: Vec<ImageInfo>,
    },
    UpdateResponse {
        success: bool,
        message: String,
    },
    QueryUser {
        username: String,
    },
    QueryUserResponse {
        user: Option<UserEntry>,
    },
    SyncState {
        users: HashMap<String, UserEntry>,
    },
    SyncStateResponse {
        success: bool,
    },
}

// =============================================================================
// DIRECTORY SERVICE STATE (WITH REPLICATION + PERSISTENCE)
// =============================================================================

pub struct DirectoryServiceState {
    users: RwLock<HashMap<String, UserEntry>>,
    heartbeat_timeout: Duration,
    peer_servers: Vec<String>,
    server_id: String,
    
    /// NEW: Path to persistent state file
    state_file: PathBuf,
}

impl DirectoryServiceState {
    pub fn new(
        heartbeat_timeout: Duration,
        server_id: String,
        peer_servers: Vec<String>,
        state_file: PathBuf,
    ) -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            heartbeat_timeout,
            peer_servers,
            server_id,
            state_file,
        }
    }
    
    /// NEW: Load state from disk
    pub async fn load_from_disk(&self) -> Result<()> {
        if !self.state_file.exists() {
            info!("[{}] No state file found, starting fresh", self.server_id);
            return Ok(());
        }
        
        let data = fs::read_to_string(&self.state_file)?;
        let loaded_users: HashMap<String, UserEntry> = serde_json::from_str(&data)?;
        
        let mut users = self.users.write().await;
        *users = loaded_users;
        
        info!("[{}] ✓ Loaded {} users from disk", self.server_id, users.len());
        
        // Mark all users as offline initially (will come back online with heartbeat)
        for user in users.values_mut() {
            user.status = UserStatus::Offline;
        }
        
        Ok(())
    }
    
    /// NEW: Save state to disk
    async fn save_to_disk(&self) -> Result<()> {
        let users = self.users.read().await;
        let data = serde_json::to_string_pretty(&*users)?;
        fs::write(&self.state_file, data)?;
        
        info!("[{}] ✓ Saved state to disk ({} users)", self.server_id, users.len());
        Ok(())
    }
    
    /// NEW: Request state from peers (for recovery)
    pub async fn sync_from_peers(&self) -> Result<()> {
        if self.peer_servers.is_empty() {
            return Ok(());
        }
        
        info!("[{}] Requesting state from peers for recovery...", self.server_id);
        
        for peer in &self.peer_servers {
            match request_state_from_peer(peer).await {
                Ok(peer_users) => {
                    let mut users = self.users.write().await;
                    
                    // Merge peer state
                    for (username, peer_user) in peer_users {
                        match users.get(&username) {
                            Some(local_user) => {
                                if peer_user.last_heartbeat > local_user.last_heartbeat {
                                    users.insert(username.clone(), peer_user);
                                }
                            }
                            None => {
                                users.insert(username.clone(), peer_user);
                            }
                        }
                    }
                    
                    info!("[{}] ✓ Synced state from peer {} ({} users total)", 
                          self.server_id, peer, users.len());
                    
                    drop(users);
                    
                    // Save the recovered state
                    self.save_to_disk().await?;
                    
                    return Ok(());
                }
                Err(e) => {
                    warn!("[{}] Could not sync from peer {}: {}", 
                          self.server_id, peer, e);
                    continue;
                }
            }
        }
        
        warn!("[{}] Could not sync from any peer, using local state", self.server_id);
        Ok(())
    }
    
    pub async fn register_user(
        &self,
        username: String,
        p2p_address: String,
        shared_images: Vec<ImageInfo>,
    ) -> Result<()> {
        let mut users = self.users.write().await;
        
        let entry = UserEntry {
            username: username.clone(),
            p2p_address,
            last_heartbeat: SystemTime::now(),
            status: UserStatus::Online,
            shared_images,
        };
        
        let image_count = entry.shared_images.len();
        users.insert(username.clone(), entry.clone());
        info!("[{}] Registered user: {} with {} shared images", 
              self.server_id, username, image_count);
        
        drop(users);
        
        // Persist to disk
        let _ = self.save_to_disk().await;
        
        // Replicate to peers
        self.replicate_state().await;
        
        Ok(())
    }
    
    pub async fn update_heartbeat(&self, username: &str) -> Result<()> {
        let mut users = self.users.write().await;
        
        if let Some(user) = users.get_mut(username) {
            user.last_heartbeat = SystemTime::now();
            user.status = UserStatus::Online;
            Ok(())
        } else {
            bail!("User {} not found", username)
        }
    }
    
    pub async fn unregister_user(&self, username: &str) -> Result<()> {
        let mut users = self.users.write().await;
        
        if let Some(user) = users.get_mut(username) {
            user.status = UserStatus::Offline;
            info!("[{}] User {} went offline", self.server_id, username);
            
            drop(users);
            
            let _ = self.save_to_disk().await;
            self.replicate_state().await;
            
            Ok(())
        } else {
            bail!("User {} not found", username)
        }
    }
    
    pub async fn get_online_peers(&self, requesting_user: &str) -> Vec<UserEntry> {
        let users = self.users.read().await;
        
        users
            .values()
            .filter(|u| {
                u.username != requesting_user
                    && u.status == UserStatus::Online
                    && self.is_user_active(u)
            })
            .cloned()
            .collect()
    }
    
    fn is_user_active(&self, user: &UserEntry) -> bool {
        if let Ok(elapsed) = user.last_heartbeat.elapsed() {
            elapsed < self.heartbeat_timeout
        } else {
            false
        }
    }
    
    pub async fn update_shared_images(
        &self,
        username: &str,
        shared_images: Vec<ImageInfo>,
    ) -> Result<()> {
        let mut users = self.users.write().await;
        
        if let Some(user) = users.get_mut(username) {
            user.shared_images = shared_images;
            info!("[{}] Updated shared images for user: {}", self.server_id, username);
            
            drop(users);
            
            let _ = self.save_to_disk().await;
            self.replicate_state().await;
            
            Ok(())
        } else {
            bail!("User {} not found", username)
        }
    }
    
    pub async fn query_user(&self, username: &str) -> Option<UserEntry> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }
    
    pub async fn cleanup_inactive_users(&self) {
        let mut users = self.users.write().await;
        
        let mut to_mark_offline = Vec::new();
        
        for (username, user) in users.iter() {
            if user.status == UserStatus::Online && !self.is_user_active(user) {
                to_mark_offline.push(username.clone());
            }
        }
        
        for username in to_mark_offline {
            if let Some(user) = users.get_mut(&username) {
                user.status = UserStatus::Offline;
                info!("[{}] Marked user {} as offline due to timeout", 
                      self.server_id, username);
            }
        }
        
        drop(users);
        
        let _ = self.save_to_disk().await;
        self.replicate_state().await;
    }
    
    async fn replicate_state(&self) {
        if self.peer_servers.is_empty() {
            return;
        }
        
        let users = self.users.read().await;
        let state_snapshot = users.clone();
        drop(users);
        
        for peer in &self.peer_servers {
            let peer_addr = peer.clone();
            let snapshot = state_snapshot.clone();
            
            tokio::spawn(async move {
                if let Err(e) = send_state_sync(&peer_addr, snapshot).await {
                    error!("Failed to replicate to {}: {}", peer_addr, e);
                }
            });
        }
    }
    
    pub async fn receive_state_sync(&self, incoming_state: HashMap<String, UserEntry>) {
        let mut users = self.users.write().await;
        
        for (username, incoming_user) in incoming_state {
            match users.get(&username) {
                Some(existing_user) => {
                    if incoming_user.last_heartbeat > existing_user.last_heartbeat {
                        users.insert(username.clone(), incoming_user);
                        info!("[{}] Updated user {} from peer sync", 
                              self.server_id, username);
                    }
                }
                None => {
                    users.insert(username.clone(), incoming_user);
                    info!("[{}] Added new user {} from peer sync", 
                          self.server_id, username);
                }
            }
        }
        
        drop(users);
        
        // Persist the merged state
        let _ = self.save_to_disk().await;
    }
    
    pub async fn get_full_state(&self) -> HashMap<String, UserEntry> {
        let users = self.users.read().await;
        users.clone()
    }
}

// =============================================================================
// DIRECTORY SERVICE SERVER
// =============================================================================

pub async fn start_directory_service(
    port: u16,
    server_id: String,
    peer_servers: Vec<String>,
    state_file: PathBuf,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    
    info!("[{}] Directory service listening on {}", server_id, bind_addr);
    info!("[{}] State file: {}", server_id, state_file.display());
    
    let state = Arc::new(DirectoryServiceState::new(
        Duration::from_secs(30),
        server_id.clone(),
        peer_servers.clone(),
        state_file,
    ));
    
    // Load state from disk
    if let Err(e) = state.load_from_disk().await {
        warn!("[{}] Could not load state from disk: {}", server_id, e);
    }
    
    // Sync from peers if available
    if !peer_servers.is_empty() {
        info!("[{}] Attempting to sync state from peers...", server_id);
        if let Err(e) = state.sync_from_peers().await {
            warn!("[{}] Could not sync from peers: {}", server_id, e);
        }
    }
    
    info!("[{}] ✓ Directory service ready!", server_id);
    
    // Spawn cleanup task
    let cleanup_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(10)).await;
            cleanup_state.cleanup_inactive_users().await;
        }
    });
    
    // Spawn periodic save task
    let save_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(60)).await;
            if let Err(e) = save_state.save_to_disk().await {
                error!("Failed to save state: {}", e);
            }
        }
    });
    
    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let state_ref = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle_directory_client(stream, addr, state_ref).await {
                        error!("Error handling directory client {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting directory connection: {}", e);
            }
        }
    }
}

async fn handle_directory_client(
    mut stream: TcpStream,
    addr: SocketAddr,
    state: Arc<DirectoryServiceState>,
) -> Result<()> {
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: DirectoryMessage = serde_json::from_slice(&msg_buf)?;
    
    let response = match message {
        DirectoryMessage::Register {
            username,
            p2p_address,
            shared_images,
        } => {
            match state.register_user(username.clone(), p2p_address, shared_images).await {
                Ok(_) => DirectoryMessage::RegisterResponse {
                    success: true,
                    message: format!("User {} registered successfully", username),
                },
                Err(e) => DirectoryMessage::RegisterResponse {
                    success: false,
                    message: format!("Registration failed: {}", e),
                },
            }
        }
        DirectoryMessage::Heartbeat { username } => {
            let success = state.update_heartbeat(&username).await.is_ok();
            DirectoryMessage::HeartbeatResponse { success }
        }
        DirectoryMessage::Unregister { username } => {
            let success = state.unregister_user(&username).await.is_ok();
            DirectoryMessage::UnregisterResponse { success }
        }
        DirectoryMessage::QueryPeers { requesting_user } => {
            let peers = state.get_online_peers(&requesting_user).await;
            DirectoryMessage::QueryPeersResponse { peers }
        }
        DirectoryMessage::UpdateSharedImages {
            username,
            shared_images,
        } => {
            match state.update_shared_images(&username, shared_images).await {
                Ok(_) => DirectoryMessage::UpdateResponse {
                    success: true,
                    message: "Shared images updated".to_string(),
                },
                Err(e) => DirectoryMessage::UpdateResponse {
                    success: false,
                    message: format!("Update failed: {}", e),
                },
            }
        }
        DirectoryMessage::QueryUser { username } => {
            let user = state.query_user(&username).await;
            DirectoryMessage::QueryUserResponse { user }
        }
        DirectoryMessage::SyncState { users } => {
            state.receive_state_sync(users).await;
            DirectoryMessage::SyncStateResponse { success: true }
        }
        _ => {
            bail!("Unexpected message type from {}", addr);
        }
    };
    
    let response_json = serde_json::to_string(&response)?;
    let response_bytes = response_json.as_bytes();
    
    stream.write_u32(response_bytes.len() as u32).await?;
    stream.write_all(response_bytes).await?;
    stream.flush().await?;
    
    Ok(())
}

// =============================================================================
// CLIENT HELPERS
// =============================================================================

pub async fn send_directory_message(
    directory_addr: &str,
    message: DirectoryMessage,
) -> Result<DirectoryMessage> {
    let mut stream = TcpStream::connect(directory_addr).await?;
    
    let msg_json = serde_json::to_string(&message)?;
    let msg_bytes = msg_json.as_bytes();
    
    stream.write_u32(msg_bytes.len() as u32).await?;
    stream.write_all(msg_bytes).await?;
    stream.flush().await?;
    
    let response_len = stream.read_u32().await?;
    let mut response_buf = vec![0u8; response_len as usize];
    stream.read_exact(&mut response_buf).await?;
    
    let response: DirectoryMessage = serde_json::from_slice(&response_buf)?;
    Ok(response)
}

async fn send_state_sync(
    peer_addr: &str,
    state: HashMap<String, UserEntry>,
) -> Result<()> {
    let message = DirectoryMessage::SyncState { users: state };
    let response = send_directory_message(peer_addr, message).await?;
    
    match response {
        DirectoryMessage::SyncStateResponse { success: true } => Ok(()),
        _ => bail!("Unexpected response from peer"),
    }
}

/// NEW: Request full state from a peer
async fn request_state_from_peer(peer_addr: &str) -> Result<HashMap<String, UserEntry>> {
    // We use QueryPeers with empty user to get all users
    // This is a workaround - in production you'd add a dedicated GetFullState message
    let message = DirectoryMessage::QueryPeers {
        requesting_user: String::new(),
    };
    
    let response = send_directory_message(peer_addr, message).await?;
    
    match response {
        DirectoryMessage::QueryPeersResponse { peers } => {
            let mut users = HashMap::new();
            for peer in peers {
                users.insert(peer.username.clone(), peer);
            }
            Ok(users)
        }
        _ => bail!("Unexpected response"),
    }
}