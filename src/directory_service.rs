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

/// Pending image request notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRequest {
    pub request_id: String,
    pub from_user: String,
    pub to_user: String,
    pub image_id: String,
    pub requested_views: u32,
    pub timestamp: SystemTime,
    pub status: RequestStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RequestStatus {
    Pending,
    Accepted,
    Rejected,
}

/// Pending permission update (for offline users)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPermissionUpdate {
    pub update_id: String,
    pub from_owner: String,
    pub target_user: String,
    pub image_id: String,
    pub new_quota: u32,
    pub timestamp: SystemTime,
    /// The embedded image data to deliver when the user comes online
    pub embedded_image: Option<Vec<u8>>,
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
    /// Query ALL peers (both online and offline)
    QueryAllPeers {
        requesting_user: String,
    },
    QueryAllPeersResponse {
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

    // Asynchronous request system
    LeaveRequest {
        from_user: String,
        to_user: String,
        image_id: String,
        requested_views: u32,
    },
    LeaveRequestResponse {
        success: bool,
        request_id: String,
        message: String,
    },
    GetPendingRequests {
        username: String,
    },
    GetPendingRequestsResponse {
        requests: Vec<PendingRequest>,
    },
    RespondToRequest {
        request_id: String,
        owner: String,
        accept: bool,
    },
    RespondToRequestResponse {
        success: bool,
        message: String,
        request: Option<PendingRequest>,
    },
    GetNotifications {
        username: String,
    },
    GetNotificationsResponse {
        notifications: Vec<PendingRequest>,
    },
    /// Store a pending permission update for an offline user
    StorePendingPermissionUpdate {
        from_owner: String,
        target_user: String,
        image_id: String,
        new_quota: u32,
        /// The embedded image data to deliver when the user comes online
        embedded_image: Option<Vec<u8>>,
    },
    StorePendingPermissionUpdateResponse {
        success: bool,
        message: String,
        update_id: String,
    },
    /// Get pending permission updates for a user
    GetPendingPermissionUpdates {
        username: String,
    },
    GetPendingPermissionUpdatesResponse {
        updates: Vec<PendingPermissionUpdate>,
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

    /// NEW: Pending requests storage
    pending_requests: RwLock<HashMap<String, PendingRequest>>,

    /// NEW: Pending permission updates storage
    pending_permission_updates: RwLock<HashMap<String, PendingPermissionUpdate>>,
}

/// Snapshot of directory service state for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectorySnapshot {
    users: HashMap<String, UserEntry>,
    pending_requests: HashMap<String, PendingRequest>,
    pending_permission_updates: HashMap<String, PendingPermissionUpdate>,
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
            pending_requests: RwLock::new(HashMap::new()),
            pending_permission_updates: RwLock::new(HashMap::new()),
        }
    }
    
    /// NEW: Load state from disk
    pub async fn load_from_disk(&self) -> Result<()> {
        if !self.state_file.exists() {
            info!("[{}] No state file found, starting fresh", self.server_id);
            return Ok(());
        }
        
        let data = fs::read_to_string(&self.state_file)?;
        
        // Try to load the new snapshot format first
        if let Ok(snapshot) = serde_json::from_str::<DirectorySnapshot>(&data) {
            let mut users = self.users.write().await;
            *users = snapshot.users;
            
            // Mark all users as offline initially (will come back online with heartbeat)
            for user in users.values_mut() {
                user.status = UserStatus::Offline;
            }
            
            let mut pending_requests = self.pending_requests.write().await;
            *pending_requests = snapshot.pending_requests;
            
            let mut pending_updates = self.pending_permission_updates.write().await;
            *pending_updates = snapshot.pending_permission_updates;
            
            info!("[{}] ✓ Loaded snapshot from disk ({} users, {} pending requests, {} pending permission updates)", 
                  self.server_id, users.len(), pending_requests.len(), pending_updates.len());
        } else {
            // Fall back to old format (just users)
            let loaded_users: HashMap<String, UserEntry> = serde_json::from_str(&data)?;
            
            let mut users = self.users.write().await;
            *users = loaded_users;
            
            info!("[{}] ✓ Loaded {} users from disk (legacy format)", self.server_id, users.len());
            
            // Mark all users as offline initially (will come back online with heartbeat)
            for user in users.values_mut() {
                user.status = UserStatus::Offline;
            }
        }
        
        Ok(())
    }
    
    /// NEW: Save state to disk
    async fn save_to_disk(&self) -> Result<()> {
        let users = self.users.read().await;
        let pending_requests = self.pending_requests.read().await;
        let pending_updates = self.pending_permission_updates.read().await;
        
        let snapshot = DirectorySnapshot {
            users: users.clone(),
            pending_requests: pending_requests.clone(),
            pending_permission_updates: pending_updates.clone(),
        };
        
        let data = serde_json::to_string_pretty(&snapshot)?;
        fs::write(&self.state_file, data)?;
        
        info!("[{}] ✓ Saved snapshot to disk ({} users, {} pending requests, {} pending permission updates)", 
              self.server_id, users.len(), pending_requests.len(), pending_updates.len());
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
            
            // Clear all notifications for this user (accepted/rejected requests they made)
            self.clear_notifications_for_user(username).await;
            
            // Optionally: Also clear pending requests TO this user that they haven't responded to
            // This prevents stale requests from accumulating
            self.clear_pending_requests_to_user(username).await;
            
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

    /// Get ALL registered peers (both online and offline), excluding the requesting user
    pub async fn get_all_peers(&self, requesting_user: &str) -> Vec<UserEntry> {
        let users = self.users.read().await;

        users
            .values()
            .filter(|u| u.username != requesting_user)
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

    // =============================================================================
    // ASYNCHRONOUS REQUEST SYSTEM
    // =============================================================================

    /// Leave a request when target user is offline
    pub async fn leave_request(
        &self,
        from_user: String,
        to_user: String,
        image_id: String,
        requested_views: u32,
    ) -> Result<String> {
        use uuid::Uuid;

        let request_id = Uuid::new_v4().to_string();
        let request = PendingRequest {
            request_id: request_id.clone(),
            from_user,
            to_user,
            image_id,
            requested_views,
            timestamp: SystemTime::now(),
            status: RequestStatus::Pending,
        };

        let mut requests = self.pending_requests.write().await;
        requests.insert(request_id.clone(), request);

        info!("[{}] New request saved: {}", self.server_id, request_id);
        Ok(request_id)
    }

    /// Get pending requests for a user (requests TO them)
    pub async fn get_pending_requests_for_user(&self, username: &str) -> Vec<PendingRequest> {
        let requests = self.pending_requests.read().await;
        requests
            .values()
            .filter(|r| r.to_user == username && r.status == RequestStatus::Pending)
            .cloned()
            .collect()
    }

    /// Respond to a request (accept or reject)
    pub async fn respond_to_request(
        &self,
        request_id: &str,
        owner: &str,
        accept: bool,
    ) -> Result<(String, PendingRequest)> {
        let mut requests = self.pending_requests.write().await;

        match requests.get_mut(request_id) {
            Some(request) => {
                // Verify the responder is the request recipient
                if request.to_user != owner {
                    bail!("Only the recipient can respond to this request");
                }

                // Update status
                request.status = if accept {
                    RequestStatus::Accepted
                } else {
                    RequestStatus::Rejected
                };

                let message = if accept {
                    format!("Request accepted. User {} can now access the image.", request.from_user)
                } else {
                    format!("Request rejected.")
                };

                info!(
                    "[{}] Request {} {} by {}",
                    self.server_id,
                    request_id,
                    if accept { "accepted" } else { "rejected" },
                    owner
                );

                // Return a clone of the updated request
                let request_copy = request.clone();
                Ok((message, request_copy))
            }
            None => bail!("Request not found"),
        }
    }

    /// Get notifications for a user (responses to their requests)
    pub async fn get_notifications_for_user(&self, username: &str) -> Vec<PendingRequest> {
        let requests = self.pending_requests.read().await;
        requests
            .values()
            .filter(|r| {
                r.from_user == username
                    && (r.status == RequestStatus::Accepted || r.status == RequestStatus::Rejected)
            })
            .cloned()
            .collect()
    }

    /// Clear all notifications for a user (called when user goes offline)
    pub async fn clear_notifications_for_user(&self, username: &str) {
        let mut requests = self.pending_requests.write().await;
        
        // Collect request IDs to remove (notifications are requests from this user that have been accepted/rejected)
        let to_remove: Vec<String> = requests
            .iter()
            .filter(|(_, r)| {
                r.from_user == username
                    && (r.status == RequestStatus::Accepted || r.status == RequestStatus::Rejected)
            })
            .map(|(id, _)| id.clone())
            .collect();
        
        let count = to_remove.len();
        for id in to_remove {
            requests.remove(&id);
        }
        
        if count > 0 {
            info!("[{}] Cleared {} notifications for user {}", self.server_id, count, username);
        }
    }

    /// Clear all pending requests TO a user (requests they haven't responded to yet)
    pub async fn clear_pending_requests_to_user(&self, username: &str) {
        let mut requests = self.pending_requests.write().await;
        
        // Remove pending requests where this user is the target (to_user)
        let to_remove: Vec<String> = requests
            .iter()
            .filter(|(_, r)| r.to_user == username && r.status == RequestStatus::Pending)
            .map(|(id, _)| id.clone())
            .collect();
        
        let count = to_remove.len();
        for id in to_remove {
            requests.remove(&id);
        }
        
        if count > 0 {
            info!("[{}] Cleared {} pending requests to user {}", self.server_id, count, username);
        }
    }

    /// Store a pending permission update for an offline user
    pub async fn store_pending_permission_update(
        &self,
        from_owner: &str,
        target_user: &str,
        image_id: &str,
        new_quota: u32,
        embedded_image: Option<Vec<u8>>,
    ) -> String {
        let update_id = format!("{}:{}:{}", from_owner, target_user, image_id);
        let has_image = embedded_image.is_some();
        
        let update = PendingPermissionUpdate {
            update_id: update_id.clone(),
            from_owner: from_owner.to_string(),
            target_user: target_user.to_string(),
            image_id: image_id.to_string(),
            new_quota,
            timestamp: SystemTime::now(),
            embedded_image,
        };

        let mut updates = self.pending_permission_updates.write().await;
        updates.insert(update_id.clone(), update);

        info!(
            "[{}] Stored pending permission update: {} wants to change {}'s quota for {} to {} views (image attached: {})",
            self.server_id, from_owner, target_user, image_id, new_quota, has_image
        );

        update_id
    }

    /// Get and remove pending permission updates for a user
    pub async fn get_and_clear_pending_updates(&self, username: &str) -> Vec<PendingPermissionUpdate> {
        let mut updates = self.pending_permission_updates.write().await;
        let user_updates: Vec<PendingPermissionUpdate> = updates
            .values()
            .filter(|u| u.target_user == username)
            .cloned()
            .collect();

        // Remove the retrieved updates
        for update in &user_updates {
            updates.remove(&update.update_id);
        }

        user_updates
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
        DirectoryMessage::QueryAllPeers { requesting_user } => {
            let peers = state.get_all_peers(&requesting_user).await;
            DirectoryMessage::QueryAllPeersResponse { peers }
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

        // Asynchronous request handling
        DirectoryMessage::LeaveRequest {
            from_user,
            to_user,
            image_id,
            requested_views,
        } => {
            match state.leave_request(from_user, to_user, image_id, requested_views).await {
                Ok(request_id) => DirectoryMessage::LeaveRequestResponse {
                    success: true,
                    request_id,
                    message: "Request saved. User will be notified when online.".to_string(),
                },
                Err(e) => DirectoryMessage::LeaveRequestResponse {
                    success: false,
                    request_id: String::new(),
                    message: format!("Failed to save request: {}", e),
                },
            }
        }

        DirectoryMessage::GetPendingRequests { username } => {
            let requests = state.get_pending_requests_for_user(&username).await;
            DirectoryMessage::GetPendingRequestsResponse { requests }
        }

        DirectoryMessage::RespondToRequest {
            request_id,
            owner,
            accept,
        } => {
            match state.respond_to_request(&request_id, &owner, accept).await {
                Ok((message, request)) => DirectoryMessage::RespondToRequestResponse {
                    success: true,
                    message,
                    request: Some(request),
                },
                Err(e) => DirectoryMessage::RespondToRequestResponse {
                    success: false,
                    message: format!("Failed to respond: {}", e),
                    request: None,
                },
            }
        }

        DirectoryMessage::GetNotifications { username } => {
            let notifications = state.get_notifications_for_user(&username).await;
            DirectoryMessage::GetNotificationsResponse { notifications }
        }

        DirectoryMessage::StorePendingPermissionUpdate {
            from_owner,
            target_user,
            image_id,
            new_quota,
            embedded_image,
        } => {
            let update_id = state
                .store_pending_permission_update(&from_owner, &target_user, &image_id, new_quota, embedded_image)
                .await;

            state.save_to_disk().await?;
            state.replicate_state().await;

            DirectoryMessage::StorePendingPermissionUpdateResponse {
                success: true,
                message: format!(
                    "Permission update queued for user '{}'. Will be applied when they come online.",
                    target_user
                ),
                update_id,
            }
        }

        DirectoryMessage::GetPendingPermissionUpdates { username } => {
            let updates = state.get_and_clear_pending_updates(&username).await;
            
            // Persist and replicate the cleared state
            if !updates.is_empty() {
                if let Err(e) = state.save_to_disk().await {
                    error!("Failed to save state after clearing pending updates: {}", e);
                }
                state.replicate_state().await;
            }
            
            DirectoryMessage::GetPendingPermissionUpdatesResponse { updates }
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