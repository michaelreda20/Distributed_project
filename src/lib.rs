use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use std::net::UdpSocket;
use anyhow::Result;

// This line makes our custom modules available
pub mod lsb;
pub mod raft;
pub mod directory_service;
pub mod p2p_protocol;

/// The address the server will listen on.
pub const ADDR: &str = "10.40.7.1:8080";

/// The data we will hide inside the image using steganography.
/// We use a HashMap to map a specific username to their allowed view count.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ImagePermissions {
    pub owner: String,
    pub quotas: HashMap<String, u32>, // username -> remaining views
}

/// This struct holds both the permissions and the raw bytes of the
/// "unified image" which will be used as the "Access Denied" image.
#[derive(Serialize, Deserialize, Debug)]
pub struct CombinedPayload {
    pub permissions: ImagePermissions,
    pub unified_image: Vec<u8>, // Raw bytes of the PNG
}

// --- RAFT MESSAGE TYPES ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        voter_id: String,
    },
    Heartbeat {
        term: u64,
        leader_id: String,
    },
    HeartbeatResponse {
        term: u64,
        follower_id: String,
        success: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerRole {
    Follower,
    Candidate,
    Leader,
}

// --- LOAD BALANCING TYPES ---

/// Server metrics for load balancing decisions
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServerMetrics {
    pub server_id: String,
    pub cpu_load: f32,              // 0.0 - 100.0 (estimated from connections)
    pub active_connections: u32,    // Current number of active clients
    pub avg_response_time_ms: u64,  // Historical average response time
    pub total_requests: u64,        // Total requests processed
    pub timestamp: SystemTime,      // When metrics were collected
}

impl ServerMetrics {
    /// Calculate load score for this server (lower is better)
    /// This score is used to determine which server should handle incoming work
    pub fn calculate_load_score(&self) -> f32 {
        // Weighted scoring formula - adjust these weights based on your priorities
        let cpu_weight = 0.4;
        let connection_weight = 0.4;
        let response_weight = 0.2;
        
        // Normalize values to 0.0-1.0 range
        let normalized_cpu = self.cpu_load / 100.0;
        let normalized_connections = (self.active_connections as f32) / 50.0; // assume max 50 connections
        let normalized_response = (self.avg_response_time_ms as f32) / 10000.0; // normalize to 10 seconds
        
        // Calculate weighted sum
        cpu_weight * normalized_cpu +
        connection_weight * normalized_connections +
        response_weight * normalized_response
    }
}

/// Messages exchanged between servers for load balancing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LoadBalancingMessage {
    /// Leader requests current metrics from a server
    MetricsRequest,
    
    /// Server responds with its current metrics
    MetricsResponse {
        metrics: ServerMetrics,
    },
    
    /// Leader forwards work to a chosen server
    ForwardWork {
        metadata: Vec<u8>,
        image_data: Vec<u8>,
    },
    
    /// Worker server sends encrypted result back to leader
    WorkResult {
        encrypted_image: Vec<u8>,
    },
}

// --- NETWORK UTILITIES ---

/// Get the local IP address by connecting to a public DNS server
/// This doesn't actually send any data, just determines which network interface would be used
pub fn get_local_ip() -> Result<String> {
    // Connect to Google's DNS (doesn't actually send data, just determines routing)
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip().to_string())
}