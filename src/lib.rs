use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// This line makes our custom lsb.rs file available as a module.
pub mod lsb;
pub mod raft;

/// The address the server will listen on.
pub const ADDR: &str = "127.0.0.1:8080";

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