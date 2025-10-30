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

/// A command to be applied to the state machine (minimal implementation)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    // In a full implementation, this would contain a command (e.g., "SetLeaderID"),
    // but for this phase, we'll keep it simple for heartbeat/indexing purposes.
    pub command: String, 
}

// Persistent state that must be saved to disk
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftPersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
}

// --- RAFT MESSAGE TYPES ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64, // Used for safety check
        last_log_term: u64,  // Used for safety check
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        voter_id: String,
    },
    // Heartbeat is now replaced by AppendEntries (RPC used for both log replication and heartbeats)
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64, // Index of log entry immediately preceding new ones
        prev_log_term: u64,  // Term of prev_log_index entry
        entries: Vec<LogEntry>, // Log entries to store (empty for heartbeats)
        leader_commit: u64,      // Leader's commit index
    },
    AppendEntriesResponse {
        term: u64,
        follower_id: String,
        success: bool,
        // The follower's last log index (used as a conflict hint)
        last_log_index: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerRole {
    Follower,
    Candidate,
    Leader,
}
