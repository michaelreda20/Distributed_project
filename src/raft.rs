// use crate::{RaftMessage, ServerRole};
// use anyhow::Result;
// use log::{debug, info};
// use rand::Rng;
// use std::collections::HashSet;
// use std::sync::Arc;
// use std::time::{Duration, Instant};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::TcpStream;
// use tokio::sync::Mutex;
// use tokio::time::sleep;

// #[derive(Debug, Clone)]
// pub struct RaftConfig {
//     pub server_id: String,
//     pub peers: Vec<String>, // List of all peer addresses (excluding self)
//     pub election_timeout_min: u64, // milliseconds
//     pub election_timeout_max: u64, // milliseconds
//     pub heartbeat_interval: u64,   // milliseconds
// }

// #[derive(Debug)]
// pub struct RaftState {
//     pub current_term: u64,
//     pub voted_for: Option<String>,
//     pub role: ServerRole,
//     pub leader_id: Option<String>,
//     pub last_heartbeat: Instant,
//     pub votes_received: HashSet<String>,
// }

// impl RaftState {
//     pub fn new() -> Self {
//         Self {
//             current_term: 0,
//             voted_for: None,
//             role: ServerRole::Follower,
//             leader_id: None,
//             last_heartbeat: Instant::now(),
//             votes_received: HashSet::new(),
//         }
//     }
// }

// pub struct RaftNode {
//     pub config: RaftConfig,
//     pub state: Arc<Mutex<RaftState>>,
// }

// impl RaftNode {
//     pub fn new(config: RaftConfig) -> Self {
//         Self {
//             config,
//             state: Arc::new(Mutex::new(RaftState::new())),
//         }
//     }

//     /// Start the Raft node (election timer and heartbeat sender)
//     pub async fn start(self: Arc<Self>) {
//         let node_election = Arc::clone(&self);
//         let node_heartbeat = Arc::clone(&self);

//         // Spawn election timeout checker
//         tokio::spawn(async move {
//             node_election.run_election_timer().await;
//         });

//         // Spawn heartbeat sender (if leader)
//         tokio::spawn(async move {
//             node_heartbeat.run_heartbeat_sender().await;
//         });
//     }

//     /// Run the election timer
//     async fn run_election_timer(&self) {
//         loop {
//             let timeout = self.get_random_election_timeout();
//             sleep(timeout).await;

//             let should_start_election = {
//                 let state = self.state.lock().await;
                
//                 // Check if we're a follower and haven't heard from leader
//                 if state.role == ServerRole::Follower {
//                     let elapsed = state.last_heartbeat.elapsed();
//                     elapsed >= timeout
//                 } else {
//                     false
//                 }
//             }; // Lock is released here

//             if should_start_election {
//                 info!("[{}] Election timeout! Starting election.", self.config.server_id);
//                 self.start_election().await;
//             }
//         }
//     }

//     /// Start a new election
//     async fn start_election(&self) {
//         let current_term = {
//             let mut state = self.state.lock().await;
            
//             // Transition to candidate
//             state.role = ServerRole::Candidate;
//             state.current_term += 1;
//             state.voted_for = Some(self.config.server_id.clone());
//             state.votes_received.clear();
//             state.votes_received.insert(self.config.server_id.clone()); // Vote for self
            
//             let current_term = state.current_term;
//             info!("[{}] Starting election for term {}", self.config.server_id, current_term);
//             current_term
//         }; // Lock released here

//         // Request votes from all peers
//         let mut vote_count = 1; // We already voted for ourselves
//         let majority = (self.config.peers.len() + 1) / 2 + 1;

//         for peer_addr in &self.config.peers {
//             let vote_request = RaftMessage::RequestVote {
//                 term: current_term,
//                 candidate_id: self.config.server_id.clone(),
//                 last_log_index: 0,
//                 last_log_term: 0,
//             };

//             match self.send_raft_message(peer_addr, &vote_request).await {
//                 Ok(Some(RaftMessage::RequestVoteResponse { term, vote_granted, voter_id })) => {
//                     if term > current_term {
//                         // Found a higher term, step down
//                         let mut state = self.state.lock().await;
//                         state.current_term = term;
//                         state.role = ServerRole::Follower;
//                         state.voted_for = None;
//                         info!("[{}] Stepping down due to higher term {}", self.config.server_id, term);
//                         return;
//                     }
                    
//                     if vote_granted {
//                         vote_count += 1;
//                         info!("[{}] Received vote from {} ({}/{})", 
//                               self.config.server_id, voter_id, vote_count, majority);
                        
//                         if vote_count >= majority {
//                             self.become_leader().await;
//                             return;
//                         }
//                     }
//                 }
//                 Ok(_) => debug!("[{}] Unexpected response from {}", self.config.server_id, peer_addr),
//                 Err(e) => debug!("[{}] Failed to get vote from {}: {}", self.config.server_id, peer_addr, e),
//             }
//         }

//         // If we didn't get majority, return to follower
//         let mut state = self.state.lock().await;
//         if state.role == ServerRole::Candidate {
//             info!("[{}] Election failed, returning to follower", self.config.server_id);
//             state.role = ServerRole::Follower;
//         }
//     }

//     /// Become the leader
//     async fn become_leader(&self) {
//         let mut state = self.state.lock().await;
//         state.role = ServerRole::Leader;
//         state.leader_id = Some(self.config.server_id.clone());
//         info!("[{}] BECAME LEADER for term {}", self.config.server_id, state.current_term);
//     }

//     /// Send heartbeats periodically if we're the leader
//     async fn run_heartbeat_sender(&self) {
//         loop {
//             sleep(Duration::from_millis(self.config.heartbeat_interval)).await;

//             let (is_leader, current_term) = {
//                 let state = self.state.lock().await;
//                 (state.role == ServerRole::Leader, state.current_term)
//             }; // Lock released here

//             if !is_leader {
//                 continue;
//             }

//             let leader_id = self.config.server_id.clone();

//             // Send heartbeats to all peers
//             for peer_addr in &self.config.peers {
//                 let heartbeat = RaftMessage::Heartbeat {
//                     term: current_term,
//                     leader_id: leader_id.clone(),
//                 };

//                 let _ = self.send_raft_message(peer_addr, &heartbeat).await;
//             }
//         }
//     }

//     /// Handle incoming Raft messages
//     pub async fn handle_raft_message(&self, message: RaftMessage) -> Option<RaftMessage> {
//         match message {
//             RaftMessage::RequestVote { term, candidate_id, .. } => {
//                 let mut state = self.state.lock().await;

//                 // If term is higher, update and step down
//                 if term > state.current_term {
//                     state.current_term = term;
//                     state.voted_for = None;
//                     state.role = ServerRole::Follower;
//                 }

//                 // Grant vote if we haven't voted or voted for this candidate
//                 let vote_granted = if term == state.current_term && 
//                                      (state.voted_for.is_none() || 
//                                       state.voted_for.as_ref() == Some(&candidate_id)) {
//                     state.voted_for = Some(candidate_id.clone());
//                     state.last_heartbeat = Instant::now();
//                     info!("[{}] Granted vote to {} for term {}", 
//                           self.config.server_id, candidate_id, term);
//                     true
//                 } else {
//                     false
//                 };

//                 Some(RaftMessage::RequestVoteResponse {
//                     term: state.current_term,
//                     vote_granted,
//                     voter_id: self.config.server_id.clone(),
//                 })
//             }
//             RaftMessage::Heartbeat { term, leader_id } => {
//                 let mut state = self.state.lock().await;

//                 if term >= state.current_term {
//                     if term > state.current_term {
//                         state.current_term = term;
//                         state.voted_for = None;
//                     }
//                     state.role = ServerRole::Follower;
//                     state.leader_id = Some(leader_id.clone());
//                     state.last_heartbeat = Instant::now();
//                 }

//                 Some(RaftMessage::HeartbeatResponse {
//                     term: state.current_term,
//                     follower_id: self.config.server_id.clone(),
//                     success: term >= state.current_term,
//                 })
//             }
//             _ => None,
//         }
//     }

//     /// Send a Raft message to a peer
//     async fn send_raft_message(&self, peer_addr: &str, message: &RaftMessage) -> Result<Option<RaftMessage>> {
//         let mut stream = TcpStream::connect(peer_addr).await?;
        
//         // Serialize and send message
//         let msg_json = serde_json::to_string(message)?;
//         let msg_bytes = msg_json.as_bytes();
//         stream.write_u32(msg_bytes.len() as u32).await?;
//         stream.write_all(msg_bytes).await?;
//         stream.flush().await?;

//         // Read response
//         let response_len = stream.read_u32().await?;
//         let mut response_buf = vec![0u8; response_len as usize];
//         stream.read_exact(&mut response_buf).await?;
        
//         let response: RaftMessage = serde_json::from_slice(&response_buf)?;
//         Ok(Some(response))
//     }

//     /// Get random election timeout
//     fn get_random_election_timeout(&self) -> Duration {
//         let mut rng = rand::thread_rng();
//         let timeout_ms = rng.gen_range(
//             self.config.election_timeout_min..=self.config.election_timeout_max
//         );
//         Duration::from_millis(timeout_ms)
//     }

//     /// Check if this node is the current leader
//     pub async fn is_leader(&self) -> bool {
//         let state = self.state.lock().await;
//         state.role == ServerRole::Leader
//     }

//     /// Get the current leader's ID
//     pub async fn get_leader_id(&self) -> Option<String> {
//         let state = self.state.lock().await;
//         state.leader_id.clone()
//     }
// }







use crate::{RaftMessage, ServerRole};
use anyhow::Result;
use log::{debug, info};
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub server_id: String,
    pub peers: Vec<String>, // List of all peer addresses (excluding self)
    pub election_timeout_min: u64, // milliseconds
    pub election_timeout_max: u64, // milliseconds
    pub heartbeat_interval: u64,   // milliseconds
}

#[derive(Debug)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: ServerRole,
    pub leader_id: Option<String>,
    pub last_heartbeat: Instant,
    pub votes_received: HashSet<String>,
    pub consecutive_failed_elections: u32, // NEW: Track failed election attempts
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            role: ServerRole::Follower,
            leader_id: None,
            last_heartbeat: Instant::now(),
            votes_received: HashSet::new(),
            consecutive_failed_elections: 0, // NEW: Initialize counter
        }
    }
}

pub struct RaftNode {
    pub config: RaftConfig,
    pub state: Arc<Mutex<RaftState>>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(RaftState::new())),
        }
    }

    /// Start the Raft node (election timer and heartbeat sender)
    pub async fn start(self: Arc<Self>) {
        let node_election = Arc::clone(&self);
        let node_heartbeat = Arc::clone(&self);

        // Spawn election timeout checker
        tokio::spawn(async move {
            node_election.run_election_timer().await;
        });

        // Spawn heartbeat sender (if leader)
        tokio::spawn(async move {
            node_heartbeat.run_heartbeat_sender().await;
        });
    }

    /// Run the election timer
    async fn run_election_timer(&self) {
        loop {
            let timeout = self.get_random_election_timeout();
            sleep(timeout).await;

            let should_start_election = {
                let state = self.state.lock().await;
                
                // Check if we're a follower and haven't heard from leader
                if state.role == ServerRole::Follower {
                    let elapsed = state.last_heartbeat.elapsed();
                    elapsed >= timeout
                } else {
                    false
                }
            }; // Lock is released here

            if should_start_election {
                info!("[{}] Election timeout! Starting election.", self.config.server_id);
                self.start_election().await;
            }
        }
    }

    /// Start a new election
    async fn start_election(&self) {
        let current_term = {
            let mut state = self.state.lock().await;
            
            // Transition to candidate
            state.role = ServerRole::Candidate;
            state.current_term += 1;
            state.voted_for = Some(self.config.server_id.clone());
            state.votes_received.clear();
            state.votes_received.insert(self.config.server_id.clone()); // Vote for self
            
            let current_term = state.current_term;
            info!("[{}] Starting election for term {}", self.config.server_id, current_term);
            current_term
        }; // Lock released here

        // Request votes from all peers
        let mut vote_count = 1; // We already voted for ourselves
        let mut failed_peers = 0; // NEW: Track failed peer connections
        let majority = (self.config.peers.len() + 1) / 2 + 1;

        for peer_addr in &self.config.peers {
            let vote_request = RaftMessage::RequestVote {
                term: current_term,
                candidate_id: self.config.server_id.clone(),
                last_log_index: 0,
                last_log_term: 0,
            };

            match self.send_raft_message(peer_addr, &vote_request).await {
                Ok(Some(RaftMessage::RequestVoteResponse { term, vote_granted, voter_id })) => {
                    if term > current_term {
                        // Found a higher term, step down
                        let mut state = self.state.lock().await;
                        state.current_term = term;
                        state.role = ServerRole::Follower;
                        state.voted_for = None;
                        state.consecutive_failed_elections = 0; // Reset counter
                        info!("[{}] Stepping down due to higher term {}", self.config.server_id, term);
                        return;
                    }
                    
                    if vote_granted {
                        vote_count += 1;
                        info!("[{}] Received vote from {} ({}/{})", 
                              self.config.server_id, voter_id, vote_count, majority);
                        
                        if vote_count >= majority {
                            // Reset the failed elections counter before becoming leader
                            let mut state = self.state.lock().await;
                            state.consecutive_failed_elections = 0;
                            drop(state);
                            
                            self.become_leader().await;
                            return;
                        }
                    } else {
                        debug!("[{}] Vote denied by {} for term {}", 
                               self.config.server_id, voter_id, current_term);
                    }
                }
                Ok(_) => {
                    debug!("[{}] Unexpected response from {}", self.config.server_id, peer_addr);
                    failed_peers += 1; // NEW: Count as failed
                }
                Err(e) => {
                    debug!("[{}] Failed to get vote from {}: {}", self.config.server_id, peer_addr, e);
                    failed_peers += 1; // NEW: Count connection failures
                }
            }
        }

        // NEW: Check for single-node scenario
        // If ALL peers are unreachable and we've tried multiple times, become leader anyway
        let mut state = self.state.lock().await;
        
        if state.role == ServerRole::Candidate {
            // Check if this is a single-node situation
            let all_peers_failed = failed_peers == self.config.peers.len();
            
            if all_peers_failed {
                state.consecutive_failed_elections += 1;
                
                // After 3 consecutive failed elections where all peers are unreachable,
                // assume we're the only node and promote to leader
                if state.consecutive_failed_elections >= 3 {
                    info!("[{}] All {} peers unreachable after {} consecutive election attempts", 
                          self.config.server_id, failed_peers, state.consecutive_failed_elections);
                    info!("[{}] Assuming single-node cluster - promoting self to leader", 
                          self.config.server_id);
                    
                    // Drop the lock before calling become_leader (which needs the lock)
                    drop(state);
                    self.become_leader().await;
                    return;
                } else {
                    info!("[{}] Election failed (all {} peers unreachable, attempt {}/3), returning to follower", 
                          self.config.server_id, failed_peers, state.consecutive_failed_elections);
                }
            } else {
                // Some peers responded but we didn't get majority - normal failure
                info!("[{}] Election failed (got {}/{} votes), returning to follower", 
                      self.config.server_id, vote_count, majority);
                state.consecutive_failed_elections = 0; // Reset counter - peers are reachable
            }
            
            state.role = ServerRole::Follower;
        }
    }

    /// Become the leader
    async fn become_leader(&self) {
        let mut state = self.state.lock().await;
        state.role = ServerRole::Leader;
        state.leader_id = Some(self.config.server_id.clone());
        state.consecutive_failed_elections = 0; // Reset counter when becoming leader
        info!("[{}] BECAME LEADER for term {}", self.config.server_id, state.current_term);
    }

    /// Send heartbeats periodically if we're the leader
    async fn run_heartbeat_sender(&self) {
        loop {
            sleep(Duration::from_millis(self.config.heartbeat_interval)).await;

            let (is_leader, current_term) = {
                let state = self.state.lock().await;
                (state.role == ServerRole::Leader, state.current_term)
            }; // Lock released here

            if !is_leader {
                continue;
            }

            let leader_id = self.config.server_id.clone();

            // Send heartbeats to all peers
            for peer_addr in &self.config.peers {
                let heartbeat = RaftMessage::Heartbeat {
                    term: current_term,
                    leader_id: leader_id.clone(),
                };

                let _ = self.send_raft_message(peer_addr, &heartbeat).await;
            }
        }
    }

    /// Handle incoming Raft messages
    pub async fn handle_raft_message(&self, message: RaftMessage) -> Option<RaftMessage> {
        match message {
            RaftMessage::RequestVote { term, candidate_id, .. } => {
                let mut state = self.state.lock().await;

                // If term is higher, update and step down
                if term > state.current_term {
                    state.current_term = term;
                    state.voted_for = None;
                    state.role = ServerRole::Follower;
                    state.consecutive_failed_elections = 0;
                    info!("[{}] Received vote request with higher term {} - stepping down to follower", 
                          self.config.server_id, term);
                }
                
                // If we're a leader with same term, step down and participate in new election
                // This handles the case where a peer joins and starts an election
                if term == state.current_term && state.role == ServerRole::Leader {
                    info!("[{}] Received vote request from {} for same term {} - stepping down to allow re-election", 
                          self.config.server_id, candidate_id, term);
                    state.role = ServerRole::Follower;
                    state.voted_for = None;
                    state.consecutive_failed_elections = 0;
                }

                // Grant vote if we haven't voted or voted for this candidate
                let vote_granted = if term == state.current_term && 
                                     (state.voted_for.is_none() || 
                                      state.voted_for.as_ref() == Some(&candidate_id)) {
                    state.voted_for = Some(candidate_id.clone());
                    state.last_heartbeat = Instant::now();
                    info!("[{}] Granted vote to {} for term {}", 
                          self.config.server_id, candidate_id, term);
                    true
                } else {
                    info!("[{}] Denied vote to {} for term {} (already voted for {:?})", 
                          self.config.server_id, candidate_id, term, state.voted_for);
                    false
                };

                Some(RaftMessage::RequestVoteResponse {
                    term: state.current_term,
                    vote_granted,
                    voter_id: self.config.server_id.clone(),
                })
            }
            RaftMessage::Heartbeat { term, leader_id } => {
                let mut state = self.state.lock().await;

                if term >= state.current_term {
                    if term > state.current_term {
                        state.current_term = term;
                        state.voted_for = None;
                    }
                    state.role = ServerRole::Follower;
                    state.leader_id = Some(leader_id.clone());
                    state.last_heartbeat = Instant::now();
                    state.consecutive_failed_elections = 0; // Reset counter when receiving heartbeat
                }

                Some(RaftMessage::HeartbeatResponse {
                    term: state.current_term,
                    follower_id: self.config.server_id.clone(),
                    success: term >= state.current_term,
                })
            }
            _ => None,
        }
    }

    /// Send a Raft message to a peer
    async fn send_raft_message(&self, peer_addr: &str, message: &RaftMessage) -> Result<Option<RaftMessage>> {
        let mut stream = TcpStream::connect(peer_addr).await?;
        
        // Serialize and send message
        let msg_json = serde_json::to_string(message)?;
        let msg_bytes = msg_json.as_bytes();
        stream.write_u32(msg_bytes.len() as u32).await?;
        stream.write_all(msg_bytes).await?;
        stream.flush().await?;

        // Read response
        let response_len = stream.read_u32().await?;
        let mut response_buf = vec![0u8; response_len as usize];
        stream.read_exact(&mut response_buf).await?;
        
        let response: RaftMessage = serde_json::from_slice(&response_buf)?;
        Ok(Some(response))
    }

    /// Get random election timeout
    fn get_random_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let timeout_ms = rng.gen_range(
            self.config.election_timeout_min..=self.config.election_timeout_max
        );
        Duration::from_millis(timeout_ms)
    }

    /// Check if this node is the current leader
    pub async fn is_leader(&self) -> bool {
        let state = self.state.lock().await;
        state.role == ServerRole::Leader
    }

    /// Get the current leader's ID
    pub async fn get_leader_id(&self) -> Option<String> {
        let state = self.state.lock().await;
        state.leader_id.clone()
    }
}