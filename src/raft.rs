use crate::{RaftMessage, ServerRole, LogEntry};
use anyhow::Result;
use log::{debug, info, error};
use rand::Rng;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::fs;
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
    
    // --- Log Replication Fields ---
    pub log: Vec<LogEntry>, // The replicated log
    pub commit_index: u64, // Index of highest log entry known to be committed
    pub last_applied: u64, // Index of highest log entry applied to state machine
    
    // --- Leader-specific Fields (Volatile) ---
    pub next_index: HashMap<String, u64>, // For each follower, index of the next log entry to send
    pub match_index: HashMap<String, u64>, // For each follower, index of the highest log entry replicated
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
            
            // Log fields start at 0 (or 1 in a full implementation, we'll use 0-indexing here)
            log: vec![LogEntry { term: 0, command: "init".to_string() }], // A dummy entry to start
            commit_index: 0,
            last_applied: 0,
            
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }
    
    pub fn last_log_index(&self) -> u64 {
        self.log.len() as u64 - 1
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }
}

pub struct RaftNode {
    pub config: RaftConfig,
    pub state: Arc<Mutex<RaftState>>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        let mut node = Self {
            config,
            state: Arc::new(Mutex::new(RaftState::new())),
        };

        // Try to load persisted state from disk
        if let Some(persistent_state) = node.load_state_from_disk() {
            if let Ok(mut state) = node.state.try_lock() {
                state.current_term = persistent_state.current_term;
                state.voted_for = persistent_state.voted_for;
                state.log = persistent_state.log;
                state.commit_index = state.last_log_index();
                state.last_applied = state.commit_index;
                info!(
                    "[{}] Loaded persisted state: term={}, voted_for={:?}, {} log entries", 
                    node.config.server_id,
                    state.current_term,
                    state.voted_for,
                    state.log.len()
                );
            }
        }

        node
    }

    /// Return path to the state file for this node
    pub fn state_file_path(&self) -> PathBuf {
        let fname = format!("raft_state_{}.bin", self.config.server_id);
        PathBuf::from(fname)
    }

    /// Persist the current state to disk (overwrites existing file). Uses bincode serialization.
    async fn persist_state_to_disk(&self) {
        let state = self.state.lock().await;
        let persistent_state = crate::RaftPersistentState {
            current_term: state.current_term,
            voted_for: state.voted_for.clone(),
            log: state.log.clone(),
        };

        let path = self.state_file_path();
        match bincode::serialize(&persistent_state) {
            Ok(bytes) => {
                if let Err(e) = tokio::fs::write(&path, bytes).await {
                    error!("[{}] Failed to write state to disk {}: {}", self.config.server_id, path.display(), e);
                }
            }
            Err(e) => error!("[{}] Failed to serialize state for disk write: {}", self.config.server_id, e),
        }
    }

    /// Load persisted state from disk, if it exists
    fn load_state_from_disk(&self) -> Option<crate::RaftPersistentState> {
        let path = self.state_file_path();
        match fs::read(&path) {
            Ok(bytes) => {
                match bincode::deserialize::<crate::RaftPersistentState>(&bytes) {
                    Ok(state) => Some(state),
                    Err(e) => {
                        error!("[{}] Failed to deserialize state from {}: {}", self.config.server_id, path.display(), e);
                        None
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                error!("[{}] Failed to read state from {}: {}", self.config.server_id, path.display(), e);
                None
            }
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
    pub async fn start_election(&self) {
        let (current_term, last_log_index, last_log_term) = {
            println!("[raft] start_election triggered on {}", self.config.server_id);
            let mut state = self.state.lock().await;
            
            // 1. Transition to candidate
            state.role = ServerRole::Candidate;
            state.current_term += 1;
            state.voted_for = Some(self.config.server_id.clone());
            state.votes_received.clear();
            state.votes_received.insert(self.config.server_id.clone()); // Vote for self
            
            info!("[{}] Starting election for term {}", self.config.server_id, state.current_term);
            
            (state.current_term, state.last_log_index(), state.last_log_term())
        };

        // Persist state changes before requesting votes
    println!("[raft] start_election: persisted state for term {}", current_term);
    self.persist_state_to_disk().await;

    // 2. Request votes from all peers
        let mut votes_granted = 1; // We already voted for ourselves
        let majority = (self.config.peers.len() + 1) / 2 + 1;

        // FIX E0308/E0282: Reverting to the simpler sequential loop structure 
        // to avoid complex Arc ownership and lifetime issues in futures.
        for peer_addr in &self.config.peers {
            let vote_request = RaftMessage::RequestVote {
                term: current_term,
                candidate_id: self.config.server_id.clone(),
                last_log_index,
                last_log_term,
            };

            match self.send_raft_message(peer_addr, &vote_request).await {
                Ok(Some(RaftMessage::RequestVoteResponse { term, vote_granted: granted, voter_id })) => {
                    if term > current_term {
                        // Found a higher term, step down
                        let mut state = self.state.lock().await;
                        state.current_term = term;
                        state.role = ServerRole::Follower;
                        state.voted_for = None;
                        info!("[{}] Stepping down due to higher term {}", self.config.server_id, term);
                        return;
                    }
                    
                    if granted {
                        votes_granted += 1;
                        println!("[raft] {} received vote from {} ({}/{})", self.config.server_id, voter_id, votes_granted, majority);

                        // If we reached majority mid-loop, become leader
                        if votes_granted >= majority {
                            println!("[raft] {} reached majority, becoming leader", self.config.server_id);
                            self.become_leader().await;
                            return;
                        }
                    }
                }
                Ok(_) => debug!("[{}] Unexpected response from {}", self.config.server_id, peer_addr),
                Err(e) => debug!("[{}] Failed to get vote from {}: {}", self.config.server_id, peer_addr, e),
            }
        }

        // Final check: If we reached majority after polling peers, become leader
        if votes_granted >= majority {
            self.become_leader().await;
            return;
        }

        // Otherwise, return to follower
        let mut state = self.state.lock().await;
        if state.role == ServerRole::Candidate {
            info!("[{}] Election failed, returning to follower", self.config.server_id);
            state.role = ServerRole::Follower;
        }
    }

    /// Become the leader (internal helper that mutates state only)
    /// NOTE: This function does not await and must be called while holding the lock.
    fn become_leader_internal(&self, state: &mut tokio::sync::MutexGuard<'_, RaftState>) {
        state.role = ServerRole::Leader;
        state.leader_id = Some(self.config.server_id.clone());
        
        // Get the last index before using the mutable state in the loop.
        let last_index = state.last_log_index();

        // Initialize leader volatile state
        for peer_addr in &self.config.peers {
            // Next index for all peers is one greater than leader's last log index
            state.next_index.insert(peer_addr.clone(), last_index + 1); 
            state.match_index.insert(peer_addr.clone(), 0);
        }
        // Leader's own match_index should reflect its last log index
        state.match_index.insert(self.config.server_id.clone(), last_index);
        info!("[{}] BECAME LEADER for term {}", self.config.server_id, state.current_term);
        // Do not send AppendEntries while holding the lock; caller should do that after releasing the lock.
    }

    // Public wrapper function: mutate state while holding lock, then release lock before awaiting
    pub async fn become_leader(&self) {
        // Capture the term and commit_index to use after we drop the lock
        let (current_term, leader_commit) = {
            let mut state = self.state.lock().await;
            self.become_leader_internal(&mut state);
            (state.current_term, state.commit_index)
        }; // lock dropped here

        // Persist state changes before sending heartbeats
        self.persist_state_to_disk().await;

        // Now safe to await and send initial heartbeats
        self.send_append_entries(current_term, leader_commit).await;
    }

    // Helper to send heartbeats/AppendEntries
    async fn send_append_entries(&self, current_term: u64, leader_commit: u64) {
        use anyhow::anyhow;
        const MAX_ENTRIES_PER_RPC: usize = 8; // chunk large backlogs into smaller RPCs
        let leader_id = self.config.server_id.clone();

        // Snapshot of leader's log index and follower next_index values for debugging
        let (leader_last_idx, next_idx_snapshot) = {
            let state = self.state.lock().await;
            let leader_idx = state.last_log_index();
            let mut map = std::collections::HashMap::new();
            for peer in &self.config.peers {
                let ni = *state.next_index.get(peer).unwrap_or(&(leader_idx + 1));
                map.insert(peer.clone(), ni);
            }
            (leader_idx, map)
        };

        println!("[raft][{}] send_append_entries: leader_last_idx={} next_index_snapshot={:?}", self.config.server_id, leader_last_idx, next_idx_snapshot);

        // Build per-peer AppendEntries messages first
        let mut tasks = Vec::new();
        for peer_addr in &self.config.peers {
            // Determine which entries (if any) need to be sent to this peer
            let (prev_log_index, prev_log_term, entries_to_send) = {
                let state = self.state.lock().await;
                let last_index = state.last_log_index();
                // If follower has no next_index set, default it to leader's last_index + 1
                let next_index = *state.next_index.get(peer_addr).unwrap_or(&(last_index + 1));
                let prev_index = if next_index > 0 { next_index - 1 } else { 0 };
                let prev_term = state.log.get(prev_index as usize).map(|e| e.term).unwrap_or(0);

                // Collect entries from next_index..end
                let mut entries = Vec::new();
                if next_index <= last_index {
                    let start = next_index as usize;
                    let end = (last_index as usize) + 1; // inclusive end-exclusive
                    entries.extend_from_slice(&state.log[start..end]);
                }

                (prev_index, prev_term, entries)
            };

                // Chunk entries to avoid sending extremely large AppendEntries payloads
                let mut entries_chunk = entries_to_send.clone();
                if entries_chunk.len() > MAX_ENTRIES_PER_RPC {
                    entries_chunk.truncate(MAX_ENTRIES_PER_RPC);
                }

                let ae = RaftMessage::AppendEntries {
                    term: current_term,
                    leader_id: leader_id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries: entries_chunk.clone(),
                    leader_commit,
                };
            let peer = peer_addr.clone();
            let prev_idx = prev_log_index;
            let entries_len = entries_chunk.len();

                // Leader-side debug: what we're about to send
                println!("[raft][{}] Sending AppendEntries -> {} prev_idx={} entries_len={}", self.config.server_id, peer, prev_idx, entries_len);

            // Spawn a task per peer with a timeout so a slow follower doesn't block leader
            let handle = tokio::spawn(async move {
                // Increase RPC timeout to account for follower disk persistence delays
                let timeout_dur = Duration::from_millis(5000);
                // Perform the TCP RPC inside the timeout
                match tokio::time::timeout(timeout_dur, async {
                    // Connect and send the serialized AppendEntries
                    let mut stream = TcpStream::connect(&peer).await?;
                    let msg_json = serde_json::to_string(&ae)?;
                    let msg_bytes = msg_json.as_bytes();
                    stream.write_u32(msg_bytes.len() as u32).await?;
                    stream.write_all(msg_bytes).await?;
                    stream.flush().await?;

                    // Read response
                    let response_len = stream.read_u32().await?;
                    let mut response_buf = vec![0u8; response_len as usize];
                    stream.read_exact(&mut response_buf).await?;
                    let response: RaftMessage = serde_json::from_slice(&response_buf)?;
                    Ok::<(RaftMessage, u64, usize), anyhow::Error>((response, prev_idx, entries_len))
                }).await {
                    Ok(Ok((resp, pidx, elen))) => Ok::<(String, RaftMessage, u64, usize), anyhow::Error>((peer, resp, pidx, elen)),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(anyhow!("timeout")),
                }
            });

            tasks.push(handle);
        }

        // Collect and process results as they complete
        for th in tasks {
            match th.await {
                Ok(Ok((peer, RaftMessage::AppendEntriesResponse { term: resp_term, follower_id: _f, success, last_log_index }, prev_idx, entries_len))) => {
                    // Leader-side: log response arrival
                    println!("[raft][{}] AppendEntriesResponse from {} (success={}, term={}, follower_last_index={})", self.config.server_id, peer, success, resp_term, last_log_index);

                    // Process response under lock
                    if resp_term > current_term {
                        let mut state = self.state.lock().await;
                        state.current_term = resp_term;
                        state.role = ServerRole::Follower;
                        state.voted_for = None;
                        state.leader_id = None;
                        info!("[{}] Stepping down due to higher term {} in AppendEntriesResponse", self.config.server_id, resp_term);
                        return;
                    }

                    if success {
                        let mut state = self.state.lock().await;
                        // Compute a safe new match index. Prefer follower-provided last_log_index as authoritative
                        let inferred_match = prev_idx + (entries_len as u64);
                        let new_match = std::cmp::max(inferred_match, last_log_index);
                        let cur_match = state.match_index.get(&peer).copied().unwrap_or(0);
                        if new_match > cur_match {
                            info!("[{}] Updating match_index for {}: {} -> {}", self.config.server_id, peer, cur_match, new_match);
                            state.match_index.insert(peer.clone(), new_match);
                        } else {
                            debug!("[{}] Not updating match_index for {} (cur={} new={})", self.config.server_id, peer, cur_match, new_match);
                        }

                        // Update next_index monotonically
                        let desired_next = new_match.saturating_add(1);
                        let cur_next = state.next_index.get(&peer).copied().unwrap_or(1);
                        if desired_next > cur_next {
                            info!("[{}] Updating next_index for {}: {} -> {}", self.config.server_id, peer, cur_next, desired_next);
                            state.next_index.insert(peer.clone(), desired_next);
                        } else {
                            debug!("[{}] Not updating next_index for {} (cur={} desired={})", self.config.server_id, peer, cur_next, desired_next);
                        }

                        // Try to advance commit_index if a majority have replicated an index
                        let last_index = state.last_log_index();
                        let cluster_size = self.config.peers.len() + 1;
                        let majority = cluster_size / 2 + 1;

                        let leader_last_idx = state.last_log_index();
                        let _ = state.match_index.entry(self.config.server_id.clone()).or_insert(leader_last_idx);

                        for n in (state.commit_index + 1)..=last_index {
                            let mut count = 0usize;
                            for (_peer, &midx) in state.match_index.iter() {
                                if midx >= n { count += 1; }
                            }
                            if count >= majority {
                                if state.log.get(n as usize).map(|e| e.term).unwrap_or(0) == state.current_term {
                                    state.commit_index = n;
                                    info!("[{}] Leader advanced commit_index to {}", self.config.server_id, state.commit_index);
                                }
                            }
                        }
                    } else {
                        // Failure: use follower-provided last_log_index as a conflict hint to adjust next_index,
                        // but only reduce it — do not increase or overwrite a larger value.
                        let mut state = self.state.lock().await;
                        let suggested = last_log_index.saturating_add(1);
                        let cur_next = state.next_index.get(&peer).copied().unwrap_or(1);
                        if suggested < cur_next {
                            let new_next = if suggested == 0 { 1 } else { suggested };
                            info!("[{}] Decreasing next_index[{}] from {} -> {} based on follower hint", self.config.server_id, peer, cur_next, new_next);
                            state.next_index.insert(peer.clone(), new_next);
                        } else {
                            debug!("[{}] Ignoring suggested next_index {} for {} because current is {}", self.config.server_id, suggested, peer, cur_next);
                        }
                    }
                }
                Ok(Ok((_peer, _other, _pidx, _elen))) => {
                    // unexpected message type; ignore
                }
                Ok(Err(e)) => {
                    // Print errors/timeouts from individual RPC tasks so they are visible in tests
                    println!("[{}] AppendEntries task failed: {}", self.config.server_id, e);
                }
                Err(e) => {
                    println!("[{}] Join error for AppendEntries task: {}", self.config.server_id, e);
                }
            }
        }
    }

    /// Send AppendEntries periodically (Heartbeat sender)
    async fn run_heartbeat_sender(&self) {
        loop {
            sleep(Duration::from_millis(self.config.heartbeat_interval)).await;

            let (is_leader, current_term, leader_commit) = {
                let state = self.state.lock().await;
                (state.role == ServerRole::Leader, state.current_term, state.commit_index)
            }; // Lock released here

            if !is_leader {
                continue;
            }

            self.send_append_entries(current_term, leader_commit).await;
        }
    }

    /// Handle incoming Raft messages
    pub async fn handle_raft_message(&self, message: RaftMessage) -> Option<RaftMessage> {
        let mut state = self.state.lock().await;
        
        // --- All Server Rules (Handle RPC Term) ---
        match &message {
            RaftMessage::RequestVote { term, .. } |
            RaftMessage::AppendEntries { term, .. } => {
                // Rule 1: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
                if *term > state.current_term {
                    info!("[{}] Received message with higher term {}. Stepping down.", self.config.server_id, term);
                    state.current_term = *term;
                    state.role = ServerRole::Follower;
                    state.voted_for = None;
                    state.leader_id = None; 
                }
            }
            _ => {}
        }
        
        // --- Main RPC Logic ---
        match message {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                let current_term = state.current_term;
                let mut vote_granted = false;

                // 1. Reply false if term < current_term (handled by the all-server rule above)
                if term < current_term {
                    // vote_granted = false
                } 
                // 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date
                else if state.voted_for.is_none() || state.voted_for.as_ref() == Some(&candidate_id) {
                    
                    // Log comparison check (Raft Rule):
                    let my_last_log_term = state.last_log_term();
                    let my_last_log_index = state.last_log_index();

                    let log_is_upto_date = last_log_term > my_last_log_term || 
                                           (last_log_term == my_last_log_term && 
                                            last_log_index >= my_last_log_index);

                    if log_is_upto_date {
                        state.voted_for = Some(candidate_id.clone());
                        state.last_heartbeat = Instant::now();
                        info!("[{}] Granted vote to {} for term {}", 
                              self.config.server_id, candidate_id, term);
                        vote_granted = true;

                        // Drop the lock before persisting
                        drop(state);
                        self.persist_state_to_disk().await;
                        state = self.state.lock().await;
                    } else {
                        info!("[{}] Denied vote to {}. Log not up-to-date (C: T={}, I={}, Me: T={}, I={})", 
                              self.config.server_id, candidate_id, last_log_term, last_log_index, my_last_log_term, my_last_log_index);
                    }
                }

                Some(RaftMessage::RequestVoteResponse {
                    term: state.current_term,
                    vote_granted,
                    voter_id: self.config.server_id.clone(),
                })
            }
            
            // AppendEntries RPC (used for Heartbeats and Log Replication)
            RaftMessage::AppendEntries { 
                term, 
                leader_id, 
                prev_log_index, 
                prev_log_term, 
                entries, 
                leader_commit 
            } => {
                // We'll build the response and optionally persist the log after releasing the lock.
                let (response, log_to_persist) = {
                    let current_term = state.current_term;
                    let mut success = false;
                    let mut log_changed = false;
                    
                    // 1. Reply false if term < current_term (handled by all-server rule)
                    if term < current_term {
                        // success = false
                    } else {
                        // On valid AppendEntries: reset timer, become follower
                        state.role = ServerRole::Follower;
                        state.leader_id = Some(leader_id.clone());
                        state.last_heartbeat = Instant::now();
                        
                        // 2. Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term
                        let prev_log_exists = (prev_log_index as usize) < state.log.len() && 
                                              state.log.get(prev_log_index as usize).map(|e| e.term).unwrap_or(0) == prev_log_term;
                        println!("[raft][{}] AppendEntries: prev_idx={} prev_term={} prev_exists={} log_len={} entries_len={}", self.config.server_id, prev_log_index, prev_log_term, prev_log_exists, state.log.len(), entries.len());
                        
                        if !prev_log_exists {
                            // success = false
                            error!("[{}] AppendEntries failed: log mismatch at index {} (Term {} != {}). Log Len: {}", 
                                  self.config.server_id, prev_log_index, 
                                  state.log.get(prev_log_index as usize).map(|e| e.term).unwrap_or(0), prev_log_term, state.log.len());
                        } else {
                            success = true; // Log matches!
                            
                            // 3. If an existing entry conflicts, delete it and all that follow.
                            // 4. Append any new entries not already in the log.
                            let mut last_new_index = prev_log_index;

                            if !entries.is_empty() {
                                log_changed = true;
                                // Perform log replication: delete conflicts and append new entries
                                let mut insert_idx = (prev_log_index as usize) + 1;
                                for entry in entries.iter() {
                                    if insert_idx < state.log.len() {
                                        // Existing entry present
                                        if state.log[insert_idx].term != entry.term {
                                            // Conflict: truncate and append
                                            println!("[raft][{}] conflict at idx {}: existing_term={} new_term={}", self.config.server_id, insert_idx, state.log[insert_idx].term, entry.term);
                                            state.log.truncate(insert_idx);
                                            state.log.push(entry.clone());
                                        } else {
                                            // Entry matches; nothing to do
                                        }
                                    } else {
                                        // Append new entry
                                        println!("[raft][{}] appending new entry at idx {} term={}", self.config.server_id, insert_idx, entry.term);
                                        state.log.push(entry.clone());
                                    }
                                    insert_idx += 1;
                                }

                                last_new_index = (insert_idx as u64).saturating_sub(1);
                                info!("[{}] Appended {} entries, last_new_index={}", self.config.server_id, entries.len(), last_new_index);
                            } else {
                                // This is a heartbeat
                                debug!("[{}] Received Heartbeat from {} (Term {})", self.config.server_id, leader_id, term);
                            }

                            // 5. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)
                            if leader_commit > state.commit_index {
                                let new_commit = std::cmp::min(leader_commit, last_new_index);
                                state.commit_index = new_commit;
                                info!("[{}] Commit index updated to {}", self.config.server_id, state.commit_index);
                            }
                        }
                    }

                    let resp = RaftMessage::AppendEntriesResponse {
                        term: state.current_term,
                        follower_id: self.config.server_id.clone(),
                        success,
                        last_log_index: state.last_log_index(),
                    };

                    let log_clone = if log_changed { Some(state.log.clone()) } else { None };
                    (resp, log_clone)
                };

                // Persist state if the log changed (best-effort async write)
                if log_to_persist.is_some() {
                    // State has changed, persist it (best-effort)
                    self.persist_state_to_disk().await;
                }

                Some(response)
            }
            
            // Responses are handled in the sender functions, return None here
            RaftMessage::RequestVoteResponse { .. } |
            RaftMessage::AppendEntriesResponse { .. } => { 
                None
            }
        }
    }

    /// Send a Raft message to a peer
    async fn send_raft_message(&self, peer_addr: &str, message: &RaftMessage) -> Result<Option<RaftMessage>> {
        let mut stream = match TcpStream::connect(peer_addr).await {
            Ok(s) => s,
            Err(e) => {
                // Log connection failure and return
                debug!("[{}] Failed to connect to {} (Raft): {}", self.config.server_id, peer_addr, e);
                return Err(e.into());
            }
        };
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

    /// Propose a new log entry (called by leader). Appends the entry to leader's log and
    /// immediately attempts to replicate it to followers by sending AppendEntries.
    /// Returns Err if this node is not the leader.
    pub async fn propose_entry(&self, command: String) -> anyhow::Result<()> {
        // Append the entry while holding the lock, but don't await while holding it.
        let (term, leader_commit) = {
            let mut state = self.state.lock().await;
            if state.role != ServerRole::Leader {
                anyhow::bail!("Not the leader");
            }

            let entry = crate::LogEntry { term: state.current_term, command };
            state.log.push(entry);

            // Update leader's own match_index to last_log_index
            let last = state.last_log_index();
            state.match_index.insert(self.config.server_id.clone(), last);

            // Ensure next_index for followers exists (do not increase existing values)
            for peer in &self.config.peers {
                state.next_index.entry(peer.clone()).or_insert(last + 1);
            }

            (state.current_term, state.commit_index)
        };

        println!("[raft] propose_entry: appended entry term={}", term);

        // Persist leader state to disk (best-effort) before replication
        println!("[raft] propose_entry: persisting state to disk");
        self.persist_state_to_disk().await;
        println!("[raft] propose_entry: persisted state, now sending append entries");

        // Now send AppendEntries to followers to replicate the new entry
        self.send_append_entries(term, leader_commit).await;
        Ok(())
    }
}