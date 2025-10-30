use anyhow::Result;
use cloud_p2p_project::raft::{RaftConfig, RaftNode};
use cloud_p2p_project::{RaftMessage, LogEntry};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

// Helper to start a tiny Raft listener that forwards incoming messages to the RaftNode
async fn start_listener(port: u16, node: Arc<RaftNode>) -> Result<()> {
    let bind = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind).await?;

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let node = Arc::clone(&node);
                    tokio::spawn(async move {
                        // Read message
                        let len = match stream.read_u32().await {
                            Ok(l) => l,
                            Err(_) => return,
                        };
                        let mut buf = vec![0u8; len as usize];
                        if stream.read_exact(&mut buf).await.is_err() { return; }
                        if let Ok(msg) = serde_json::from_slice::<RaftMessage>(&buf) {
                            if let Some(resp) = node.handle_raft_message(msg).await {
                                let resp_bytes = serde_json::to_vec(&resp).unwrap();
                                let _ = stream.write_u32(resp_bytes.len() as u32).await;
                                let _ = stream.write_all(&resp_bytes).await;
                                let _ = stream.flush().await;
                            }
                        }
                    });
                }
                Err(_) => continue,
            }
        }
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_basic() -> Result<()> {
    // Use ports 9001,9002,9003 for raft listeners
    let ports = [9001u16, 9002, 9003];

    let ids = ["n1".to_string(), "n2".to_string(), "n3".to_string()];

    // Prepare peer address lists (raft ports)
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect();

    // Create nodes
    let mut nodes = Vec::new();
    // Clean up any previous persisted state files for these node IDs to ensure tests are deterministic
    for id in &ids {
        let path = format!("raft_state_{}.bin", id);
        let _ = tokio::fs::remove_file(path).await;
    }
    for i in 0..3 {
        let peers: Vec<String> = addrs.iter().enumerate().filter_map(|(j,a)| if j!=i { Some(a.clone()) } else { None }).collect();
        let cfg = RaftConfig { server_id: ids[i].clone(), peers, election_timeout_min: 500, election_timeout_max: 800, heartbeat_interval: 100 };
        let node = Arc::new(RaftNode::new(cfg));
        nodes.push(node);
    }

    // Start listeners and nodes
    for (i,node) in nodes.iter().enumerate() {
        start_listener(ports[i], Arc::clone(node)).await?;
        let n = Arc::clone(node);
        n.start().await;
    }

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force first node to become leader
    nodes[0].become_leader().await;

    // Propose an entry on leader
    nodes[0].propose_entry("hello-entry".to_string()).await?;

    // Wait for replication to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check followers have the entry
    for i in 1..3 {
        let state = nodes[i].state.lock().await;
        let found = state.log.iter().any(|e| e.command == "hello-entry");
        assert!(found, "Node {} did not replicate the entry", i+1);
    }

    Ok(())
}

/// Multi-node replication and commit correctness test
#[tokio::test]
async fn replication_multi_node() -> Result<()> {
    // Use ports 9201,9202,9203 for raft listeners
    let ports = [9201u16, 9202, 9203];
    let ids = ["m1".to_string(), "m2".to_string(), "m3".to_string()];

    // Prepare peer address lists (raft ports)
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect();

    // Create nodes
    let mut nodes = Vec::new();
    // Clean up any previous persisted state files for these node IDs to ensure tests are deterministic
    for id in &ids {
        let path = format!("raft_state_{}.bin", id);
        let _ = tokio::fs::remove_file(path).await;
    }
    for i in 0..3 {
        let peers: Vec<String> = addrs.iter().enumerate().filter_map(|(j,a)| if j!=i { Some(a.clone()) } else { None }).collect();
    let cfg = RaftConfig { server_id: ids[i].clone(), peers, election_timeout_min: 800, election_timeout_max: 1200, heartbeat_interval: 100 };
        let node = Arc::new(RaftNode::new(cfg));
        nodes.push(node);
    }

    // Start listeners and nodes
    for (i,node) in nodes.iter().enumerate() {
        start_listener(ports[i], Arc::clone(node)).await?;
        Arc::clone(node).start().await;
    }

    // Give time for election to occur
    println!("[test] started listeners and nodes, sleeping briefly");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Trigger election on node 0 (should become leader)
    println!("[test] triggering election on node 0");
    nodes[0].start_election().await;

    // Wait for leadership to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Ensure node 0 is leader
    let is_leader = nodes[0].is_leader().await;
    println!("[test] node0.is_leader = {}", is_leader);
    assert!(is_leader, "Node 0 should be leader");

    // Propose entries
    println!("[test] proposing alpha");
    nodes[0].propose_entry("alpha".to_string()).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    println!("[test] proposing beta");
    nodes[0].propose_entry("beta".to_string()).await?;

    // Wait for replication
    println!("[test] waiting for replication to propagate");
    // Increase wait to allow multiple heartbeats/replication rounds
    tokio::time::sleep(Duration::from_millis(4000)).await;

    // Check followers have the entries
    for i in 1..3 {
        let state = nodes[i].state.lock().await;
        let found_alpha = state.log.iter().any(|e| e.command == "alpha");
        let found_beta = state.log.iter().any(|e| e.command == "beta");
        assert!(found_alpha && found_beta, "Follower {} did not replicate all entries", i+1);
    }

    // Check leader commit_index advanced (should be at least 1)
    let leader_state = nodes[0].state.lock().await;
    assert!(leader_state.commit_index >= 1, "Leader commit_index should have advanced");

    Ok(())
}

/// Test that log entries persist across node restarts
#[tokio::test]
async fn persistence_restart() -> Result<()> {
    // Set up unique test ports to avoid conflict with other tests
    let ports = [9101u16, 9102, 9103];
    let test_id = "persist_test";

    let node_cfg = RaftConfig {
        server_id: test_id.to_string(),
        peers: vec![], // single node for simplicity
        election_timeout_min: 500,
        election_timeout_max: 800,
        heartbeat_interval: 100,
    };

    // Create node and trigger an election (single-node cluster will win)
    let node = Arc::new(RaftNode::new(node_cfg.clone()));
    start_listener(ports[0], Arc::clone(&node)).await?;
    Arc::clone(&node).start().await;

    // Trigger an election; in a single-node setup this should make us leader and persist term/vote
    node.start_election().await;

    // Propose some test entries with synchronization
    node.propose_entry("test1".to_string()).await?;
    // Wait for persistence
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    node.propose_entry("test2".to_string()).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    node.propose_entry("test3".to_string()).await?;
    // Wait a bit longer for final persistence
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Print the state before restart
    {
        let state = node.state.lock().await;
        println!("Before restart - Log entries: {}", state.log.len());
        for (i, entry) in state.log.iter().enumerate() {
            println!("Entry {}: {}", i, entry.command);
        }
    }

    // Drop the node (simulates process exit)
    drop(node);

    // Create new node with same ID (simulates restart)
    let restarted = Arc::new(RaftNode::new(node_cfg));
    
    // Verify log was loaded
    let state = restarted.state.lock().await;
    println!("\nAfter restart - Log entries: {}", state.log.len());
    for (i, entry) in state.log.iter().enumerate() {
        println!("Entry {}: {}", i, entry.command);
    }
    
    assert_eq!(state.log.len(), 4, "Expected 4 entries (init + 3 test entries)"); // init entry + 3 test entries
    assert_eq!(state.log[1].command, "test1", "First test entry should be 'test1'");
    assert_eq!(state.log[2].command, "test2", "Second test entry should be 'test2'");
    assert_eq!(state.log[3].command, "test3", "Third test entry should be 'test3'");

    // Clean up test log file
    // Also verify term and vote were preserved
    assert_eq!(state.current_term, 1); // Should be term 1 since we became leader
    assert_eq!(state.voted_for, Some(test_id.to_string())); // Should have voted for self

    // Clean up state file
    let state_path = restarted.state_file_path();
    if state_path.exists() {
        tokio::fs::remove_file(state_path).await?;
    }

    Ok(())
}
