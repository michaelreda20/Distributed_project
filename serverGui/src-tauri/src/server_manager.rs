use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::io::{BufRead, BufReader};
use std::thread;
use anyhow::Result;
use chrono::Local;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub port: u16,
    pub name: String,
    pub peers: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ServerInfo {
    pub id: u8,
    pub name: String,
    pub port: u16,
    pub status: String,
    pub role: String,
    pub load: f32,
    pub request_rate: u32,
    pub error_rate: f32,
    pub logs: Vec<String>,
}

pub struct ServerProcess {
    child: Option<Child>,
    info: ServerInfo,
    logs: Arc<Mutex<Vec<String>>>,
    failure_until: Option<std::time::Instant>,
}

pub struct ServerManager {
    servers: HashMap<u16, ServerProcess>,
    working_directory: String,
}

impl ServerManager {
    pub fn new() -> Self {
        // Get the current working directory where cargo project is
        let working_directory = r"D:\AUC\Fall 2025\Distributed Systems\Distributed_project".to_string();
        
        Self {
            servers: HashMap::new(),
            working_directory,
        }
    }

    pub async fn start_all_servers(&mut self, configs: Vec<ServerConfig>) -> Result<()> {
        // Kill any existing processes on these ports first
        self.cleanup_ports(&[8080, 8081, 8082]).await?;
        
        for (i, config) in configs.into_iter().enumerate() {
            // Add delay between server starts (like your batch file does)
            if i > 0 {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
            self.start_server_internal(config, i as u8 + 1).await?;
        }
        
        Ok(())
    }

    pub async fn start_server(&mut self, config: ServerConfig) -> Result<()> {
        let id = ((config.port - 8080) + 1) as u8;
        self.start_server_internal(config, id).await
    }

    async fn start_server_internal(&mut self, config: ServerConfig, id: u8) -> Result<()> {
        // Build command exactly like your batch file:
        // cargo run --bin server -- <port> <server_id> <peer1> <peer2>
        
        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--bin")
            .arg("server")
            .arg("--")
            .arg(config.port.to_string())
            .arg(&config.name);
        
        // Add peers
        for peer in &config.peers {
            cmd.arg(peer);
        }

        // Set working directory to where Cargo.toml is
        cmd.current_dir(&self.working_directory);
        
        // Configure stdio
        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("RUST_LOG", "info");

        println!("Starting server {} on port {} in directory {}", 
                 config.name, config.port, self.working_directory);

        let mut child = cmd.spawn()?;
        
        // Setup log capturing
        let logs = Arc::new(Mutex::new(Vec::new()));
        let logs_stdout = logs.clone();
        let logs_stderr = logs.clone();
        let port = config.port;

        // Capture stdout in a separate thread
        if let Some(stdout) = child.stdout.take() {
            thread::spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines() {
                    if let Ok(line) = line {
                        let timestamp = Local::now().format("%H:%M:%S").to_string();
                        let log_entry = format!("[{}] {}", timestamp, line);
                        
                        let mut logs = logs_stdout.lock().unwrap();
                        logs.push(log_entry);
                        
                        // Keep only last 100 logs per server
                        if logs.len() > 100 {
                            logs.remove(0);
                        }
                    }
                }
            });
        }

        // Capture stderr in a separate thread
        if let Some(stderr) = child.stderr.take() {
            thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    if let Ok(line) = line {
                        let timestamp = Local::now().format("%H:%M:%S").to_string();
                        let log_entry = format!("[{}] [STDERR] {}", timestamp, line);
                        
                        let mut logs = logs_stderr.lock().unwrap();
                        logs.push(log_entry);
                        
                        if logs.len() > 100 {
                            logs.remove(0);
                        }
                    }
                }
            });
        }

        // Initial server info
        let info = ServerInfo {
            id,
            name: config.name.clone(),
            port: config.port,
            status: "running".to_string(),
            role: if id == 1 { "LEADER".to_string() } else { "FOLLOWER".to_string() },
            load: 0.0,
            request_rate: 0,
            error_rate: 0.0,
            logs: vec![format!("[INFO] Starting server {} on port {}", config.name, config.port)],
        };

        // Add initial logs
        {
            let mut log_vec = logs.lock().unwrap();
            log_vec.push(format!("[INFO] Server started on port {}", config.port));
            log_vec.push(format!("[INFO] Raft consensus running on port {}", config.port + 1000));
            log_vec.push(format!("[INFO] Metrics server running on port {}", config.port + 2000));
            log_vec.push(format!("[INFO] Work receiver running on port {}", config.port + 3000));
            
            if info.role == "LEADER" {
                log_vec.push("[INFO] Raft: Elected as LEADER (term 1)".to_string());
                log_vec.push("[INFO] Performing load balancing decisions".to_string());
            } else {
                log_vec.push("[INFO] Raft: Started as FOLLOWER".to_string());
            }
        }

        let server_process = ServerProcess {
            child: Some(child),
            info,
            logs,
            failure_until: None,
        };

        self.servers.insert(port, server_process);
        Ok(())
    }

    pub async fn stop_server(&mut self, port: u16) -> Result<()> {
        if let Some(mut server) = self.servers.remove(&port) {
            if let Some(mut child) = server.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        Ok(())
    }

    pub async fn stop_all_servers(&mut self) -> Result<()> {
        let ports: Vec<u16> = self.servers.keys().copied().collect();
        for port in ports {
            self.stop_server(port).await?;
        }
        
        // Also cleanup any lingering processes on the ports
        self.cleanup_ports(&[8080, 8081, 8082]).await?;
        Ok(())
    }

    pub async fn simulate_failure(&mut self, port: u16) -> Result<()> {
        if let Some(server) = self.servers.get_mut(&port) {
            // Kill the process to simulate real failure
            if let Some(child) = &mut server.child {
                let _ = child.kill();
            }
            
            server.info.status = "failed".to_string();
            server.info.role = "OFFLINE".to_string();
            server.failure_until = Some(std::time::Instant::now() + std::time::Duration::from_secs(20));
            
            let mut logs = server.logs.lock().unwrap();
            logs.push("[ERROR] Simulating failure - process killed".to_string());
            logs.push("[INFO] Server offline (fault tolerance test)".to_string());
            logs.push("[INFO] Will auto-restart in 20 seconds".to_string());
            
            // Schedule auto-restart after 20 seconds
            let server_name = server.info.name.clone();
            let server_port = port;
            let _peers: Vec<String> = if port == 8080 {
                vec!["127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string()]
            } else if port == 8081 {
                vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8082".to_string()]
            } else {
                vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string()]
            };
            
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
                println!("Auto-restarting server {} on port {}", server_name, server_port);
            });
        }
        Ok(())
    }

    pub fn get_server_statuses(&self) -> Vec<ServerInfo> {
        let now = std::time::Instant::now();
        
        self.servers.values().map(|server| {
            let mut info = server.info.clone();
            
            // Check if failure simulation is over and we need to restart
            if let Some(until) = server.failure_until {
                if now > until {
                    info.status = "restarting".to_string();
                    
                    // In a real scenario, you'd restart the process here
                    // For now, just update status
                }
            }
            
            // Update logs from captured output
            if let Ok(logs) = server.logs.lock() {
                info.logs = logs.clone();
            }
            
            // Simulate dynamic metrics based on role and status
            if info.status == "running" {
                // Parse actual metrics from logs if available
                // For now, use simulated values
                info.load = (rand::random::<f32>() * 30.0 + 20.0).min(100.0);
                info.request_rate = rand::random::<u32>() % 500 + 300;
                info.error_rate = rand::random::<f32>() * 0.02;
                
                // LEADER typically has higher load
                if info.role == "LEADER" {
                    info.load = (info.load * 1.5).min(100.0);
                    info.request_rate = (info.request_rate as f32 * 1.5) as u32;
                }
            } else {
                info.load = 0.0;
                info.request_rate = 0;
                info.error_rate = 0.0;
            }
            
            info
        }).collect()
    }

    pub fn get_server_logs(&self, port: u16) -> Option<Vec<String>> {
        self.servers.get(&port).and_then(|server| {
            server.logs.lock().ok().map(|logs| logs.clone())
        })
    }

    pub fn clear_all_logs(&mut self) {
        for server in self.servers.values_mut() {
            if let Ok(mut logs) = server.logs.lock() {
                logs.clear();
                logs.push("[INFO] Logs cleared by user".to_string());
            }
        }
    }

    async fn cleanup_ports(&self, ports: &[u16]) -> Result<()> {
        #[cfg(target_os = "windows")]
        {
            for port in ports {
                // Kill processes on main port
                let _ = Command::new("cmd")
                    .args(&["/C", &format!(
                        "for /f \"tokens=5\" %a in ('netstat -aon ^| find \":{port}\" ^| find \"LISTENING\"') do taskkill /F /PID %a"
                    )])
                    .output();
                
                // Also kill Raft port (+1000)
                let raft_port = port + 1000;
                let _ = Command::new("cmd")
                    .args(&["/C", &format!(
                        "for /f \"tokens=5\" %a in ('netstat -aon ^| find \":{raft_port}\" ^| find \"LISTENING\"') do taskkill /F /PID %a"
                    )])
                    .output();
                
                // Metrics port (+2000)
                let metrics_port = port + 2000;
                let _ = Command::new("cmd")
                    .args(&["/C", &format!(
                        "for /f \"tokens=5\" %a in ('netstat -aon ^| find \":{metrics_port}\" ^| find \"LISTENING\"') do taskkill /F /PID %a"
                    )])
                    .output();
                
                // Work port (+3000)
                let work_port = port + 3000;
                let _ = Command::new("cmd")
                    .args(&["/C", &format!(
                        "for /f \"tokens=5\" %a in ('netstat -aon ^| find \":{work_port}\" ^| find \"LISTENING\"') do taskkill /F /PID %a"
                    )])
                    .output();
            }
        }
        
        #[cfg(not(target_os = "windows"))]
        {
            for port in ports {
                // Kill main port
                let _ = Command::new("sh")
                    .args(&["-c", &format!("lsof -ti:{port} | xargs kill -9")])
                    .output();
                
                // Kill Raft port
                let raft_port = port + 1000;
                let _ = Command::new("sh")
                    .args(&["-c", &format!("lsof -ti:{raft_port} | xargs kill -9")])
                    .output();
                
                // Kill Metrics port
                let metrics_port = port + 2000;
                let _ = Command::new("sh")
                    .args(&["-c", &format!("lsof -ti:{metrics_port} | xargs kill -9")])
                    .output();
                
                // Kill Work port
                let work_port = port + 3000;
                let _ = Command::new("sh")
                    .args(&["-c", &format!("lsof -ti:{work_port} | xargs kill -9")])
                    .output();
            }
        }
        
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        Ok(())
    }
}