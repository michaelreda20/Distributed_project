//run outside src
// # Run 1000 requests with 10 threads
// cargo run --bin stress_test -- -n 1000 -t 10 -i my_image.png

// # Run 5000 requests with 20 threads, verbose output
// cargo run --bin stress_test -- -n 5000 -t 20 -i test_image.png -v

// # With custom timeouts and delay
// cargo run --bin stress_test -- -n 2000 -t 15 --connect-timeout 10 --rw-timeout 60 -d 100
// ```

// **Available options:**
// - `-n, --num-requests`: Number of total requests (default: 1000)
// - `-t, --num-threads`: Concurrent threads (default: 10)
// - `-i, --input-image`: Test image file (default: test_image.png)
// - `-s, --server-config`: Server config file (default: servers.conf)
// - `-d, --delay-ms`: Delay between requests per thread in ms (default: 0)
// - `--connect-timeout`: Connection timeout in seconds (default: 5)
// - `--rw-timeout`: Read/write timeout in seconds (default: 30)
// - `-v, --verbose`: Enable verbose output


use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::ImagePermissions;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use clap::Parser;

// ============================================================================
// CLI ARGUMENTS
// ============================================================================

#[derive(Parser, Clone)]
#[command(version, about = "Stress test tool for distributed image encryption", long_about = None)]
struct Cli {
    /// Number of requests to send
    #[arg(short = 'n', long, default_value = "1000")]
    num_requests: usize,

    /// Number of concurrent threads
    #[arg(short = 't', long, default_value = "10")]
    num_threads: usize,

    /// Input test image file
    #[arg(short = 'i', long, default_value = "test_image.png")]
    input_image: PathBuf,

    /// Server configuration file
    #[arg(short = 's', long, default_value = "servers.conf")]
    server_config: String,

    /// Delay between requests per thread (milliseconds)
    #[arg(short = 'd', long, default_value = "0")]
    delay_ms: u64,

    /// Connection timeout (seconds)
    #[arg(long, default_value = "5")]
    connect_timeout: u64,

    /// Read/Write timeout (seconds)
    #[arg(long, default_value = "30")]
    rw_timeout: u64,

    /// Maximum retry attempts per request
    #[arg(short = 'r', long, default_value = "5")]
    max_retries: usize,

    /// Base delay for exponential backoff (milliseconds)
    #[arg(long, default_value = "100")]
    retry_backoff_ms: u64,

    /// Enable verbose output
    #[arg(short = 'v', long)]
    verbose: bool,
}

// ============================================================================
// STATISTICS TRACKING
// ============================================================================

#[derive(Debug)]
struct TestStatistics {
    // Success/Failure counts
    total_requests: AtomicUsize,
    successful_requests: AtomicUsize,
    failed_requests: AtomicUsize,
    
    // Retry statistics
    total_retries: AtomicUsize,
    requests_with_retries: AtomicUsize,
    
    // Error breakdown
    connection_errors: AtomicUsize,
    timeout_errors: AtomicUsize,
    not_leader_errors: AtomicUsize,
    no_leader_errors: AtomicUsize,
    invalid_response_errors: AtomicUsize,
    other_errors: AtomicUsize,
    
    // Timing statistics
    total_response_time_ms: AtomicU64,
    min_response_time_ms: AtomicU64,
    max_response_time_ms: AtomicU64,
    
    // Response times for percentile calculation
    response_times: Mutex<Vec<u64>>,
    
    // Leader election tracking
    leader_changes: AtomicUsize,
    last_known_leader: Mutex<Option<String>>,
    
    // Throughput tracking
    start_time: Instant,
}

impl TestStatistics {
    fn new() -> Self {
        Self {
            total_requests: AtomicUsize::new(0),
            successful_requests: AtomicUsize::new(0),
            failed_requests: AtomicUsize::new(0),
            total_retries: AtomicUsize::new(0),
            requests_with_retries: AtomicUsize::new(0),
            connection_errors: AtomicUsize::new(0),
            timeout_errors: AtomicUsize::new(0),
            not_leader_errors: AtomicUsize::new(0),
            no_leader_errors: AtomicUsize::new(0),
            invalid_response_errors: AtomicUsize::new(0),
            other_errors: AtomicUsize::new(0),
            total_response_time_ms: AtomicU64::new(0),
            min_response_time_ms: AtomicU64::new(u64::MAX),
            max_response_time_ms: AtomicU64::new(0),
            response_times: Mutex::new(Vec::new()),
            leader_changes: AtomicUsize::new(0),
            last_known_leader: Mutex::new(None),
            start_time: Instant::now(),
        }
    }
    
    fn record_success(&self, response_time_ms: u64, leader_id: Option<String>, retry_count: usize) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_ms.fetch_add(response_time_ms, Ordering::Relaxed);
        
        // Track retries
        if retry_count > 0 {
            self.requests_with_retries.fetch_add(1, Ordering::Relaxed);
            self.total_retries.fetch_add(retry_count, Ordering::Relaxed);
        }
        
        // Update min/max response times
        let mut current_min = self.min_response_time_ms.load(Ordering::Relaxed);
        while response_time_ms < current_min {
            match self.min_response_time_ms.compare_exchange(
                current_min,
                response_time_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }
        
        let mut current_max = self.max_response_time_ms.load(Ordering::Relaxed);
        while response_time_ms > current_max {
            match self.max_response_time_ms.compare_exchange(
                current_max,
                response_time_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }
        
        // Store response time for percentile calculation
        self.response_times.lock().unwrap().push(response_time_ms);
        
        // Track leader changes
        if let Some(leader) = leader_id {
            let mut last_leader = self.last_known_leader.lock().unwrap();
            if let Some(ref prev_leader) = *last_leader {
                if prev_leader != &leader {
                    self.leader_changes.fetch_add(1, Ordering::Relaxed);
                }
            }
            *last_leader = Some(leader);
        }
    }
    
    fn record_failure(&self, error_type: ErrorType, retry_count: usize) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
        
        if retry_count > 0 {
            self.requests_with_retries.fetch_add(1, Ordering::Relaxed);
            self.total_retries.fetch_add(retry_count, Ordering::Relaxed);
        }
        
        match error_type {
            ErrorType::Connection => self.connection_errors.fetch_add(1, Ordering::Relaxed),
            ErrorType::Timeout => self.timeout_errors.fetch_add(1, Ordering::Relaxed),
            ErrorType::NotLeader => self.not_leader_errors.fetch_add(1, Ordering::Relaxed),
            ErrorType::NoLeader => self.no_leader_errors.fetch_add(1, Ordering::Relaxed),
            ErrorType::InvalidResponse => self.invalid_response_errors.fetch_add(1, Ordering::Relaxed),
            ErrorType::Other => self.other_errors.fetch_add(1, Ordering::Relaxed),
        };
    }
    
    fn print_report(&self) {
        let total = self.total_requests.load(Ordering::Relaxed);
        let success = self.successful_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);
        let total_time = self.start_time.elapsed().as_secs_f64();
        let total_retries = self.total_retries.load(Ordering::Relaxed);
        let requests_with_retries = self.requests_with_retries.load(Ordering::Relaxed);
        
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║              STRESS TEST RESULTS                             ║");
        println!("╚══════════════════════════════════════════════════════════════╝");
        
        println!("\n📊 OVERALL STATISTICS");
        println!("─────────────────────────────────────────────────────────────");
        println!("  Total Requests:       {}", total);
        println!("  Successful:           {} ({:.2}%)", success, (success as f64 / total as f64) * 100.0);
        println!("  Failed:               {} ({:.2}%)", failed, (failed as f64 / total as f64) * 100.0);
        println!("  Test Duration:        {:.2} seconds", total_time);
        println!("  Throughput:           {:.2} requests/second", total as f64 / total_time);
        
        println!("\n🔄 RETRY STATISTICS");
        println!("─────────────────────────────────────────────────────────────");
        println!("  Total Retries:        {}", total_retries);
        println!("  Requests with Retries: {} ({:.2}%)", 
                 requests_with_retries, 
                 (requests_with_retries as f64 / total as f64) * 100.0);
        if requests_with_retries > 0 {
            println!("  Avg Retries/Request:  {:.2}", 
                     total_retries as f64 / requests_with_retries as f64);
        }
        
        println!("\n❌ ERROR BREAKDOWN");
        println!("─────────────────────────────────────────────────────────────");
        println!("  Connection Errors:    {}", self.connection_errors.load(Ordering::Relaxed));
        println!("  Timeout Errors:       {}", self.timeout_errors.load(Ordering::Relaxed));
        println!("  NOT_LEADER Errors:    {}", self.not_leader_errors.load(Ordering::Relaxed));
        println!("  NO_LEADER Errors:     {}", self.no_leader_errors.load(Ordering::Relaxed));
        println!("  Invalid Response:     {}", self.invalid_response_errors.load(Ordering::Relaxed));
        println!("  Other Errors:         {}", self.other_errors.load(Ordering::Relaxed));
        
        if success > 0 {
            let total_response = self.total_response_time_ms.load(Ordering::Relaxed);
            let avg_response = total_response / success as u64;
            let min_response = self.min_response_time_ms.load(Ordering::Relaxed);
            let max_response = self.max_response_time_ms.load(Ordering::Relaxed);
            
            println!("\n⏱️  RESPONSE TIME STATISTICS");
            println!("─────────────────────────────────────────────────────────────");
            println!("  Average:              {} ms", avg_response);
            println!("  Minimum:              {} ms", min_response);
            println!("  Maximum:              {} ms", max_response);
            
            // Calculate percentiles
            let mut times = self.response_times.lock().unwrap();
            if !times.is_empty() {
                times.sort_unstable();
                let p50 = times[times.len() * 50 / 100];
                let p90 = times[times.len() * 90 / 100];
                let p95 = times[times.len() * 95 / 100];
                let p99 = times[times.len() * 99 / 100];
                
                println!("  50th Percentile (p50): {} ms", p50);
                println!("  90th Percentile (p90): {} ms", p90);
                println!("  95th Percentile (p95): {} ms", p95);
                println!("  99th Percentile (p99): {} ms", p99);
            }
        }
        
        println!("\n🔄 LEADER ELECTION STATISTICS");
        println!("─────────────────────────────────────────────────────────────");
        println!("  Leader Changes:       {}", self.leader_changes.load(Ordering::Relaxed));
        if let Some(ref leader) = *self.last_known_leader.lock().unwrap() {
            println!("  Final Leader:         {}", leader);
        }
        
        // Success rate assessment
        let success_rate = (success as f64 / total as f64) * 100.0;
        println!("\n📈 ASSESSMENT");
        println!("─────────────────────────────────────────────────────────────");
        if success_rate >= 99.0 {
            println!("  ✅ EXCELLENT: Success rate >= 99%");
        } else if success_rate >= 95.0 {
            println!("  ✓ GOOD: Success rate >= 95%");
        } else if success_rate >= 90.0 {
            println!("  ⚠️  FAIR: Success rate >= 90%");
        } else {
            println!("  ❌ POOR: Success rate < 90%");
        }
        
        println!("\n");
    }
    
    fn save_to_file(&self, filename: &str) -> Result<()> {
        let total = self.total_requests.load(Ordering::Relaxed);
        let success = self.successful_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);
        let total_time = self.start_time.elapsed().as_secs_f64();
        let total_retries = self.total_retries.load(Ordering::Relaxed);
        
        let report = format!(
            "Stress Test Report - {}\n\
             ═══════════════════════════════════════\n\
             \n\
             Overall Statistics:\n\
             - Total Requests: {}\n\
             - Successful: {} ({:.2}%)\n\
             - Failed: {} ({:.2}%)\n\
             - Test Duration: {:.2}s\n\
             - Throughput: {:.2} req/s\n\
             \n\
             Retry Statistics:\n\
             - Total Retries: {}\n\
             - Requests with Retries: {}\n\
             \n\
             Error Breakdown:\n\
             - Connection Errors: {}\n\
             - Timeout Errors: {}\n\
             - NOT_LEADER Errors: {}\n\
             - NO_LEADER Errors: {}\n\
             - Invalid Response: {}\n\
             - Other Errors: {}\n\
             \n\
             Response Time Statistics (ms):\n\
             - Average: {}\n\
             - Minimum: {}\n\
             - Maximum: {}\n\
             \n\
             Leader Election:\n\
             - Leader Changes: {}\n\
             ",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            total,
            success, (success as f64 / total as f64) * 100.0,
            failed, (failed as f64 / total as f64) * 100.0,
            total_time,
            total as f64 / total_time,
            total_retries,
            self.requests_with_retries.load(Ordering::Relaxed),
            self.connection_errors.load(Ordering::Relaxed),
            self.timeout_errors.load(Ordering::Relaxed),
            self.not_leader_errors.load(Ordering::Relaxed),
            self.no_leader_errors.load(Ordering::Relaxed),
            self.invalid_response_errors.load(Ordering::Relaxed),
            self.other_errors.load(Ordering::Relaxed),
            if success > 0 { self.total_response_time_ms.load(Ordering::Relaxed) / success as u64 } else { 0 },
            self.min_response_time_ms.load(Ordering::Relaxed),
            self.max_response_time_ms.load(Ordering::Relaxed),
            self.leader_changes.load(Ordering::Relaxed),
        );
        
        fs::write(filename, report)?;
        println!("📄 Detailed report saved to: {}", filename);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum ErrorType {
    Connection,
    Timeout,
    NotLeader,
    NoLeader,
    InvalidResponse,
    Other,
}

// ============================================================================
// MAIN TEST LOGIC
// ============================================================================

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║        DISTRIBUTED IMAGE ENCRYPTION STRESS TEST              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    
    // Load servers
    let servers = load_servers(&cli.server_config)?;
    println!("🖥️  Loaded {} servers from '{}'", servers.len(), cli.server_config);
    
    // Load test image
    let img_data = fs::read(&cli.input_image)?;
    println!("🖼️  Loaded test image: {} ({} bytes)", cli.input_image.display(), img_data.len());
    
    // Prepare metadata
    let mut quotas = HashMap::new();
    quotas.insert("test_user".to_string(), 5);
    let permissions = ImagePermissions {
        owner: "test_owner".to_string(),
        quotas,
    };
    let meta_bytes = bincode::serialize(&permissions)?;
    
    println!("\n📋 TEST CONFIGURATION");
    println!("─────────────────────────────────────────────────────────────");
    println!("  Total Requests:       {}", cli.num_requests);
    println!("  Concurrent Threads:   {}", cli.num_threads);
    println!("  Delay per request:    {} ms", cli.delay_ms);
    println!("  Connect timeout:      {} seconds", cli.connect_timeout);
    println!("  Read/Write timeout:   {} seconds", cli.rw_timeout);
    println!("  Max Retries:          {}", cli.max_retries);
    println!("  Retry Backoff:        {} ms", cli.retry_backoff_ms);
    println!("  Verbose mode:         {}", if cli.verbose { "enabled" } else { "disabled" });
    
    println!("\n🚀 Starting stress test...\n");
    
    // Create statistics tracker
    let stats = Arc::new(TestStatistics::new());
    
    // Calculate requests per thread
    let requests_per_thread = cli.num_requests / cli.num_threads;
    let remainder = cli.num_requests % cli.num_threads;
    
    // Spawn worker threads
    let mut handles = vec![];
    for thread_id in 0..cli.num_threads {
        let requests = if thread_id < remainder {
            requests_per_thread + 1
        } else {
            requests_per_thread
        };
        
        let stats_clone = Arc::clone(&stats);
        let servers_clone = servers.clone();
        let meta_clone = meta_bytes.clone();
        let img_clone = img_data.clone();
        let config = cli.clone();
        
        let handle = thread::spawn(move || {
            run_worker(
                thread_id,
                requests,
                servers_clone,
                meta_clone,
                img_clone,
                stats_clone,
                config,
            )
        });
        
        handles.push(handle);
    }
    
    // Progress monitoring thread
    let stats_monitor = Arc::clone(&stats);
    let total_requests = cli.num_requests;
    let monitor_handle = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(2));
            let completed = stats_monitor.total_requests.load(Ordering::Relaxed);
            let success = stats_monitor.successful_requests.load(Ordering::Relaxed);
            let retries = stats_monitor.total_retries.load(Ordering::Relaxed);
            let progress = (completed as f64 / total_requests as f64) * 100.0;
            
            print!("\r⏳ Progress: {}/{} ({:.1}%) | ✓ Success: {} | ✗ Failed: {} | 🔄 Retries: {}    ",
                   completed, total_requests, progress, success,
                   stats_monitor.failed_requests.load(Ordering::Relaxed),
                   retries);
            std::io::stdout().flush().ok();
            
            if completed >= total_requests {
                break;
            }
        }
    });
    
    // Wait for all workers to complete
    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }
    
    // Wait for monitor thread
    monitor_handle.join().ok();
    println!("\n\n✅ All requests completed!");
    
    // Print and save results
    stats.print_report();
    
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let report_filename = format!("stress_test_report_{}.txt", timestamp);
    stats.save_to_file(&report_filename)?;
    
    Ok(())
}

// ============================================================================
// WORKER LOGIC WITH RETRY MECHANISM
// ============================================================================

fn run_worker(
    thread_id: usize,
    num_requests: usize,
    servers: Vec<String>,
    meta_bytes: Vec<u8>,
    img_data: Vec<u8>,
    stats: Arc<TestStatistics>,
    config: Cli,
) {
    for request_id in 0..num_requests {
        let start_time = Instant::now();
        
        // Retry loop: try up to max_retries times
        let mut attempt = 0;
        let mut success = false;
        let mut last_error = ErrorType::Other;
        let mut response_leader: Option<String> = None;
        
        // Keep retrying until success or max retries reached
        while attempt <= config.max_retries && !success {
            if config.verbose && attempt > 0 {
                println!("[Thread-{}] Request #{}: Retry attempt {} of {}",
                         thread_id, request_id, attempt, config.max_retries);
            }
            
            // Try each server until one succeeds
            for server_addr in &servers {
                match send_encryption_request(
                    server_addr,
                    &meta_bytes,
                    &img_data,
                    config.connect_timeout,
                    config.rw_timeout,
                ) {
                    Ok((encrypted_data, leader_id)) => {
                        // Verify response is valid (not suspiciously small)
                        if encrypted_data.len() > 1000 {
                            let response_time = start_time.elapsed().as_millis() as u64;
                            stats.record_success(response_time, leader_id.clone(), attempt);
                            response_leader = leader_id;
                            success = true;
                            
                            if config.verbose {
                                println!("[Thread-{}] Request #{}: SUCCESS in {}ms after {} attempts (leader: {:?})",
                                         thread_id, request_id, response_time, attempt + 1, response_leader);
                            }
                            break; // Exit server loop on success
                        } else {
                            last_error = ErrorType::InvalidResponse;
                            if config.verbose {
                                println!("[Thread-{}] Request #{}: Invalid response size from {} ({}B)",
                                         thread_id, request_id, server_addr, encrypted_data.len());
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        
                        // Classify the error type
                        last_error = if err_msg.contains("NOT_LEADER") {
                            ErrorType::NotLeader
                        } else if err_msg.contains("NO_LEADER") {
                            ErrorType::NoLeader
                        } else if err_msg.contains("timed out") || err_msg.contains("timeout") {
                            ErrorType::Timeout
                        } else if err_msg.contains("Connection refused") || err_msg.contains("connect") {
                            ErrorType::Connection
                        } else {
                            ErrorType::Other
                        };
                        
                        if config.verbose {
                            println!("[Thread-{}] Request #{}: Failed on {} - {:?} (attempt {})",
                                     thread_id, request_id, server_addr, last_error, attempt + 1);
                        }
                    }
                }
            }
            
            // If still not successful, wait before retry (exponential backoff)
            if !success && attempt < config.max_retries {
                let backoff_time = config.retry_backoff_ms * 2u64.pow(attempt as u32);
                if config.verbose {
                    println!("[Thread-{}] Request #{}: Waiting {}ms before retry",
                             thread_id, request_id, backoff_time);
                }
                thread::sleep(Duration::from_millis(backoff_time));
            }
            
            attempt += 1;
        }
        
        // Record final result
        if !success {
            stats.record_failure(last_error, attempt - 1);
            if config.verbose {
                println!("[Thread-{}] Request #{}: PERMANENTLY FAILED after {} attempts",
                         thread_id, request_id, attempt);
            }
        }
        
        // Delay between requests if specified
        if config.delay_ms > 0 {
            thread::sleep(Duration::from_millis(config.delay_ms));
        }
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn load_servers(config_file: &str) -> Result<Vec<String>> {
    let content = fs::read_to_string(config_file)?;
    let servers: Vec<String> = content
        .lines()
        .filter(|s| !s.trim().is_empty())
        .map(String::from)
        .collect();
    
    if servers.is_empty() {
        bail!("No servers found in '{}'", config_file);
    }
    
    Ok(servers)
}

fn send_encryption_request(
    addr: &str,
    meta_bytes: &[u8],
    img_buf: &[u8],
    connect_timeout_sec: u64,
    rw_timeout_sec: u64,
) -> Result<(Vec<u8>, Option<String>)> {
    // Connect with timeout
    let mut stream = TcpStream::connect_timeout(
        &addr.parse()?,
        Duration::from_secs(connect_timeout_sec),
    )?;
    
    stream.set_read_timeout(Some(Duration::from_secs(rw_timeout_sec)))?;
    stream.set_write_timeout(Some(Duration::from_secs(rw_timeout_sec)))?;
    
    // Send metadata
    let meta_size = meta_bytes.len() as u64;
    stream.write_all(&meta_size.to_be_bytes())?;
    stream.write_all(meta_bytes)?;
    
    // Send image
    let img_size = img_buf.len() as u64;
    stream.write_all(&img_size.to_be_bytes())?;
    stream.write_all(img_buf)?;
    
    // Receive response size
    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes)?;
    let response_size = u64::from_be_bytes(size_bytes);
    
    // Read response
    let mut response_buf = vec![0; response_size as usize];
    stream.read_exact(&mut response_buf)?;
    
    // Check for error messages
    if let Ok(msg) = std::str::from_utf8(&response_buf) {
        if msg.starts_with("NOT_LEADER:") {
            let leader = msg.strip_prefix("NOT_LEADER:").map(String::from);
            bail!("NOT_LEADER:{}", leader.unwrap_or_else(|| "unknown".to_string()));
        }
        if msg.starts_with("NO_LEADER") {
            bail!("NO_LEADER");
        }
    }
    
    // Extract leader ID if possible
    let leader_id = None; // TODO: Server could include leader ID in response
    
    Ok((response_buf, leader_id))
}

// Timestamp formatting module
mod chrono {
    use std::time::SystemTime;
    
    pub struct Local;
    
    impl Local {
        pub fn now() -> DateTime {
            DateTime { time: SystemTime::now() }
        }
    }
    
    pub struct DateTime {
        time: SystemTime,
    }
    
    impl DateTime {
        pub fn format(&self, _: &str) -> String {
            format!("{:?}", self.time)
        }
    }
}