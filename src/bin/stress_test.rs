//! Complete Stress Test with Enhanced Image Validation
//! 
//! Run examples:
//! # Run 1000 requests with 10 threads
//! cargo run --bin stress_test -- -n 1000 -t 10 -i my_image.jpg
//!
//! # Run 5000 requests with 20 threads, verbose output
//! cargo run --bin stress_test -- -n 5000 -t 20 -i my_image.jpg -v
//!
//! # With custom timeouts and delay
//! cargo run --bin stress_test -- -n 2000 -t 15 --connect-timeout 10 --rw-timeout 60 -d 100

// // # Build in release mode for better performance
// cargo build --release --bin stress_test

// cargo run --release --bin stress_test -- \
//   -n 1000 \
//   -t 10 \
//   -i my_image.png \
//   -s servers.conf


use anyhow::{bail, Result};
use bincode;
use cloud_p2p_project::ImagePermissions;
use image::{ImageFormat, GenericImageView};
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
    
    // Image validation metrics
    valid_images: AtomicUsize,           // Images that passed PNG validation
    invalid_images: AtomicUsize,         // Images that failed PNG validation
    total_image_bytes: AtomicU64,        // Total bytes of all valid images
    min_image_size: AtomicU64,           // Smallest valid image
    max_image_size: AtomicU64,           // Largest valid image
    
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
            valid_images: AtomicUsize::new(0),
            invalid_images: AtomicUsize::new(0),
            total_image_bytes: AtomicU64::new(0),
            min_image_size: AtomicU64::new(u64::MAX),
            max_image_size: AtomicU64::new(0),
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
    
    fn record_valid_image(&self, image_size: u64) {
        self.valid_images.fetch_add(1, Ordering::Relaxed);
        self.total_image_bytes.fetch_add(image_size, Ordering::Relaxed);
        
        // Update min size
        let mut current_min = self.min_image_size.load(Ordering::Relaxed);
        while image_size < current_min {
            match self.min_image_size.compare_exchange(
                current_min,
                image_size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }
        
        // Update max size
        let mut current_max = self.max_image_size.load(Ordering::Relaxed);
        while image_size > current_max {
            match self.max_image_size.compare_exchange(
                current_max,
                image_size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }
    }
    
    fn record_invalid_image(&self) {
        self.invalid_images.fetch_add(1, Ordering::Relaxed);
    }
    
    fn record_success(&self, response_time_ms: u64, leader_id: Option<String>, retry_count: usize, image_size: u64, is_valid_image: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_ms.fetch_add(response_time_ms, Ordering::Relaxed);
        
        // Track image validation
        if is_valid_image {
            self.record_valid_image(image_size);
        } else {
            self.record_invalid_image();
        }
        
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
        
        // Image validation stats
        let valid_imgs = self.valid_images.load(Ordering::Relaxed);
        let invalid_imgs = self.invalid_images.load(Ordering::Relaxed);
        let total_img_bytes = self.total_image_bytes.load(Ordering::Relaxed);
        let min_img = self.min_image_size.load(Ordering::Relaxed);
        let max_img = self.max_image_size.load(Ordering::Relaxed);
        
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║              STRESS TEST RESULTS                             ║");
        println!("╚═══════════════════════════════════════════════════════════════╝");
        
        println!("\n📊 OVERALL STATISTICS");
        println!("───────────────────────────────────────────────────────────────");
        println!("  Total Requests:       {}", total);
        println!("  Successful:           {} ({:.2}%)", success, (success as f64 / total as f64) * 100.0);
        println!("  Failed:               {} ({:.2}%)", failed, (failed as f64 / total as f64) * 100.0);
        println!("  Test Duration:        {:.2} seconds", total_time);
        println!("  Throughput:           {:.2} requests/second", total as f64 / total_time);
        
        println!("\n🖼️  IMAGE VALIDATION");
        println!("───────────────────────────────────────────────────────────────");
        if success > 0 {
            println!("  Valid PNG Images:     {} ({:.2}%)", 
                     valid_imgs, (valid_imgs as f64 / success as f64) * 100.0);
            println!("  Invalid Images:       {} ({:.2}%)", 
                     invalid_imgs, (invalid_imgs as f64 / success as f64) * 100.0);
            
            if valid_imgs > 0 {
                let avg_size = total_img_bytes / valid_imgs as u64;
                println!("  Avg Image Size:       {:.2} KB", avg_size as f64 / 1024.0);
                if min_img != u64::MAX {
                    println!("  Min Image Size:       {:.2} KB", min_img as f64 / 1024.0);
                }
                if max_img > 0 {
                    println!("  Max Image Size:       {:.2} KB", max_img as f64 / 1024.0);
                }
                println!("  Total Data Transfer:  {:.2} MB", total_img_bytes as f64 / 1_048_576.0);
            }
        } else {
            println!("  No successful responses to validate");
        }
        
        println!("\n🔄 RETRY STATISTICS");
        println!("───────────────────────────────────────────────────────────────");
        println!("  Total Retries:        {}", total_retries);
        println!("  Requests with Retries: {} ({:.2}%)", 
                 requests_with_retries, 
                 (requests_with_retries as f64 / total as f64) * 100.0);
        if requests_with_retries > 0 {
            println!("  Avg Retries/Request:  {:.2}", 
                     total_retries as f64 / requests_with_retries as f64);
        }
        
        println!("\n❌ ERROR BREAKDOWN");
        println!("───────────────────────────────────────────────────────────────");
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
            println!("───────────────────────────────────────────────────────────────");
            println!("  Average:              {} ms", avg_response);
            if min_response != u64::MAX {
                println!("  Minimum:              {} ms", min_response);
            }
            if max_response > 0 {
                println!("  Maximum:              {} ms", max_response);
            }
            
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
        println!("───────────────────────────────────────────────────────────────");
        println!("  Leader Changes:       {}", self.leader_changes.load(Ordering::Relaxed));
        if let Some(ref leader) = *self.last_known_leader.lock().unwrap() {
            println!("  Final Leader:         {}", leader);
        }
        
        // Success rate assessment
        let success_rate = (success as f64 / total as f64) * 100.0;
        println!("\n📈 ASSESSMENT");
        println!("───────────────────────────────────────────────────────────────");
        if success_rate >= 99.0 {
            println!("  ✅ EXCELLENT: Success rate >= 99%");
        } else if success_rate >= 95.0 {
            println!("  ✓ GOOD: Success rate >= 95%");
        } else if success_rate >= 90.0 {
            println!("  ⚠️  FAIR: Success rate >= 90%");
        } else {
            println!("  ❌ POOR: Success rate < 90%");
        }
        
        // Image validation assessment
        if success > 0 {
            let valid_rate = (valid_imgs as f64 / success as f64) * 100.0;
            if valid_rate >= 99.0 {
                println!("  ✅ EXCELLENT: Image validation rate >= 99%");
            } else if valid_rate >= 95.0 {
                println!("  ✓ GOOD: Image validation rate >= 95%");
            } else {
                println!("  ⚠️  WARNING: Image validation rate < 95%");
            }
        }
        
        println!("\n");
    }
    
    fn save_to_file(&self, filename: &str) -> Result<()> {
        let total = self.total_requests.load(Ordering::Relaxed);
        let success = self.successful_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);
        let total_time = self.start_time.elapsed().as_secs_f64();
        let total_retries = self.total_retries.load(Ordering::Relaxed);
        let valid_imgs = self.valid_images.load(Ordering::Relaxed);
        let invalid_imgs = self.invalid_images.load(Ordering::Relaxed);
        let total_img_bytes = self.total_image_bytes.load(Ordering::Relaxed);
        
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
             Image Validation:\n\
             - Valid PNG Images: {} ({:.2}%)\n\
             - Invalid Images: {} ({:.2}%)\n\
             - Total Data Transfer: {:.2} MB\n\
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
            format_timestamp(),
            total,
            success, (success as f64 / total as f64) * 100.0,
            failed, (failed as f64 / total as f64) * 100.0,
            total_time,
            total as f64 / total_time,
            valid_imgs, if success > 0 { (valid_imgs as f64 / success as f64) * 100.0 } else { 0.0 },
            invalid_imgs, if success > 0 { (invalid_imgs as f64 / success as f64) * 100.0 } else { 0.0 },
            total_img_bytes as f64 / 1_048_576.0,
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
// IMAGE VALIDATION
// ============================================================================

/// Validate that the encrypted data is a proper PNG image
fn validate_encrypted_image(data: &[u8]) -> Result<bool> {
    // Check minimum size
    if data.len() < 8 {
        return Ok(false);
    }
    
    // Check PNG signature (first 8 bytes)
    let png_signature: [u8; 8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    if &data[0..8] != &png_signature {
        return Ok(false);
    }
    
    // Try to load the image to ensure it's valid
    match image::load_from_memory_with_format(data, ImageFormat::Png) {
        Ok(img) => {
            let (width, height) = img.dimensions();
            // Ensure image has reasonable dimensions
            Ok(width > 0 && height > 0 && data.len() > 1000)
        }
        Err(_) => Ok(false)
    }
}

/// Save sample encrypted images for manual inspection
// fn save_sample_image(data: &[u8], sample_id: usize, thread_id: usize) -> Result<()> {
//     // Create samples directory if it doesn't exist
//     let samples_dir = "stress_test_samples";
//     fs::create_dir_all(samples_dir)?;
    
//     let filename = format!("{}/encrypted_sample_t{}_r{}.png", samples_dir, thread_id, sample_id);
//     fs::write(&filename, data)?;
    
//     Ok(())
// }

// ============================================================================
// MAIN TEST LOGIC
// ============================================================================

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║        DISTRIBUTED IMAGE ENCRYPTION STRESS TEST              ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");
    
    // Load servers
    let servers = load_servers(&cli.server_config)?;
    println!("🖥️  Loaded {} servers from '{}'", servers.len(), cli.server_config);
    
    // Load test image
    let img_data = fs::read(&cli.input_image)?;
    println!("🖼️  Loaded test image: {} ({:.2} KB)", 
             cli.input_image.display(), 
             img_data.len() as f64 / 1024.0);
    
    // Prepare metadata
    let mut quotas = HashMap::new();
    quotas.insert("test_user".to_string(), 5);
    let permissions = ImagePermissions {
        owner: "test_owner".to_string(),
        quotas,
    };
    let meta_bytes = bincode::serialize(&permissions)?;
    
    println!("\n📋 TEST CONFIGURATION");
    println!("───────────────────────────────────────────────────────────────");
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
            let valid = stats_monitor.valid_images.load(Ordering::Relaxed);
            let progress = (completed as f64 / total_requests as f64) * 100.0;
            
            print!("\r⏳ Progress: {}/{} ({:.1}%) | ✓ Success: {} | ✗ Failed: {} | 🔄 Retries: {} | ✅ Valid: {}    ",
                   completed, total_requests, progress, success,
                   stats_monitor.failed_requests.load(Ordering::Relaxed),
                   retries, valid);
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
    
    let timestamp = format_timestamp();
    let report_filename = format!("stress_test_report_{}.txt", timestamp);
    stats.save_to_file(&report_filename)?;
    
    // Compare image sizes
    // compare_image_sizes(&cli.input_image)?;
    
    Ok(())
}

// ============================================================================
// WORKER LOGIC WITH RETRY MECHANISM (MODIFIED FOR TRUE MULTICAST)
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
    // let mut samples_saved = 0;
    // let max_samples_per_thread = 3; // Save first 3 successful images per thread
    
    for request_id in 0..num_requests {
        let start_time = Instant::now();
        
        // Retry loop: try up to max_retries times
        let mut attempt = 0;
        let mut success_reported = false; // <-- NEW FLAG: Tracks if a success has been recorded for this REQUEST
        let mut last_error = ErrorType::Other;
        
        // Keep retrying until a request succeeds or max retries reached
        while attempt <= config.max_retries && !success_reported {
            if config.verbose && attempt > 0 {
                println!("[Thread-{}] Request #{}: Retry attempt {} of {}",
                         thread_id, request_id, attempt, config.max_retries);
            }
            
            // *******************************************************************
            // MODIFIED LOGIC: TRUE MULTICAST - Send to all servers and only
            // record the FIRST success received for this request attempt.
            // *******************************************************************
            for server_addr in &servers {
                let mut current_server_success = false;

                match send_encryption_request(
                    server_addr,
                    &meta_bytes,
                    &img_data,
                    config.connect_timeout,
                    config.rw_timeout,
                ) {
                    Ok((encrypted_data, leader_id)) => {
                        current_server_success = true; // This server responded successfully
                        
                        // ONLY record success metrics/samples if we haven't already recorded one
                        if !success_reported { 
                            match validate_encrypted_image(&encrypted_data) {
                                Ok(true) => {
                                    let response_time = start_time.elapsed().as_millis() as u64;
                                    let image_size = encrypted_data.len() as u64;
                                    stats.record_success(response_time, leader_id.clone(), attempt, image_size, true);
                                    success_reported = true; // Mark as successful response received

                                    // // Save sample images for manual verification
                                    // if samples_saved < max_samples_per_thread {
                                    //     if let Ok(_) = save_sample_image(&encrypted_data, request_id, thread_id) {
                                    //         samples_saved += 1;
                                    //     }
                                    // }
                                    
                                    if config.verbose {
                                        println!("[Thread-{}] Request #{}: SUCCESS from {} - Valid PNG ({:.2}KB) in {}ms (leader: {:?})",
                                                 thread_id, request_id, server_addr,
                                                 encrypted_data.len() as f64 / 1024.0,
                                                 response_time, leader_id);
                                    }
                                }
                                Ok(false) => {
                                    if config.verbose {
                                        println!("[Thread-{}] Request #{}: Invalid PNG from {} ({}B, signature check failed)",
                                                 thread_id, request_id, server_addr, encrypted_data.len());
                                    }
                                    // If validation fails, it's treated as a potential retryable failure (or just ignored for success counting)
                                    // last_error remains the last encountered *fatal* error type.
                                }
                                Err(e) => {
                                    if config.verbose {
                                        println!("[Thread-{}] Request #{}: Image validation error from {}: {}", 
                                                 thread_id, request_id, server_addr, e);
                                    }
                                }
                            }
                        } else if config.verbose {
                            // This path means a success was already received from a previous server in this loop
                            println!("[Thread-{}] Request #{}: IGNORED SUCCESS from {} (already received success from another server)",
                                     thread_id, request_id, server_addr);
                        }
                    }
                    Err(e) => {
                        // ... (Error classification remains the same)
                        let err_msg = e.to_string();
                        
                        // Classify the error type
                        let current_error = if err_msg.contains("NOT_LEADER") {
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

                        // Only update last_error if we haven't successfully reported for this request yet
                        if !success_reported {
                             last_error = current_error;
                        }
                        
                        if config.verbose {
                            println!("[Thread-{}] Request #{}: Failed on {} - {:?} (attempt {})",
                                     thread_id, request_id, server_addr, current_error, attempt + 1);
                        }
                    }
                }
            } // END of Multicast Loop (sends to all 3 servers)
            // *******************************************************************
            
            // If the request was not successful on ANY server in this attempt, wait before retry
            if !success_reported && attempt < config.max_retries {
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
        if !success_reported {
            stats.record_failure(last_error, attempt - 1);
            if config.verbose {
                println!("[Thread-{}] Request #{}: PERMANENTLY FAILED after {} attempts - {:?}",
                         thread_id, request_id, attempt, last_error);
            }
        }
        
        // Delay between requests if specified
        if config.delay_ms > 0 {
            thread::sleep(Duration::from_millis(config.delay_ms));
        }
    }
    
    // if config.verbose || samples_saved > 0 {
    //     println!("[Thread-{}] Completed. Saved {} sample images to stress_test_samples/", 
    //              thread_id, samples_saved);
    // }
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
    stream.flush()?;
    
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
    
    // Extract leader ID if possible (for future enhancement)
    let leader_id = None;
    
    Ok((response_buf, leader_id))
}

// fn compare_image_sizes(original_path: &PathBuf) -> Result<()> {
//     let original_size = fs::metadata(original_path)?.len();
    
//     println!("\n🔍 IMAGE SIZE COMPARISON");
//     println!("───────────────────────────────────────────────────────────────");
//     println!("  Original image size:  {:.2} KB", original_size as f64 / 1024.0);
    
//     // Check sample encrypted images
//     let samples_dir = "stress_test_samples";
//     if let Ok(entries) = fs::read_dir(samples_dir) {
//         let mut total_encrypted = 0u64;
//         let mut count = 0;
//         let mut sizes = Vec::new();
        
//         for entry in entries.flatten() {
//             if let Ok(metadata) = entry.metadata() {
//                 let size = metadata.len();
//                 total_encrypted += size;
//                 sizes.push(size);
//                 count += 1;
//             }
//         }
        
//         if count > 0 {
//             let avg_encrypted = total_encrypted / count;
//             sizes.sort_unstable();
            
//             println!("  Encrypted samples:    {} files", count);
//             println!("  Avg encrypted size:   {:.2} KB", avg_encrypted as f64 / 1024.0);
//             println!("  Min encrypted size:   {:.2} KB", sizes[0] as f64 / 1024.0);
//             println!("  Max encrypted size:   {:.2} KB", sizes[sizes.len() - 1] as f64 / 1024.0);
//             println!("  Size increase:        {:.1}%", 
//                      ((avg_encrypted as f64 - original_size as f64) / original_size as f64) * 100.0);
//             println!("\n  📁 Sample images saved in: {}/", samples_dir);
//         } else {
//             println!("  No sample images found");
//         }
//     } else {
//         println!("  Sample directory not found (no successful requests)");
//     }
    
//     Ok(())
// }

fn format_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap();
    let secs = duration.as_secs();
    
    // Simple timestamp: YYYYMMDD_HHMMSS
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    
    // Approximate date calculation (simplified, starts from epoch)
    let years = days / 365;
    let remaining_days = days % 365;
    let months = remaining_days / 30;
    let day_of_month = remaining_days % 30;
    
    format!("{:04}{:02}{:02}_{:02}{:02}{:02}",
            1970 + years,
            1 + months,
            1 + day_of_month,
            hours,
            minutes,
            seconds)
}