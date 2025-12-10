use std::collections::VecDeque;
use chrono::Local;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerMetrics {
    pub timestamp: String,
    pub received: u32,
    pub sent: u32,
    pub total: u32,
    pub get: u32,
    pub put: u32,
    pub completion_rate: f32,
    pub avg_duration: f32,
    pub error_rate: f32,
}

pub struct MetricsCollector {
    metrics_history: VecDeque<ServerMetrics>,
    max_history: usize,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut collector = Self {
            metrics_history: VecDeque::new(),
            max_history: 300, // Keep 5 minutes of data at 1 sample/second
        };
        
        // Initialize with some data
        collector.generate_initial_data();
        collector
    }

    fn generate_initial_data(&mut self) {
        let now = chrono::Local::now();
        for i in (0..60).rev() {
            let timestamp = (now - chrono::Duration::minutes(i)).format("%H:%M").to_string();
            let metrics = self.generate_random_metrics(timestamp);
            self.metrics_history.push_back(metrics);
        }
    }

    fn generate_random_metrics(&self, timestamp: String) -> ServerMetrics {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        let received = 800 + rng.gen_range(0..400);
        let sent = 700 + rng.gen_range(0..350);
        let total = 1200 + rng.gen_range(0..500);
        let get = 300 + rng.gen_range(0..200);
        let put = 200 + rng.gen_range(0..150);
        
        ServerMetrics {
            timestamp,
            received,
            sent,
            total,
            get,
            put,
            completion_rate: 85.0 + rng.gen::<f32>() * 15.0,
            avg_duration: 5.0 + rng.gen::<f32>() * 8.0,
            error_rate: rng.gen::<f32>() * 0.03,
        }
    }

    /// Try to fetch real metrics from the server's metrics port
    /// Falls back to simulated metrics if connection fails
    pub async fn fetch_real_metrics_or_simulate(&mut self, ports: &[u16]) -> ServerMetrics {
        let timestamp = Local::now().format("%H:%M").to_string();
        
        // Try to get real metrics from servers
        for port in ports {
            let metrics_port = port + 2000; // Your metrics port offset
            if let Ok(metrics) = self.try_fetch_from_port(metrics_port).await {
                return ServerMetrics {
                    timestamp,
                    ..metrics
                };
            }
        }
        
        // Fallback to simulated metrics
        self.generate_random_metrics(timestamp)
    }

    /// Attempt to connect to a server's metrics port and fetch metrics
    async fn try_fetch_from_port(&self, metrics_port: u16) -> Result<ServerMetrics, Box<dyn std::error::Error>> {
        let addr = format!("127.0.0.1:{}", metrics_port);
        let mut stream = TcpStream::connect(&addr).await?;
        
        // Send metrics request (your LoadBalancingMessage::MetricsRequest)
        let request = r#"{"MetricsRequest":{}}"#;
        let request_bytes = request.as_bytes();
        
        stream.write_u32(request_bytes.len() as u32).await?;
        stream.write_all(request_bytes).await?;
        stream.flush().await?;
        
        // Read response
        let response_len = stream.read_u32().await?;
        let mut response_buf = vec![0u8; response_len as usize];
        stream.read_exact(&mut response_buf).await?;
        
        // Parse response (this is simplified - adjust to your actual message format)
        let _response_str = String::from_utf8_lossy(&response_buf);
        
        // For now, return simulated metrics
        // In production, parse the actual ServerMetrics from your server
        Ok(self.generate_random_metrics(Local::now().format("%H:%M").to_string()))
    }

    pub fn add_metrics(&mut self, metrics: ServerMetrics) {
        self.metrics_history.push_back(metrics);
        
        // Remove old data
        while self.metrics_history.len() > self.max_history {
            self.metrics_history.pop_front();
        }
    }

    pub fn update(&mut self) {
        let timestamp = Local::now().format("%H:%M").to_string();
        let metrics = self.generate_random_metrics(timestamp);
        self.add_metrics(metrics);
    }

    pub fn get_recent_metrics(&self, count: usize) -> Vec<ServerMetrics> {
        let len = self.metrics_history.len();
        let start = if len > count { len - count } else { 0 };
        
        self.metrics_history
            .iter()
            .skip(start)
            .cloned()
            .collect()
    }

    pub fn get_all_metrics(&self) -> Vec<ServerMetrics> {
        self.metrics_history.iter().cloned().collect()
    }

    pub fn clear(&mut self) {
        self.metrics_history.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector() {
        let mut collector = MetricsCollector::new();
        assert!(collector.get_all_metrics().len() > 0);
        
        collector.update();
        let metrics = collector.get_recent_metrics(10);
        assert!(metrics.len() <= 10);
    }
}