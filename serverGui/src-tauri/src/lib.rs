// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::{Manager, Emitter};
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

pub mod server_manager;
pub mod metrics;

use server_manager::{ServerManager, ServerConfig};
use metrics::MetricsCollector;

// Global state
struct AppState {
    server_manager: Mutex<ServerManager>,
    metrics_collector: Mutex<MetricsCollector>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ServerStatus {
    id: u8,
    name: String,
    port: u16,
    status: String,
    role: String,
    load: f32,
    request_rate: u32,
    error_rate: f32,
    logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct PerformanceData {
    time: String,
    received: u32,
    sent: u32,
    total: u32,
    get: u32,
    put: u32,
    completion_rate: f32,
    avg_duration: f32,
    error_rate: f32,
}

// Tauri Commands

#[tauri::command]
async fn start_all_servers(state: tauri::State<'_, AppState>) -> Result<String, String> {
    let servers = vec![
        ServerConfig {
            port: 8080,
            name: "server_1".to_string(),
            peers: vec!["127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string()],
        },
        ServerConfig {
            port: 8081,
            name: "server_2".to_string(),
            peers: vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8082".to_string()],
        },
        ServerConfig {
            port: 8082,
            name: "server_3".to_string(),
            peers: vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string()],
        },
    ];

    let mut manager = state.server_manager.lock().await;
    manager.start_all_servers(servers).await
        .map_err(|e| e.to_string())?;
    
    Ok("All servers started successfully".to_string())
}

#[tauri::command]
async fn stop_all_servers(state: tauri::State<'_, AppState>) -> Result<String, String> {
    let mut manager = state.server_manager.lock().await;
    manager.stop_all_servers().await
        .map_err(|e| e.to_string())?;
    
    Ok("All servers stopped successfully".to_string())
}

#[tauri::command]
async fn start_server(
    state: tauri::State<'_, AppState>,
    port: u16,
    name: String,
    peers: Vec<String>
) -> Result<String, String> {
    let config = ServerConfig { port, name, peers };
    
    let mut manager = state.server_manager.lock().await;
    manager.start_server(config).await
        .map_err(|e| e.to_string())?;
    
    Ok(format!("Server on port {} started", port))
}

#[tauri::command]
async fn stop_server(
    state: tauri::State<'_, AppState>,
    port: u16
) -> Result<String, String> {
    let mut manager = state.server_manager.lock().await;
    manager.stop_server(port).await
        .map_err(|e| e.to_string())?;
    
    Ok(format!("Server on port {} stopped", port))
}

#[tauri::command]
async fn simulate_failure(
    state: tauri::State<'_, AppState>,
    port: u16
) -> Result<String, String> {
    let mut manager = state.server_manager.lock().await;
    manager.simulate_failure(port).await
        .map_err(|e| e.to_string())?;
    
    Ok(format!("Simulating failure on port {} for 20 seconds", port))
}

#[tauri::command]
async fn get_server_statuses(state: tauri::State<'_, AppState>) -> Result<Vec<ServerStatus>, String> {
    let manager = state.server_manager.lock().await;
    let statuses = manager.get_server_statuses();
    
    Ok(statuses.into_iter().map(|s| ServerStatus {
        id: s.id,
        name: s.name,
        port: s.port,
        status: s.status,
        role: s.role,
        load: s.load,
        request_rate: s.request_rate,
        error_rate: s.error_rate,
        logs: s.logs,
    }).collect())
}

#[tauri::command]
async fn get_server_logs(
    state: tauri::State<'_, AppState>,
    port: u16
) -> Result<Vec<String>, String> {
    let manager = state.server_manager.lock().await;
    manager.get_server_logs(port)
        .ok_or_else(|| format!("Server on port {} not found", port))
}

#[tauri::command]
async fn get_performance_metrics(
    state: tauri::State<'_, AppState>
) -> Result<Vec<PerformanceData>, String> {
    let collector = state.metrics_collector.lock().await;
    let metrics = collector.get_recent_metrics(60); // Last 60 data points
    
    Ok(metrics.into_iter().map(|m| PerformanceData {
        time: m.timestamp,
        received: m.received,
        sent: m.sent,
        total: m.total,
        get: m.get,
        put: m.put,
        completion_rate: m.completion_rate,
        avg_duration: m.avg_duration,
        error_rate: m.error_rate,
    }).collect())
}

#[tauri::command]
async fn clear_logs(state: tauri::State<'_, AppState>) -> Result<String, String> {
    let mut manager = state.server_manager.lock().await;
    manager.clear_all_logs();
    Ok("All logs cleared".to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // Initialize state
            let server_manager = ServerManager::new();
            let metrics_collector = MetricsCollector::new();
            
            app.manage(AppState {
                server_manager: Mutex::new(server_manager),
                metrics_collector: Mutex::new(metrics_collector),
            });

            // Start metrics collection in background
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    
                    // Emit metrics update event
                    if let Some(window) = app_handle.get_webview_window("main") {
                        let _ = window.emit("metrics-update", ());
                    }
                }
            });

            Ok(())
        })
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            start_all_servers,
            stop_all_servers,
            start_server,
            stop_server,
            simulate_failure,
            get_server_statuses,
            get_server_logs,
            get_performance_metrics,
            clear_logs,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}