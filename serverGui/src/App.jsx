import React, { useState, useEffect } from 'react';
import { Play, Square, Activity, Zap, Server, Trash2 } from 'lucide-react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

const ServerDashboard = () => {
  const [servers, setServers] = useState([]);
  const [perfData, setPerfData] = useState([]);
  const [requestData, setRequestData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');

  // Fetch server statuses
  const fetchServerStatuses = async () => {
    try {
      const statuses = await invoke('get_server_statuses');
      setServers(statuses);
    } catch (error) {
      console.error('Error fetching server statuses:', error);
    }
  };

  // Fetch performance metrics
  const fetchPerformanceMetrics = async () => {
    try {
      const metrics = await invoke('get_performance_metrics');
      setPerfData(metrics);
    } catch (error) {
      console.error('Error fetching metrics:', error);
    }
  };

  // Generate request data by size
  const generateRequestsBySize = () => {
    const sizes = ['0-4 KB', '4-8 KB', '8-16 KB', '16-32 KB', '32-64 KB', '64-128 KB', '128 KB+'];
    return sizes.map(size => ({
      size,
      read: Math.floor(Math.random() * 500),
      write: Math.floor(Math.random() * 300)
    }));
  };

  // Initial data load
  useEffect(() => {
    fetchServerStatuses();
    fetchPerformanceMetrics();
    setRequestData(generateRequestsBySize());

    // Setup polling
    const statusInterval = setInterval(fetchServerStatuses, 2000);
    const metricsInterval = setInterval(fetchPerformanceMetrics, 2000);
    const requestInterval = setInterval(() => {
      setRequestData(generateRequestsBySize());
    }, 5000);

    // Listen for metrics update events
    const unlisten = listen('metrics-update', () => {
      fetchServerStatuses();
      fetchPerformanceMetrics();
    });

    return () => {
      clearInterval(statusInterval);
      clearInterval(metricsInterval);
      clearInterval(requestInterval);
      unlisten.then(fn => fn());
    };
  }, []);

  // Command handlers
  const handleStartAll = async () => {
    setLoading(true);
    setStatusMessage('Starting all servers...');
    try {
      const result = await invoke('start_all_servers');
      setStatusMessage(result);
      setTimeout(() => setStatusMessage(''), 3000);
    } catch (error) {
      setStatusMessage(`Error: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  const handleStopAll = async () => {
    setLoading(true);
    setStatusMessage('Stopping all servers...');
    try {
      const result = await invoke('stop_all_servers');
      setStatusMessage(result);
      setTimeout(() => setStatusMessage(''), 3000);
    } catch (error) {
      setStatusMessage(`Error: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSimulateFailure = async () => {
    // Simulate failure on Server-3 (port 8082)
    setLoading(true);
    setStatusMessage('Simulating failure on Server-3...');
    try {
      const result = await invoke('simulate_failure', { port: 8082 });
      setStatusMessage(result);
      setTimeout(() => setStatusMessage(''), 3000);
    } catch (error) {
      setStatusMessage(`Error: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  const handleClearLogs = async () => {
    try {
      const result = await invoke('clear_logs');
      setStatusMessage(result);
      setTimeout(() => setStatusMessage(''), 2000);
    } catch (error) {
      setStatusMessage(`Error: ${error}`);
    }
  };

  // Calculate cluster health
  const getClusterHealth = () => {
    if (servers.length === 0) return 'UNKNOWN';
    const runningCount = servers.filter(s => s.status === 'running').length;
    if (runningCount === servers.length) return 'HEALTHY';
    if (runningCount > 0) return 'DEGRADED';
    return 'CRITICAL';
  };

  const healthColor = {
    'HEALTHY': 'text-green-500',
    'DEGRADED': 'text-yellow-500',
    'CRITICAL': 'text-red-500',
    'UNKNOWN': 'text-gray-500'
  };

  return (
    <div className="min-h-screen bg-black p-4 font-mono">
      {/* Scanline effect */}
      <div className="fixed inset-0 pointer-events-none opacity-10"
        style={{
          background: 'repeating-linear-gradient(0deg, rgba(0,255,0,0.1), rgba(0,255,0,0.1) 1px, transparent 1px, transparent 2px)'
        }}
      ></div>

      <div className="relative z-10 border-2 border-green-500 p-6">
        {/* ASCII Header */}
        <pre className="text-green-500 text-xs mb-6 overflow-x-auto">
{`
╔═══════════════════════════════════════════════════════════════════════════╗
║              CLOUD P2P SERVER CONTROL TERMINAL - RAFT PROTOCOL            ║
║                         v1.0.0 - Fall 2025                                ║
║                  [DISTRIBUTED ELECTION • FAULT TOLERANCE]                 ║
╚═══════════════════════════════════════════════════════════════════════════╝
`}
        </pre>

        {/* Status Message */}
        {statusMessage && (
          <div className="bg-green-950 border border-green-500 p-3 mb-4 text-green-400 text-sm">
            ▶ {statusMessage}
          </div>
        )}

        {/* Command Bar */}
        <div className="bg-green-950 border border-green-500 p-4 mb-6">
          <div className="flex gap-4 flex-wrap items-center">
            <button 
              onClick={handleStartAll}
              disabled={loading}
              className="px-4 py-2 bg-green-700 hover:bg-green-600 disabled:bg-gray-700 text-black font-bold border-2 border-green-500 transition-all flex items-center gap-2"
            >
              <Play size={16} />
              [START ALL]
            </button>
            <button 
              onClick={handleStopAll}
              disabled={loading}
              className="px-4 py-2 bg-red-900 hover:bg-red-800 disabled:bg-gray-700 text-green-500 font-bold border-2 border-red-500 transition-all flex items-center gap-2"
            >
              <Square size={16} />
              [STOP ALL]
            </button>
            <button 
              onClick={handleSimulateFailure}
              disabled={loading}
              className="px-4 py-2 bg-yellow-900 hover:bg-yellow-800 disabled:bg-gray-700 text-green-500 font-bold border-2 border-yellow-500 transition-all flex items-center gap-2"
            >
              <Zap size={16} />
              [SIMULATE FAILURE]
            </button>
            <button 
              onClick={handleClearLogs}
              className="px-4 py-2 bg-gray-900 hover:bg-gray-800 text-green-500 font-bold border-2 border-gray-500 transition-all flex items-center gap-2"
            >
              <Trash2 size={16} />
              [CLEAR LOGS]
            </button>
            <div className="flex-1"></div>
            <div className="text-green-500 space-x-4">
              <span>CLUSTER_HEALTH: <span className={healthColor[getClusterHealth()]}>{getClusterHealth()}</span></span>
              <span>SYSTEM_TIME: {new Date().toLocaleTimeString()}</span>
            </div>
          </div>
        </div>

        {/* Server Grid */}
        <div className="grid grid-cols-3 gap-4 mb-6">
          {servers.length > 0 ? servers.map((server) => (
            <div key={server.id} className={`border-2 p-4 ${
              server.status === 'running' ? 'border-green-500 bg-green-950/30' : 'border-red-500 bg-red-950/30'
            }`}>
              <div className="text-green-500 space-y-2">
                <div className="flex justify-between items-center border-b border-green-500 pb-2">
                  <span className="font-bold text-lg flex items-center gap-2">
                    <Server size={16} />
                    [ {server.name} ]
                  </span>
                  <span className={server.status === 'running' ? 'text-green-400 animate-pulse' : 'text-red-500'}>
                    {server.status === 'running' ? '●ONLINE' : '●OFFLINE'}
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div>PORT: {server.port}</div>
                  <div className={server.role === 'LEADER' ? 'text-yellow-500 font-bold' : ''}>
                    ROLE: {server.role}
                  </div>
                </div>
                <div>LOAD: {'█'.repeat(Math.floor(server.load/10))}{'░'.repeat(10-Math.floor(server.load/10))} {server.load.toFixed(0)}%</div>
                <div className="text-xs">
                  <div>REQ/s: {server.request_rate}</div>
                  <div>ERROR: {(server.error_rate * 100).toFixed(3)}%</div>
                </div>
                <div className="text-xs pt-2 border-t border-green-500">
                  CONN: 127.0.0.1:{server.port}
                </div>
              </div>
            </div>
          )) : (
            <div className="col-span-3 text-center text-gray-500 py-8 border-2 border-gray-700">
              No servers running. Click [START ALL] to begin.
            </div>
          )}
        </div>

        {/* Performance Graphs Section */}
        {perfData.length > 0 && (
          <div className="grid grid-cols-2 gap-4 mb-6">
            {/* Load Balancer Request Traffic */}
            <div className="border-2 border-green-500 p-4 bg-black">
              <div className="text-green-500 mb-3 font-bold flex items-center gap-2">
                <Activity size={16} />
                <span>LOAD BALANCER REQUEST TRAFFIC</span>
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={perfData.slice(-30)}>
                  <defs>
                    <linearGradient id="colorReceived" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0.1}/>
                    </linearGradient>
                    <linearGradient id="colorSent" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#eab308" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#eab308" stopOpacity={0.1}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#163520" />
                  <XAxis dataKey="time" stroke="#22c55e" style={{fontSize: '10px'}} />
                  <YAxis stroke="#22c55e" style={{fontSize: '10px'}} />
                  <Tooltip 
                    contentStyle={{backgroundColor: '#000', border: '1px solid #22c55e', fontFamily: 'monospace'}}
                    labelStyle={{color: '#22c55e'}}
                  />
                  <Area type="monotone" dataKey="received" stroke="#22c55e" fillOpacity={1} fill="url(#colorReceived)" />
                  <Area type="monotone" dataKey="sent" stroke="#eab308" fillOpacity={1} fill="url(#colorSent)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Completion Rate */}
            <div className="border-2 border-green-500 p-4 bg-black">
              <div className="text-green-500 mb-3 font-bold flex items-center gap-2">
                <Activity size={16} />
                <span>REQUEST COMPLETION RATE</span>
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={perfData.slice(-30)}>
                  <defs>
                    <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.1}/>
                    </linearGradient>
                    <linearGradient id="colorGet" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0.1}/>
                    </linearGradient>
                    <linearGradient id="colorPut" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#eab308" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#eab308" stopOpacity={0.1}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#163520" />
                  <XAxis dataKey="time" stroke="#22c55e" style={{fontSize: '10px'}} />
                  <YAxis stroke="#22c55e" style={{fontSize: '10px'}} />
                  <Tooltip 
                    contentStyle={{backgroundColor: '#000', border: '1px solid #22c55e', fontFamily: 'monospace'}}
                    labelStyle={{color: '#22c55e'}}
                  />
                  <Area type="monotone" dataKey="total" stroke="#3b82f6" fillOpacity={1} fill="url(#colorTotal)" />
                  <Area type="monotone" dataKey="get" stroke="#22c55e" fillOpacity={1} fill="url(#colorGet)" />
                  <Area type="monotone" dataKey="put" stroke="#eab308" fillOpacity={1} fill="url(#colorPut)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Error Response Rate */}
            <div className="border-2 border-green-500 p-4 bg-black">
              <div className="text-green-500 mb-3 font-bold">ERROR RESPONSE RATE</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={perfData.slice(-30)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#163520" />
                  <XAxis dataKey="time" stroke="#22c55e" style={{fontSize: '10px'}} />
                  <YAxis stroke="#22c55e" style={{fontSize: '10px'}} />
                  <Tooltip 
                    contentStyle={{backgroundColor: '#000', border: '1px solid #22c55e', fontFamily: 'monospace'}}
                    labelStyle={{color: '#22c55e'}}
                    formatter={(value) => `${(value * 100).toFixed(3)}%`}
                  />
                  <Line type="monotone" dataKey="error_rate" stroke="#ef4444" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Average Request Duration */}
            <div className="border-2 border-green-500 p-4 bg-black">
              <div className="text-green-500 mb-3 font-bold">AVG REQUEST DURATION (Non-Error)</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={perfData.slice(-30)}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#163520" />
                  <XAxis dataKey="time" stroke="#22c55e" style={{fontSize: '10px'}} />
                  <YAxis stroke="#22c55e" style={{fontSize: '10px'}} />
                  <Tooltip 
                    contentStyle={{backgroundColor: '#000', border: '1px solid #22c55e', fontFamily: 'monospace'}}
                    labelStyle={{color: '#22c55e'}}
                    formatter={(value) => `${value.toFixed(2)}s`}
                  />
                  <Line type="monotone" dataKey="avg_duration" stroke="#22c55e" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/* Request Rate by Object Size */}
        {requestData.length > 0 && (
          <div className="border-2 border-green-500 p-4 bg-black mb-6">
            <div className="text-green-500 mb-3 font-bold">REQUEST RATE BY OBJECT SIZE</div>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={requestData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#163520" />
                <XAxis dataKey="size" stroke="#22c55e" style={{fontSize: '10px'}} />
                <YAxis stroke="#22c55e" style={{fontSize: '10px'}} />
                <Tooltip 
                  contentStyle={{backgroundColor: '#000', border: '1px solid #22c55e', fontFamily: 'monospace'}}
                  labelStyle={{color: '#22c55e'}}
                />
                <Bar dataKey="read" fill="#3b82f6" name="Read Requests" />
                <Bar dataKey="write" fill="#ef4444" name="Write Requests" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Console Output */}
        <div className="border-2 border-green-500 p-4 bg-black">
          <div className="text-green-500 mb-2 flex items-center gap-2">
            <Activity size={16} />
            <span className="font-bold">SYSTEM LOGS</span>
            <span className="text-xs ml-auto">[ LIVE FEED ]</span>
          </div>
          <div className="text-xs space-y-1 max-h-64 overflow-y-auto">
            {servers.length > 0 ? (
              <>
                {servers.flatMap(s => s.logs).map((log, i) => (
                  <div key={i} className={
                    log.includes('ERROR') ? 'text-red-500' : 
                    log.includes('WARN') ? 'text-yellow-500' : 
                    log.includes('LEADER') ? 'text-yellow-400' :
                    'text-green-400'
                  }>
                    {log}
                  </div>
                ))}
                <div className="text-green-500 animate-pulse">█ _</div>
              </>
            ) : (
              <div className="text-gray-500">No logs available. Start servers to see logs.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ServerDashboard;