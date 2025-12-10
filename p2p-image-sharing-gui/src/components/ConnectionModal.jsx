import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { homeDir } from '@tauri-apps/api/path';
import {
  Wifi, User, Server, X, Shield, Zap
} from 'lucide-react';

function ConnectionModal({ onClose, onConnect, loading, directoryServers }) {
  const [username, setUsername] = useState('');
  const [port, setPort] = useState(8001);
  const [imagesDir, setImagesDir] = useState('');

  // Auto-detect home directory on mount
  useEffect(() => {
    const detectHomeDir = async () => {
      try {
        const home = await homeDir();
        // Set fixed path: ~/Documents/Distributed_project
        // homeDir() returns path with trailing slash, so we add Documents directly
        const basePath = home.endsWith('/') ? home.slice(0, -1) : home;
        setImagesDir(`${basePath}/Documents/Distributed_project`);
      } catch (e) {
        console.error('Failed to detect home directory:', e);
        // Fallback
        setImagesDir('/home/user/Documents/Distributed_project');
      }
    };
    detectHomeDir();
  }, []);

  const handleConnect = () => {
    if (username && port && imagesDir) {
      onConnect(username, port, imagesDir);
    }
  };

  const isValid = username && port && imagesDir;

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop p-4"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.9, opacity: 0, y: 20 }}
        animate={{ scale: 1, opacity: 1, y: 0 }}
        exit={{ scale: 0.9, opacity: 0, y: 20 }}
        onClick={(e) => e.stopPropagation()}
        className="bg-cyber-darker border border-purple-500/30 rounded-2xl w-full max-w-lg overflow-hidden"
      >
        {/* Header */}
        <div className="relative p-6 border-b border-purple-900/30 bg-gradient-to-r from-purple-900/30 to-pink-900/30">
          <button
            onClick={onClose}
            className="absolute top-4 right-4 p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
          
          <div className="flex items-center gap-4">
            <div className="p-3 rounded-xl bg-gradient-to-br from-purple-600 to-pink-600 glow-purple">
              <Wifi className="w-8 h-8 text-white" />
            </div>
            <div>
              <h2 className="text-2xl font-display font-bold text-white">Connect to Network</h2>
              <p className="text-gray-400 mt-1">Join the P2P image sharing network</p>
            </div>
          </div>
        </div>

        {/* Form */}
        <div className="p-6 space-y-6">
          {/* Username */}
          <div>
            <label className="block text-sm font-medium text-gray-400 mb-2">
              Username (Linux username)
            </label>
            <div className="relative">
              <User className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter your Linux username"
                className="w-full pl-12 pr-4 py-3 rounded-xl cyber-input text-white placeholder-gray-500"
              />
            </div>
            <p className="text-xs text-gray-500 mt-2">
              Your system username (e.g., andrew, freddy)
            </p>
          </div>

          {/* Port */}
          <div>
            <label className="block text-sm font-medium text-gray-400 mb-2">
              P2P Port
            </label>
            <div className="relative">
              <Server className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="number"
                value={port}
                onChange={(e) => setPort(parseInt(e.target.value) || 8001)}
                placeholder="8001"
                className="w-full pl-12 pr-4 py-3 rounded-xl cyber-input text-white placeholder-gray-500"
              />
            </div>
            <p className="text-xs text-gray-500 mt-2">
              Port for P2P connections. Default: 8001
            </p>
          </div>

          {/* Directory servers info */}
          <div className="p-4 rounded-xl bg-white/5 border border-purple-900/20">
            <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
              <Shield className="w-4 h-4 text-cyan-400" />
              Directory Servers
            </div>
            <div className="space-y-1">
              {directoryServers.slice(0, 3).map((server, i) => (
                <p key={i} className="text-xs font-mono text-gray-500">{server}</p>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="p-6 border-t border-purple-900/30 bg-black/20">
          <div className="flex gap-3">
            <button
              onClick={onClose}
              className="flex-1 px-4 py-3 rounded-xl border border-purple-500/30 text-gray-400 hover:bg-white/5 transition-colors font-medium"
            >
              Cancel
            </button>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={handleConnect}
              disabled={!isValid || loading}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 text-white font-medium disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? (
                <>
                  <div className="spinner w-5 h-5" />
                  Connecting...
                </>
              ) : (
                <>
                  <Zap className="w-5 h-5" />
                  Connect
                </>
              )}
            </motion.button>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
}

export default ConnectionModal;
