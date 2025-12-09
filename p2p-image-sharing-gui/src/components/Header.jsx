import React from 'react';
import { motion } from 'framer-motion';
import { Wifi, WifiOff, User, Power, Activity } from 'lucide-react';

function Header({ isOnline, username, onConnectionClick }) {
  return (
    <header className="h-16 bg-cyber-darker/80 backdrop-blur-sm border-b border-purple-900/30 flex items-center justify-between px-6">
      {/* Left side - breadcrumb/title area */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2 text-sm">
          <Activity className="w-4 h-4 text-purple-400 animate-pulse" />
          <span className="text-gray-400">System Status:</span>
          <span className={`font-semibold ${isOnline ? 'text-green-400' : 'text-red-400'}`}>
            {isOnline ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </div>

      {/* Right side - user info and controls */}
      <div className="flex items-center gap-4">
        {/* Network indicator */}
        <div className="flex items-center gap-3 px-4 py-2 rounded-lg bg-white/5 border border-purple-900/30">
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${
              isOnline ? 'bg-green-500 animate-pulse' : 'bg-red-500'
            }`} />
            <span className="text-sm text-gray-400">Network</span>
          </div>
          {isOnline && (
            <>
              <div className="w-px h-4 bg-purple-900/50" />
              <div className="flex items-center gap-2">
                <User className="w-4 h-4 text-purple-400" />
                <span className="text-sm font-medium text-white">{username}</span>
              </div>
            </>
          )}
        </div>

        {/* Connection button */}
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={onConnectionClick}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all cyber-button ${
            isOnline
              ? 'bg-red-600/20 border border-red-500/30 text-red-400 hover:bg-red-600/30'
              : 'bg-gradient-to-r from-purple-600 to-pink-600 text-white glow-purple'
          }`}
        >
          {isOnline ? (
            <>
              <WifiOff className="w-4 h-4" />
              <span>Disconnect</span>
            </>
          ) : (
            <>
              <Wifi className="w-4 h-4" />
              <span>Connect</span>
            </>
          )}
        </motion.button>
      </div>
    </header>
  );
}

export default Header;
