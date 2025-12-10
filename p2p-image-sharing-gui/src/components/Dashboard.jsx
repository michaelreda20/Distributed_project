import React from 'react';
import { motion } from 'framer-motion';
import {
  Users, Image, Inbox, Bell, Wifi, WifiOff,
  Shield, Activity, Server, Globe, TrendingUp, Zap
} from 'lucide-react';

const statCards = [
  {
    id: 'peers',
    label: 'Peers',
    icon: Users,
    color: 'cyan',
    gradient: 'from-cyan-600 to-blue-600'
  },
  {
    id: 'images',
    label: 'Shared Images',
    icon: Image,
    color: 'purple',
    gradient: 'from-purple-600 to-pink-600'
  },
  {
    id: 'requests',
    label: 'Pending Requests',
    icon: Inbox,
    color: 'yellow',
    gradient: 'from-yellow-600 to-orange-600'
  },
  {
    id: 'notifications',
    label: 'Notifications',
    icon: Bell,
    color: 'green',
    gradient: 'from-green-600 to-emerald-600'
  }
];

function Dashboard({
  isOnline,
  username,
  peersCount,
  imagesCount,
  requestsCount,
  notificationsCount,
  onGoOnline,
  onGoOffline
}) {
  const stats = {
    peers: peersCount,
    images: imagesCount,
    requests: requestsCount,
    notifications: notificationsCount
  };

  return (
    <div className="space-y-6">
      {/* Welcome section */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-purple-900/40 to-pink-900/40 border border-purple-500/20 p-8">
        <div className="relative z-10">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 }}
          >
            <h1 className="font-display text-4xl font-bold text-white mb-2">
              {isOnline ? `Welcome back, ${username}` : 'P2P Image Sharing Network'}
            </h1>
            <p className="text-lg text-gray-300 max-w-2xl">
              {isOnline
                ? 'Your secure peer-to-peer network is active. Share images with encryption and view permissions.'
                : 'Connect to the distributed network to share encrypted images with controlled viewing permissions.'}
            </p>
          </motion.div>

          {!isOnline && (
            <motion.button
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={onGoOnline}
              className="mt-6 inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-gradient-to-r from-purple-600 to-pink-600 text-white font-semibold glow-purple cyber-button"
            >
              <Wifi className="w-5 h-5" />
              Connect to Network
            </motion.button>
          )}
        </div>

        {/* Background decoration */}
        <div className="absolute top-0 right-0 w-96 h-96 bg-gradient-to-br from-purple-500/20 to-pink-500/20 rounded-full blur-3xl" />
        <div className="absolute -bottom-20 -left-20 w-64 h-64 bg-gradient-to-br from-cyan-500/10 to-blue-500/10 rounded-full blur-3xl" />
        
        {/* Grid pattern overlay */}
        <div className="absolute inset-0 cyber-grid opacity-30" />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {statCards.map((card, index) => {
          const Icon = card.icon;
          const value = stats[card.id];

          return (
            <motion.div
              key={card.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 + index * 0.05 }}
              className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
            >
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-sm text-gray-400 mb-1">{card.label}</p>
                  <p className="text-3xl font-display font-bold text-white">
                    {isOnline ? value : '-'}
                  </p>
                </div>
                <div className={`p-3 rounded-lg bg-gradient-to-br ${card.gradient} bg-opacity-20`}>
                  <Icon className="w-6 h-6 text-white" />
                </div>
              </div>
              {isOnline && (
                <div className="mt-4 pt-4 border-t border-purple-900/30">
                  <div className="flex items-center gap-2 text-sm">
                    <TrendingUp className={`w-4 h-4 text-${card.color}-400`} />
                    <span className="text-gray-400">Active</span>
                  </div>
                </div>
              )}
            </motion.div>
          );
        })}
      </div>

      {/* System info grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Network Status */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
        >
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 rounded-lg bg-purple-600/20">
              <Globe className="w-5 h-5 text-purple-400" />
            </div>
            <h3 className="font-semibold text-white">Network Status</h3>
          </div>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Connection</span>
              <span className={`text-sm font-medium ${isOnline ? 'text-green-400' : 'text-red-400'}`}>
                {isOnline ? 'Active' : 'Inactive'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Protocol</span>
              <span className="text-sm font-medium text-white">TCP/P2P</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Encryption</span>
              <span className="text-sm font-medium text-cyan-400">LSB Steganography</span>
            </div>
          </div>
        </motion.div>

        {/* Security Info */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.35 }}
          className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
        >
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 rounded-lg bg-green-600/20">
              <Shield className="w-5 h-5 text-green-400" />
            </div>
            <h3 className="font-semibold text-white">Security</h3>
          </div>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">View Tracking</span>
              <span className="text-sm font-medium text-green-400">Enabled</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Permission Control</span>
              <span className="text-sm font-medium text-green-400">Active</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Quota System</span>
              <span className="text-sm font-medium text-green-400">Enforced</span>
            </div>
          </div>
        </motion.div>

        {/* Server Info */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
        >
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 rounded-lg bg-cyan-600/20">
              <Server className="w-5 h-5 text-cyan-400" />
            </div>
            <h3 className="font-semibold text-white">Directory Service</h3>
          </div>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Servers</span>
              <span className="text-sm font-medium text-white">3 Replicated</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Consensus</span>
              <span className="text-sm font-medium text-cyan-400">Raft Protocol</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-400">Persistence</span>
              <span className="text-sm font-medium text-green-400">Enabled</span>
            </div>
          </div>
        </motion.div>
      </div>

      {/* Features overview */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.45 }}
        className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
      >
        <h3 className="font-semibold text-white mb-4 flex items-center gap-2">
          <Zap className="w-5 h-5 text-yellow-400" />
          Key Features
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { title: 'Encrypted Sharing', desc: 'LSB steganography protection' },
            { title: 'View Quotas', desc: 'Control how many times images can be viewed' },
            { title: 'Offline Requests', desc: 'Queue requests for offline users' },
            { title: 'Permission Updates', desc: 'Remotely modify access rights' },
          ].map((feature, i) => (
            <div key={i} className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
              <h4 className="font-medium text-white mb-1">{feature.title}</h4>
              <p className="text-sm text-gray-400">{feature.desc}</p>
            </div>
          ))}
        </div>
      </motion.div>
    </div>
  );
}

export default Dashboard;
