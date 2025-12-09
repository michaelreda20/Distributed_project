import React from 'react';
import { motion } from 'framer-motion';
import {
  LayoutDashboard, Users, Image, Bell, Settings, Shield,
  ChevronLeft, ChevronRight, Inbox, Zap
} from 'lucide-react';

const menuItems = [
  { id: 'dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { id: 'peers', icon: Users, label: 'Peers' },
  { id: 'images', icon: Image, label: 'Images' },
  { id: 'requests', icon: Inbox, label: 'Requests', badge: 'requestCount' },
  { id: 'notifications', icon: Bell, label: 'Notifications', badge: 'notificationCount' },
  { id: 'settings', icon: Settings, label: 'Settings' },
];

function Sidebar({ 
  activeTab, 
  setActiveTab, 
  isOnline, 
  collapsed, 
  onToggleCollapse,
  notificationCount,
  requestCount 
}) {
  const badges = {
    requestCount,
    notificationCount
  };

  return (
    <motion.aside
      initial={false}
      animate={{ width: collapsed ? 80 : 260 }}
      transition={{ duration: 0.3, ease: 'easeInOut' }}
      className="relative h-full bg-cyber-darker border-r border-purple-900/30 flex flex-col z-10"
    >
      {/* Logo section */}
      <div className="h-20 flex items-center justify-center border-b border-purple-900/30 px-4">
        <motion.div
          initial={false}
          animate={{ opacity: collapsed ? 0 : 1, width: collapsed ? 0 : 'auto' }}
          className="flex items-center gap-3 overflow-hidden"
        >
          <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-purple-600 to-pink-600 flex items-center justify-center glow-purple">
            <Shield className="w-6 h-6 text-white" />
          </div>
          <div className="whitespace-nowrap">
            <h1 className="font-display font-bold text-lg text-white">P2P Share</h1>
            <p className="text-xs text-purple-400 font-mono">SECURE NETWORK</p>
          </div>
        </motion.div>
        {collapsed && (
          <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-purple-600 to-pink-600 flex items-center justify-center glow-purple">
            <Shield className="w-6 h-6 text-white" />
          </div>
        )}
      </div>

      {/* Status indicator */}
      <div className="px-4 py-4 border-b border-purple-900/30">
        <div className={`flex items-center gap-3 px-3 py-2 rounded-lg ${
          isOnline 
            ? 'bg-green-500/10 border border-green-500/30' 
            : 'bg-red-500/10 border border-red-500/30'
        }`}>
          <div className={`w-3 h-3 rounded-full ${
            isOnline ? 'bg-green-500 status-online' : 'bg-red-500'
          }`} />
          {!collapsed && (
            <span className={`text-sm font-medium ${
              isOnline ? 'text-green-400' : 'text-red-400'
            }`}>
              {isOnline ? 'Online' : 'Offline'}
            </span>
          )}
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {menuItems.map((item) => {
          const Icon = item.icon;
          const isActive = activeTab === item.id;
          const badgeCount = item.badge ? badges[item.badge] : 0;

          return (
            <motion.button
              key={item.id}
              whileHover={{ x: 4 }}
              whileTap={{ scale: 0.98 }}
              onClick={() => setActiveTab(item.id)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 relative ${
                isActive
                  ? 'bg-gradient-to-r from-purple-600/20 to-pink-600/20 text-white border border-purple-500/30'
                  : 'text-gray-400 hover:text-white hover:bg-white/5'
              }`}
            >
              <Icon className={`w-5 h-5 flex-shrink-0 ${isActive ? 'text-purple-400' : ''}`} />
              
              {!collapsed && (
                <>
                  <span className="font-medium">{item.label}</span>
                  {badgeCount > 0 && (
                    <span className="ml-auto px-2 py-0.5 text-xs font-bold rounded-full bg-pink-600 text-white">
                      {badgeCount}
                    </span>
                  )}
                </>
              )}

              {collapsed && badgeCount > 0 && (
                <span className="absolute -top-1 -right-1 w-5 h-5 text-xs font-bold rounded-full bg-pink-600 text-white flex items-center justify-center">
                  {badgeCount}
                </span>
              )}

              {isActive && (
                <motion.div
                  layoutId="activeTab"
                  className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-8 bg-gradient-to-b from-purple-500 to-pink-500 rounded-r-full"
                />
              )}
            </motion.button>
          );
        })}
      </nav>

      {/* Collapse toggle */}
      <button
        onClick={onToggleCollapse}
        className="absolute -right-3 top-24 w-6 h-6 rounded-full bg-cyber-darker border border-purple-500/30 flex items-center justify-center text-purple-400 hover:text-white hover:border-purple-500 transition-colors"
      >
        {collapsed ? (
          <ChevronRight className="w-4 h-4" />
        ) : (
          <ChevronLeft className="w-4 h-4" />
        )}
      </button>

      {/* Footer */}
      <div className="px-4 py-4 border-t border-purple-900/30">
        <div className={`flex items-center gap-2 text-xs text-gray-500 ${collapsed ? 'justify-center' : ''}`}>
          <Zap className="w-4 h-4 text-purple-500" />
          {!collapsed && <span>P2P Network v1.0</span>}
        </div>
      </div>
    </motion.aside>
  );
}

export default Sidebar;
