import React from 'react';
import { motion } from 'framer-motion';
import {
  Bell, RefreshCw, Check, X, Clock, Image, User,
  Eye, CheckCircle, XCircle, AlertCircle, WifiOff
} from 'lucide-react';

function NotificationsPanel({ notifications, loading, onRefresh, isOnline }) {
  if (!isOnline) {
    return (
      <div className="flex flex-col items-center justify-center h-96 text-center">
        <div className="p-4 rounded-full bg-red-500/20 mb-4">
          <WifiOff className="w-12 h-12 text-red-400" />
        </div>
        <h3 className="text-xl font-semibold text-white mb-2">Not Connected</h3>
        <p className="text-gray-400 max-w-md">
          You need to be online to view notifications.
          Connect to the network to see responses to your requests.
        </p>
      </div>
    );
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'Accepted':
        return <CheckCircle className="w-5 h-5 text-green-400" />;
      case 'Rejected':
        return <XCircle className="w-5 h-5 text-red-400" />;
      default:
        return <Clock className="w-5 h-5 text-yellow-400" />;
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'Accepted':
        return 'green';
      case 'Rejected':
        return 'red';
      default:
        return 'yellow';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-white flex items-center gap-3">
            <Bell className="w-7 h-7 text-purple-400" />
            Notifications
          </h2>
          <p className="text-gray-400 mt-1">Track responses to your image requests</p>
        </div>
        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          onClick={onRefresh}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600/20 border border-purple-500/30 text-purple-400 hover:bg-purple-600/30 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </motion.button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: 'Accepted', status: 'Accepted', color: 'green' },
          { label: 'Rejected', status: 'Rejected', color: 'red' },
          { label: 'Pending', status: 'Pending', color: 'yellow' },
        ].map(({ label, status, color }) => {
          const count = notifications.filter(n => n.status === status).length;
          return (
            <div
              key={status}
              className={`p-4 rounded-xl bg-${color}-500/10 border border-${color}-500/20`}
            >
              <div className="flex items-center gap-2 mb-1">
                {getStatusIcon(status)}
                <span className={`text-sm text-${color}-400`}>{label}</span>
              </div>
              <p className="text-2xl font-display font-bold text-white">{count}</p>
            </div>
          );
        })}
      </div>

      {/* Notifications list */}
      {loading && notifications.length === 0 ? (
        <div className="flex items-center justify-center h-48">
          <div className="spinner w-8 h-8" />
        </div>
      ) : notifications.length === 0 ? (
        <div className="text-center py-16">
          <Bell className="w-16 h-16 text-gray-600 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-white mb-2">No notifications</h3>
          <p className="text-gray-400">
            When you request images from other users, responses will appear here.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {notifications.map((notification, index) => {
            const color = getStatusColor(notification.status);
            
            return (
              <motion.div
                key={notification.request_id}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.05 }}
                className={`cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm overflow-hidden border-l-4 border-${color}-500`}
              >
                <div className="p-6">
                  <div className="flex items-start justify-between">
                    <div className="flex items-start gap-4">
                      {/* Status icon */}
                      <div className={`p-3 rounded-xl bg-${color}-500/20`}>
                        {getStatusIcon(notification.status)}
                      </div>
                      
                      <div>
                        <div className="flex items-center gap-2 mb-2">
                          <span className={`px-2 py-0.5 rounded-full text-xs font-medium bg-${color}-500/20 text-${color}-400`}>
                            {notification.status}
                          </span>
                          <span className="text-gray-500 text-sm">•</span>
                          <span className="text-gray-400 text-sm flex items-center gap-1">
                            <Clock className="w-3 h-3" />
                            {notification.timestamp}
                          </span>
                        </div>
                        
                        <p className="text-white mb-2">
                          Your request to <span className="text-cyan-400 font-medium">{notification.to_user}</span> for image
                        </p>
                        
                        <div className="flex items-center gap-2 mb-3">
                          <Image className="w-4 h-4 text-purple-400" />
                          <span className="text-purple-400 font-medium">{notification.image_id}</span>
                        </div>

                        <div className="flex items-center gap-2 text-sm">
                          <div className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-cyan-600/20">
                            <Eye className="w-4 h-4 text-cyan-400" />
                            <span className="text-cyan-400">{notification.requested_views} views</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Action hint for accepted requests */}
                  {notification.status === 'Accepted' && (
                    <div className="mt-4 p-4 rounded-lg bg-green-500/10 border border-green-500/20">
                      <div className="flex items-start gap-2">
                        <CheckCircle className="w-5 h-5 text-green-400 flex-shrink-0" />
                        <div>
                          <p className="text-sm text-green-400 font-medium">Request Accepted!</p>
                          <p className="text-sm text-gray-400 mt-1">
                            The image should now be available in your Received Images.
                            Check the Images panel to view it.
                          </p>
                        </div>
                      </div>
                    </div>
                  )}

                  {notification.status === 'Rejected' && (
                    <div className="mt-4 p-4 rounded-lg bg-red-500/10 border border-red-500/20">
                      <div className="flex items-start gap-2">
                        <XCircle className="w-5 h-5 text-red-400 flex-shrink-0" />
                        <div>
                          <p className="text-sm text-red-400 font-medium">Request Rejected</p>
                          <p className="text-sm text-gray-400 mt-1">
                            The owner has declined your request. You may try requesting
                            again with different view parameters.
                          </p>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </motion.div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default NotificationsPanel;
