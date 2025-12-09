import React from 'react';
import { motion } from 'framer-motion';
import {
  Inbox, RefreshCw, Check, X, Clock, Image, User,
  Eye, AlertCircle, WifiOff
} from 'lucide-react';

function RequestsPanel({ requests, loading, onRefresh, onRespond, isOnline }) {
  if (!isOnline) {
    return (
      <div className="flex flex-col items-center justify-center h-96 text-center">
        <div className="p-4 rounded-full bg-red-500/20 mb-4">
          <WifiOff className="w-12 h-12 text-red-400" />
        </div>
        <h3 className="text-xl font-semibold text-white mb-2">Not Connected</h3>
        <p className="text-gray-400 max-w-md">
          You need to be online to view and respond to requests.
          Pending requests will be shown when you connect.
        </p>
      </div>
    );
  }

  const pendingRequests = requests.filter(r => r.status === 'Pending');

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-white flex items-center gap-3">
            <Inbox className="w-7 h-7 text-purple-400" />
            Pending Requests
          </h2>
          <p className="text-gray-400 mt-1">Review and respond to image access requests</p>
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

      {/* Info banner */}
      <div className="flex items-start gap-3 p-4 rounded-xl bg-yellow-500/10 border border-yellow-500/20">
        <AlertCircle className="w-5 h-5 text-yellow-400 flex-shrink-0 mt-0.5" />
        <div>
          <p className="text-sm text-yellow-400 font-medium">About Requests</p>
          <p className="text-sm text-gray-400 mt-1">
            When you accept a request, the image will be shared with the requesting user with the specified view quota.
            You can later update or revoke their access from the Images panel.
          </p>
        </div>
      </div>

      {/* Requests list */}
      {loading && requests.length === 0 ? (
        <div className="flex items-center justify-center h-48">
          <div className="spinner w-8 h-8" />
        </div>
      ) : pendingRequests.length === 0 ? (
        <div className="text-center py-16">
          <Inbox className="w-16 h-16 text-gray-600 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-white mb-2">No pending requests</h3>
          <p className="text-gray-400">
            When other users request access to your images, they will appear here.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {pendingRequests.map((request, index) => (
            <motion.div
              key={request.request_id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.05 }}
              className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-4">
                  {/* Avatar */}
                  <div className="w-12 h-12 rounded-full bg-gradient-to-br from-cyan-600 to-blue-600 flex items-center justify-center text-white font-bold text-lg flex-shrink-0">
                    {request.from_user.charAt(0).toUpperCase()}
                  </div>
                  
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <User className="w-4 h-4 text-gray-400" />
                      <span className="font-medium text-white">{request.from_user}</span>
                      <span className="text-gray-500">requests access to</span>
                    </div>
                    
                    <div className="flex items-center gap-2 mb-3">
                      <Image className="w-4 h-4 text-purple-400" />
                      <span className="text-purple-400 font-medium">{request.image_id}</span>
                    </div>

                    <div className="flex items-center gap-4 text-sm">
                      <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-cyan-600/20">
                        <Eye className="w-4 h-4 text-cyan-400" />
                        <span className="text-cyan-400">{request.requested_views} views</span>
                      </div>
                      <div className="flex items-center gap-2 text-gray-400">
                        <Clock className="w-4 h-4" />
                        <span>{request.timestamp}</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Actions */}
                <div className="flex items-center gap-2">
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={() => onRespond(request.request_id, false)}
                    className="p-3 rounded-xl bg-red-600/20 border border-red-500/30 text-red-400 hover:bg-red-600/30 transition-colors"
                    title="Reject"
                  >
                    <X className="w-5 h-5" />
                  </motion.button>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={() => onRespond(request.request_id, true)}
                    className="p-3 rounded-xl bg-green-600/20 border border-green-500/30 text-green-400 hover:bg-green-600/30 transition-colors"
                    title="Accept"
                  >
                    <Check className="w-5 h-5" />
                  </motion.button>
                </div>
              </div>

              {/* Request ID */}
              <div className="mt-4 pt-4 border-t border-purple-900/30">
                <p className="text-xs text-gray-500 font-mono">
                  Request ID: {request.request_id}
                </p>
              </div>
            </motion.div>
          ))}
        </div>
      )}

      {/* Request count summary */}
      {pendingRequests.length > 0 && (
        <div className="flex items-center justify-center">
          <div className="px-4 py-2 rounded-full bg-purple-600/20 border border-purple-500/30">
            <span className="text-sm text-purple-400">
              {pendingRequests.length} pending request{pendingRequests.length !== 1 ? 's' : ''}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}

export default RequestsPanel;
