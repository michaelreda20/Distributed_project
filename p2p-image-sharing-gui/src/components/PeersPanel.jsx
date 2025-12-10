import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { invoke } from '@tauri-apps/api/core';
import {
  Users, RefreshCw, Search, Image, Send, Eye, Clock,
  ChevronDown, ChevronUp, Globe, Wifi, WifiOff, Loader
} from 'lucide-react';

function PeersPanel({ peers, loading, onRefresh, onRequestImage, isOnline }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedPeer, setExpandedPeer] = useState(null);
  const [requestModal, setRequestModal] = useState(null);
  const [requestViews, setRequestViews] = useState(5);
  const [thumbnails, setThumbnails] = useState({}); // { "peer_imageId": dataUrl }
  const [loadingThumbnails, setLoadingThumbnails] = useState({}); // { "peer_imageId": true/false }

  // Fetch thumbnails when peer is expanded
  useEffect(() => {
    if (expandedPeer) {
      const peer = peers.find(p => p.username === expandedPeer);
      if (peer && peer.shared_images && peer.status === 'Online') {
        peer.shared_images.forEach(async (image) => {
          const key = `${peer.username}_${image.image_id}`;
          // Only fetch if we don't have it and aren't already loading it
          if (!thumbnails[key] && !loadingThumbnails[key]) {
            setLoadingThumbnails(prev => ({ ...prev, [key]: true }));
            try {
              const result = await invoke('get_image_thumbnail', {
                peerUsername: peer.username,
                imageId: image.image_id
              });
              if (result.success && result.data) {
                setThumbnails(prev => ({ ...prev, [key]: result.data }));
              }
            } catch (e) {
              console.error('Failed to fetch thumbnail:', e);
            } finally {
              setLoadingThumbnails(prev => ({ ...prev, [key]: false }));
            }
          }
        });
      }
    }
  }, [expandedPeer, peers]);

  const filteredPeers = peers.filter(peer =>
    peer.username.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleRequestSubmit = () => {
    if (requestModal) {
      onRequestImage(requestModal.peer, requestModal.imageId, requestViews);
      setRequestModal(null);
      setRequestViews(5);
    }
  };

  if (!isOnline) {
    return (
      <div className="flex flex-col items-center justify-center h-96 text-center">
        <div className="p-4 rounded-full bg-red-500/20 mb-4">
          <WifiOff className="w-12 h-12 text-red-400" />
        </div>
        <h3 className="text-xl font-semibold text-white mb-2">Not Connected</h3>
        <p className="text-gray-400 max-w-md">
          You need to be online to discover and interact with peers.
          Connect to the network to see available peers.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-white flex items-center gap-3">
            <Users className="w-7 h-7 text-purple-400" />
            Peers
          </h2>
          <p className="text-gray-400 mt-1">Discover and interact with peers on the network</p>
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

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search peers..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-12 pr-4 py-3 rounded-xl cyber-input text-white placeholder-gray-500"
        />
      </div>

      {/* Peers list */}
      {loading && peers.length === 0 ? (
        <div className="flex items-center justify-center h-48">
          <div className="spinner w-8 h-8" />
        </div>
      ) : filteredPeers.length === 0 ? (
        <div className="text-center py-12">
          <Users className="w-12 h-12 text-gray-600 mx-auto mb-4" />
          <p className="text-gray-400">
            {searchTerm ? 'No peers match your search' : 'No peers online'}
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredPeers.map((peer, index) => (
            <motion.div
              key={peer.username}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.05 }}
              className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm overflow-hidden"
            >
              {/* Peer header */}
              <div
                className="p-4 flex items-center justify-between cursor-pointer hover:bg-white/5 transition-colors"
                onClick={() => setExpandedPeer(expandedPeer === peer.username ? null : peer.username)}
              >
                <div className="flex items-center gap-4">
                  <div className="w-12 h-12 rounded-full bg-gradient-to-br from-purple-600 to-pink-600 flex items-center justify-center text-white font-bold text-lg">
                    {peer.username.charAt(0).toUpperCase()}
                  </div>
                  <div>
                    <h3 className="font-semibold text-white">{peer.username}</h3>
                    <p className="text-sm text-gray-400 flex items-center gap-2">
                      <Globe className="w-3 h-3" />
                      {peer.p2p_address}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-2">
                    <div className={`w-2 h-2 rounded-full ${
                      peer.status === 'Online' ? 'bg-green-500' : 'bg-red-500'
                    }`} />
                    <span className={`text-sm ${
                      peer.status === 'Online' ? 'text-green-400' : 'text-red-400'
                    }`}>
                      {peer.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <Image className="w-4 h-4" />
                    {peer.shared_images?.length || 0} images
                  </div>
                  {expandedPeer === peer.username ? (
                    <ChevronUp className="w-5 h-5 text-gray-400" />
                  ) : (
                    <ChevronDown className="w-5 h-5 text-gray-400" />
                  )}
                </div>
              </div>

              {/* Expanded content - shared images */}
              <AnimatePresence>
                {expandedPeer === peer.username && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="border-t border-purple-900/30"
                  >
                    <div className="p-4">
                      <h4 className="text-sm font-medium text-gray-400 mb-3">Shared Images</h4>
                      {peer.shared_images && peer.shared_images.length > 0 ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                          {peer.shared_images.map((image) => {
                            const thumbnailKey = `${peer.username}_${image.image_id}`;
                            const thumbnail = thumbnails[thumbnailKey];
                            const isLoadingThumb = loadingThumbnails[thumbnailKey];
                            
                            return (
                              <div
                                key={image.image_id}
                                className="rounded-xl bg-white/5 border border-purple-900/30 overflow-hidden hover:border-purple-500/50 transition-all"
                              >
                                {/* Thumbnail Preview */}
                                <div className="relative w-full h-32 bg-gradient-to-br from-purple-900/20 to-pink-900/20 flex items-center justify-center">
                                  {isLoadingThumb ? (
                                    <Loader className="w-8 h-8 text-purple-400 animate-spin" />
                                  ) : thumbnail ? (
                                    <img 
                                      src={thumbnail} 
                                      alt={image.image_name}
                                      className="w-full h-full object-cover"
                                    />
                                  ) : (
                                    <div className="flex flex-col items-center gap-2">
                                      <Image className="w-10 h-10 text-purple-400/50" />
                                      <span className="text-xs text-gray-500">Preview unavailable</span>
                                    </div>
                                  )}
                                  {/* Blurred overlay indicator */}
                                  {thumbnail && (
                                    <div className="absolute bottom-2 right-2 px-2 py-1 rounded-md bg-black/60 backdrop-blur-sm">
                                      <span className="text-xs text-gray-300">Preview</span>
                                    </div>
                                  )}
                                </div>
                                
                                {/* Image info and request button */}
                                <div className="p-3 flex items-center justify-between">
                                  <div className="flex-1 min-w-0">
                                    <p className="text-sm font-medium text-white truncate">
                                      {image.image_name}
                                    </p>
                                    <p className="text-xs text-gray-500 truncate">
                                      ID: {image.image_id.slice(0, 12)}...
                                    </p>
                                  </div>
                                  <motion.button
                                    whileHover={{ scale: 1.05 }}
                                    whileTap={{ scale: 0.95 }}
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setRequestModal({ 
                                        peer: peer.username, 
                                        imageId: image.image_id, 
                                        imageName: image.image_name,
                                        thumbnail: thumbnail 
                                      });
                                    }}
                                    className="ml-2 p-2 rounded-lg bg-cyan-600/20 text-cyan-400 hover:bg-cyan-600/30 transition-colors flex items-center gap-1"
                                  >
                                    <Send className="w-4 h-4" />
                                  </motion.button>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      ) : (
                        <p className="text-sm text-gray-500">No images shared</p>
                      )}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          ))}
        </div>
      )}

      {/* Request Modal */}
      <AnimatePresence>
        {requestModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop"
            onClick={() => setRequestModal(null)}
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
              className="bg-cyber-darker border border-purple-500/30 rounded-2xl p-6 w-full max-w-md glow-purple"
            >
              <h3 className="text-xl font-display font-bold text-white mb-4">Request Image</h3>
              
              <div className="space-y-4">
                {/* Thumbnail preview in modal */}
                {requestModal.thumbnail && (
                  <div className="relative w-full h-40 rounded-lg overflow-hidden bg-gradient-to-br from-purple-900/20 to-pink-900/20">
                    <img 
                      src={requestModal.thumbnail} 
                      alt={requestModal.imageName}
                      className="w-full h-full object-cover"
                    />
                    <div className="absolute bottom-2 right-2 px-2 py-1 rounded-md bg-black/60 backdrop-blur-sm">
                      <span className="text-xs text-gray-300">Blurred Preview</span>
                    </div>
                  </div>
                )}
                
                <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
                  <p className="text-sm text-gray-400">From</p>
                  <p className="text-white font-medium">{requestModal.peer}</p>
                </div>
                
                <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
                  <p className="text-sm text-gray-400">Image</p>
                  <p className="text-white font-medium">{requestModal.imageName}</p>
                </div>

                <div>
                  <label className="block text-sm text-gray-400 mb-2">
                    Requested Views
                  </label>
                  <div className="flex items-center gap-4">
                    <input
                      type="range"
                      min="1"
                      max="100"
                      value={requestViews}
                      onChange={(e) => setRequestViews(parseInt(e.target.value))}
                      className="flex-1 h-2 bg-purple-900/30 rounded-full appearance-none cursor-pointer"
                    />
                    <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600/20 border border-purple-500/30">
                      <Eye className="w-4 h-4 text-purple-400" />
                      <span className="text-white font-mono w-8 text-center">{requestViews}</span>
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex gap-3 mt-6">
                <button
                  onClick={() => setRequestModal(null)}
                  className="flex-1 px-4 py-3 rounded-lg border border-purple-500/30 text-gray-400 hover:bg-white/5 transition-colors"
                >
                  Cancel
                </button>
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleRequestSubmit}
                  className="flex-1 px-4 py-3 rounded-lg bg-gradient-to-r from-purple-600 to-pink-600 text-white font-medium"
                >
                  Send Request
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default PeersPanel;
