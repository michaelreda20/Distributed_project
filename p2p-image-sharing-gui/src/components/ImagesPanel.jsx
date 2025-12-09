import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Image, Upload, Lock, Unlock, Eye, Edit, Trash2,
  FolderOpen, HardDrive, Download, Search, Filter,
  RefreshCw, Shield, WifiOff
} from 'lucide-react';

function ImagesPanel({ localImages, receivedImages, encryptedImages, onEncrypt, onUpdatePermissions, onRefresh, loading, isOnline }) {
  const [activeTab, setActiveTab] = useState('local');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedImage, setSelectedImage] = useState(null);
  const [permissionModal, setPermissionModal] = useState(null);
  const [newQuota, setNewQuota] = useState(5);
  const [targetUser, setTargetUser] = useState('');

  const filteredLocalImages = localImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const filteredReceivedImages = receivedImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const filteredEncryptedImages = encryptedImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleSelectFolder = () => {
    // In a real app, you'd use native file dialog
    // For now, we just show an alert
    alert('Please specify the images folder when connecting to the network');
  };

  const handleEncrypt = async (imagePath) => {
    const result = await onEncrypt(imagePath);
    if (result) {
      setSelectedImage(null);
    }
  };

  const handleUpdatePermissions = () => {
    if (permissionModal && targetUser) {
      onUpdatePermissions(targetUser, permissionModal.image_id, newQuota);
      setPermissionModal(null);
      setTargetUser('');
      setNewQuota(5);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-white flex items-center gap-3">
            <Image className="w-7 h-7 text-purple-400" />
            Image Library
          </h2>
          <p className="text-gray-400 mt-1">Manage your local and received images</p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-4 border-b border-purple-900/30">
        <button
          onClick={() => setActiveTab('local')}
          className={`px-4 py-3 text-sm font-medium transition-colors relative ${
            activeTab === 'local'
              ? 'text-white'
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <div className="flex items-center gap-2">
            <HardDrive className="w-4 h-4" />
            Local Images
            <span className="px-2 py-0.5 text-xs rounded-full bg-purple-600/20 text-purple-400">
              {localImages.length}
            </span>
          </div>
          {activeTab === 'local' && (
            <motion.div
              layoutId="imageTab"
              className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-purple-500 to-pink-500"
            />
          )}
        </button>
        <button
          onClick={() => setActiveTab('encrypted')}
          className={`px-4 py-3 text-sm font-medium transition-colors relative ${
            activeTab === 'encrypted'
              ? 'text-white'
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <div className="flex items-center gap-2">
            <Lock className="w-4 h-4" />
            Encrypted Images
            <span className="px-2 py-0.5 text-xs rounded-full bg-green-600/20 text-green-400">
              {encryptedImages.length}
            </span>
          </div>
          {activeTab === 'encrypted' && (
            <motion.div
              layoutId="imageTab"
              className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-purple-500 to-pink-500"
            />
          )}
        </button>
        <button
          onClick={() => setActiveTab('received')}
          className={`px-4 py-3 text-sm font-medium transition-colors relative ${
            activeTab === 'received'
              ? 'text-white'
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <div className="flex items-center gap-2">
            <Download className="w-4 h-4" />
            Received Images
            <span className="px-2 py-0.5 text-xs rounded-full bg-cyan-600/20 text-cyan-400">
              {receivedImages.length}
            </span>
          </div>
          {activeTab === 'received' && (
            <motion.div
              layoutId="imageTab"
              className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-purple-500 to-pink-500"
            />
          )}
        </button>
      </div>

      {/* Search and filters */}
      <div className="flex items-center gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search images..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-12 pr-4 py-3 rounded-xl cyber-input text-white placeholder-gray-500"
          />
        </div>
        {activeTab === 'local' && (
          <>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={onRefresh}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-3 rounded-xl bg-cyan-600/20 border border-cyan-500/30 text-cyan-400 hover:bg-cyan-600/30 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </motion.button>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={handleSelectFolder}
              className="flex items-center gap-2 px-4 py-3 rounded-xl bg-purple-600/20 border border-purple-500/30 text-purple-400 hover:bg-purple-600/30 transition-colors"
            >
              <FolderOpen className="w-4 h-4" />
              Select Folder
            </motion.button>
          </>
        )}
      </div>

      {/* Content */}
      <AnimatePresence mode="wait">
        {activeTab === 'local' ? (
          <motion.div
            key="local"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
          >
            {filteredLocalImages.length === 0 ? (
              <div className="text-center py-16">
                <Image className="w-16 h-16 text-gray-600 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-white mb-2">No local images</h3>
                <p className="text-gray-400 mb-4">
                  {searchTerm ? 'No images match your search' : 'Add images to your folder to share them'}
                </p>
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleSelectFolder}
                  className="inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-gradient-to-r from-purple-600 to-pink-600 text-white font-medium"
                >
                  <FolderOpen className="w-5 h-5" />
                  Select Images Folder
                </motion.button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredLocalImages.map((image, index) => (
                  <motion.div
                    key={image.image_id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.05 }}
                    className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm overflow-hidden"
                  >
                    {/* Image preview placeholder */}
                    <div className="h-40 bg-gradient-to-br from-purple-900/40 to-pink-900/40 flex items-center justify-center">
                      <Image className="w-16 h-16 text-purple-400/50" />
                    </div>
                    
                    <div className="p-4">
                      <div className="flex items-start justify-between mb-2">
                        <div>
                          <h3 className="font-medium text-white truncate" title={image.file_name}>
                            {image.file_name}
                          </h3>
                          <p className="text-sm text-gray-400">{image.file_size_kb} KB</p>
                        </div>
                        {image.is_encrypted ? (
                          <div className="p-1.5 rounded-lg bg-green-600/20">
                            <Lock className="w-4 h-4 text-green-400" />
                          </div>
                        ) : (
                          <div className="p-1.5 rounded-lg bg-yellow-600/20">
                            <Unlock className="w-4 h-4 text-yellow-400" />
                          </div>
                        )}
                      </div>

                      <div className="flex items-center gap-2 mt-4">
                        {!image.is_encrypted && isOnline && (
                          <motion.button
                            whileHover={{ scale: 1.02 }}
                            whileTap={{ scale: 0.98 }}
                            onClick={() => handleEncrypt(image.file_path)}
                            className="flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-purple-600/20 border border-purple-500/30 text-purple-400 text-sm hover:bg-purple-600/30 transition-colors"
                          >
                            <Shield className="w-4 h-4" />
                            Encrypt
                          </motion.button>
                        )}
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => setPermissionModal(image)}
                          className="flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-cyan-600/20 border border-cyan-500/30 text-cyan-400 text-sm hover:bg-cyan-600/30 transition-colors"
                        >
                          <Edit className="w-4 h-4" />
                          Permissions
                        </motion.button>
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            )}
          </motion.div>
        ) : activeTab === 'encrypted' ? (
          <motion.div
            key="encrypted"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
          >
            {filteredEncryptedImages.length === 0 ? (
              <div className="text-center py-16">
                <Lock className="w-16 h-16 text-gray-600 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-white mb-2">No encrypted images</h3>
                <p className="text-gray-400">
                  {searchTerm ? 'No images match your search' : 'Encrypted images will appear here'}
                </p>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredEncryptedImages.map((image, index) => (
                  <motion.div
                    key={image.image_id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.05 }}
                    className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm overflow-hidden"
                  >
                    {/* Image preview placeholder */}
                    <div className="h-40 bg-gradient-to-br from-green-900/40 to-emerald-900/40 flex items-center justify-center">
                      <Shield className="w-16 h-16 text-green-400/50" />
                    </div>

                    <div className="p-4">
                      <div className="flex items-start justify-between mb-2">
                        <div>
                          <h3 className="font-medium text-white truncate" title={image.file_name}>
                            {image.file_name}
                          </h3>
                          <p className="text-sm text-gray-400">{image.file_size_kb} KB</p>
                        </div>
                        <div className="p-1.5 rounded-lg bg-green-600/20">
                          <Lock className="w-4 h-4 text-green-400" />
                        </div>
                      </div>

                      <div className="flex items-center gap-2 mt-4">
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => setPermissionModal(image)}
                          className="flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-cyan-600/20 border border-cyan-500/30 text-cyan-400 text-sm hover:bg-cyan-600/30 transition-colors"
                        >
                          <Edit className="w-4 h-4" />
                          Permissions
                        </motion.button>
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            )}
          </motion.div>
        ) : (
          <motion.div
            key="received"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
          >
            {filteredReceivedImages.length === 0 ? (
              <div className="text-center py-16">
                <Download className="w-16 h-16 text-gray-600 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-white mb-2">No received images</h3>
                <p className="text-gray-400">
                  {searchTerm ? 'No images match your search' : 'Images shared with you will appear here'}
                </p>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredReceivedImages.map((image, index) => (
                  <motion.div
                    key={image.image_id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.05 }}
                    className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm overflow-hidden"
                  >
                    {/* Image preview placeholder */}
                    <div className="h-40 bg-gradient-to-br from-cyan-900/40 to-blue-900/40 flex items-center justify-center">
                      <Image className="w-16 h-16 text-cyan-400/50" />
                    </div>
                    
                    <div className="p-4">
                      <div className="flex items-start justify-between mb-2">
                        <div>
                          <h3 className="font-medium text-white truncate" title={image.file_name}>
                            {image.file_name}
                          </h3>
                          <p className="text-sm text-gray-400">From: {image.from_owner}</p>
                        </div>
                        <div className="flex items-center gap-1 px-2 py-1 rounded-lg bg-cyan-600/20">
                          <Eye className="w-3 h-3 text-cyan-400" />
                          <span className="text-xs font-mono text-cyan-400">{image.views_remaining}</span>
                        </div>
                      </div>

                      <p className="text-xs text-gray-500 mt-2">
                        Received: {image.received_at}
                      </p>

                      <motion.button
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                        disabled={image.views_remaining === 0}
                        className="w-full flex items-center justify-center gap-2 px-3 py-2 mt-4 rounded-lg bg-gradient-to-r from-cyan-600/20 to-blue-600/20 border border-cyan-500/30 text-cyan-400 text-sm hover:from-cyan-600/30 hover:to-blue-600/30 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        <Eye className="w-4 h-4" />
                        View Image
                      </motion.button>
                    </div>
                  </motion.div>
                ))}
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Permission Modal */}
      <AnimatePresence>
        {permissionModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop"
            onClick={() => setPermissionModal(null)}
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
              className="bg-cyber-darker border border-purple-500/30 rounded-2xl p-6 w-full max-w-md glow-purple"
            >
              <h3 className="text-xl font-display font-bold text-white mb-4">Update Permissions</h3>
              
              <div className="space-y-4">
                <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
                  <p className="text-sm text-gray-400">Image</p>
                  <p className="text-white font-medium">{permissionModal.file_name}</p>
                </div>

                <div>
                  <label className="block text-sm text-gray-400 mb-2">
                    Target Username
                  </label>
                  <input
                    type="text"
                    value={targetUser}
                    onChange={(e) => setTargetUser(e.target.value)}
                    placeholder="Enter username"
                    className="w-full px-4 py-3 rounded-lg cyber-input text-white placeholder-gray-500"
                  />
                </div>

                <div>
                  <label className="block text-sm text-gray-400 mb-2">
                    New View Quota
                  </label>
                  <div className="flex items-center gap-4">
                    <input
                      type="range"
                      min="0"
                      max="100"
                      value={newQuota}
                      onChange={(e) => setNewQuota(parseInt(e.target.value))}
                      className="flex-1 h-2 bg-purple-900/30 rounded-full appearance-none cursor-pointer"
                    />
                    <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600/20 border border-purple-500/30">
                      <Eye className="w-4 h-4 text-purple-400" />
                      <span className="text-white font-mono w-8 text-center">{newQuota}</span>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 mt-2">
                    Set to 0 to revoke access
                  </p>
                </div>
              </div>

              <div className="flex gap-3 mt-6">
                <button
                  onClick={() => setPermissionModal(null)}
                  className="flex-1 px-4 py-3 rounded-lg border border-purple-500/30 text-gray-400 hover:bg-white/5 transition-colors"
                >
                  Cancel
                </button>
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleUpdatePermissions}
                  disabled={!targetUser}
                  className="flex-1 px-4 py-3 rounded-lg bg-gradient-to-r from-purple-600 to-pink-600 text-white font-medium disabled:opacity-50"
                >
                  Update
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default ImagesPanel;
