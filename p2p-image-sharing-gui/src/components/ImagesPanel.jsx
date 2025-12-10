import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { convertFileSrc } from '@tauri-apps/api/core';
import {
  Image, Upload, Lock, Unlock, Eye, Edit, Trash2,
  HardDrive, Download, Search,
  RefreshCw, Shield, WifiOff, X
} from 'lucide-react';

function ImagesPanel({ localImages, receivedImages, encryptedImages, onEncrypt, onUpdatePermissions, onRefresh, onViewImage, onDeleteImage, loading, isOnline }) {
  const [activeTab, setActiveTab] = useState('local');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedImage, setSelectedImage] = useState(null);
  const [permissionModal, setPermissionModal] = useState(null);
  const [newQuota, setNewQuota] = useState(5);
  const [targetUser, setTargetUser] = useState('');
  const [viewingImage, setViewingImage] = useState(null);
  const [viewedImagePath, setViewedImagePath] = useState(null);
  const [deleteConfirmModal, setDeleteConfirmModal] = useState(null);

  const filteredLocalImages = localImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const filteredReceivedImages = receivedImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const filteredEncryptedImages = encryptedImages.filter(img =>
    img.file_name.toLowerCase().includes(searchTerm.toLowerCase())
  );


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

  const handleViewImage = async (image) => {
    if (image.views_remaining <= 0) {
      // No views remaining, show the cover image (the encrypted carrier)
      setViewingImage({...image, views_remaining: 0});
      setViewedImagePath(null); // Will display the cover/carrier image
      return;
    }
    
    // Attempt to view the image (decrements quota)
    const viewablePath = await onViewImage(image.file_path);
    if (viewablePath) {
      // Successfully viewed - update the views count in the modal
      setViewingImage({...image, views_remaining: image.views_remaining - 1});
      setViewedImagePath(viewablePath);
    } else {
      // Access denied - show the cover image
      setViewingImage({...image, views_remaining: 0});
      setViewedImagePath(null);
    }
  };

  const closeImageViewer = () => {
    setViewingImage(null);
    setViewedImagePath(null);
  };

  const handleDeleteConfirm = async () => {
    if (deleteConfirmModal) {
      await onDeleteImage(deleteConfirmModal.file_path, deleteConfirmModal.type);
      setDeleteConfirmModal(null);
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
                <p className="text-gray-400">
                  {searchTerm ? 'No images match your search' : 'Add images to your folder to share them'}
                </p>
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
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => setDeleteConfirmModal({ ...image, type: 'local' })}
                          className="p-2 rounded-lg bg-red-600/20 border border-red-500/30 text-red-400 hover:bg-red-600/30 transition-colors"
                          title="Delete image"
                        >
                          <Trash2 className="w-4 h-4" />
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
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => setDeleteConfirmModal({ ...image, type: 'encrypted' })}
                          className="p-2 rounded-lg bg-red-600/20 border border-red-500/30 text-red-400 hover:bg-red-600/30 transition-colors"
                          title="Delete image"
                        >
                          <Trash2 className="w-4 h-4" />
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

                      <div className="flex items-center gap-2 mt-4">
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => handleViewImage(image)}
                          className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded-lg ${
                            image.views_remaining === 0
                              ? 'bg-gray-600/20 border border-gray-500/30 text-gray-400'
                              : 'bg-gradient-to-r from-cyan-600/20 to-blue-600/20 border border-cyan-500/30 text-cyan-400 hover:from-cyan-600/30 hover:to-blue-600/30'
                          } text-sm transition-colors`}
                        >
                          <Eye className="w-4 h-4" />
                          {image.views_remaining === 0 ? 'View Cover' : 'View Image'}
                        </motion.button>
                        <motion.button
                          whileHover={{ scale: 1.02 }}
                          whileTap={{ scale: 0.98 }}
                          onClick={() => setDeleteConfirmModal({ ...image, type: 'received' })}
                          className="p-2 rounded-lg bg-red-600/20 border border-red-500/30 text-red-400 hover:bg-red-600/30 transition-colors"
                          title="Delete image"
                        >
                          <Trash2 className="w-4 h-4" />
                        </motion.button>
                      </div>
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
                  {newQuota === 0 ? (
                    <p className="text-xs text-red-400 mt-2">
                      ⚠️ Setting quota to 0 will REVOKE access for this user
                    </p>
                  ) : (
                    <p className="text-xs text-gray-500 mt-2">
                      Set to 0 to revoke access
                    </p>
                  )}
                </div>

                {/* Info about multicast */}
                <div className="p-3 rounded-lg bg-cyan-900/20 border border-cyan-500/20">
                  <p className="text-xs text-cyan-400">
                    📡 This update will be multicast to all directory servers and delivered to the user
                    {targetUser ? ` "${targetUser}"` : ''}.
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
                  className={`flex-1 px-4 py-3 rounded-lg text-white font-medium disabled:opacity-50 ${
                    newQuota === 0 
                      ? 'bg-gradient-to-r from-red-600 to-orange-600' 
                      : 'bg-gradient-to-r from-purple-600 to-pink-600'
                  }`}
                >
                  {newQuota === 0 ? 'Revoke Access' : 'Update & Send'}
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Image Viewer Modal */}
      <AnimatePresence>
        {viewingImage && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop bg-black/80"
            onClick={closeImageViewer}
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
              className="bg-cyber-darker border border-cyan-500/30 rounded-2xl p-6 w-full max-w-3xl max-h-[90vh] overflow-auto glow-cyan"
            >
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xl font-display font-bold text-white">
                  {viewedImagePath ? 'Viewing Image' : 'Access Denied - Cover Image'}
                </h3>
                <button
                  onClick={closeImageViewer}
                  className="p-2 rounded-lg bg-white/5 hover:bg-white/10 transition-colors"
                >
                  <X className="w-5 h-5 text-gray-400" />
                </button>
              </div>

              <div className="space-y-4">
                {/* Image display */}
                <div className="relative rounded-lg overflow-hidden bg-black/50 flex items-center justify-center min-h-[300px]">
                  {viewedImagePath ? (
                    <img
                      src={convertFileSrc(viewedImagePath)}
                      alt={viewingImage.file_name}
                      className="max-w-full max-h-[60vh] object-contain"
                    />
                  ) : (
                    <div className="flex flex-col items-center justify-center p-8 text-center">
                      <Lock className="w-16 h-16 text-red-400/50 mb-4" />
                      <p className="text-red-400 font-medium mb-2">No Views Remaining</p>
                      <p className="text-gray-500 text-sm">
                        You have used all your views for this image.
                      </p>
                      {/* Show the carrier/cover image */}
                      <div className="mt-4 p-4 rounded-lg bg-gradient-to-br from-gray-800/50 to-gray-900/50 border border-gray-700/30">
                        <p className="text-gray-400 text-xs mb-2">Cover Image (Encrypted)</p>
                        <img
                          src={convertFileSrc(viewingImage.file_path)}
                          alt="Cover"
                          className="max-w-full max-h-[200px] object-contain opacity-50"
                        />
                      </div>
                    </div>
                  )}
                </div>

                {/* Image info */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="p-3 rounded-lg bg-white/5 border border-cyan-900/20">
                    <p className="text-xs text-gray-400">File Name</p>
                    <p className="text-white font-medium truncate">{viewingImage.file_name}</p>
                  </div>
                  <div className="p-3 rounded-lg bg-white/5 border border-cyan-900/20">
                    <p className="text-xs text-gray-400">From</p>
                    <p className="text-white font-medium">{viewingImage.from_owner}</p>
                  </div>
                  <div className="p-3 rounded-lg bg-white/5 border border-cyan-900/20">
                    <p className="text-xs text-gray-400">Views Remaining</p>
                    <p className={`font-medium ${viewingImage.views_remaining > 0 ? 'text-cyan-400' : 'text-red-400'}`}>
                      {viewingImage.views_remaining} views
                    </p>
                  </div>
                  <div className="p-3 rounded-lg bg-white/5 border border-cyan-900/20">
                    <p className="text-xs text-gray-400">Received</p>
                    <p className="text-white font-medium">{viewingImage.received_at}</p>
                  </div>
                </div>

                {viewedImagePath && (
                  <p className="text-center text-yellow-400 text-sm">
                    ⚠️ This view has been counted. You have {viewingImage.views_remaining} views remaining.
                  </p>
                )}
              </div>

              <div className="flex justify-end mt-6">
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={closeImageViewer}
                  className="px-6 py-3 rounded-lg bg-gradient-to-r from-cyan-600 to-blue-600 text-white font-medium"
                >
                  Close
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Delete Confirmation Modal */}
      <AnimatePresence>
        {deleteConfirmModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop"
            onClick={() => setDeleteConfirmModal(null)}
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
              className="bg-cyber-darker border border-red-500/30 rounded-2xl p-6 w-full max-w-md"
            >
              <div className="flex items-center gap-3 mb-4">
                <div className="p-3 rounded-lg bg-red-600/20">
                  <Trash2 className="w-6 h-6 text-red-400" />
                </div>
                <h3 className="text-xl font-display font-bold text-white">Delete Image</h3>
              </div>
              
              <div className="space-y-4">
                <p className="text-gray-300">
                  Are you sure you want to delete this image? This action cannot be undone.
                </p>
                
                <div className="p-4 rounded-lg bg-white/5 border border-red-900/20">
                  <p className="text-sm text-gray-400">File</p>
                  <p className="text-white font-medium truncate">{deleteConfirmModal.file_name}</p>
                </div>

                {deleteConfirmModal.type === 'encrypted' && (
                  <div className="p-3 rounded-lg bg-yellow-900/20 border border-yellow-500/20">
                    <p className="text-xs text-yellow-400">
                      ⚠️ This is an encrypted image. Deleting it will remove it from your shared images and others will no longer be able to request it.
                    </p>
                  </div>
                )}
              </div>

              <div className="flex gap-3 mt-6">
                <button
                  onClick={() => setDeleteConfirmModal(null)}
                  className="flex-1 px-4 py-3 rounded-lg border border-gray-500/30 text-gray-400 hover:bg-white/5 transition-colors"
                >
                  Cancel
                </button>
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={handleDeleteConfirm}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-3 rounded-lg bg-gradient-to-r from-red-600 to-red-700 text-white font-medium"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete
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
