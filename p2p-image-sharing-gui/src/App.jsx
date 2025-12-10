import React, { useState, useEffect, useCallback } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Wifi, WifiOff, Users, Image, Bell, Settings, Shield,
  Upload, Download, Send, Check, X, RefreshCw, Eye,
  Lock, Unlock, Clock, AlertCircle, ChevronRight,
  Server, Activity, Zap, Globe, HardDrive, Menu
} from 'lucide-react';

// Import components
import Sidebar from './components/Sidebar';
import Header from './components/Header';
import Dashboard from './components/Dashboard';
import PeersPanel from './components/PeersPanel';
import ImagesPanel from './components/ImagesPanel';
import RequestsPanel from './components/RequestsPanel';
import NotificationsPanel from './components/NotificationsPanel';
import SettingsPanel from './components/SettingsPanel';
import ConnectionModal from './components/ConnectionModal';
import Toast from './components/Toast';

function App() {
  // Connection state
  const [isOnline, setIsOnline] = useState(false);
  const [username, setUsername] = useState('');
  const [port, setPort] = useState(8001);
  const [directoryServers, setDirectoryServers] = useState([
    '10.7.57.239:9000',
    '10.7.57.240:9000',
    '10.7.57.99:9000'
  ]);

  // UI state
  const [activeTab, setActiveTab] = useState('dashboard');
  const [showConnectionModal, setShowConnectionModal] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  // Data state
  const [peers, setPeers] = useState([]);
  const [localImages, setLocalImages] = useState([]);
  const [encryptedImages, setEncryptedImages] = useState([]);
  const [receivedImages, setReceivedImages] = useState([]);
  const [pendingRequests, setPendingRequests] = useState([]);
  const [notifications, setNotifications] = useState([]);

  // Toast notifications
  const [toasts, setToasts] = useState([]);

  // Loading states
  const [loading, setLoading] = useState({
    peers: false,
    requests: false,
    notifications: false,
    connection: false,
    images: false
  });

  // Show toast notification
  const showToast = useCallback((message, type = 'info') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts(prev => prev.filter(t => t.id !== id));
    }, 5000);
  }, []);

  // Initialize directory servers
  useEffect(() => {
    const initServers = async () => {
      try {
        await invoke('set_directory_servers', { servers: directoryServers });
      } catch (error) {
        console.error('Failed to set directory servers:', error);
      }
    };
    initServers();
  }, [directoryServers]);

  // Heartbeat interval - handles auto-disconnect when servers are down
  useEffect(() => {
    if (!isOnline) return;

    const heartbeatInterval = setInterval(async () => {
      try {
        const response = await invoke('send_heartbeat');
        
        // Check if we got disconnected due to server failures
        if (response.data?.disconnected) {
          setIsOnline(false);
          setUsername('');
          setPeers([]);
          setPendingRequests([]);
          setNotifications([]);
          showToast('Disconnected: All directory servers are unreachable', 'error');
        } else if (!response.success && response.data?.failures > 0) {
          // Warn user about connection issues
          showToast(`Connection unstable: ${response.data.failures}/3 failures`, 'warning');
        }
      } catch (error) {
        console.error('Heartbeat failed:', error);
        // If invoke itself fails, assume disconnected
        setIsOnline(false);
        setUsername('');
        showToast('Connection lost: Unable to reach backend', 'error');
      }
    }, 10000);

    return () => clearInterval(heartbeatInterval);
  }, [isOnline, showToast]);

  // Check for pending permission updates periodically
  useEffect(() => {
    if (!isOnline) return;

    const checkPermissionUpdates = async () => {
      try {
        const response = await invoke('check_pending_permission_updates');
        if (response.success && response.data && response.data.length > 0) {
          // Show notifications for each update
          for (const update of response.data) {
            if (update.new_quota === 0) {
              showToast(`⚠️ ${update.from_owner} revoked your access to "${update.image_id}"`, 'warning');
            } else {
              showToast(`📬 ${update.from_owner} updated your permissions for "${update.image_id}" (${update.new_quota} views)`, 'info');
            }
          }
          // Refresh received images to show updates
          await fetchReceivedImages();
        }
      } catch (error) {
        console.error('Failed to check permission updates:', error);
      }
    };

    // Check immediately on login
    checkPermissionUpdates();
    
    // Then check every 15 seconds
    const updateInterval = setInterval(checkPermissionUpdates, 15000);
    return () => clearInterval(updateInterval);
  }, [isOnline, showToast]);

  // Auto-refresh data when online
  useEffect(() => {
    if (!isOnline) return;

    const refreshData = async () => {
      await Promise.all([
        fetchPeers(),
        fetchPendingRequests(),
        fetchNotifications(),
        fetchReceivedImages()
      ]);
    };

    refreshData();
    const refreshInterval = setInterval(refreshData, 30000);
    return () => clearInterval(refreshInterval);
  }, [isOnline]);

  // Connection handlers
  const handleGoOnline = async (user, p2pPort, imagesDir) => {
    setLoading(prev => ({ ...prev, connection: true }));
    try {
      const response = await invoke('go_online', {
        username: user,
        port: p2pPort,
        imagesDir: imagesDir
      });

      if (response.success) {
        setIsOnline(true);
        setUsername(user);
        setPort(p2pPort);
        setLocalImages(response.data || []);
        showToast(`Welcome, ${user}! You are now online.`, 'success');
        setShowConnectionModal(false);
        
        // Fetch initial data
        await Promise.all([
          fetchPeers(),
          fetchPendingRequests(),
          fetchNotifications(),
          fetchReceivedImages(),
          fetchEncryptedImages()
        ]);
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Connection failed: ${error}`, 'error');
    }
    setLoading(prev => ({ ...prev, connection: false }));
  };

  const handleGoOffline = async () => {
    try {
      await invoke('go_offline');
      setIsOnline(false);
      setUsername('');
      setPeers([]);
      setPendingRequests([]);
      setNotifications([]);
      showToast('You are now offline', 'info');
    } catch (error) {
      showToast(`Error going offline: ${error}`, 'error');
    }
  };

  // Data fetching
  const fetchPeers = async () => {
    if (!isOnline) return;
    setLoading(prev => ({ ...prev, peers: true }));
    try {
      const response = await invoke('discover_peers');
      if (response.success) {
        setPeers(response.data || []);
      }
    } catch (error) {
      console.error('Failed to fetch peers:', error);
    }
    setLoading(prev => ({ ...prev, peers: false }));
  };

  const fetchPendingRequests = async () => {
    if (!isOnline) return;
    setLoading(prev => ({ ...prev, requests: true }));
    try {
      const response = await invoke('get_pending_requests');
      if (response.success) {
        setPendingRequests(response.data || []);
      }
    } catch (error) {
      console.error('Failed to fetch requests:', error);
    }
    setLoading(prev => ({ ...prev, requests: false }));
  };

  const fetchNotifications = async () => {
    if (!isOnline) return;
    setLoading(prev => ({ ...prev, notifications: true }));
    try {
      const response = await invoke('get_notifications');
      if (response.success) {
        setNotifications(response.data || []);
      }
    } catch (error) {
      console.error('Failed to fetch notifications:', error);
    }
    setLoading(prev => ({ ...prev, notifications: false }));
  };

  const fetchReceivedImages = async () => {
    try {
      const response = await invoke('get_received_images');
      console.log('Received images response:', response);
      if (response.success) {
        setReceivedImages(response.data || []);
      }
    } catch (error) {
      console.error('Failed to fetch received images:', error);
    }
  };

  const fetchEncryptedImages = async () => {
    try {
      const response = await invoke('get_encrypted_images');
      console.log('Encrypted images response:', response);
      if (response.success) {
        setEncryptedImages(response.data || []);
      }
    } catch (error) {
      console.error('Failed to fetch encrypted images:', error);
    }
  };

  // Request handlers
  const handleRequestImage = async (peerUsername, imageId, views) => {
    try {
      const response = await invoke('request_image', {
        peerUsername,
        imageId,
        views: parseInt(views)
      });

      if (response.success) {
        showToast(`Request sent to ${peerUsername}`, 'success');
        await fetchNotifications();
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Request failed: ${error}`, 'error');
    }
  };

  const handleRespondToRequest = async (requestId, accept) => {
    try {
      const response = await invoke('respond_to_request', {
        requestId,
        accept
      });

      if (response.success) {
        showToast(accept ? 'Request accepted!' : 'Request rejected', accept ? 'success' : 'info');
        await fetchPendingRequests();
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Response failed: ${error}`, 'error');
    }
  };

  const handleUpdatePermissions = async (targetUser, imageId, newQuota) => {
    try {
      const response = await invoke('update_permissions', {
        targetUser,
        imageId,
        newQuota: parseInt(newQuota)
      });

      if (response.success) {
        showToast(`Permissions updated for ${targetUser}`, 'success');
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Update failed: ${error}`, 'error');
    }
  };

  const handleEncryptImage = async (imagePath) => {
    try {
      const response = await invoke('encrypt_image', { imagePath });
      if (response.success) {
        showToast('Image encrypted successfully!', 'success');
        // Auto-refresh images after encryption
        await refreshImages();
        return response.data;
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Encryption failed: ${error}`, 'error');
    }
    return null;
  };

  const refreshImages = async () => {
    setLoading(prev => ({ ...prev, images: true }));
    try {
      const response = await invoke('refresh_images');
      if (response.success) {
        setLocalImages(response.data || []);
        showToast(`Found ${response.data?.length || 0} images`, 'success');
      } else {
        showToast(response.message, 'error');
      }
      // Also fetch received and encrypted images
      await fetchReceivedImages();
      await fetchEncryptedImages();
    } catch (error) {
      showToast(`Failed to refresh images: ${error}`, 'error');
    }
    setLoading(prev => ({ ...prev, images: false }));
  };

  const handleViewImage = async (imagePath) => {
    try {
      const response = await invoke('view_image', { imagePath });
      if (response.success) {
        showToast('Image viewed successfully!', 'success');
        // Refresh received images to update the view count
        await fetchReceivedImages();
        // Return the path to the viewable image
        return response.data;
      } else {
        showToast(response.message, 'error');
        return null;
      }
    } catch (error) {
      showToast(`Failed to view image: ${error}`, 'error');
      return null;
    }
  };

  const handleDeleteImage = async (imagePath, imageType) => {
    try {
      const response = await invoke('delete_image', { filePath: imagePath });
      if (response.success) {
        showToast(response.message, 'success');
        // Refresh the appropriate image list based on type
        if (imageType === 'local') {
          await refreshImages();
        } else if (imageType === 'encrypted') {
          await fetchEncryptedImages();
        } else if (imageType === 'received') {
          await fetchReceivedImages();
        }
      } else {
        showToast(response.message, 'error');
      }
    } catch (error) {
      showToast(`Failed to delete image: ${error}`, 'error');
    }
  };

  // Render panel based on active tab
  const renderPanel = () => {
    switch (activeTab) {
      case 'dashboard':
        return (
          <Dashboard
            isOnline={isOnline}
            username={username}
            peersCount={peers.length}
            imagesCount={localImages.length}
            requestsCount={pendingRequests.length}
            notificationsCount={notifications.length}
            onGoOnline={() => setShowConnectionModal(true)}
            onGoOffline={handleGoOffline}
          />
        );
      case 'peers':
        return (
          <PeersPanel
            peers={peers}
            loading={loading.peers}
            onRefresh={fetchPeers}
            onRequestImage={handleRequestImage}
            isOnline={isOnline}
          />
        );
      case 'images':
        return (
          <ImagesPanel
            localImages={localImages}
            encryptedImages={encryptedImages}
            receivedImages={receivedImages}
            onEncrypt={handleEncryptImage}
            onUpdatePermissions={handleUpdatePermissions}
            onRefresh={refreshImages}
            onViewImage={handleViewImage}
            onDeleteImage={handleDeleteImage}
            loading={loading.images}
            isOnline={isOnline}
          />
        );
      case 'requests':
        return (
          <RequestsPanel
            requests={pendingRequests}
            loading={loading.requests}
            onRefresh={fetchPendingRequests}
            onRespond={handleRespondToRequest}
            isOnline={isOnline}
          />
        );
      case 'notifications':
        return (
          <NotificationsPanel
            notifications={notifications}
            loading={loading.notifications}
            onRefresh={fetchNotifications}
            isOnline={isOnline}
          />
        );
      case 'settings':
        return (
          <SettingsPanel
            directoryServers={directoryServers}
            onUpdateServers={setDirectoryServers}
          />
        );
      default:
        return null;
    }
  };

  return (
    <div className="flex h-screen bg-cyber-dark overflow-hidden">
      {/* Sidebar */}
      <Sidebar
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        isOnline={isOnline}
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        notificationCount={notifications.filter(n => n.status === 'Pending').length}
        requestCount={pendingRequests.length}
      />

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden min-w-0">
        {/* Header */}
        <Header
          isOnline={isOnline}
          username={username}
          onConnectionClick={() => isOnline ? handleGoOffline() : setShowConnectionModal(true)}
        />

        {/* Content area */}
        <main className="flex-1 overflow-y-auto p-6 bg-cyber-dark">
          {/* Background effects */}
          <div className="fixed inset-0 cyber-grid pointer-events-none z-0" />
          
          <div className="relative z-10">
            <AnimatePresence mode="wait">
              <motion.div
                key={activeTab}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ duration: 0.2 }}
              >
                {renderPanel()}
              </motion.div>
            </AnimatePresence>
          </div>
        </main>
      </div>

      {/* Connection Modal */}
      <AnimatePresence>
        {showConnectionModal && (
          <ConnectionModal
            onClose={() => setShowConnectionModal(false)}
            onConnect={handleGoOnline}
            loading={loading.connection}
            directoryServers={directoryServers}
          />
        )}
      </AnimatePresence>

      {/* Toast notifications */}
      <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
        <AnimatePresence>
          {toasts.map(toast => (
            <Toast
              key={toast.id}
              message={toast.message}
              type={toast.type}
              onClose={() => setToasts(prev => prev.filter(t => t.id !== toast.id))}
            />
          ))}
        </AnimatePresence>
      </div>
    </div>
  );
}

export default App;
