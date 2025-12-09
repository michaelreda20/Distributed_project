import React, { useState } from 'react';
import { motion } from 'framer-motion';
import {
  Settings, Server, Plus, Trash2, Save, RefreshCw,
  Globe, Shield, Database, AlertCircle, Check
} from 'lucide-react';

function SettingsPanel({ directoryServers, onUpdateServers }) {
  const [servers, setServers] = useState(directoryServers);
  const [newServer, setNewServer] = useState('');
  const [saved, setSaved] = useState(false);

  const handleAddServer = () => {
    if (newServer && !servers.includes(newServer)) {
      setServers([...servers, newServer]);
      setNewServer('');
    }
  };

  const handleRemoveServer = (index) => {
    setServers(servers.filter((_, i) => i !== index));
  };

  const handleSave = () => {
    onUpdateServers(servers);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-display font-bold text-white flex items-center gap-3">
          <Settings className="w-7 h-7 text-purple-400" />
          Settings
        </h2>
        <p className="text-gray-400 mt-1">Configure your P2P network settings</p>
      </div>

      {/* Directory Servers Section */}
      <div className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2 rounded-lg bg-purple-600/20">
            <Server className="w-5 h-5 text-purple-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white">Directory Servers</h3>
            <p className="text-sm text-gray-400">Configure the directory service endpoints</p>
          </div>
        </div>

        {/* Current servers */}
        <div className="space-y-3 mb-4">
          {servers.map((server, index) => (
            <motion.div
              key={index}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.05 }}
              className="flex items-center gap-3 p-3 rounded-lg bg-white/5 border border-purple-900/20"
            >
              <Globe className="w-4 h-4 text-cyan-400" />
              <span className="flex-1 font-mono text-sm text-white">{server}</span>
              <button
                onClick={() => handleRemoveServer(index)}
                className="p-1.5 rounded-lg text-red-400 hover:bg-red-600/20 transition-colors"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </motion.div>
          ))}
        </div>

        {/* Add new server */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1">
            <Server className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={newServer}
              onChange={(e) => setNewServer(e.target.value)}
              placeholder="Enter server address (e.g., 192.168.1.1:9000)"
              className="w-full pl-10 pr-4 py-3 rounded-lg cyber-input text-white placeholder-gray-500 font-mono text-sm"
              onKeyDown={(e) => e.key === 'Enter' && handleAddServer()}
            />
          </div>
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={handleAddServer}
            disabled={!newServer}
            className="p-3 rounded-lg bg-purple-600/20 border border-purple-500/30 text-purple-400 hover:bg-purple-600/30 transition-colors disabled:opacity-50"
          >
            <Plus className="w-5 h-5" />
          </motion.button>
        </div>

        {/* Save button */}
        <div className="flex justify-end mt-6 pt-6 border-t border-purple-900/30">
          <motion.button
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            onClick={handleSave}
            className={`flex items-center gap-2 px-6 py-3 rounded-lg font-medium transition-all ${
              saved
                ? 'bg-green-600/20 border border-green-500/30 text-green-400'
                : 'bg-gradient-to-r from-purple-600 to-pink-600 text-white'
            }`}
          >
            {saved ? (
              <>
                <Check className="w-4 h-4" />
                Saved!
              </>
            ) : (
              <>
                <Save className="w-4 h-4" />
                Save Changes
              </>
            )}
          </motion.button>
        </div>
      </div>

      {/* Network Info Section */}
      <div className="cyber-card rounded-xl bg-cyber-darker/80 backdrop-blur-sm p-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2 rounded-lg bg-cyan-600/20">
            <Shield className="w-5 h-5 text-cyan-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white">Network Information</h3>
            <p className="text-sm text-gray-400">Details about the P2P network architecture</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
            <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
              <Database className="w-4 h-4" />
              Replication
            </div>
            <p className="text-white font-medium">3-way replicated</p>
            <p className="text-xs text-gray-500 mt-1">Fault tolerant directory service</p>
          </div>
          
          <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
            <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
              <RefreshCw className="w-4 h-4" />
              Consensus
            </div>
            <p className="text-white font-medium">Raft Protocol</p>
            <p className="text-xs text-gray-500 mt-1">Leader election and log replication</p>
          </div>
          
          <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
            <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
              <Shield className="w-4 h-4" />
              Encryption
            </div>
            <p className="text-white font-medium">LSB Steganography</p>
            <p className="text-xs text-gray-500 mt-1">Metadata embedded in images</p>
          </div>
          
          <div className="p-4 rounded-lg bg-white/5 border border-purple-900/20">
            <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
              <Globe className="w-4 h-4" />
              Protocol
            </div>
            <p className="text-white font-medium">TCP/IP P2P</p>
            <p className="text-xs text-gray-500 mt-1">Direct peer-to-peer connections</p>
          </div>
        </div>
      </div>

      {/* Info banner */}
      <div className="flex items-start gap-3 p-4 rounded-xl bg-blue-500/10 border border-blue-500/20">
        <AlertCircle className="w-5 h-5 text-blue-400 flex-shrink-0 mt-0.5" />
        <div>
          <p className="text-sm text-blue-400 font-medium">About Directory Servers</p>
          <p className="text-sm text-gray-400 mt-1">
            Directory servers maintain the list of online peers and handle request routing.
            For fault tolerance, configure at least 3 servers. The system uses multicast
            to communicate with all servers simultaneously.
          </p>
        </div>
      </div>
    </div>
  );
}

export default SettingsPanel;
