# P2P Image Sharing - Tauri Desktop Application

A beautiful, cyberpunk-themed desktop application for your P2P image sharing network built with **Tauri 2.0** and **React**.

## ✨ Features

This GUI covers all the functionality from your Rust P2P project:

### 1. **Connection Management**
- Go online/offline with a single click
- Configure multiple directory servers
- Automatic heartbeat to maintain connection
- Real-time connection status indicator

### 2. **Peer Discovery**
- View all online peers in the network
- See peer status (online/offline)
- Browse shared images from each peer
- Search and filter peers

### 3. **Image Management**
- **Local Images**: View and manage your shared images
- **Received Images**: Access images shared with you
- **Encryption**: Send images to servers for encryption
- **Permission Control**: Update view quotas for any user

### 4. **Request System**
- **Incoming Requests**: Review requests from other peers
- Accept or reject with one click
- See requester info and requested view count
- Requests are queued when offline

### 5. **Notifications**
- Track responses to your image requests
- See accepted/rejected/pending status
- Get notified when offline requests are processed

### 6. **Settings**
- Configure directory servers
- View network architecture info
- Save and manage configurations

---

## 🚀 Setup Instructions

### Prerequisites

1. **Node.js** (v18 or later)
   ```bash
   # Check your version
   node --version
   ```

2. **Rust** (latest stable)
   ```bash
   # Install Rust
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   
   # Check version
   rustc --version
   ```

3. **Tauri CLI**
   ```bash
   # Install Tauri CLI
   cargo install tauri-cli
   ```

4. **System Dependencies** (Linux only)
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install libwebkit2gtk-4.1-dev build-essential curl wget file libssl-dev libayatana-appindicator3-dev librsvg2-dev
   ```

### Installation

1. **Navigate to the project directory:**
   ```bash
   cd p2p-image-sharing-gui
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

3. **Run in development mode:**
   ```bash
   npm run tauri dev
   ```

4. **Build for production:**
   ```bash
   npm run tauri build
   ```
   The built application will be in `src-tauri/target/release/`

---

## 🔧 Configuration

### Directory Servers

By default, the app is configured to use these directory servers:
- `10.7.57.239:9000`
- `10.7.57.240:9000`
- `10.7.57.99:9000`

You can change these in:
1. The **Settings** panel in the app
2. Or edit `src/App.jsx` and modify the `directoryServers` state

### Encryption Servers

The encryption servers are loaded from `servers.conf` file. Create this file in your working directory:
```
# servers.conf
10.7.57.239:8080
10.7.57.240:8080
10.7.57.99:8080
```

### P2P Port

When connecting, you can specify any available port. Default is `8001`.

---

## 📁 Project Structure

```
p2p-image-sharing-gui/
├── src/                      # React frontend
│   ├── App.jsx              # Main application component
│   ├── main.jsx             # Entry point
│   ├── index.css            # Global styles
│   └── components/          # React components
│       ├── Sidebar.jsx      # Navigation sidebar
│       ├── Header.jsx       # Top header bar
│       ├── Dashboard.jsx    # Main dashboard view
│       ├── PeersPanel.jsx   # Peer discovery & requests
│       ├── ImagesPanel.jsx  # Image management
│       ├── RequestsPanel.jsx    # Incoming requests
│       ├── NotificationsPanel.jsx  # Notifications
│       ├── SettingsPanel.jsx    # Settings
│       ├── ConnectionModal.jsx  # Connection dialog
│       └── Toast.jsx        # Toast notifications
│
├── src-tauri/               # Rust backend
│   ├── Cargo.toml           # Rust dependencies
│   ├── tauri.conf.json      # Tauri configuration
│   ├── build.rs             # Build script
│   └── src/
│       └── main.rs          # Tauri commands & TCP handlers
│
├── package.json             # Node.js dependencies
├── vite.config.js           # Vite configuration
├── tailwind.config.js       # Tailwind CSS configuration
└── postcss.config.js        # PostCSS configuration
```

---

## 🎨 Design

The application features a **cyberpunk-inspired** design with:

- **Dark theme** with purple/pink gradients
- **Glowing effects** on interactive elements
- **Animated borders** and transitions
- **Custom fonts**: Orbitron (display), Rajdhani (body), JetBrains Mono (code)
- **Grid patterns** and noise textures for depth

---

## 🔌 Integration with Your Backend

The Tauri backend (`src-tauri/src/main.rs`) implements all the TCP communication protocols from your original client:

### Supported Operations

| Operation | Function | Description |
|-----------|----------|-------------|
| `go_online` | Register with directory | Registers user and scans images |
| `go_offline` | Unregister | Removes user from directory |
| `discover_peers` | Query peers | Gets list of online peers |
| `request_image` | Leave request | Sends image access request |
| `get_pending_requests` | Check requests | Gets incoming requests |
| `respond_to_request` | Respond | Accept/reject requests |
| `get_notifications` | Check notifications | Gets request responses |
| `update_permissions` | Update quota | Changes user's view quota |
| `encrypt_image` | Encrypt | Sends image to encryption servers |
| `send_heartbeat` | Heartbeat | Maintains online status |

---

## 🔒 Important Notes

### Network Configuration

1. **Update IP addresses** in the code to match your network:
   - `src/App.jsx` - `directoryServers` array
   - `src-tauri/src/main.rs` - if hardcoded IPs exist

2. **Firewall**: Ensure the P2P port you choose is open

3. **Same Network**: All peers must be on the same network or have proper routing

### Running with Your Rust Backend

1. Start your directory servers first:
   ```bash
   # Server 1
   cargo run --bin directory_server 9000 dir1 10.7.57.240:9000 10.7.57.99:9000
   
   # Server 2
   cargo run --bin directory_server 9000 dir2 10.7.57.239:9000 10.7.57.99:9000
   
   # Server 3
   cargo run --bin directory_server 9000 dir3 10.7.57.239:9000 10.7.57.240:9000
   ```

2. Start encryption servers if needed:
   ```bash
   cargo run --bin server 8080 server1 ...
   ```

3. Then launch the GUI application

---

## 🐛 Troubleshooting

### "Failed to connect to directory service"
- Ensure directory servers are running
- Check IP addresses and ports
- Verify network connectivity

### "Images not showing"
- Check that the images directory exists
- Ensure images are PNG, JPG, or JPEG format
- Verify file permissions

### Build errors
```bash
# Clear Rust build cache
cd src-tauri && cargo clean && cd ..

# Clear Node.js cache
rm -rf node_modules && npm install
```

### Linux WebKit errors
```bash
sudo apt install libwebkit2gtk-4.1-dev
```

---

## 📄 License

This project is part of your distributed systems coursework.

---

## 🎓 For Your Professor

This application demonstrates:

1. **Distributed Systems Concepts**
   - Directory service with replication
   - Raft consensus (in backend)
   - Fault-tolerant multicast

2. **P2P Architecture**
   - Peer discovery and registration
   - Direct peer-to-peer communication
   - Heartbeat mechanism

3. **Security Features**
   - LSB steganography encryption
   - View quota management
   - Permission control system

4. **Offline Support**
   - Queued requests for offline users
   - Pending permission updates
   - Notification system

5. **Modern Technology Stack**
   - Tauri 2.0 (Rust + Web)
   - React with hooks
   - Framer Motion animations
   - Tailwind CSS
