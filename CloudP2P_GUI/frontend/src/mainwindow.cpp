#include "mainwindow.h"
#include <QDateTime>
#include <QFile>
#include <QBuffer>
#include <QFileInfo>
#include <QMap>
#include <QDir>
#include <QTcpSocket>
#include <QDataStream>
#include <QThread>
#include <QDesktopServices>
#include <QMenu>
#include <QUrl>

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent), isRegistered(false) {
    
    // Initialize server addresses (3 cloud servers) - RAW TCP PORTS
    serverAddresses << "10.40.7.1:8080"
                    << "10.40.6.227:8081"
                    << "10.40.6.26:8082";
    
    networkManager = new QNetworkAccessManager(this);
    
    // Status update timer
    statusTimer = new QTimer(this);
    connect(statusTimer, &QTimer::timeout, this, &MainWindow::updateConnectionStatus);
    statusTimer->start(5000); // Update every 5 seconds
    
    setupUI();
    setWindowTitle("Cloud P2P Image Sharing Client");
    resize(900, 700);
}

MainWindow::~MainWindow() {
    delete networkManager;
}

void MainWindow::setupUI() {
    centralWidget = new QWidget(this);
    setCentralWidget(centralWidget);
    
    QVBoxLayout *mainLayout = new QVBoxLayout(centralWidget);
    
    // Connection Status
    connectionStatusLabel = new QLabel("Status: Disconnected", this);
    connectionStatusLabel->setStyleSheet("QLabel { padding: 5px; background-color: #ffcccc; }");
    mainLayout->addWidget(connectionStatusLabel);
    
    // User Registration Section
    QGroupBox *registrationGroup = new QGroupBox("User Registration", this);
    QHBoxLayout *regLayout = new QHBoxLayout(registrationGroup);
    
    regLayout->addWidget(new QLabel("Username:", this));
    usernameInput = new QLineEdit(this);
    usernameInput->setPlaceholderText("Enter your username");
    regLayout->addWidget(usernameInput);
    
    registerBtn = new QPushButton("Register", this);
    connect(registerBtn, &QPushButton::clicked, this, &MainWindow::onRegisterClicked);
    regLayout->addWidget(registerBtn);
    
    statusLabel = new QLabel("Not registered", this);
    regLayout->addWidget(statusLabel);
    
    mainLayout->addWidget(registrationGroup);
    
    // Image Management Section
    QGroupBox *imageGroup = new QGroupBox("Image Encryption Service", this);
    QVBoxLayout *imgLayout = new QVBoxLayout(imageGroup);
    
    QHBoxLayout *selectLayout = new QHBoxLayout();
    selectImageBtn = new QPushButton("Select Image", this);
    connect(selectImageBtn, &QPushButton::clicked, this, &MainWindow::onSelectImageClicked);
    selectLayout->addWidget(selectImageBtn);
    
    selectedImageLabel = new QLabel("No image selected", this);
    selectedImageLabel->setWordWrap(true);
    selectLayout->addWidget(selectedImageLabel, 1);
    imgLayout->addLayout(selectLayout);
    
    encryptImageBtn = new QPushButton("Encrypt and Upload to Cloud", this);
    encryptImageBtn->setEnabled(false);
    connect(encryptImageBtn, &QPushButton::clicked, this, &MainWindow::onEncryptImageClicked);
    imgLayout->addWidget(encryptImageBtn);
    
    imgLayout->addWidget(new QLabel("My Encrypted Images:", this));
    imagesList = new QListWidget(this);
    imagesList->setMaximumHeight(100);
    imagesList->setContextMenuPolicy(Qt::CustomContextMenu);  // ADD THIS
    connect(imagesList, &QListWidget::itemDoubleClicked, this, &MainWindow::onImageDoubleClicked);
    connect(imagesList, &QListWidget::customContextMenuRequested, this, &MainWindow::onImageRightClicked);  // ADD THIS
    imgLayout->addWidget(imagesList);
    
    mainLayout->addWidget(imageGroup);
    
    // P2P Discovery Section
    QGroupBox *p2pGroup = new QGroupBox("Peer Discovery & P2P Sharing", this);
    QVBoxLayout *p2pLayout = new QVBoxLayout(p2pGroup);
    
    QHBoxLayout *peerControlLayout = new QHBoxLayout();
    refreshPeersBtn = new QPushButton("Refresh Online Peers", this);
    connect(refreshPeersBtn, &QPushButton::clicked, this, &MainWindow::onRefreshPeersClicked);
    peerControlLayout->addWidget(refreshPeersBtn);
    
    requestImageBtn = new QPushButton("Request Image from Peer", this);
    connect(requestImageBtn, &QPushButton::clicked, this, &MainWindow::onRequestImageClicked);
    peerControlLayout->addWidget(requestImageBtn);
    p2pLayout->addLayout(peerControlLayout);
    
    p2pLayout->addWidget(new QLabel("Online Peers:", this));
    peersList = new QListWidget(this);
    peersList->setMaximumHeight(100);
    p2pLayout->addWidget(peersList);
    
    mainLayout->addWidget(p2pGroup);
    
    // Log Section
    QGroupBox *logGroup = new QGroupBox("Activity Log", this);
    QVBoxLayout *logLayout = new QVBoxLayout(logGroup);
    
    logTextEdit = new QTextEdit(this);
    logTextEdit->setReadOnly(true);
    logTextEdit->setMaximumHeight(150);
    logLayout->addWidget(logTextEdit);
    
    mainLayout->addWidget(logGroup);
    
    logMessage("Application started. Please register to begin.");
}

void MainWindow::onRegisterClicked() {
    QString username = usernameInput->text().trimmed();
    
    if (username.isEmpty()) {
        QMessageBox::warning(this, "Input Error", "Please enter a username.");
        return;
    }
    
    // Local registration (no server-side registration needed for this phase)
    isRegistered = true;
    currentUsername = username;
    statusLabel->setText("✓ Registered as: " + username);
    statusLabel->setStyleSheet("QLabel { color: green; font-weight: bold; }");
    usernameInput->setEnabled(false);
    registerBtn->setEnabled(false);
    logMessage("Successfully registered locally as: " + username);
    
    // Update connection status
    connectionStatusLabel->setText("Status: Registered - Ready to encrypt");
    connectionStatusLabel->setStyleSheet("QLabel { padding: 5px; background-color: #ccffcc; }");
}

void MainWindow::onSelectImageClicked() {
    QString filePath = QFileDialog::getOpenFileName(
        this,
        "Select Image",
        QDir::homePath(),
        "Images (*.png *.jpg *.jpeg *.bmp)"
    );
    
    if (!filePath.isEmpty()) {
        selectedImagePath = filePath;
        QFileInfo fileInfo(filePath);
        selectedImageLabel->setText("Selected: " + fileInfo.fileName() + 
                                   QString(" (%1 KB)").arg(fileInfo.size() / 1024.0, 0, 'f', 2));
        encryptImageBtn->setEnabled(true);
        logMessage("Image selected: " + filePath);
    }
}

void MainWindow::onEncryptImageClicked() {
    if (selectedImagePath.isEmpty() || !isRegistered) {
        QMessageBox::warning(this, "Error", "Please register and select an image first.");
        return;
    }
    
    QFile imageFile(selectedImagePath);
    if (!imageFile.open(QIODevice::ReadOnly)) {
        QMessageBox::critical(this, "Error", "Cannot open image file: " + imageFile.errorString());
        return;
    }
    
    QByteArray imageData = imageFile.readAll();
    imageFile.close();
    
    logMessage("Preparing to send image to cloud servers...");
    logMessage(QString("Image size: %1 KB").arg(imageData.size() / 1024.0, 0, 'f', 2));
    
    // Create simple metadata (just the username for now)
    // Create ImagePermissions metadata matching Rust struct
    QJsonObject permissions;
    permissions["owner"] = currentUsername;

    QJsonObject quotas;
    quotas[currentUsername] = 3;  // Owner gets 3 views
    quotas["alice"] = 2;          // alice gets 2 views
    quotas["bob"] = 1;            // bob gets 1 view

    permissions["quotas"] = quotas;

    // Serialize to JSON then to bytes
    QJsonDocument doc(permissions);
    QByteArray metadata = doc.toJson(QJsonDocument::Compact);
    
    // Disable button during processing
    encryptImageBtn->setEnabled(false);
    encryptImageBtn->setText("Processing...");
    
    // Send to all 3 servers (multicast simulation)
    int successCount = 0;
    for (const QString &serverAddr : serverAddresses) {
        QStringList parts = serverAddr.split(":");
        if (parts.size() != 2) {
            logMessage("Invalid server address: " + serverAddr);
            continue;
        }
        
        QString host = parts[0];
        quint16 port = parts[1].toUInt();
        
        if (sendRawTCPRequest(host, port, metadata, imageData)) {
            successCount++;
            break; // One success is enough
        }
    }
    
    // Re-enable button
    encryptImageBtn->setEnabled(true);
    encryptImageBtn->setText("Encrypt and Upload to Cloud");
    
    if (successCount == 0) {
        QMessageBox::warning(this, "Encryption Failed", 
            "Could not connect to any server.\n\n"
            "Please ensure:\n"
            "1. Backend servers are running (start_servers.bat)\n"
            "2. Servers are on ports 8001, 8002, 8003\n"
            "3. No firewall blocking connections");
    }
}

void MainWindow::onRefreshPeersClicked() {
    if (!isRegistered) {
        QMessageBox::warning(this, "Error", "Please register first.");
        return;
    }
    
    // Simulate peer discovery (Phase 2 feature)
    peersList->clear();
    peersList->addItem("peer_alice (Online)");
    peersList->addItem("peer_bob (Online)");
    peersList->addItem("peer_charlie (Online)");
    
    logMessage("Peer list refreshed (simulated - Phase 2 feature)");
}

void MainWindow::onRequestImageClicked() {
    QListWidgetItem *selectedPeer = peersList->currentItem();
    
    if (!selectedPeer) {
        QMessageBox::warning(this, "Error", "Please select a peer first.");
        return;
    }
    
    QString peerId = selectedPeer->text();
    logMessage("P2P feature coming in Phase 2: " + peerId);
    QMessageBox::information(this, "P2P Connection", 
        "P2P image sharing will be implemented in Phase 2.\n\n"
        "Selected peer: " + peerId);
}

void MainWindow::updateConnectionStatus() {
    if (isRegistered) {
        connectionStatusLabel->setText("Status: Registered - Ready to encrypt");
        connectionStatusLabel->setStyleSheet("QLabel { padding: 5px; background-color: #ccffcc; }");
    } else {
        connectionStatusLabel->setText("Status: Not Registered");
        connectionStatusLabel->setStyleSheet("QLabel { padding: 5px; background-color: #ffffcc; }");
    }
}

void MainWindow::connectToPeer(const QString &peerId) {
    logMessage("P2P: Phase 2 feature - " + peerId);
}

void MainWindow::logMessage(const QString &message) {
    QString timestamp = QDateTime::currentDateTime().toString("hh:mm:ss");
    logTextEdit->append("[" + timestamp + "] " + message);
}

bool MainWindow::sendRawTCPRequest(const QString &host, quint16 port,
                                    const QByteArray &metadata,
                                    const QByteArray &imageData) {
    QTcpSocket *socket = new QTcpSocket(this);
    
    logMessage(QString("Connecting to %1:%2...").arg(host).arg(port));
    
    // Create a local event loop for synchronous operation
    QEventLoop loop;
    bool success = false;
    bool finished = false;
    QByteArray receivedData;
    
    // Connection established
    connect(socket, &QTcpSocket::connected, [&]() {
        logMessage(QString("✓ Connected to %1:%2").arg(host).arg(port));
        
        // Build request following Rust protocol CORRECTLY
        QByteArray request;
        
        // Convert sizes to big-endian format manually
        quint64 metaSize = metadata.size();
        quint64 imgSize = imageData.size();
        
        // Convert to big-endian bytes (network byte order)
        unsigned char metaSizeBytes[8];
        metaSizeBytes[0] = (metaSize >> 56) & 0xFF;
        metaSizeBytes[1] = (metaSize >> 48) & 0xFF;
        metaSizeBytes[2] = (metaSize >> 40) & 0xFF;
        metaSizeBytes[3] = (metaSize >> 32) & 0xFF;
        metaSizeBytes[4] = (metaSize >> 24) & 0xFF;
        metaSizeBytes[5] = (metaSize >> 16) & 0xFF;
        metaSizeBytes[6] = (metaSize >> 8) & 0xFF;
        metaSizeBytes[7] = metaSize & 0xFF;
        
        unsigned char imgSizeBytes[8];
        imgSizeBytes[0] = (imgSize >> 56) & 0xFF;
        imgSizeBytes[1] = (imgSize >> 48) & 0xFF;
        imgSizeBytes[2] = (imgSize >> 40) & 0xFF;
        imgSizeBytes[3] = (imgSize >> 32) & 0xFF;
        imgSizeBytes[4] = (imgSize >> 24) & 0xFF;
        imgSizeBytes[5] = (imgSize >> 16) & 0xFF;
        imgSizeBytes[6] = (imgSize >> 8) & 0xFF;
        imgSizeBytes[7] = imgSize & 0xFF;
        
        // Build request in correct order:
        // [8 bytes: metadata size][metadata][8 bytes: image size][image data]
        request.append(reinterpret_cast<const char*>(metaSizeBytes), 8);
        request.append(metadata);
        request.append(reinterpret_cast<const char*>(imgSizeBytes), 8);
        request.append(imageData);
        
        logMessage(QString("→ Sending: meta_size=%1, meta=%2 bytes, img_size=%3, img=%4 bytes")
                   .arg(metaSize).arg(metadata.size())
                   .arg(imgSize).arg(imageData.size()));
        
        qint64 written = socket->write(request);
        socket->flush();
        
        logMessage(QString("→ Sent %1 KB total to %2:%3")
                   .arg(request.size() / 1024.0, 0, 'f', 2)
                   .arg(host).arg(port));
        
        if (written != request.size()) {
            logMessage(QString("✗ Warning: Wrote %1/%2 bytes").arg(written).arg(request.size()));
        }
    });
    
    // Data received from server
    connect(socket, &QTcpSocket::readyRead, [&]() {
        receivedData.append(socket->readAll());
        
        // Check if we have at least the size header (8 bytes)
        if (receivedData.size() < 8) {
            logMessage(QString("Waiting for more data from %1:%2...").arg(host).arg(port));
            return;
        }
        
        // Read response size
        QDataStream sizeStream(receivedData.left(8));
        sizeStream.setByteOrder(QDataStream::BigEndian);
        quint64 responseSize;
        sizeStream >> responseSize;
        
        // Check if we have the complete response
        // if (receivedData.size() < (int)(8 + responseSize)) {
        //     logMessage(QString("Received %1/%2 bytes from %3:%4...")
        //               .arg(receivedData.size()).arg(8 + responseSize)
        //               .arg(host).arg(port));
        //     return;
        // }


        // ==================
        //      FIX
        // ==================

        // 1. Sanity Check: (e.g., 256MB max)
        const quint64 MAX_ALLOWED_SIZE = 256 * 1024 * 1024; 
        if (responseSize > MAX_ALLOWED_SIZE) {
            logMessage(QString("✗ Error: Server reported insane response size: %1 GB")
                      .arg(responseSize / 1024.0 / 1024.0 / 1024.0, 0, 'f', 2));
            socket->abort();
            finished = true;
            loop.quit();
            return;
        }

        // 2. Correct Size Check:
        // Cast the int to quint64 for the comparison, NOT the other way.
        quint64 totalExpectedSize = 8 + responseSize;
        if ((quint64)receivedData.size() < totalExpectedSize) {
            logMessage(QString("Received %1/%2 bytes from %3:%4...")
                      .arg(receivedData.size()).arg(totalExpectedSize)
                      .arg(host).arg(port));
            return; // Not enough data yet, wait for more
        }

        // ==================
        //   END OF FIX
        // ==================
        
        QByteArray response = receivedData.mid(8, responseSize);
        
        // Check if it's an error message
        QString responseStr = QString::fromUtf8(response);
        if (responseStr.startsWith("NOT_LEADER")) {
            logMessage(QString("⚠ Server %1:%2 is NOT_LEADER: %3")
                      .arg(host).arg(port).arg(responseStr));
            finished = true;
            loop.quit();
        } else if (responseStr.startsWith("NO_LEADER")) {
            logMessage(QString("⚠ Server %1:%2 says NO_LEADER (election in progress)")
                      .arg(host).arg(port));
            finished = true;
            loop.quit();
        } else {
            // Success! We got an encrypted image
            QString filename = QString("encrypted_%1_%2_%3.png")
                              .arg(currentUsername)
                              .arg(QDateTime::currentDateTime().toString("yyyyMMdd_hhmmss"))
                              .arg(port);
            
            QFile outFile(filename);
            if (outFile.open(QIODevice::WriteOnly)) {
                outFile.write(response);
                outFile.close();
                
                imagesList->addItem(filename);
                logMessage(QString("✓ SUCCESS from %1:%2!")
                          .arg(host).arg(port));
                logMessage(QString("  Saved as: %1 (%2 KB)")
                          .arg(filename)
                          .arg(response.size() / 1024.0, 0, 'f', 2));
                
                QMessageBox::information(this, "Encryption Successful!", 
                    QString("Image encrypted by server %1:%2\n\n"
                            "Saved as: %3\n"
                            "Size: %4 KB")
                    .arg(host).arg(port)
                    .arg(filename)
                    .arg(response.size() / 1024.0, 0, 'f', 2));
                
                success = true;
            } else {
                logMessage("✗ Failed to save encrypted image: " + outFile.errorString());
            }
            
            finished = true;
            loop.quit();
        }
    });
    
    // Connection error
    connect(socket, &QAbstractSocket::errorOccurred, 
            [&](QAbstractSocket::SocketError error) {
        Q_UNUSED(error);
        logMessage(QString("✗ Connection error to %1:%2 - %3")
                   .arg(host).arg(port)
                   .arg(socket->errorString()));
        finished = true;
        loop.quit();
    });
    
    // Timeout timer
    QTimer timeoutTimer;
    timeoutTimer.setSingleShot(true);
    connect(&timeoutTimer, &QTimer::timeout, [&]() {
        if (!finished) {
            logMessage(QString("✗ Timeout waiting for response from %1:%2")
                      .arg(host).arg(port));
            socket->abort();
            finished = true;
            loop.quit();
        }
    });
    timeoutTimer.start(120000); // 120 second timeout
    
    // Connect to server
    socket->connectToHost(host, port);
    
    // Wait for operation to complete
    if (!finished) {
        loop.exec();
    }
    
    socket->deleteLater();
    return success;
}

// Remove unused functions
void MainWindow::sendMulticastRequest(const QString &endpoint, const QJsonObject &data) {
    Q_UNUSED(endpoint);
    Q_UNUSED(data);
    // Not used in Phase 1
}

void MainWindow::onServerResponseReceived(QNetworkReply *reply) {
    Q_UNUSED(reply);
    // Not used in Phase 1
}

void MainWindow::onImageRightClicked(const QPoint &pos) {
    QListWidgetItem *item = imagesList->itemAt(pos);
    if (!item) return;
    
    QString imagePath = item->text();
    
    QMenu contextMenu(this);
    
    QAction *viewAction = contextMenu.addAction("👁️ View Image");
    QAction *openFolderAction = contextMenu.addAction("📁 Open Folder");
    QAction *deleteAction = contextMenu.addAction("🗑️ Delete");
    
    QAction *selectedAction = contextMenu.exec(imagesList->mapToGlobal(pos));
    
    if (selectedAction == viewAction) {
        onImageDoubleClicked(item);
    }
    else if (selectedAction == openFolderAction) {
        QFileInfo fileInfo(imagePath);
        QString folderPath = fileInfo.absolutePath();
        QDesktopServices::openUrl(QUrl::fromLocalFile(folderPath));
        logMessage("📁 Opened folder: " + folderPath);
    }
    else if (selectedAction == deleteAction) {
        QMessageBox::StandardButton reply = QMessageBox::question(
            this, "Delete Image",
            "Are you sure you want to delete this encrypted image?\n\n" + imagePath,
            QMessageBox::Yes | QMessageBox::No
        );
        
        if (reply == QMessageBox::Yes) {
            if (QFile::remove(imagePath)) {
                delete item;
                logMessage("🗑️ Deleted image: " + imagePath);
                QMessageBox::information(this, "Deleted", "Image deleted successfully.");
            } else {
                QMessageBox::critical(this, "Error", "Failed to delete image file.");
                logMessage("❌ Failed to delete: " + imagePath);
            }
        }
    }
}

void MainWindow::onImageDoubleClicked(QListWidgetItem *item) {
    if (!item) return;
    
    QString imagePath = item->text();
    
    // Check if file exists
    QFileInfo fileInfo(imagePath);
    if (!fileInfo.exists()) {
        QMessageBox::warning(this, "File Not Found", 
            "Image file not found: " + imagePath);
        logMessage("❌ Cannot open image - file not found: " + imagePath);
        return;
    }
    
    // Create a dialog to show the image
    QDialog *imageDialog = new QDialog(this);
    imageDialog->setWindowTitle("Encrypted Image: " + fileInfo.fileName());
    imageDialog->resize(800, 600);
    
    QVBoxLayout *layout = new QVBoxLayout(imageDialog);
    
    // Create label to display image
    QLabel *imageLabel = new QLabel(imageDialog);
    QPixmap pixmap(imagePath);
    
    if (pixmap.isNull()) {
        QMessageBox::critical(this, "Error", 
            "Failed to load image: " + imagePath);
        logMessage("❌ Failed to load image: " + imagePath);
        delete imageDialog;
        return;
    }
    
    // Scale image to fit dialog while maintaining aspect ratio
    QPixmap scaledPixmap = pixmap.scaled(750, 550, Qt::KeepAspectRatio, Qt::SmoothTransformation);
    imageLabel->setPixmap(scaledPixmap);
    imageLabel->setAlignment(Qt::AlignCenter);
    
    layout->addWidget(imageLabel);
    
    // Add info label
    QLabel *infoLabel = new QLabel(
        QString("File: %1\nSize: %2 KB\nDimensions: %3x%4")
            .arg(fileInfo.fileName())
            .arg(fileInfo.size() / 1024.0, 0, 'f', 2)
            .arg(pixmap.width())
            .arg(pixmap.height()),
        imageDialog
    );
    infoLabel->setStyleSheet("QLabel { padding: 10px; background-color: #9f9d9d87; }");
    layout->addWidget(infoLabel);
    
    // Add close button
    QPushButton *closeBtn = new QPushButton("Close", imageDialog);
    connect(closeBtn, &QPushButton::clicked, imageDialog, &QDialog::accept);
    layout->addWidget(closeBtn);
    
    imageDialog->setLayout(layout);
    
    logMessage("📷 Viewing image: " + imagePath);
    imageDialog->exec();
    
    delete imageDialog;
}
