#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QListWidget>
#include <QPushButton>
#include <QLabel>
#include <QLineEdit>
#include <QTextEdit>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QGroupBox>
#include <QNetworkAccessManager>
#include <QNetworkReply>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>
#include <QFileDialog>
#include <QMessageBox>
#include <QTimer>
#include <QTcpSocket>
#include <QEventLoop>

class MainWindow : public QMainWindow {
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

private slots:
    void onRegisterClicked();
    void onSelectImageClicked();
    void onEncryptImageClicked();
    void onRefreshPeersClicked();
    void onRequestImageClicked();
    void onServerResponseReceived(QNetworkReply *reply);
    void updateConnectionStatus();
    void onImageDoubleClicked(QListWidgetItem *item);
    void onImageRightClicked(const QPoint &pos);

private:
    void setupUI();
    void sendMulticastRequest(const QString &endpoint, const QJsonObject &data);
    void connectToPeer(const QString &peerId);
    void logMessage(const QString &message);
    
    // TCP communication for Rust backend (matches backend protocol)
    bool sendRawTCPRequest(const QString &host, quint16 port,
                           const QByteArray &metadata,
                           const QByteArray &imageData);
    
    // UI Components
    QWidget *centralWidget;
    QLineEdit *usernameInput;
    QPushButton *registerBtn;
    QLabel *statusLabel;
    QLabel *selectedImageLabel;
    QPushButton *selectImageBtn;
    QPushButton *encryptImageBtn;
    QListWidget *imagesList;
    QListWidget *peersList;
    QPushButton *refreshPeersBtn;
    QPushButton *requestImageBtn;
    QTextEdit *logTextEdit;
    QLabel *connectionStatusLabel;
    
    // Network
    QNetworkAccessManager *networkManager;
    QTimer *statusTimer;
    
    // Data
    QString currentUsername;
    QString selectedImagePath;
    QStringList serverAddresses;
    bool isRegistered;
};

#endif // MAINWINDOW_H