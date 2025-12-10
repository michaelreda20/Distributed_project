use anyhow::{bail, Context, Result};
use bincode;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// =============================================================================
// P2P MESSAGE PROTOCOL
// =============================================================================

/// P2P messages exchanged between clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum P2PMessage {
    /// Request to view an image with a specific number of views
    ImageRequest {
        requesting_user: String,
        image_id: String,
        requested_views: u32,
    },
    
    /// Response with the encrypted image or rejection
    ImageResponse {
        success: bool,
        message: String,
        encrypted_image: Option<Vec<u8>>, // The encrypted image with embedded permissions
    },
    
    /// Query available images from a peer
    ListImages {
        requesting_user: String,
    },
    
    /// Response with list of available images
    ListImagesResponse {
        images: Vec<ImageMetadata>,
    },
    
    /// Request to update permissions for an already-shared image
    UpdatePermissions {
        owner: String,
        image_id: String,
        username: String,
        new_quota: u32,
    },
    
    /// Response to permission update request
    UpdatePermissionsResponse {
        success: bool,
        message: String,
    },

    /// Deliver image to requester after owner accepts (push model)
    DeliverImage {
        from_owner: String,
        image_id: String,
        requested_views: u32,
        encrypted_image: Vec<u8>, // The actual image data with embedded permissions
    },

    /// Response to image delivery
    DeliverImageResponse {
        success: bool,
        message: String,
    },

    /// Remote permission update: Owner asks requester to update their local copy's permissions
    RemoteUpdatePermissions {
        from_owner: String,
        image_id: String,
        for_user: String,
        new_quota: u32,
    },

    /// Response to remote permission update
    RemoteUpdatePermissionsResponse {
        success: bool,
        message: String,
    },

    /// Request a low-resolution thumbnail preview of an image
    ThumbnailRequest {
        requesting_user: String,
        image_id: String,
    },

    /// Response with low-resolution thumbnail
    ThumbnailResponse {
        success: bool,
        message: String,
        thumbnail: Option<Vec<u8>>, // Low-res blurred preview as PNG bytes
    },
}

/// Metadata about an available image
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image_id: String,
    pub image_name: String,
    pub owner: String,
    pub description: Option<String>,
    pub file_size_kb: u64,
}

// =============================================================================
// P2P REQUEST HANDLER
// =============================================================================

/// Information about images that this peer owns
pub struct PeerImageStore {
    /// Map of image_id -> (file_path, metadata)
    images: HashMap<String, (PathBuf, ImageMetadata)>,
    /// Directory where received images should be saved
    received_images_dir: Option<PathBuf>,
}

impl PeerImageStore {
    pub fn new() -> Self {
        Self {
            images: HashMap::new(),
            received_images_dir: None,
        }
    }
    
    /// Set the directory where received images should be saved
    pub fn set_received_images_dir(&mut self, dir: PathBuf) {
        self.received_images_dir = Some(dir);
    }
    
    /// Get the directory where received images should be saved
    pub fn get_received_images_dir(&self) -> Option<&PathBuf> {
        self.received_images_dir.as_ref()
    }
    
    /// Add an image to the store
    pub fn add_image(
        &mut self,
        image_id: String,
        file_path: PathBuf,
        metadata: ImageMetadata,
    ) {
        self.images.insert(image_id, (file_path, metadata));
    }
    
    /// Get image file path
    pub fn get_image_path(&self, image_id: &str) -> Option<&PathBuf> {
        self.images.get(image_id).map(|(path, _)| path)
    }
    
    /// Get all image metadata
    pub fn get_all_metadata(&self) -> Vec<ImageMetadata> {
        self.images
            .values()
            .map(|(_, metadata)| metadata.clone())
            .collect()
    }
    
    /// Remove an image from the store
    pub fn remove_image(&mut self, image_id: &str) {
        self.images.remove(image_id);
    }
}

// =============================================================================
// P2P SERVER
// =============================================================================

/// Start a P2P server to handle incoming requests from other peers
pub async fn start_p2p_server(
    port: u16,
    username: String,
    image_store: std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> Result<()> {
    let bind_addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("P2P server for user '{}' listening on {}", username, bind_addr);
    
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Received P2P connection from {}", addr);
                println!("[INFO] Received P2P connection from {}", addr);
                let username_clone = username.clone();
                let store_clone = image_store.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_p2p_request(stream, username_clone, store_clone).await {
                        error!("Error handling P2P request from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting P2P connection: {}", e);
            }
        }
    }
}

/// Handle a single P2P request
async fn handle_p2p_request(
    mut stream: TcpStream,
    owner_username: String,
    image_store: std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> Result<()> {
    // Read message
    let msg_len = stream.read_u32().await?;
    let mut msg_buf = vec![0u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    
    let message: P2PMessage = serde_json::from_slice(&msg_buf)?;
    
    // Process message
    let response = match message {
        P2PMessage::ImageRequest {
            requesting_user,
            image_id,
            requested_views,
        } => {
            info!(
                "Image request from {} for {} ({} views)",
                requesting_user, image_id, requested_views
            );
            println!(
                "[INFO] Image request from {} for {} ({} views)",
                requesting_user, image_id, requested_views
            );

            let response = handle_image_request(
                &owner_username,
                &requesting_user,
                &image_id,
                requested_views,
                &image_store,
            )
            .await;

            // Log the result
            match &response {
                P2PMessage::ImageResponse { success: true, .. } => {
                    info!("✓ Granted access to {}", requesting_user);
                    println!("[INFO] ✓ Granted access to {}", requesting_user);
                }
                P2PMessage::ImageResponse { success: false, message, .. } => {
                    info!("✗ Denied access to {}: {}", requesting_user, message);
                    println!("[INFO] ✗ Denied access to {}: {}", requesting_user, message);
                }
                _ => {}
            }

            response
        }
        
        P2PMessage::ListImages { requesting_user } => {
            // Only log if it's not a self-request (connectivity check)
            if requesting_user != owner_username {
                info!("List images request from {}", requesting_user);
                println!("[INFO] List images request from {}", requesting_user);
            }

            let store = image_store.read().await;
            let images = store.get_all_metadata();

            if requesting_user != owner_username {
                println!("[INFO] Sending {} images to {}", images.len(), requesting_user);
            }

            P2PMessage::ListImagesResponse { images }
        }
        
        P2PMessage::UpdatePermissions {
            owner,
            image_id,
            username,
            new_quota,
        } => {
            info!(
                "Update permissions request from {} for user {} on image {} -> {} views",
                owner, username, image_id, new_quota
            );
            println!(
                "[INFO] Update permissions request from {} for user {} on {} -> {} views",
                owner, username, image_id, new_quota
            );

            // Verify the requester is the owner
            if owner != owner_username {
                println!("[INFO] ✗ Denied - only owner can update permissions");
                P2PMessage::UpdatePermissionsResponse {
                    success: false,
                    message: "Only the owner can update permissions".to_string(),
                }
            } else {
                let response = handle_update_permissions(&image_id, &username, new_quota, &image_store).await;

                // Log the result
                match &response {
                    P2PMessage::UpdatePermissionsResponse { success: true, .. } => {
                        if new_quota == 0 {
                            info!("✓ Revoked access for {}", username);
                            println!("[INFO] ✓ Revoked access for {} on {}", username, image_id);
                        } else {
                            info!("✓ Updated {} to {} views", username, new_quota);
                            println!("[INFO] ✓ Updated {} to {} views on {}", username, new_quota, image_id);
                        }
                    }
                    P2PMessage::UpdatePermissionsResponse { success: false, message } => {
                        info!("✗ Failed to update permissions: {}", message);
                        println!("[INFO] ✗ Failed: {}", message);
                    }
                    _ => {}
                }

                response
            }
        }

        P2PMessage::DeliverImage {
            from_owner,
            image_id,
            requested_views,
            encrypted_image,
        } => {
            info!(
                "Receiving image delivery from {} for image {} ({} views)",
                from_owner, image_id, requested_views
            );
            println!(
                "\n🎉 ========================================");
            println!("   IMAGE DELIVERED!");
            println!("========================================");
            println!("📥 Received: {}", image_id);
            println!("👤 From: {}", from_owner);
            println!("👁  Views granted: {}", requested_views);
            println!("========================================\n");

            // Generate filename: from_{owner}_{image_id}
            let file_name = format!("from_{}_{}", from_owner, image_id);
            
            // Determine save path - use received_images_dir if set, otherwise current directory
            let save_path = {
                let store = image_store.read().await;
                match store.get_received_images_dir() {
                    Some(dir) => dir.join(&file_name),
                    None => PathBuf::from(&file_name),
                }
            };

            match fs::write(&save_path, &encrypted_image) {
                Ok(_) => {
                    let file_size = encrypted_image.len() / 1024;
                    println!("✅ Image saved to: {}", save_path.display());
                    println!("📊 Size: {} KB", file_size);
                    println!("\n💡 You can now view the image with:");
                    println!("   cargo run --bin client -- view --input {} --user {}",
                             save_path.display(), owner_username);

                    P2PMessage::DeliverImageResponse {
                        success: true,
                        message: format!("Image '{}' delivered and saved to {}", image_id, save_path.display()),
                    }
                }
                Err(e) => {
                    error!("Failed to save delivered image: {}", e);
                    println!("❌ Failed to save image: {}", e);

                    P2PMessage::DeliverImageResponse {
                        success: false,
                        message: format!("Failed to save image: {}", e),
                    }
                }
            }
        }

        P2PMessage::RemoteUpdatePermissions {
            from_owner,
            image_id,
            for_user,
            new_quota,
        } => {
            info!(
                "Remote permission update from {} for user {} on image {} -> {} views",
                from_owner, for_user, image_id, new_quota
            );
            println!("\n🔄 ========================================");
            println!("   PERMISSION UPDATE RECEIVED!");
            println!("========================================");
            println!("📥 From owner: {}", from_owner);
            println!("🖼  Image: {}", image_id);
            println!("👤 For user: {}", for_user);
            println!("👁  New quota: {} views", new_quota);
            println!("========================================\n");

            // Verify this update is for the current user
            if for_user != owner_username {
                println!("⚠ This update is not for you (it's for {})", for_user);
                P2PMessage::RemoteUpdatePermissionsResponse {
                    success: false,
                    message: format!("Permission update is for user '{}', not '{}'", for_user, owner_username),
                }
            } else {
                // Find the local image file: from_{owner}_{image_id} in received_images_dir or current directory
                let file_name = format!("from_{}_{}", from_owner, image_id);
                let local_image_path = {
                    let store = image_store.read().await;
                    match store.get_received_images_dir() {
                        Some(dir) => dir.join(&file_name),
                        None => PathBuf::from(&file_name),
                    }
                };

                if !local_image_path.exists() {
                    println!("❌ Local image not found: {}", local_image_path.display());
                    P2PMessage::RemoteUpdatePermissionsResponse {
                        success: false,
                        message: format!("Image not found locally: {}", local_image_path.display()),
                    }
                } else {
                    println!("🔍 Found local image: {}", local_image_path.display());
                    println!("🔧 Updating embedded permissions...");

                    // Re-encrypt the image with new permissions
                    match update_local_image_permissions(&local_image_path, &for_user, new_quota) {
                        Ok(()) => {
                            if new_quota == 0 {
                                println!("\n✅ Permission revoked!");
                                println!("   You can no longer view this image.");
                                P2PMessage::RemoteUpdatePermissionsResponse {
                                    success: true,
                                    message: format!("Permissions revoked. Image '{}' access removed.", image_id),
                                }
                            } else {
                                println!("\n✅ Permissions updated successfully!");
                                println!("   You now have {} views remaining.", new_quota);
                                P2PMessage::RemoteUpdatePermissionsResponse {
                                    success: true,
                                    message: format!("Permissions updated. You now have {} views for '{}'", new_quota, image_id),
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to update local permissions: {}", e);
                            println!("❌ Failed to update permissions: {}", e);
                            P2PMessage::RemoteUpdatePermissionsResponse {
                                success: false,
                                message: format!("Failed to update local image: {}", e),
                            }
                        }
                    }
                }
            }
        }

        P2PMessage::ThumbnailRequest {
            requesting_user,
            image_id,
        } => {
            info!("Thumbnail request from {} for {}", requesting_user, image_id);
            println!("[INFO] Thumbnail request from {} for {}", requesting_user, image_id);

            handle_thumbnail_request(&image_id, &image_store).await
        }

        _ => {
            bail!("Unexpected P2P message type");
        }
    };
    
    // Send response
    let response_json = serde_json::to_string(&response)?;
    let response_bytes = response_json.as_bytes();
    
    stream.write_u32(response_bytes.len() as u32).await?;
    stream.write_all(response_bytes).await?;
    stream.flush().await?;
    
    Ok(())
}

/// Handle an image request - grant access by modifying the encrypted image
async fn handle_image_request(
    _owner: &str,
    requesting_user: &str,
    image_id: &str,
    requested_views: u32,
    image_store: &std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> P2PMessage {
    // Get the image path
    let image_path = {
        let store = image_store.read().await;
        match store.get_image_path(image_id) {
            Some(path) => path.clone(),
            None => {
                return P2PMessage::ImageResponse {
                    success: false,
                    message: format!("Image {} not found", image_id),
                    encrypted_image: None,
                };
            }
        }
    };
    
    // Read the encrypted image
    let encrypted_data = match fs::read(&image_path) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to read image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Load and decode the image to extract permissions
    let carrier_img = match image::load_from_memory(&encrypted_data) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to load image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Decode embedded payload
    use crate::lsb;
    use crate::CombinedPayload;
    
    let payload = match lsb::decode(&carrier_img) {
        Ok(Some(data)) => data,
        Ok(None) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: "No embedded data found in image".to_string(),
                encrypted_image: None,
            };
        }
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to decode image: {}", e),
                encrypted_image: None,
            };
        }
    };
    
    // Deserialize the combined payload
    let mut combined_data: CombinedPayload = match bincode::deserialize(&payload) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to deserialize payload: {}", e),
                encrypted_image: None,
            };
        }
    };

    // Check if requesting user is the owner - owners don't consume quota
    let is_owner = requesting_user == &combined_data.permissions.owner;

    if !is_owner {
        // Only enforce and decrement quota for non-owners
        let existing_quota = combined_data.permissions.quotas.get(requesting_user).copied();

        match existing_quota {
            Some(0) => {
                // User was explicitly revoked (quota = 0)
                info!("Denied {} - access was revoked by owner", requesting_user);
                return P2PMessage::ImageResponse {
                    success: false,
                    message: format!("Access denied. Owner has revoked your permissions."),
                    encrypted_image: None,
                };
            }
            Some(current_quota) => {
                // User already has access — this is being called to SET the quota (grant permission)
                // NOT to decrement it. The requested_views IS the quota to grant.
                println!("[DEBUG] Existing user {} has quota: {}, setting to: {}", requesting_user, current_quota, requested_views);
                
                // Set the quota to exactly what was requested - this is granting access
                combined_data
                    .permissions
                    .quotas
                    .insert(requesting_user.to_string(), requested_views);

                info!("Set {} views for {} (was: {})", requested_views, requesting_user, current_quota);
                println!("[DEBUG] After update, quota for '{}': {}", requesting_user, requested_views);
            }
            None => {
                // New user - grant requested access
                combined_data
                    .permissions
                    .quotas
                    .insert(requesting_user.to_string(), requested_views);

                info!("Granted {} views to {} for image {}", requested_views, requesting_user, image_id);
                println!("[DEBUG] New user quota - inserted {} views for '{}' in quotas", requested_views, requesting_user);
                println!("[DEBUG] Updated quotas after insert: {:?}", combined_data.permissions.quotas);
            }
        }
    } else {
        // Owner has unlimited access - don't modify quotas
        info!("Owner {} accessing their own image - unlimited access", requesting_user);
    }

    // DEBUG: Log the final quotas before re-encoding
    println!("[DEBUG] Final quotas before re-encoding: {:?}", combined_data.permissions.quotas);

    // Re-serialize and re-encode
    let updated_payload = match bincode::serialize(&combined_data) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to serialize updated payload: {}", e),
                encrypted_image: None,
            };
        }
    };

    let updated_carrier = match lsb::encode(&carrier_img, &updated_payload) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ImageResponse {
                success: false,
                message: format!("Failed to encode updated image: {}", e),
                encrypted_image: None,
            };
        }
    };

    // Persist the updated carrier back to disk so changes (decrements/revocations) are authoritative
    if let Err(e) = updated_carrier.save(&image_path) {
        return P2PMessage::ImageResponse {
            success: false,
            message: format!("Failed to save updated image after permission change: {}", e),
            encrypted_image: None,
        };
    }

    // Convert to PNG bytes
    use image::ImageOutputFormat;
    use std::io::Cursor;

    let mut out_buf = Vec::new();
    if let Err(e) = updated_carrier.write_to(&mut Cursor::new(&mut out_buf), ImageOutputFormat::Png)
    {
        return P2PMessage::ImageResponse {
            success: false,
            message: format!("Failed to write image: {}", e),
            encrypted_image: None,
        };
    }

    P2PMessage::ImageResponse {
        success: true,
        message: format!(
            "Access granted: {} views for user {}",
            requested_views, requesting_user
        ),
        encrypted_image: Some(out_buf),
    }
}

/// Handle updating permissions for an existing user
async fn handle_update_permissions(
    image_id: &str,
    username: &str,
    new_quota: u32,
    image_store: &std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> P2PMessage {
    // Similar to handle_image_request but updates existing user quota
    let image_path = {
        let store = image_store.read().await;
        match store.get_image_path(image_id) {
            Some(path) => path.clone(),
            None => {
                return P2PMessage::UpdatePermissionsResponse {
                    success: false,
                    message: format!("Image {} not found", image_id),
                };
            }
        }
    };
    
    // Read, decode, update, encode, write back
    let encrypted_data = match fs::read(&image_path) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to read image: {}", e),
            };
        }
    };
    
    let carrier_img = match image::load_from_memory(&encrypted_data) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to load image: {}", e),
            };
        }
    };
    
    use crate::lsb;
    use crate::CombinedPayload;
    
    let payload = match lsb::decode(&carrier_img) {
        Ok(Some(data)) => data,
        Ok(None) | Err(_) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: "Failed to decode image".to_string(),
            };
        }
    };
    
    let mut combined_data: CombinedPayload = match bincode::deserialize(&payload) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to deserialize: {}", e),
            };
        }
    };
    
    // Update the quota
    combined_data
        .permissions
        .quotas
        .insert(username.to_string(), new_quota);
    
    // Re-encode and save
    let updated_payload = match bincode::serialize(&combined_data) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to serialize: {}", e),
            };
        }
    };
    
    let updated_carrier = match lsb::encode(&carrier_img, &updated_payload) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::UpdatePermissionsResponse {
                success: false,
                message: format!("Failed to encode: {}", e),
            };
        }
    };
    
    // Save back to the same file
    if let Err(e) = updated_carrier.save(&image_path) {
        return P2PMessage::UpdatePermissionsResponse {
            success: false,
            message: format!("Failed to save updated image: {}", e),
        };
    }
    
    P2PMessage::UpdatePermissionsResponse {
        success: true,
        message: format!("Updated {} to {} views", username, new_quota),
    }
}

/// Handle a thumbnail request - return a low-resolution blurred preview
async fn handle_thumbnail_request(
    image_id: &str,
    image_store: &std::sync::Arc<tokio::sync::RwLock<PeerImageStore>>,
) -> P2PMessage {
    use crate::lsb;
    use crate::CombinedPayload;
    use image::imageops;
    use std::io::Cursor;

    // Get the image path
    let image_path = {
        let store = image_store.read().await;
        match store.get_image_path(image_id) {
            Some(path) => path.clone(),
            None => {
                return P2PMessage::ThumbnailResponse {
                    success: false,
                    message: format!("Image {} not found", image_id),
                    thumbnail: None,
                };
            }
        }
    };

    // Read the encrypted image
    let encrypted_data = match fs::read(&image_path) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: format!("Failed to read image: {}", e),
                thumbnail: None,
            };
        }
    };

    // Load the image
    let carrier_img = match image::load_from_memory(&encrypted_data) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: format!("Failed to load image: {}", e),
                thumbnail: None,
            };
        }
    };

    // Decode embedded payload to get the actual image
    let payload = match lsb::decode(&carrier_img) {
        Ok(Some(data)) => data,
        Ok(None) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: "No embedded data found".to_string(),
                thumbnail: None,
            };
        }
        Err(e) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: format!("Failed to decode: {}", e),
                thumbnail: None,
            };
        }
    };

    let combined_data: CombinedPayload = match bincode::deserialize(&payload) {
        Ok(data) => data,
        Err(e) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: format!("Failed to deserialize: {}", e),
                thumbnail: None,
            };
        }
    };

    // Load the unified image from the payload
    let actual_img = match image::load_from_memory(&combined_data.unified_image) {
        Ok(img) => img,
        Err(e) => {
            return P2PMessage::ThumbnailResponse {
                success: false,
                message: format!("Failed to load embedded image: {}", e),
                thumbnail: None,
            };
        }
    };

    // Create a low-resolution thumbnail (150x150) with blur
    let thumbnail = actual_img.resize(150, 150, imageops::FilterType::Lanczos3);
    // Apply heavy blur to make it a preview only (sigma=8.0)
    let blurred = imageops::blur(&thumbnail, 8.0);

    // Convert to PNG bytes
    let mut thumb_buf = Cursor::new(Vec::new());
    if let Err(e) = blurred.write_to(&mut thumb_buf, image::ImageFormat::Png) {
        return P2PMessage::ThumbnailResponse {
            success: false,
            message: format!("Failed to encode thumbnail: {}", e),
            thumbnail: None,
        };
    }

    info!("Generated thumbnail for {} ({}x{} blurred)", image_id, 150, 150);
    println!("[INFO] Generated thumbnail for {}", image_id);

    P2PMessage::ThumbnailResponse {
        success: true,
        message: "Thumbnail generated".to_string(),
        thumbnail: Some(thumb_buf.into_inner()),
    }
}

/// Update permissions in a local image file (used for remote permission updates)
fn update_local_image_permissions(
    image_path: &PathBuf,
    user: &str,
    new_quota: u32,
) -> Result<()> {
    use crate::lsb;
    use crate::CombinedPayload;

    // Read the encrypted image file
    let encrypted_data = fs::read(image_path)
        .with_context(|| format!("Failed to read image file: {}", image_path.display()))?;

    // Load the image
    let carrier_img = image::load_from_memory(&encrypted_data)
        .with_context(|| format!("Failed to load image: {}", image_path.display()))?;

    // Decode embedded payload
    let payload = lsb::decode(&carrier_img)?
        .ok_or_else(|| anyhow::anyhow!("No embedded data found in image"))?;

    // Deserialize the combined payload
    let mut combined_data: CombinedPayload = bincode::deserialize(&payload)
        .context("Failed to deserialize payload")?;

    // Update the quota for the specified user
    combined_data.permissions.quotas.insert(user.to_string(), new_quota);

    info!("Updated local permissions for user {} to {} views", user, new_quota);

    // Re-serialize the updated payload
    let updated_payload = bincode::serialize(&combined_data)
        .context("Failed to serialize updated payload")?;

    // Re-encode into the carrier image
    let updated_carrier = lsb::encode(&carrier_img, &updated_payload)
        .context("Failed to encode updated image")?;

    // Save the updated image back to disk
    updated_carrier.save(image_path)
        .with_context(|| format!("Failed to save updated image to {}", image_path.display()))?;

    info!("Successfully saved updated image to {}", image_path.display());

    Ok(())
}

// =============================================================================
// P2P CLIENT HELPERS
// =============================================================================

/// Send a P2P message and receive response
pub async fn send_p2p_message(peer_addr: &str, message: P2PMessage) -> Result<P2PMessage> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    
    // Send message
    let msg_json = serde_json::to_string(&message)?;
    let msg_bytes = msg_json.as_bytes();
    
    stream.write_u32(msg_bytes.len() as u32).await?;
    stream.write_all(msg_bytes).await?;
    stream.flush().await?;
    
    // Read response
    let response_len = stream.read_u32().await?;
    let mut response_buf = vec![0u8; response_len as usize];
    stream.read_exact(&mut response_buf).await?;
    
    let response: P2PMessage = serde_json::from_slice(&response_buf)?;
    Ok(response)
}

/// Request an image from a peer
pub async fn request_image_from_peer(
    peer_addr: &str,
    requesting_user: &str,
    image_id: &str,
    requested_views: u32,
) -> Result<Vec<u8>> {
    let message = P2PMessage::ImageRequest {
        requesting_user: requesting_user.to_string(),
        image_id: image_id.to_string(),
        requested_views,
    };
    
    let response = send_p2p_message(peer_addr, message).await?;
    
    match response {
        P2PMessage::ImageResponse {
            success: true,
            encrypted_image: Some(data),
            ..
        } => Ok(data),
        P2PMessage::ImageResponse {
            success: false,
            message,
            ..
        } => bail!("Request failed: {}", message),
        _ => bail!("Unexpected response type"),
    }
}

/// List available images from a peer
pub async fn list_peer_images(peer_addr: &str, requesting_user: &str) -> Result<Vec<ImageMetadata>> {
    let message = P2PMessage::ListImages {
        requesting_user: requesting_user.to_string(),
    };
    
    let response = send_p2p_message(peer_addr, message).await?;
    
    match response {
        P2PMessage::ListImagesResponse { images } => Ok(images),
        _ => bail!("Unexpected response type"),
    }
}

/// Request a low-resolution thumbnail preview from a peer
pub async fn request_thumbnail_from_peer(
    peer_addr: &str,
    requesting_user: &str,
    image_id: &str,
) -> Result<Vec<u8>> {
    let message = P2PMessage::ThumbnailRequest {
        requesting_user: requesting_user.to_string(),
        image_id: image_id.to_string(),
    };
    
    let response = send_p2p_message(peer_addr, message).await?;
    
    match response {
        P2PMessage::ThumbnailResponse {
            success: true,
            thumbnail: Some(data),
            ..
        } => Ok(data),
        P2PMessage::ThumbnailResponse {
            success: false,
            message,
            ..
        } => bail!("Thumbnail request failed: {}", message),
        _ => bail!("Unexpected response type"),
    }
}