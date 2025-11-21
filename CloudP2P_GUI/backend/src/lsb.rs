//! Manual implementation of Least Significant Bit (LSB) steganography.

use anyhow::{bail, Result};
// use image::{DynamicImage, GenericImageView, Rgba};
use image::{DynamicImage, GenericImageView};

/// Encodes a payload of bytes into the least significant bits of an image's pixels.
pub fn encode(img: &DynamicImage, payload: &[u8]) -> Result<DynamicImage> {
    let mut img_buf = img.to_rgba8();
    let (width, height) = img.dimensions();

    // Total bytes available for hiding data (1 bit per color channel byte)
    let capacity = (width * height * 4) as usize;

    // Total bits to encode: 32 bits for the payload length + payload bits
    let total_bits_needed = (payload.len() + 4) * 8;
    if total_bits_needed > capacity {
        bail!(
            "Image capacity too small. Needs {} bits, has {} bits available.",
            total_bits_needed,
            capacity
        );
    }

    // 1. Encode the payload length (as 32 bits)
    let len_bytes = (payload.len() as u32).to_be_bytes();

    // Create an iterator of bits to be encoded. First the length, then the payload.
    let bits_to_encode = len_bytes
        .iter()
        .chain(payload.iter())
        .flat_map(|&byte| (0..8).map(move |i| (byte >> (7 - i)) & 1));

    // Get a mutable iterator over the raw pixel data bytes
    let mut pixels = img_buf.iter_mut();

    // 2. Encode the bits into the LSB of each pixel byte
    for bit in bits_to_encode {
        if let Some(pixel_byte) = pixels.next() {
            // Clear the LSB (set to 0)
            *pixel_byte &= 0xFE;
            // Set the LSB to our data bit
            *pixel_byte |= bit;
        }
    }

    Ok(DynamicImage::ImageRgba8(img_buf))
}

/// Decodes a payload of bytes from the least significant bits of an image's pixels.
pub fn decode(img: &DynamicImage) -> Result<Option<Vec<u8>>> {
    let pixels: Vec<u8> = img.to_rgba8().into_raw();
    let mut bits = pixels.iter().map(|byte| byte & 1);

    // 1. Decode the payload length (first 32 bits)
    let mut len_bits = 0u32;
    for _ in 0..32 {
        len_bits = (len_bits << 1) | bits.next().unwrap_or(0) as u32;
    }
    let payload_len = len_bits as usize;

    // Check if the decoded length is plausible
    if payload_len > (pixels.len() - 32) / 8 {
        return Ok(None); // Likely no message here
    }

    // 2. Decode the payload data
    let mut payload = Vec::with_capacity(payload_len);
    for _ in 0..payload_len {
        let mut byte = 0u8;
        for _ in 0..8 {
            byte = (byte << 1) | bits.next().unwrap_or(0);
        }
        payload.push(byte);
    }

    Ok(Some(payload))
}
