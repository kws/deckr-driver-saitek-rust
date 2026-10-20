use anyhow::{bail, Context, Result};
use image::imageops::FilterType;

use crate::protocol::{validate_frame_size, BYTES_PER_PIXEL, FRAME_BYTES, HEIGHT, WIDTH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOrder {
    Rgb,
    Bgr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOrder {
    TopDown,
    BottomUp,
}

pub const DEFAULT_CHANNEL_ORDER: ChannelOrder = ChannelOrder::Bgr;
pub const DEFAULT_ROW_ORDER: RowOrder = RowOrder::BottomUp;

pub fn encoded_image_to_fip_frame(image: &[u8]) -> Result<Vec<u8>> {
    let decoded = image::load_from_memory(image).context("decoding raster command image")?;
    let rgb = decoded.to_rgb8();
    let normalized = if rgb.width() == WIDTH as u32 && rgb.height() == HEIGHT as u32 {
        rgb
    } else {
        image::imageops::resize(&rgb, WIDTH as u32, HEIGHT as u32, FilterType::Lanczos3)
    };
    frame_from_top_down_rgb_bytes(
        normalized.as_raw(),
        DEFAULT_CHANNEL_ORDER,
        DEFAULT_ROW_ORDER,
    )
}

/// Manager-local notice: never leave an old radar image looking live.
pub fn make_stream_lost_frame() -> Result<Vec<u8>> {
    let mut rgb = vec![0u8; FRAME_BYTES];
    draw_notice_line(&mut rgb, "STREAM LOST", 88, 4, [255, 170, 40]);
    draw_notice_line(&mut rgb, "WAITING FOR FRAMES", 144, 2, [200, 200, 200]);
    frame_from_top_down_rgb_bytes(&rgb, DEFAULT_CHANNEL_ORDER, DEFAULT_ROW_ORDER)
}

fn draw_notice_line(rgb: &mut [u8], text: &str, top: usize, scale: usize, color: [u8; 3]) {
    let left = (WIDTH - (text.len() * 6 - 1) * scale) / 2;
    for (index, letter) in text.bytes().enumerate() {
        // A small fixed font keeps the USB fallback independent of host fonts.
        let rows = match letter {
            b'A' => [14, 17, 17, 31, 17, 17, 17],
            b'E' => [31, 16, 16, 30, 16, 16, 31],
            b'F' => [31, 16, 16, 30, 16, 16, 16],
            b'G' => [14, 17, 16, 23, 17, 17, 14],
            b'I' => [31, 4, 4, 4, 4, 4, 31],
            b'L' => [16, 16, 16, 16, 16, 16, 31],
            b'M' => [17, 27, 21, 21, 17, 17, 17],
            b'N' => [17, 25, 25, 21, 19, 19, 17],
            b'O' => [14, 17, 17, 17, 17, 17, 14],
            b'R' => [30, 17, 17, 30, 20, 18, 17],
            b'S' => [15, 16, 16, 14, 1, 1, 30],
            b'T' => [31, 4, 4, 4, 4, 4, 4],
            b'W' => [17, 17, 17, 21, 21, 27, 17],
            _ => [0; 7],
        };
        for (y, row) in rows.iter().enumerate() {
            for x in 0..5 {
                if row & (1 << (4 - x)) == 0 {
                    continue;
                }
                for dy in 0..scale {
                    for dx in 0..scale {
                        let offset =
                            ((top + y * scale + dy) * WIDTH + left + (index * 6 + x) * scale + dx)
                                * BYTES_PER_PIXEL;
                        rgb[offset..offset + 3].copy_from_slice(&color);
                    }
                }
            }
        }
    }
}

pub fn make_quadrant_frame(channel_order: ChannelOrder, row_order: RowOrder) -> Result<Vec<u8>> {
    let mut top_down_rgb = vec![0u8; FRAME_BYTES];

    for logical_y in 0..HEIGHT {
        for x in 0..WIDTH {
            let rgb = quadrant_color(x, logical_y);
            let offset = (logical_y * WIDTH + x) * BYTES_PER_PIXEL;
            top_down_rgb[offset..offset + BYTES_PER_PIXEL].copy_from_slice(&rgb);
        }
    }

    frame_from_top_down_rgb_bytes(&top_down_rgb, channel_order, row_order)
}

pub fn frame_from_top_down_rgb_bytes(
    rgb_bytes: &[u8],
    channel_order: ChannelOrder,
    row_order: RowOrder,
) -> Result<Vec<u8>> {
    validate_frame_size(rgb_bytes)?;

    let row_bytes = WIDTH * BYTES_PER_PIXEL;
    let mut frame = vec![0u8; FRAME_BYTES];

    for logical_y in 0..HEIGHT {
        let source_row_start = logical_y * row_bytes;
        for x in 0..WIDTH {
            let payload_y = payload_y_for_pixel(logical_y, row_order);
            let source_offset = source_row_start + x * BYTES_PER_PIXEL;
            let target_offset = (payload_y * WIDTH + x) * BYTES_PER_PIXEL;
            let rgb = &rgb_bytes[source_offset..source_offset + BYTES_PER_PIXEL];
            encode_rgb_bytes(
                rgb,
                channel_order,
                &mut frame[target_offset..target_offset + 3],
            )?;
        }
    }

    Ok(frame)
}

fn quadrant_color(x: usize, y: usize) -> [u8; 3] {
    let left = x < WIDTH / 2;
    let top = y < HEIGHT / 2;

    match (top, left) {
        (true, true) => [255, 0, 0],
        (true, false) => [0, 255, 0],
        (false, true) => [0, 0, 255],
        (false, false) => [255, 255, 255],
    }
}

fn encode_rgb_bytes(rgb: &[u8], channel_order: ChannelOrder, out: &mut [u8]) -> Result<()> {
    if rgb.len() != 3 || out.len() != 3 {
        bail!("pixel slices must be exactly three bytes");
    }
    match channel_order {
        ChannelOrder::Rgb => out.copy_from_slice(rgb),
        ChannelOrder::Bgr => {
            out[0] = rgb[2];
            out[1] = rgb[1];
            out[2] = rgb[0];
        }
    }
    Ok(())
}

fn payload_y_for_pixel(logical_y: usize, row_order: RowOrder) -> usize {
    match row_order {
        RowOrder::TopDown => logical_y,
        RowOrder::BottomUp => HEIGHT - 1 - logical_y,
    }
}

#[cfg(test)]
mod tests {
    use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb};
    use std::io::Cursor;

    use super::*;

    #[test]
    fn quadrant_frame_defaults_to_confirmed_fip_layout() {
        let frame = make_quadrant_frame(DEFAULT_CHANNEL_ORDER, DEFAULT_ROW_ORDER).unwrap();

        assert_eq!(frame.len(), FRAME_BYTES);
        assert_eq!(payload_pixel(&frame, 0, 0), &[255, 0, 0]);
        assert_eq!(payload_pixel(&frame, WIDTH - 1, 0), &[255, 255, 255]);
        assert_eq!(payload_pixel(&frame, 0, HEIGHT - 1), &[0, 0, 255]);
        assert_eq!(payload_pixel(&frame, WIDTH - 1, HEIGHT - 1), &[0, 255, 0]);
    }

    #[test]
    fn rgb_top_down_is_available_for_comparison() {
        let frame = make_quadrant_frame(ChannelOrder::Rgb, RowOrder::TopDown).unwrap();

        assert_eq!(pixel(&frame, 0, 0), &[255, 0, 0]);
        assert_eq!(pixel(&frame, WIDTH - 1, 0), &[0, 255, 0]);
        assert_eq!(pixel(&frame, 0, HEIGHT - 1), &[0, 0, 255]);
        assert_eq!(pixel(&frame, WIDTH - 1, HEIGHT - 1), &[255, 255, 255]);
    }

    #[test]
    fn bgr_channel_order_swaps_red_and_blue() {
        let frame = make_quadrant_frame(ChannelOrder::Bgr, RowOrder::TopDown).unwrap();

        assert_eq!(pixel(&frame, 0, 0), &[0, 0, 255]);
        assert_eq!(pixel(&frame, 0, HEIGHT - 1), &[255, 0, 0]);
    }

    #[test]
    fn bottom_up_row_order_starts_payload_with_bottom_left() {
        let frame = make_quadrant_frame(ChannelOrder::Rgb, RowOrder::BottomUp).unwrap();

        assert_eq!(payload_pixel(&frame, 0, 0), &[0, 0, 255]);
        assert_eq!(payload_pixel(&frame, 0, HEIGHT - 1), &[255, 0, 0]);
    }

    #[test]
    fn encoded_png_converts_to_default_fip_layout() {
        let mut image = ImageBuffer::new(WIDTH as u32, HEIGHT as u32);
        for y in 0..HEIGHT {
            for x in 0..WIDTH {
                image.put_pixel(x as u32, y as u32, Rgb(quadrant_color(x, y)));
            }
        }
        let mut png = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut png, ImageFormat::Png)
            .unwrap();

        let frame = encoded_image_to_fip_frame(&png.into_inner()).unwrap();

        assert_eq!(frame.len(), FRAME_BYTES);
        assert_eq!(payload_pixel(&frame, 0, 0), &[255, 0, 0]);
        assert_eq!(payload_pixel(&frame, WIDTH - 1, HEIGHT - 1), &[0, 255, 0]);
    }

    fn pixel(frame: &[u8], x: usize, logical_y: usize) -> &[u8] {
        let offset = (logical_y * WIDTH + x) * BYTES_PER_PIXEL;
        &frame[offset..offset + BYTES_PER_PIXEL]
    }

    fn payload_pixel(frame: &[u8], x: usize, payload_y: usize) -> &[u8] {
        let offset = (payload_y * WIDTH + x) * BYTES_PER_PIXEL;
        &frame[offset..offset + BYTES_PER_PIXEL]
    }
}
