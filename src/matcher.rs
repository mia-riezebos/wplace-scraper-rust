use image::GenericImageView;
use rayon::prelude::*;
use std::sync::Mutex;

// Pack RGB into u32 for faster comparison (0xRRGGBB format)
#[derive(Clone, Copy)]
struct PackedPixel {
    rgb: u32, // Packed as 0xRRGGBB (alpha not needed for template)
}

impl PackedPixel {
    #[inline(always)]
    fn from_rgb(r: u8, g: u8, b: u8) -> Self {
        Self {
            rgb: ((r as u32) << 16) | ((g as u32) << 8) | (b as u32),
        }
    }
}

pub struct TemplatePixel {
    #[allow(dead_code)]
    pub x: u32,
    #[allow(dead_code)]
    pub y: u32,
    #[allow(dead_code)]
    packed_rgb: PackedPixel,
}

pub struct MatchResult {
    pub matched: bool,
    pub center_x: f64,
    pub center_y: f64,
    pub match_percent: f64,
    // When matching stitched rows, store which tile(s) the match spans
    pub tile_x: Option<i32>,
    pub tile_y: Option<i32>,
}

// Pre-computed tile pixel data for faster access
struct TilePixels {
    width: u32,
    #[allow(dead_code)]
    height: u32,
    pixels: Vec<u32>, // Packed RGBA format (0xRRGGBBAA), flat array for cache-friendly access
}

impl TilePixels {
    fn from_image(img: &image::RgbaImage) -> Self {
        let (width, height) = img.dimensions();
        // Use raw buffer access for faster conversion
        let raw = img.as_raw();
        let mut pixels = Vec::with_capacity((width * height) as usize);

        // Process in chunks of 4 bytes (RGBA) for better cache locality
        for chunk in raw.chunks_exact(4) {
            let r = chunk[0] as u32;
            let g = chunk[1] as u32;
            let b = chunk[2] as u32;
            let a = chunk[3] as u32;
            // Pack as 0xRRGGBBAA
            pixels.push((r << 24) | (g << 16) | (b << 8) | a);
        }

        Self {
            width,
            height,
            pixels,
        }
    }

    #[inline(always)]
    fn get_pixel(&self, x: u32, y: u32) -> u32 {
        unsafe {
            // Safe because we check bounds before calling, and we've pre-allocated
            *self.pixels.get_unchecked((y * self.width + x) as usize)
        }
    }
}

pub struct TemplateMatcher {
    width: u32,
    height: u32,
    non_transparent_pixels: Vec<TemplatePixel>,
    // Pre-computed offsets for faster access
    pixel_offsets: Vec<(u32, u32, PackedPixel)>,
}

impl TemplateMatcher {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let img = image::open(path)?;
        let (width, height) = img.dimensions();

        let mut non_transparent_pixels = Vec::new();
        let mut pixel_offsets = Vec::new();

        for y in 0..height {
            for x in 0..width {
                let pixel = img.get_pixel(x, y);
                let alpha = pixel[3];

                if alpha > 0 {
                    let packed = PackedPixel::from_rgb(pixel[0], pixel[1], pixel[2]);
                    non_transparent_pixels.push(TemplatePixel {
                        x,
                        y,
                        packed_rgb: packed,
                    });
                    pixel_offsets.push((x, y, packed));
                }
            }
        }

        Ok(TemplateMatcher {
            width,
            height,
            non_transparent_pixels,
            pixel_offsets,
        })
    }

    pub fn find_match(
        &self,
        tile_buffer: &[u8],
        threshold: f64,
    ) -> Result<MatchResult, Box<dyn std::error::Error + Send + Sync>> {
        let tile_img = image::load_from_memory(tile_buffer)?;
        let tile_rgba = tile_img.to_rgba8();
        let (tile_width, tile_height) = tile_rgba.dimensions();

        if self.non_transparent_pixels.is_empty() {
            return Ok(MatchResult {
                matched: false,
                center_x: 0.0,
                center_y: 0.0,
                match_percent: 0.0,
                tile_x: None,
                tile_y: None,
            });
        }

        // Pre-compute tile pixel data for faster access
        let tile_pixels = TilePixels::from_image(&tile_rgba);

        let total_non_transparent_pixels = self.non_transparent_pixels.len();
        let min_matches_needed = (total_non_transparent_pixels as f64 * threshold).ceil() as usize;

        // Early exit: if template is larger than tile, no match possible
        if self.width > tile_width || self.height > tile_height {
            return Ok(MatchResult {
                matched: false,
                center_x: 0.0,
                center_y: 0.0,
                match_percent: 0.0,
                tile_x: None,
                tile_y: None,
            });
        }

        let best_match = Mutex::new(None::<(f64, f64, f64)>);

        // Parallelize the outer loop (offset_y positions)
        let max_offset_y = tile_height.saturating_sub(self.height);
        let max_offset_x = tile_width.saturating_sub(self.width);

        // Process offset_y positions in parallel
        (0..=max_offset_y).into_par_iter().for_each(|offset_y| {
            let mut local_best: Option<(f64, f64, f64)> = None;

            for offset_x in 0..=max_offset_x {
                let mut matches = 0;
                let mut checked_pixels = 0;

                // Pre-check bounds for this offset position
                let max_x = offset_x + self.width;
                let max_y = offset_y + self.height;
                
                if max_x > tile_width || max_y > tile_height {
                    continue;
                }

                // Check each non-transparent template pixel against tile
                // Use pre-computed offsets for better cache locality
                for (template_x, template_y, template_packed) in &self.pixel_offsets {
                    let tile_x = offset_x + template_x;
                    let tile_y = offset_y + template_y;

                    // Bounds check already done above, but double-check for safety
                    if tile_x >= tile_width || tile_y >= tile_height {
                        continue;
                    }

                    let tile_pixel = tile_pixels.get_pixel(tile_x, tile_y);
                    let tile_alpha = tile_pixel & 0xFF; // Extract alpha channel

                    // Skip transparent pixels in tile
                    if tile_alpha == 0 {
                        checked_pixels += 1;
                        continue;
                    }

                    checked_pixels += 1;

                    // Early exit optimization: if remaining pixels can't reach threshold, stop checking
                    let remaining_pixels = total_non_transparent_pixels - checked_pixels;
                    let max_possible_matches = matches + remaining_pixels;
                    if max_possible_matches < min_matches_needed {
                        break; // Can't reach threshold, skip this offset position
                    }

                    // Fast color comparison using packed RGB (compare top 24 bits, ignore alpha)
                    let tile_rgb = (tile_pixel >> 8) & 0xFFFFFF; // Extract RGB (top 24 bits)
                    if template_packed.rgb == tile_rgb {
                        matches += 1;
                    }
                }

                // Calculate match percentage based on non-transparent pixels only
                let match_percent = matches as f64 / total_non_transparent_pixels as f64;

                // Update best match if this position meets threshold and is better than previous
                if match_percent >= threshold {
                    if local_best.is_none() || match_percent > local_best.unwrap().0 {
                        let center_x = (offset_x + self.width / 2) as f64;
                        let center_y = (offset_y + self.height / 2) as f64;
                        local_best = Some((match_percent, center_x, center_y));
                    }
                }
            }

            // Update global best match atomically
            if let Some(local_best_val) = local_best {
                let mut best = best_match.lock().unwrap();
                if best.is_none() || local_best_val.0 > best.unwrap().0 {
                    *best = Some(local_best_val);
                }
            }
        });

        // Extract best match
        if let Some((match_percent, center_x, center_y)) = best_match.into_inner().unwrap() {
            Ok(MatchResult {
                matched: true,
                center_x,
                center_y,
                match_percent,
                tile_x: None,
                tile_y: None,
            })
        } else {
            Ok(MatchResult {
                matched: false,
                center_x: 0.0,
                center_y: 0.0,
                match_percent: 0.0,
                tile_x: None,
                tile_y: None,
            })
        }
    }

    /// Match template against a stitched row (multiple tiles horizontally)
    /// Returns matches with tile coordinates mapped back
    pub fn find_matches_in_row(
        &self,
        stitched_row_buffer: &[u8],
        row_y: i32,
        threshold: f64,
        tile_width: u32, // Width of each individual tile (e.g., 1000)
    ) -> Result<Vec<MatchResult>, Box<dyn std::error::Error + Send + Sync>> {
        let row_img = image::load_from_memory(stitched_row_buffer)?;
        let row_rgba = row_img.to_rgba8();
        let (row_width, row_height) = row_rgba.dimensions();

        if self.non_transparent_pixels.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-compute row pixel data
        let row_pixels = TilePixels::from_image(&row_rgba);

        let total_non_transparent_pixels = self.non_transparent_pixels.len();
        let min_matches_needed = (total_non_transparent_pixels as f64 * threshold).ceil() as usize;

        if self.width > row_width || self.height > row_height {
            return Ok(Vec::new());
        }

        let matches = Mutex::new(Vec::new());

        let max_offset_y = row_height.saturating_sub(self.height);
        let max_offset_x = row_width.saturating_sub(self.width);

        // Process offset_y positions in parallel
        (0..=max_offset_y).into_par_iter().for_each(|offset_y| {
            let mut local_matches: Vec<(f64, f64, f64)> = Vec::new();

            for offset_x in 0..=max_offset_x {
                let mut matches_count = 0;
                let mut checked_pixels = 0;

                let max_x = offset_x + self.width;
                let max_y = offset_y + self.height;

                if max_x > row_width || max_y > row_height {
                    continue;
                }

                for (template_x, template_y, template_packed) in &self.pixel_offsets {
                    let row_x = offset_x + template_x;
                    let row_y = offset_y + template_y;

                    if row_x >= row_width || row_y >= row_height {
                        continue;
                    }

                    let row_pixel = row_pixels.get_pixel(row_x, row_y);
                    let row_alpha = row_pixel & 0xFF;

                    if row_alpha == 0 {
                        checked_pixels += 1;
                        continue;
                    }

                    checked_pixels += 1;

                    let remaining_pixels = total_non_transparent_pixels - checked_pixels;
                    let max_possible_matches = matches_count + remaining_pixels;
                    if max_possible_matches < min_matches_needed {
                        break;
                    }

                    let row_rgb = (row_pixel >> 8) & 0xFFFFFF;
                    if template_packed.rgb == row_rgb {
                        matches_count += 1;
                    }
                }

                let match_percent = matches_count as f64 / total_non_transparent_pixels as f64;

                if match_percent >= threshold {
                    let center_x = (offset_x + self.width / 2) as f64;
                    let center_y = (offset_y + self.height / 2) as f64;
                    local_matches.push((match_percent, center_x, center_y));
                }
            }

            // Add local matches to global list
            let mut global_matches = matches.lock().unwrap();
            global_matches.extend(local_matches);
        });

        // Convert matches to MatchResult, mapping pixel coordinates back to tile coordinates
        let mut results = Vec::new();
        for (match_percent, pixel_x, pixel_y) in matches.into_inner().unwrap() {
            // Map pixel coordinates back to tile coordinates
            // pixel_x is in the stitched row (0 to row_width-1)
            // Each tile is tile_width pixels wide
            let tile_x = (pixel_x / tile_width as f64) as i32;
            // Center within the tile
            let center_x_in_tile = pixel_x - (tile_x as f64 * tile_width as f64);

            results.push(MatchResult {
                matched: true,
                center_x: center_x_in_tile,
                center_y: pixel_y,
                match_percent,
                tile_x: Some(tile_x),
                tile_y: Some(row_y),
            });
        }

        Ok(results)
    }
}
