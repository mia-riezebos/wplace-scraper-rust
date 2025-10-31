use image::GenericImageView;
use rayon::prelude::*;
use std::sync::Mutex;

pub struct TemplatePixel {
    pub x: u32,
    pub y: u32,
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

pub struct MatchResult {
    pub matched: bool,
    pub center_x: f64,
    pub center_y: f64,
    pub match_percent: f64,
}

// Pre-computed tile pixel data for faster access
struct TilePixels {
    width: u32,
    height: u32,
    pixels: Vec<[u8; 4]>, // RGBA format, flat array
}

impl TilePixels {
    fn from_image(img: &image::RgbaImage) -> Self {
        let (width, height) = img.dimensions();
        let mut pixels = Vec::with_capacity((width * height) as usize);
        
        for y in 0..height {
            for x in 0..width {
                let pixel = img.get_pixel(x, y);
                pixels.push([pixel[0], pixel[1], pixel[2], pixel[3]]);
            }
        }
        
        Self { width, height, pixels }
    }
    
    #[inline(always)]
    fn get_pixel(&self, x: u32, y: u32) -> &[u8; 4] {
        let index = (y * self.width + x) as usize;
        &self.pixels[index]
    }
}

pub struct TemplateMatcher {
    width: u32,
    height: u32,
    non_transparent_pixels: Vec<TemplatePixel>,
}

impl TemplateMatcher {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let img = image::open(path)?;
        let (width, height) = img.dimensions();
        
        let mut non_transparent_pixels = Vec::new();
        
        for y in 0..height {
            for x in 0..width {
                let pixel = img.get_pixel(x, y);
                let alpha = pixel[3];
                
                if alpha > 0 {
                    non_transparent_pixels.push(TemplatePixel {
                        x,
                        y,
                        r: pixel[0],
                        g: pixel[1],
                        b: pixel[2],
                    });
                }
            }
        }
        
        Ok(TemplateMatcher {
            width,
            height,
            non_transparent_pixels,
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
                let mut remaining_pixels = total_non_transparent_pixels;

                // Check each non-transparent template pixel against tile
                for template_pixel in &self.non_transparent_pixels {
                    let tile_x = offset_x + template_pixel.x;
                    let tile_y = offset_y + template_pixel.y;

                    // Skip if template pixel extends beyond tile boundaries
                    if tile_x >= tile_width || tile_y >= tile_height {
                        remaining_pixels -= 1;
                        continue;
                    }

                    let tile_pixel = tile_pixels.get_pixel(tile_x, tile_y);
                    let tile_alpha = tile_pixel[3];

                    // Skip transparent pixels in tile
                    if tile_alpha == 0 {
                        remaining_pixels -= 1;
                        continue;
                    }

                    // Early exit optimization: if remaining pixels can't reach threshold, stop checking
                    let max_possible_matches = matches + remaining_pixels;
                    if max_possible_matches < min_matches_needed {
                        break; // Can't reach threshold, skip this offset position
                    }
                    remaining_pixels -= 1;

                    // Check exact color match (no tolerance)
                    if template_pixel.r == tile_pixel[0]
                        && template_pixel.g == tile_pixel[1]
                        && template_pixel.b == tile_pixel[2]
                    {
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
            })
        } else {
            Ok(MatchResult {
                matched: false,
                center_x: 0.0,
                center_y: 0.0,
                match_percent: 0.0,
            })
        }
    }
}

