use crate::coordinate::generate_url;
use crate::db::Cache;
use crate::logger::Logger;
use crate::matcher::TemplateMatcher;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::time::Instant;

// Helper function to get current hour in HH00 format (e.g., "0800", "1400")
fn get_current_hour_dir() -> String {
    let now = chrono::Local::now();
    now.format("%H00").to_string()
}

pub struct MatchStats {
    pub tiles_matched: Arc<std::sync::Mutex<u64>>,
    pub matches_found: Arc<std::sync::Mutex<u64>>,
    pub errors: Arc<std::sync::Mutex<u64>>,
    pub match_timestamps: Arc<std::sync::Mutex<VecDeque<Instant>>>,
}

impl MatchStats {
    pub fn new() -> Self {
        Self {
            tiles_matched: Arc::new(std::sync::Mutex::new(0)),
            matches_found: Arc::new(std::sync::Mutex::new(0)),
            errors: Arc::new(std::sync::Mutex::new(0)),
            match_timestamps: Arc::new(std::sync::Mutex::new(VecDeque::new())),
        }
    }

    pub fn record_match(&self) {
        let mut timestamps = self.match_timestamps.lock().unwrap();
        let now = Instant::now();
        timestamps.push_back(now);
        
        // Keep only last 10 seconds of timestamps
        let cutoff = now.checked_sub(tokio::time::Duration::from_secs(10)).unwrap_or(now);
        while timestamps.front().map_or(false, |&t| t < cutoff) {
            timestamps.pop_front();
        }
    }

    pub fn tiles_per_second(&self) -> f64 {
        let timestamps = self.match_timestamps.lock().unwrap();
        if timestamps.len() < 2 {
            return 0.0;
        }
        
        let now = Instant::now();
        let cutoff = now.checked_sub(tokio::time::Duration::from_secs(1)).unwrap_or(now);
        let recent_count = timestamps.iter().filter(|&&t| t >= cutoff).count();
        recent_count as f64
    }
}

pub async fn match_tile_worker(
    worker_id: usize,
    config: Arc<crate::config::Config>,
    cache: Arc<std::sync::Mutex<Cache>>,
    matcher: Arc<TemplateMatcher>,
    stats: Arc<MatchStats>,
    running: Arc<std::sync::Mutex<bool>>,
    processed_tiles: Arc<std::sync::Mutex<HashSet<(i32, i32)>>>,
    logger: Arc<Logger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logger.log(format!("[Match Worker {}] Started", worker_id + 1));

    loop {
        // Check if we should be running (skip processing if stopped)
        if !*running.lock().unwrap() {
            // Worker is stopped, wait a bit before checking again
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            continue;
        }
        
        // Find tile files to process
        let tiles_dir = if config.save_to_hour_dir {
            // Only from current hour directory
            let current_hour_dir = get_current_hour_dir();
            PathBuf::from("outputs/tiles").join(&current_hour_dir)
        } else {
            // From root tiles directory
            PathBuf::from("outputs/tiles")
        };
        
        let tile_files = find_tile_files(&tiles_dir.to_string_lossy()).await?;

        if tile_files.is_empty() {
            // No tiles found, wait a bit
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            continue;
        }
        
        // Find a tile that hasn't been processed yet
        let mut found_tile: Option<(PathBuf, (i32, i32))> = None;
        
        {
            let mut processed = processed_tiles.lock().unwrap();
            let cache_guard = cache.lock().unwrap();
            for file in &tile_files {
                if let Ok((x, y)) = parse_tile_file_name(file) {
                    // Check both in-memory set and database
                    let is_processed = processed.contains(&(x, y)) || 
                        cache_guard.is_processed(x, y).unwrap_or(false);
                    
                    if !is_processed {
                        processed.insert((x, y));
                        found_tile = Some((file.clone(), (x, y)));
                        break;
                    }
                }
            }
        }
        
        let (tile_file, (x, y)) = match found_tile {
            Some((file, coords)) => (file, coords),
            None => {
                // All tiles have been processed, wait a bit
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        logger.log(format!("[Match Worker {}] Processing: {}", worker_id + 1, tile_file.display()));

        match fs::read(&tile_file).await {
            Ok(tile_buffer) => {
                let match_result = matcher.find_match(&tile_buffer, config.match_threshold);
                
                match match_result {
                    Ok(mr) => {
                        {
                            let mut stats_guard = stats.tiles_matched.lock().unwrap();
                            *stats_guard += 1;
                        }

                        // Record match timestamp for rate calculation
                        stats.record_match();

                        // Mark tile as processed in database
                        {
                            let cache_guard = cache.lock().unwrap();
                            cache_guard.mark_processed(x, y).unwrap_or_else(|e| {
                                logger.log(format!("[Match Worker {}] Error marking tile ({}, {}) as processed: {}", 
                                    worker_id + 1, x, y, e));
                            });
                        }

                        if mr.matched {
                            // Convert pixel coordinates to grid coordinates (tile is 1000x1000 pixels)
                            let x_grid = x as f64 + mr.center_x / 1000.0;
                            let y_grid = y as f64 + mr.center_y / 1000.0;
                            let match_url = generate_url(
                                x_grid,
                                y_grid,
                                &config.zoom_level,
                                true,
                            );

                            // Check if match already exists before adding
                            let should_add = {
                                let cache_guard = cache.lock().unwrap();
                                !cache_guard.is_match_exists(&match_url).unwrap_or(false)
                            };

                            if should_add {
                                // Store match in database with match-centered URL
                                {
                                    let mut cache_guard = cache.lock().unwrap();
                                    cache_guard.mark_match(x, y, 200, &match_url)?;
                                }

                                // Append to matches file
                                append_match(&match_url).await?;

                                logger.log(format!(
                                    "[Match Worker {}] ✓ Match found at tile ({}, {}) - {:.1}% match",
                                    worker_id + 1,
                                    x,
                                    y,
                                    mr.match_percent * 100.0
                                ));
                                logger.log(format!("[Match Worker {}]   URL: {}", worker_id + 1, match_url));

                                {
                                    let mut stats_guard = stats.matches_found.lock().unwrap();
                                    *stats_guard += 1;
                                }
                            } else {
                                logger.log(format!(
                                    "[Match Worker {}] Match found at tile ({}, {}) but already exists - skipping",
                                    worker_id + 1,
                                    x,
                                    y
                                ));
                            }
                        } else {
                            logger.log(format!(
                                "[Match Worker {}] No match found at tile ({}, {}) - {:.1}% match",
                                worker_id + 1,
                                x,
                                y,
                                mr.match_percent * 100.0
                            ));
                        }
                    }
                    Err(e) => {
                        logger.log(format!("[Match Worker {}] Error matching tile {}: {}", 
                            worker_id + 1, tile_file.display(), e));
                        {
                            let mut stats_guard = stats.errors.lock().unwrap();
                            *stats_guard += 1;
                        }
                    }
                }
            }
            Err(e) => {
                logger.log(format!("[Match Worker {}] Error reading tile {}: {}", 
                    worker_id + 1, tile_file.display(), e));
                {
                    let mut stats_guard = stats.errors.lock().unwrap();
                    *stats_guard += 1;
                }
            }
        }

        // Tiles are NOT deleted - they remain in outputs/tiles/ for reference
    }
    
    // Note: This code is unreachable as the loop runs indefinitely
    // Workers are cancelled when the process exits
    Ok(())
}

async fn find_tile_files(dir: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tile_files = Vec::new();
    
    if let Ok(mut entries) = fs::read_dir(dir).await {
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            
            if path.is_dir() {
                // Recursively scan subdirectories - use Box::pin for recursive async
                let sub_files = Box::pin(find_tile_files(path.to_str().unwrap())).await?;
                tile_files.extend(sub_files);
            } else if path.extension().and_then(|s| s.to_str()) == Some("png") {
                tile_files.push(path);
            }
        }
    }
    
    Ok(tile_files)
}

fn parse_tile_file_name(file_path: &Path) -> Result<(i32, i32), Box<dyn std::error::Error + Send + Sync>> {
    let components: Vec<_> = file_path.components().collect();
    
    // Find 'tiles' in path
    let tiles_idx = components.iter()
        .position(|c| c.as_os_str() == "tiles")
        .ok_or("Invalid tile file path: 'tiles' not found")?;
    
    // New structure: tiles/HH00/x/y.png
    // Old structure: tiles/x/y.png (for backward compatibility)
    // Check if next component is HH00 format (4 digits) or x coordinate
    let hour_idx = tiles_idx + 1;
    let x_idx = if hour_idx < components.len() {
        let hour_str = components[hour_idx].as_os_str().to_str().unwrap_or("");
        // Check if it's a 4-digit hour format (HH00)
        if hour_str.len() == 4 && hour_str.chars().all(|c| c.is_ascii_digit()) {
            hour_idx + 1 // Skip hour directory
        } else {
            hour_idx // It's the x coordinate (old format)
        }
    } else {
        return Err("Invalid tile file path: insufficient components".into());
    };
    
    if x_idx + 1 >= components.len() {
        return Err("Invalid tile file path: insufficient components".into());
    }
    
    let x_str = components[x_idx].as_os_str().to_str()
        .ok_or("Invalid tile X coordinate")?;
    let y_str = components[x_idx + 1].as_os_str().to_str()
        .ok_or("Invalid tile Y coordinate")?
        .replace(".png", "");
    
    let x = x_str.parse::<i32>()?;
    let y = y_str.parse::<i32>()?;
    
    Ok((x, y))
}

async fn append_match(url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let matches_file = PathBuf::from("outputs/matches.txt");
    
    // Create directory if it doesn't exist
    if let Some(parent) = matches_file.parent() {
        fs::create_dir_all(parent).await?;
    }
    
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&matches_file)
        .await?
        .write_all(format!("{}\n", url).as_bytes())
        .await?;
    
    Ok(())
}

use tokio::io::AsyncWriteExt;

