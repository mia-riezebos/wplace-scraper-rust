use crate::coordinate::generate_url;
use crate::db::Cache;
use crate::logger::Logger;
use crate::matcher::TemplateMatcher;
use crate::utils;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::time::Instant;

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
        let cutoff = now
            .checked_sub(tokio::time::Duration::from_secs(10))
            .unwrap_or(now);
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
        let cutoff = now
            .checked_sub(tokio::time::Duration::from_secs(1))
            .unwrap_or(now);
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

        // If in consecutive mode, check for complete rows first (much faster!)
        if config.tile_selection_mode == crate::config::TileSelectionMode::Consecutive {
            if let Ok(Some(row_y)) = find_complete_row(&cache, processed_tiles.clone()).await {
                // Process complete row with stitching
                match process_row_with_stitching(
                    worker_id,
                    row_y,
                    &config,
                    &cache,
                    &matcher,
                    &stats,
                    processed_tiles.clone(),
                    &logger,
                )
                .await
                {
                    Ok(_) => {
                        logger.log(format!(
                            "[Match Worker {}] ✓ Processed complete row {} with stitching",
                            worker_id + 1,
                            row_y
                        ));
                        continue; // Continue to next iteration
                    }
                    Err(e) => {
                        logger.log(format!(
                            "[Match Worker {}] Error processing row {}: {}",
                            worker_id + 1,
                            row_y,
                            e
                        ));
                        // Fall through to individual tile processing
                    }
                }
            }
        }

               // Find tile files to process
               let tile_files = if config.save_to_hour_dir {
                   // Only from current hour directory
                   let current_hour_dir = utils::get_current_hour_dir();
                   let tiles_dir = PathBuf::from("outputs/tiles").join(&current_hour_dir);
                   find_tile_files(&tiles_dir.to_string_lossy()).await?
               } else {
                   // From root tiles directory - process ALL tiles (including hour subdirectories)
                   let tiles_dir = PathBuf::from("outputs/tiles");
                   find_tile_files(&tiles_dir.to_string_lossy()).await?
               };

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
                       if let Ok((x, y)) = utils::parse_tile_file_name(file) {
                           // Check both in-memory set and database
                           let is_processed = processed.contains(&(x, y))
                               || cache_guard.is_processed(x, y).unwrap_or(false);

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

        logger.log(format!(
            "[Match Worker {}] Processing: {}",
            worker_id + 1,
            tile_file.display()
        ));

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
                            // Use tile coordinates from match result if available (from stitched rows), otherwise use current tile
                            let (match_x, match_y) = match (mr.tile_x, mr.tile_y) {
                                (Some(tx), Some(ty)) => (tx, ty),
                                _ => (x, y),
                            };
                            
                            // Convert pixel coordinates to grid coordinates (tile is 1000x1000 pixels)
                            let x_grid = match_x as f64 + mr.center_x / 1000.0;
                            let y_grid = match_y as f64 + mr.center_y / 1000.0;
                            let match_url = generate_url(x_grid, y_grid, &config.zoom_level, true);

                            // Check if match already exists before adding
                            let should_add = {
                                let cache_guard = cache.lock().unwrap();
                                !cache_guard.is_match_exists(&match_url).unwrap_or(false)
                            };

                            if should_add {
                                // Store match in database with match-centered URL
                                {
                                    let mut cache_guard = cache.lock().unwrap();
                                    cache_guard.mark_match(match_x, match_y, 200, &match_url)?;
                                }

                                // Append to matches file
                                append_match(&match_url).await?;

                                logger.log(format!(
                                    "[Match Worker {}] ✓ Match found at tile ({}, {}) - {:.1}% match",
                                    worker_id + 1,
                                    match_x,
                                    match_y,
                                    mr.match_percent * 100.0
                                ));
                                logger.log(format!(
                                    "[Match Worker {}]   URL: {}",
                                    worker_id + 1,
                                    match_url
                                ));

                                {
                                    let mut stats_guard = stats.matches_found.lock().unwrap();
                                    *stats_guard += 1;
                                }
                            } else {
                                logger.log(format!(
                                    "[Match Worker {}] Match found at tile ({}, {}) but already exists - skipping",
                                    worker_id + 1,
                                    match_x,
                                    match_y
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
                        logger.log(format!(
                            "[Match Worker {}] Error matching tile {}: {}",
                            worker_id + 1,
                            tile_file.display(),
                            e
                        ));
                        {
                            let mut stats_guard = stats.errors.lock().unwrap();
                            *stats_guard += 1;
                        }
                    }
                }
            }
            Err(e) => {
                logger.log(format!(
                    "[Match Worker {}] Error reading tile {}: {}",
                    worker_id + 1,
                    tile_file.display(),
                    e
                ));
                {
                    let mut stats_guard = stats.errors.lock().unwrap();
                    *stats_guard += 1;
                }
            }
        }

        // Tiles are NOT deleted - they remain in outputs/tiles/ for reference
    }
}

/// Check if a complete row exists (all tiles from x=0 to x=2047 for a given y)
async fn find_complete_row(
    cache: &Arc<std::sync::Mutex<Cache>>,
    processed_tiles: Arc<std::sync::Mutex<HashSet<(i32, i32)>>>,
) -> Result<Option<i32>, Box<dyn std::error::Error + Send + Sync>> {
    // Get all cached tiles
    let cached_tiles = {
        let cache_guard = cache.lock().unwrap();
        cache_guard.get_all_cached_tiles().unwrap_or_default()
    };

    // Group tiles by y coordinate
    use std::collections::HashMap;
    let mut tiles_by_y: HashMap<i32, HashSet<i32>> = HashMap::new();
    let processed = processed_tiles.lock().unwrap();

    for (x, y) in cached_tiles {
        // Skip if already processed
        if processed.contains(&(x, y)) {
            continue;
        }

        tiles_by_y.entry(y).or_insert_with(HashSet::new).insert(x);
    }

    // Find a row with all tiles from 0 to 2047
    for y in 0..2048 {
        if let Some(x_set) = tiles_by_y.get(&y) {
            if x_set.len() == 2048 {
                // Check if all tiles from 0 to 2047 exist
                let mut complete = true;
                for x in 0..2048 {
                    if !x_set.contains(&x) {
                        complete = false;
                        break;
                    }
                }
                if complete {
                    return Ok(Some(y));
                }
            }
        }
    }

    Ok(None)
}

/// Stitch tiles for a row horizontally and match against the stitched row
async fn process_row_with_stitching(
    worker_id: usize,
    row_y: i32,
    config: &Arc<crate::config::Config>,
    cache: &Arc<std::sync::Mutex<Cache>>,
    matcher: &Arc<TemplateMatcher>,
    stats: &Arc<MatchStats>,
    processed_tiles: Arc<std::sync::Mutex<HashSet<(i32, i32)>>>,
    logger: &Arc<Logger>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logger.log(format!(
        "[Match Worker {}] Stitching row {} (2048 tiles)",
        worker_id + 1,
        row_y
    ));

    // Load all tiles for this row
    // If tiles are missing, insert transparent 1000x1000 placeholder tiles
    let mut tile_images = Vec::new();
    let mut missing_tiles = Vec::new();
    const DEFAULT_TILE_SIZE: u32 = 1000; // Standard tile size

    // First pass: determine tile dimensions from first successful tile load
    let mut tile_width = DEFAULT_TILE_SIZE;
    let mut tile_height = DEFAULT_TILE_SIZE;
    let mut found_dimensions = false;

    for x in 0..2048 {
        let tile_path = utils::build_tile_path(x, row_y, config.save_to_hour_dir);

        match fs::read(&tile_path).await {
            Ok(tile_buffer) => {
                match image::load_from_memory(&tile_buffer) {
                    Ok(img) => {
                        let rgba_img = img.to_rgba8();
                        if !found_dimensions {
                            tile_width = rgba_img.width();
                            tile_height = rgba_img.height();
                            found_dimensions = true;
                        }
                        // Ensure tile is the correct size (resize if necessary)
                        let normalized_tile = if rgba_img.width() != tile_width || rgba_img.height() != tile_height {
                            image::imageops::resize(&rgba_img, tile_width, tile_height, image::imageops::FilterType::Nearest)
                        } else {
                            rgba_img
                        };
                        tile_images.push(normalized_tile);
                    }
                    Err(e) => {
                        logger.log(format!(
                            "[Match Worker {}] Error loading tile ({}, {}): {} - inserting transparent placeholder",
                            worker_id + 1, x, row_y, e
                        ));
                        // Create transparent placeholder with determined dimensions
                        let transparent_tile = image::RgbaImage::from_pixel(tile_width, tile_height, image::Rgba([0, 0, 0, 0]));
                        tile_images.push(transparent_tile);
                        missing_tiles.push((x, row_y));
                    }
                }
            }
            Err(_) => {
                // File doesn't exist - create transparent placeholder
                logger.log(format!(
                    "[Match Worker {}] Tile ({}, {}) not found - inserting transparent placeholder",
                    worker_id + 1, x, row_y
                ));
                // Create transparent placeholder with determined dimensions
                let transparent_tile = image::RgbaImage::from_pixel(tile_width, tile_height, image::Rgba([0, 0, 0, 0]));
                tile_images.push(transparent_tile);
                missing_tiles.push((x, row_y));
            }
        }
    }

    if !missing_tiles.is_empty() {
        logger.log(format!(
            "[Match Worker {}] Row {}: {} missing tiles replaced with transparent placeholders",
            worker_id + 1, row_y, missing_tiles.len()
        ));
    }

    // Stitch tiles horizontally
    // Each tile is tile_width x tile_height pixels, so stitched row will be (tile_width * 2048) x tile_height
    let stitched_width = tile_width * 2048;
    let mut stitched_image = image::RgbaImage::new(stitched_width, tile_height);

    for (x, tile_img) in tile_images.iter().enumerate() {
        image::imageops::overlay(
            &mut stitched_image,
            tile_img,
            (x as u32 * tile_width) as i64,
            0,
        );
    }

    // Encode stitched image to buffer
    let mut stitched_buffer = Vec::new();
    {
        let encoder = image::codecs::png::PngEncoder::new(&mut stitched_buffer);
        encoder.encode(
            stitched_image.as_raw(),
            stitched_width,
            tile_height,
            image::ColorType::Rgba8,
        )?;
    }

    // Match against stitched row
    let matches = matcher.find_matches_in_row(&stitched_buffer, row_y, config.match_threshold, tile_width)?;

    logger.log(format!(
        "[Match Worker {}] Row {} processed: {} matches found",
        worker_id + 1,
        row_y,
        matches.len()
    ));

    // Process matches
    for mr in matches {
        if mr.matched {
            let match_x = mr.tile_x.unwrap_or(0);
            let match_y = mr.tile_y.unwrap_or(row_y);

            // Convert pixel coordinates to grid coordinates
            let x_grid = match_x as f64 + mr.center_x / 1000.0;
            let y_grid = match_y as f64 + mr.center_y / 1000.0;
            let match_url = generate_url(x_grid, y_grid, &config.zoom_level, true);

            // Check if match already exists
            let should_add = {
                let cache_guard = cache.lock().unwrap();
                !cache_guard.is_match_exists(&match_url).unwrap_or(false)
            };

            if should_add {
                // Store match in database
                {
                    let mut cache_guard = cache.lock().unwrap();
                    cache_guard.mark_match(match_x, match_y, 200, &match_url)?;
                }

                // Append to matches file
                append_match(&match_url).await?;

                logger.log(format!(
                    "[Match Worker {}] ✓ Match found at tile ({}, {}) - {:.1}% match",
                    worker_id + 1,
                    match_x,
                    match_y,
                    mr.match_percent * 100.0
                ));

                {
                    let mut stats_guard = stats.matches_found.lock().unwrap();
                    *stats_guard += 1;
                }
            }
        }
    }

    // Mark all tiles in row as processed
    {
        let mut processed = processed_tiles.lock().unwrap();
        let mut cache_guard = cache.lock().unwrap();
        for x in 0..2048 {
            processed.insert((x, row_y));
            cache_guard.mark_processed(x, row_y).unwrap_or_else(|e| {
                logger.log(format!(
                    "[Match Worker {}] Error marking tile ({}, {}) as processed: {}",
                    worker_id + 1, x, row_y, e
                ));
            });
        }
    }

    // Update stats
    {
        let mut stats_guard = stats.tiles_matched.lock().unwrap();
        *stats_guard += 2048;
    }
    stats.record_match();

    Ok(())
}

async fn find_tile_files(
    dir: &str,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tile_files = Vec::new();

    match fs::read_dir(dir).await {
        Ok(mut entries) => {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_dir() {
                    // Recursively scan subdirectories - use Box::pin for recursive async
                    match Box::pin(find_tile_files(path.to_str().unwrap())).await {
                        Ok(sub_files) => tile_files.extend(sub_files),
                        Err(e) => {
                            // Log error but continue scanning other directories
                            eprintln!(
                                "Warning: Error scanning subdirectory {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                } else if path.extension().and_then(|s| s.to_str()) == Some("png") {
                    tile_files.push(path);
                }
            }
        }
        Err(e) => {
            // Return error if we can't read the directory at all
            return Err(format!("Failed to read directory {}: {}", dir, e).into());
        }
    }

    Ok(tile_files)
}

async fn append_match(url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let matches_file = PathBuf::from("outputs/matches.txt");

    // Create directory if it doesn't exist
    if let Some(parent) = matches_file.parent() {
        fs::create_dir_all(parent).await?;
    }

    use tokio::io::AsyncWriteExt;
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&matches_file)
        .await?
        .write_all(format!("{}\n", url).as_bytes())
        .await?;

    Ok(())
}
