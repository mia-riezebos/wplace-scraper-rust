use crate::config::TileSelectionMode;
use crate::coordinate::generate_url;
use crate::db::Cache;
use crate::logger::Logger;
use crate::utils;
use rand::Rng;
use reqwest::Client;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::time::{sleep, Duration, Instant};
use url::Url;

pub struct FetchStats {
    pub tiles_fetched: Arc<std::sync::Mutex<u64>>,
    pub errors: Arc<std::sync::Mutex<u64>>,
    pub fetch_timestamps: Arc<std::sync::Mutex<VecDeque<Instant>>>,
    pub status_200: Arc<std::sync::Mutex<u64>>,
    pub status_404: Arc<std::sync::Mutex<u64>>,
}

impl FetchStats {
    pub fn new() -> Self {
        Self {
            tiles_fetched: Arc::new(std::sync::Mutex::new(0)),
            errors: Arc::new(std::sync::Mutex::new(0)),
            fetch_timestamps: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            status_200: Arc::new(std::sync::Mutex::new(0)),
            status_404: Arc::new(std::sync::Mutex::new(0)),
        }
    }

    pub fn record_fetch(&self) {
        let mut timestamps = self.fetch_timestamps.lock().unwrap();
        let now = Instant::now();
        timestamps.push_back(now);

        // Keep only last 10 seconds of timestamps
        let cutoff = now.checked_sub(Duration::from_secs(10)).unwrap_or(now);
        while timestamps.front().map_or(false, |&t| t < cutoff) {
            timestamps.pop_front();
        }
    }

    pub fn tiles_per_second(&self) -> f64 {
        let timestamps = self.fetch_timestamps.lock().unwrap();
        if timestamps.len() < 2 {
            return 0.0;
        }

        let now = Instant::now();
        let cutoff = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        let recent_count = timestamps.iter().filter(|&&t| t >= cutoff).count();
        recent_count as f64
    }

    pub fn update_status_counts(&self, status_200: u64, status_404: u64) {
        let mut status_200_guard = self.status_200.lock().unwrap();
        *status_200_guard = status_200;

        let mut status_404_guard = self.status_404.lock().unwrap();
        *status_404_guard = status_404;
    }
}

pub struct GridProgress {
    // Track progress per grid square (keyed by proxy index)
    counters: Arc<std::sync::Mutex<HashMap<usize, AtomicU64>>>,
    grid_size: usize,
    tiles_per_grid: usize,
}

impl GridProgress {
    pub fn new(proxy_count: usize) -> Self {
        // Calculate grid dimensions: ceil(sqrt(proxy_count))
        let grid_size = (proxy_count as f64).sqrt().ceil() as usize;
        let tiles_per_grid = (2048usize / grid_size).max(1);

        Self {
            counters: Arc::new(std::sync::Mutex::new(HashMap::new())),
            grid_size,
            tiles_per_grid,
        }
    }

    pub fn initialize_from_cache(
        &self,
        cache: &crate::db::Cache,
        proxy_count: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Load progress for each grid square from cache
        let cached_tiles = cache
            .get_all_cached_tiles()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut counters_guard = self.counters.lock().unwrap();

        for (x, y) in cached_tiles {
            // Determine which proxy/grid square this tile belongs to
            let grid_x = (x as usize) / self.tiles_per_grid;
            let grid_y = (y as usize) / self.tiles_per_grid;
            let grid_size = self.grid_size;

            // Calculate proxy index: proxy_index = grid_y * grid_size + grid_x
            let proxy_index = (grid_y * grid_size + grid_x).min(proxy_count - 1);

            // Calculate local position within grid square
            let local_x = (x as usize) % self.tiles_per_grid;
            let local_y = (y as usize) % self.tiles_per_grid;
            let local_index = local_y * self.tiles_per_grid + local_x;

            // Update counter for this proxy's grid square
            let counter = counters_guard
                .entry(proxy_index)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0));

            // Set counter to max(current, local_index + 1) to continue from last fetched tile
            let current = counter.load(std::sync::atomic::Ordering::Relaxed);
            if local_index as u64 >= current {
                counter.store(local_index as u64 + 1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn get_next_tile(&self, proxy_index: usize) -> (u32, u32) {
        let mut counters_guard = self.counters.lock().unwrap();

        // Get or create counter for this proxy's grid square
        let counter = counters_guard
            .entry(proxy_index)
            .or_insert_with(|| AtomicU64::new(0));

        // Get next index within this grid square
        let index = counter.fetch_add(1, Ordering::Relaxed);

        // Calculate which grid square this proxy is assigned to
        let grid_x = proxy_index % self.grid_size;
        let grid_y = proxy_index / self.grid_size;

        // Calculate tile coordinates within this grid square
        let local_x = index % self.tiles_per_grid as u64;
        let local_y = index / self.tiles_per_grid as u64;

        // Map to global tile coordinates
        let x = (grid_x * self.tiles_per_grid + local_x as usize).min(2047) as u32;
        let y = (grid_y * self.tiles_per_grid + local_y as usize).min(2047) as u32;

        (x, y)
    }

    pub fn get_grid_info(&self) -> (usize, usize) {
        (self.grid_size, self.tiles_per_grid)
    }
}

pub async fn fetch_tile_worker(
    worker_id: usize,
    config: Arc<crate::config::Config>,
    proxy_manager: Arc<crate::proxy::ProxyManager>,
    cache: Arc<std::sync::Mutex<Cache>>,
    stats: Arc<FetchStats>,
    rate_limit: Arc<std::sync::Mutex<f64>>,
    proxy_count: usize,
    running: Arc<std::sync::Mutex<bool>>,
    logger: Arc<Logger>,
    tile_counter: Option<Arc<AtomicU64>>,
    grid_progress: Option<Arc<GridProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let worker_count = config.get_worker_count(proxy_count);
    let mut last_request = Instant::now();
    
    // Cache HTTP clients per proxy to avoid creating new clients for each request
    // This prevents connection pool exhaustion
    let mut client_cache: HashMap<String, Client> = HashMap::new();

    logger.log(format!("[Fetch Worker {}] Started", worker_id + 1));

    loop {
        // Calculate rate per worker dynamically from current rate limit
        let current_rate_limit = *rate_limit.lock().unwrap();
        let rate_per_worker = (current_rate_limit / worker_count as f64) * proxy_count as f64;
        let interval = Duration::from_secs_f64(1.0 / rate_per_worker);

        // Check if we should be running (skip processing if stopped)
        if !*running.lock().unwrap() {
            // Worker is stopped, wait a bit before checking again
            sleep(Duration::from_millis(500)).await;
            continue;
        }

        // Worker is running - proceed with fetching
        // Select tile coordinates based on mode
        // For grid mode, we need to get the proxy first to determine grid square
        let (x, y, proxy) = match config.tile_selection_mode {
            TileSelectionMode::Grid => {
                // Get proxy first to determine which grid square to use
                let proxy = proxy_manager.get_next_available();
                let proxy_index = proxy_manager.get_proxy_index(&proxy).unwrap_or(0);

                if let Some(ref grid_prog) = grid_progress {
                    let (tile_x, tile_y) = grid_prog.get_next_tile(proxy_index);
                    (tile_x, tile_y, Some(proxy))
                } else {
                    // Fallback to random if grid_progress not provided
                    (
                        rand::thread_rng().gen_range(0..2048u32),
                        rand::thread_rng().gen_range(0..2048u32),
                        Some(proxy),
                    )
                }
            }
            TileSelectionMode::Consecutive => {
                if let Some(ref counter) = tile_counter {
                    // Get current index atomically and increment
                    let index = counter.fetch_add(1, Ordering::Relaxed);
                    
                    // Clamp index to valid range [0, 4194303]
                    const MAX_INDEX: u64 = 2047 * 2048 + 2047; // 4194303
                    
                    // If index exceeded bounds, wrap around
                    let clamped_index = if index > MAX_INDEX {
                        // Wrap around: use modulo to get valid index
                        let wrapped = index % (MAX_INDEX + 1);
                        // Try to reset counter atomically to the wrapped value
                        // Only one worker will succeed, but that's okay - others will continue with their wrapped index
                        let _ = counter.compare_exchange(index, wrapped, Ordering::Relaxed, Ordering::Relaxed);
                        wrapped
                    } else {
                        index
                    };
                    
                    // Now clamped_index is guaranteed to be in valid range [0, MAX_INDEX]
                    // Convert index to x, y coordinates
                    // Traverse row by row: (0,0) to (0,2047) then (1,0) to (1,2047), etc.
                    // So: x = clamped_index / 2048 (row number), y = clamped_index % 2048 (column within row)
                    let x_coord = clamped_index / 2048;
                    let y_coord = clamped_index % 2048;
                    
                    // Safety check (shouldn't be needed since we clamped index, but defensive)
                    if x_coord >= 2048 {
                        // This shouldn't happen, but handle it gracefully
                        logger.log(format!(
                            "[Fetch Worker {}] WARNING: Invalid coordinates calculated: index={}, clamped_index={}, x={}, y={}",
                            worker_id + 1, index, clamped_index, x_coord, y_coord
                        ));
                        (0, 0, None)
                    } else {
                        // Debug logging for ALL tiles initially to see what's happening
                        if clamped_index <= 100 {
                            use std::fs::OpenOptions;
                            use std::io::Write;
                            if let Ok(mut file) = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("outputs/debug.txt")
                            {
                                let _ = file.write_all(format!(
                                    "[DEBUG] [Fetch Worker {}] Consecutive mode: index={}, clamped_index={}, x={}, y={}, counter_value={}\n",
                                    worker_id + 1, index, clamped_index, x_coord, y_coord, counter.load(Ordering::Relaxed)
                                ).as_bytes());
                            }
                        } else if clamped_index % 1000 == 0 {
                            use std::fs::OpenOptions;
                            use std::io::Write;
                            if let Ok(mut file) = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("outputs/debug.txt")
                            {
                                let _ = file.write_all(format!(
                                    "[DEBUG] [Fetch Worker {}] Consecutive mode: index={}, clamped_index={}, x={}, y={}\n",
                                    worker_id + 1, index, clamped_index, x_coord, y_coord
                                ).as_bytes());
                            }
                        }
                        (x_coord as u32, y_coord as u32, None)
                    }
                } else {
                    // Fallback to random if counter not provided
                    logger.log(format!(
                        "[Fetch Worker {}] ERROR: Consecutive mode but no tile_counter provided! Falling back to random",
                        worker_id + 1
                    ));
                    // Write to debug file
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("outputs/debug.txt")
                    {
                        let _ = file.write_all(format!(
                            "[DEBUG] [Fetch Worker {}] ERROR: Consecutive mode but no tile_counter provided! Falling back to random\n",
                            worker_id + 1
                        ).as_bytes());
                    }
                    (
                        rand::thread_rng().gen_range(0..2048u32),
                        rand::thread_rng().gen_range(0..2048u32),
                        None,
                    )
                }
            }
            TileSelectionMode::Random => (
                rand::thread_rng().gen_range(0..2048u32),
                rand::thread_rng().gen_range(0..2048u32),
                None,
            ),
        };

        // Check cache
        let is_cached = {
            let cache_guard = cache.lock().unwrap();
            cache_guard.is_cached(x as i32, y as i32).unwrap_or(false)
        };

        if is_cached {
            // Skip cached tiles - but still log which tile we're skipping for debugging
            // Only log occasionally (every 100th skip) to show worker is active
            if x % 100 == 0 && y % 100 == 0 {
                logger.log(format!(
                    "[Fetch Worker {}] Skipping cached tile ({}, {})",
                    worker_id + 1,
                    x,
                    y
                ));
            }
            // Don't sleep - immediately continue to next iteration to get next tile
            continue;
        }

        // Tile is not cached - proceed with fetch
        logger.log(format!(
            "[Fetch Worker {}] Selected tile ({}, {}) - not cached, fetching...",
            worker_id + 1,
            x,
            y
        ));

        // Rate limiting
        let elapsed = last_request.elapsed();
        if elapsed < interval {
            sleep(interval - elapsed).await;
        }
        last_request = Instant::now();

        // Get proxy (already obtained for grid mode, otherwise get now)
        let proxy = proxy.unwrap_or_else(|| proxy_manager.get_next_available());

        // Create cache key for this proxy (reuse client per proxy to avoid connection pool exhaustion)
        let proxy_key = if let (Some(user), Some(pass)) = (&proxy.username, &proxy.password) {
            format!("{}:{}@{}:{}", user, pass, proxy.hostname, proxy.port)
        } else {
            format!("{}:{}", proxy.hostname, proxy.port)
        };

        // Get or create HTTP client for this proxy
        let client = if let Some(cached_client) = client_cache.get(&proxy_key) {
            cached_client.clone()
        } else {
            // Build new HTTP client for this proxy using utility function
            let new_client = match utils::build_proxy_client(&proxy) {
                Ok(c) => c,
                Err(e) => {
                    logger.log(format!(
                        "[Fetch Worker {}] Error building HTTP client: {}",
                        worker_id + 1,
                        e
                    ));
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };

            // Cache the client (limit cache size to prevent memory issues)
            if client_cache.len() >= 100 {
                // Remove oldest entry (simple FIFO - remove first entry)
                if let Some(first_key) = client_cache.keys().next().cloned() {
                    client_cache.remove(&first_key);
                }
            }
            client_cache.insert(proxy_key.clone(), new_client.clone());
            new_client
        };

        let tile_path = if config.tile_path.ends_with('/') {
            config.tile_path.clone()
        } else {
            format!("{}/", config.tile_path)
        };

        // Construct URL properly using Url::parse to ensure valid URL format
        let tile_url_str = format!("{}{}{}/{}.png", config.api_endpoint, tile_path, x, y);
        let tile_url = match Url::parse(&tile_url_str) {
            Ok(url) => url,
            Err(e) => {
                logger.log_error(format!(
                    "[Fetch Worker {}] Invalid URL format: {} - {}",
                    worker_id + 1,
                    tile_url_str,
                    e
                ));
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };

        let proxy_info = if let (Some(user), Some(pass)) = (&proxy.username, &proxy.password) {
            format!("{}:{}@{}:{}", user, pass, proxy.hostname, proxy.port)
        } else {
            format!("{}:{}", proxy.hostname, proxy.port)
        };

        logger.log(format!(
            "[Fetch Worker {}] Fetching: {} (tile {}, {}) via proxy {}",
            worker_id + 1,
            tile_url.as_str(),
            x,
            y,
            proxy_info
        ));

        // Single fetch attempt - no retries (failed tiles will be retried later since they're not cached)
        match client.get(tile_url.clone()).send().await {
            Ok(response) => {
                let status = response.status();

                // Generate URL to center of tile
                let center_url =
                    generate_url(x as f64 + 0.5, y as f64 + 0.5, &config.zoom_level, true);

                match status.as_u16() {
                    200 => {
                        match response.bytes().await {
                                   Ok(bytes) if bytes.len() > 0 => {
                                       let tile_file = utils::build_tile_path(x as i32, y as i32, config.save_to_hour_dir);
                                       if let Some(parent) = tile_file.parent() {
                                           if let Err(e) = fs::create_dir_all(parent).await {
                                               logger.log(format!(
                                                   "[Fetch Worker {}] Error creating tile directory: {}",
                                                   worker_id + 1,
                                                   e
                                               ));
                                               continue; // Continue to next tile
                                           }
                                       }

                                       if let Err(e) = fs::write(&tile_file, &bytes).await {
                                    logger.log(format!(
                                        "[Fetch Worker {}] Error writing tile file: {}",
                                        worker_id + 1,
                                        e
                                    ));
                                    continue; // Continue to next tile
                                }

                                {
                                    let mut stats_guard = stats.tiles_fetched.lock().unwrap();
                                    *stats_guard += 1;
                                }

                                {
                                    let mut stats_guard = stats.status_200.lock().unwrap();
                                    *stats_guard += 1;
                                }

                                // Record fetch timestamp for rate calculation
                                stats.record_fetch();

                                {
                                    let cache_guard = cache.lock().unwrap();
                                    if let Err(e) = cache_guard.mark_cached(
                                        x as i32,
                                        y as i32,
                                        200,
                                        &center_url,
                                    ) {
                                        logger.log(format!(
                                            "[Fetch Worker {}] Error caching tile: {}",
                                            worker_id + 1,
                                            e
                                        ));
                                    }
                                }

                                logger.log(format!(
                                    "[Fetch Worker {}] ✓ Saved tile ({}, {})",
                                    worker_id + 1,
                                    x,
                                    y
                                ));
                            }
                            _ => {
                                // Empty response, cache as 200 but don't save file
                                {
                                    let cache_guard = cache.lock().unwrap();
                                    if let Err(e) = cache_guard.mark_cached(
                                        x as i32,
                                        y as i32,
                                        200,
                                        &center_url,
                                    ) {
                                        logger.log(format!(
                                            "[Fetch Worker {}] Error caching tile: {}",
                                            worker_id + 1,
                                            e
                                        ));
                                    }
                                } // Lock is dropped here

                                {
                                    let mut stats_guard = stats.status_200.lock().unwrap();
                                    *stats_guard += 1;
                                }
                            }
                        }
                    }
                    404 => {
                        // Cache 404s - tile not drawn
                        {
                            let cache_guard = cache.lock().unwrap();
                            if let Err(e) =
                                cache_guard.mark_cached(x as i32, y as i32, 404, &center_url)
                            {
                                logger.log(format!(
                                    "[Fetch Worker {}] Error caching tile: {}",
                                    worker_id + 1,
                                    e
                                ));
                            }
                        } // Lock is dropped here

                        {
                            let mut stats_guard = stats.tiles_fetched.lock().unwrap();
                            *stats_guard += 1;
                        }

                        {
                            let mut stats_guard = stats.status_404.lock().unwrap();
                            *stats_guard += 1;
                        }

                        // Record fetch timestamp for rate calculation
                        stats.record_fetch();

                        logger.log(format!(
                            "[Fetch Worker {}] Tile ({}, {}) not found (404) - cached",
                            worker_id + 1,
                            x,
                            y
                        ));
                    }
                    429 => {
                        // Rate limit exceeded - log and continue (will retry later since not cached)
                        logger.log(format!("[Fetch Worker {}] HTTP 429: Rate limit exceeded for tile ({}, {}), skipping", 
                    worker_id + 1, x, y));
                        {
                            let mut stats_guard = stats.errors.lock().unwrap();
                            *stats_guard += 1;
                        }
                        // Don't cache - will retry later
                    }
                    _ => {
                        // Other HTTP errors - log and continue (will retry later since not cached)
                        logger.log(format!(
                            "[Fetch Worker {}] Error fetching tile ({}, {}): HTTP {}",
                            worker_id + 1,
                            x,
                            y,
                            status
                        ));
                        {
                            let mut stats_guard = stats.errors.lock().unwrap();
                            *stats_guard += 1;
                        }
                        // Don't cache - will retry later
                    }
                }
            }
            Err(e) => {
                // Network/connection errors - log and continue (will retry later since not cached)
                logger.log(format!(
                    "[Fetch Worker {}] Error fetching tile ({}, {}): {}",
                    worker_id + 1,
                    x,
                    y,
                    e
                ));
                {
                    let mut stats_guard = stats.errors.lock().unwrap();
                    *stats_guard += 1;
                }
                // Don't cache - will retry later
            }
        }
    }
}
