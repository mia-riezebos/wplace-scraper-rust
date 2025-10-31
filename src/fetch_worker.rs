use crate::coordinate::generate_url;
use crate::db::Cache;
use crate::logger::Logger;
use crate::config::TileSelectionMode;
use rand::Rng;
use reqwest::Client;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::VecDeque;
use std::collections::HashMap;
use tokio::fs;
use tokio::time::{sleep, Duration, Instant};
use url::Url;

// Helper function to get current hour in HH00 format (e.g., "0800", "1400")
fn get_current_hour_dir() -> String {
    let now = chrono::Local::now();
    now.format("%H00").to_string()
}

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

    pub fn initialize_from_cache(&self, cache: &crate::db::Cache, proxy_count: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Load progress for each grid square from cache
        let cached_tiles = cache.get_all_cached_tiles()
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
    rate_per_worker: f64,
    running: Arc<std::sync::Mutex<bool>>,
    logger: Arc<Logger>,
    tile_counter: Option<Arc<AtomicU64>>,
    grid_progress: Option<Arc<GridProgress>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let interval = Duration::from_secs_f64(1.0 / rate_per_worker);
    let mut last_request = Instant::now();

    logger.log(format!("[Fetch Worker {}] Started", worker_id + 1));

    loop {
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
                let proxy_index = proxy_manager.get_proxy_index(&proxy)
                    .unwrap_or(0);
                
                if let Some(ref grid_prog) = grid_progress {
                    let (tile_x, tile_y) = grid_prog.get_next_tile(proxy_index);
                    (tile_x, tile_y, Some(proxy))
                } else {
                    // Fallback to random if grid_progress not provided
                    (rand::thread_rng().gen_range(0..2048u32), rand::thread_rng().gen_range(0..2048u32), Some(proxy))
                }
            }
            TileSelectionMode::Consecutive => {
                if let Some(ref counter) = tile_counter {
                    let index = counter.fetch_add(1, Ordering::Relaxed);
                    // Convert index to x, y coordinates
                    // x goes from 0 to 2047 for each y value (0 to 2047)
                    // So: x = index % 2048, y = index / 2048
                    let x_coord = index % 2048;
                    let y_coord = index / 2048;
                    // Wrap around if we exceed the grid
                    if y_coord >= 2048 {
                        // Reset or fallback to random
                        (rand::thread_rng().gen_range(0..2048u32), rand::thread_rng().gen_range(0..2048u32), None)
                    } else {
                        (x_coord as u32, y_coord as u32, None)
                    }
                } else {
                    // Fallback to random if counter not provided
                    (rand::thread_rng().gen_range(0..2048u32), rand::thread_rng().gen_range(0..2048u32), None)
                }
            }
            TileSelectionMode::Random => {
                (rand::thread_rng().gen_range(0..2048u32), rand::thread_rng().gen_range(0..2048u32), None)
            }
        };

        // Check cache
        let is_cached = {
            let cache_guard = cache.lock().unwrap();
            cache_guard.is_cached(x as i32, y as i32).unwrap_or(false)
        };
        
        if is_cached {
            // Skip cached tiles - don't log every skip to avoid spam
            // Only log occasionally (every 100th skip) to show worker is active
            if x % 100 == 0 && y % 100 == 0 {
                logger.log(format!("[Fetch Worker {}] Skipping cached tile ({}, {})", worker_id + 1, x, y));
            }
            sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Tile is not cached - proceed with fetch
        logger.log(format!("[Fetch Worker {}] Selected tile ({}, {}) - not cached, fetching...", worker_id + 1, x, y));

        // Rate limiting
        let elapsed = last_request.elapsed();
        if elapsed < interval {
            sleep(interval - elapsed).await;
        }
        last_request = Instant::now();

        // Get proxy (already obtained for grid mode, otherwise get now)
        let proxy = proxy.unwrap_or_else(|| proxy_manager.get_next_available());
        
        // Build HTTP proxy with authentication if needed
        // Disable automatic proxy detection from environment variables
        // Set longer timeouts for proxy connections (connect timeout: 15s, read timeout: 45s)
        let mut client_builder = Client::builder()
            .connect_timeout(Duration::from_secs(15))  // Connection timeout (how long to wait to establish connection)
            .timeout(Duration::from_secs(45))          // Read timeout (how long to wait for response)
            .no_proxy();
        
        let proxy_url = format!("http://{}:{}", proxy.hostname, proxy.port);
        
        if let (Some(username), Some(password)) = (&proxy.username, &proxy.password) {
            // Configure HTTP proxy with auth
            match reqwest::Proxy::http(&proxy_url) {
                Ok(http_proxy) => {
                    let http_proxy = http_proxy.basic_auth(username, password);
                    client_builder = client_builder.proxy(http_proxy);
                }
                Err(e) => {
                    logger.log(format!("[Fetch Worker {}] Error creating HTTP proxy: {}", worker_id + 1, e));
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            }
            
            // Configure HTTPS proxy with auth (same proxy server)
            match reqwest::Proxy::https(&proxy_url) {
                Ok(https_proxy) => {
                    let https_proxy = https_proxy.basic_auth(username, password);
                    client_builder = client_builder.proxy(https_proxy);
                }
                Err(e) => {
                    logger.log(format!("[Fetch Worker {}] Error creating HTTPS proxy: {}", worker_id + 1, e));
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            }
        } else {
            // No authentication
            match reqwest::Proxy::http(&proxy_url) {
                Ok(http_proxy) => {
                    client_builder = client_builder.proxy(http_proxy);
                }
                Err(e) => {
                    logger.log(format!("[Fetch Worker {}] Error creating HTTP proxy: {}", worker_id + 1, e));
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            }
            
            match reqwest::Proxy::https(&proxy_url) {
                Ok(https_proxy) => {
                    client_builder = client_builder.proxy(https_proxy);
                }
                Err(e) => {
                    logger.log(format!("[Fetch Worker {}] Error creating HTTPS proxy: {}", worker_id + 1, e));
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            }
        }
        
        let client = match client_builder.build() {
            Ok(c) => c,
            Err(e) => {
                logger.log(format!("[Fetch Worker {}] Error building HTTP client: {}", worker_id + 1, e));
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };

        let tile_path = if config.tile_path.ends_with('/') {
            config.tile_path.clone()
        } else {
            format!("{}/", config.tile_path)
        };
        
        // Construct URL properly using Url::parse to ensure valid URL format
        // This helps avoid connection issues that can occur with string URLs
        let tile_url_str = format!("{}{}{}/{}.png", config.api_endpoint, tile_path, x, y);
        let tile_url = match Url::parse(&tile_url_str) {
            Ok(url) => url,
            Err(e) => {
                logger.log_error(format!("[Fetch Worker {}] Invalid URL format: {} - {}", worker_id + 1, tile_url_str, e));
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };

        let proxy_info = if let (Some(user), Some(pass)) = (&proxy.username, &proxy.password) {
            format!("{}:{}@{}:{}", user, pass, proxy.hostname, proxy.port)
        } else {
            format!("{}:{}", proxy.hostname, proxy.port)
        };
        
        logger.log(format!("[Fetch Worker {}] Fetching: {} (tile {}, {}) via proxy {}", 
            worker_id + 1, tile_url.as_str(), x, y, proxy_info));

        // Retry logic for fetch errors
        let mut retry_count = 0;
        let mut current_proxy = proxy;
        let mut current_client = client;
        
        loop {
            // Get a new proxy and rebuild client for retries (except first attempt)
            if retry_count > 0 {
                current_proxy = proxy_manager.get_next_available();
                let proxy_url = format!("http://{}:{}", current_proxy.hostname, current_proxy.port);
                
                let mut client_builder = Client::builder()
                    .connect_timeout(Duration::from_secs(15))  // Connection timeout
                    .timeout(Duration::from_secs(45))          // Read timeout
                    .no_proxy();
                
                if let (Some(username), Some(password)) = (&current_proxy.username, &current_proxy.password) {
                    match reqwest::Proxy::http(&proxy_url) {
                        Ok(http_proxy) => {
                            let http_proxy = http_proxy.basic_auth(username, password);
                            client_builder = client_builder.proxy(http_proxy);
                        }
                        Err(_) => {
                            // Skip this retry if proxy setup fails
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                break;
                            }
                            continue;
                        }
                    }
                    
                    match reqwest::Proxy::https(&proxy_url) {
                        Ok(https_proxy) => {
                            let https_proxy = https_proxy.basic_auth(username, password);
                            client_builder = client_builder.proxy(https_proxy);
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                break;
                            }
                            continue;
                        }
                    }
                } else {
                    match reqwest::Proxy::http(&proxy_url) {
                        Ok(http_proxy) => {
                            client_builder = client_builder.proxy(http_proxy);
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                break;
                            }
                            continue;
                        }
                    }
                    
                    match reqwest::Proxy::https(&proxy_url) {
                        Ok(https_proxy) => {
                            client_builder = client_builder.proxy(https_proxy);
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                break;
                            }
                            continue;
                        }
                    }
                }
                
                match client_builder.build() {
                    Ok(c) => current_client = c,
                    Err(_) => {
                        retry_count += 1;
                        if retry_count >= config.fetch_retry_count {
                            break;
                        }
                        continue;
                    }
                }
                
                let new_proxy_info = if let (Some(user), Some(pass)) = (&current_proxy.username, &current_proxy.password) {
                    format!("{}:{}@{}:{}", user, pass, current_proxy.hostname, current_proxy.port)
                } else {
                    format!("{}:{}", current_proxy.hostname, current_proxy.port)
                };
                
                logger.log(format!("[Fetch Worker {}] Retry {}: Using new proxy {}", 
                    worker_id + 1, retry_count, new_proxy_info));
            }
            
            match current_client.get(tile_url.clone()).send().await {
                Ok(response) => {
                    let status = response.status();
                    
                    // Generate URL to center of tile
                    let center_url = generate_url(
                        x as f64 + 0.5,
                        y as f64 + 0.5,
                        &config.zoom_level,
                        true,
                    );

                    match status.as_u16() {
                                200 => {
                            match response.bytes().await {
                                Ok(bytes) if bytes.len() > 0 => {
                                    let hour_dir = get_current_hour_dir();
                                    let tile_dir = PathBuf::from("outputs/tiles")
                                        .join(&hour_dir)
                                        .join(x.to_string());
                                    if let Err(e) = fs::create_dir_all(&tile_dir).await {
                                        logger.log(format!("[Fetch Worker {}] Error creating tile directory: {}", worker_id + 1, e));
                                        sleep(Duration::from_millis(1000)).await;
                                        break; // Exit retry loop, continue to next tile
                                    }
                                    
                                    let tile_file = tile_dir.join(format!("{}.png", y));
                                    if let Err(e) = fs::write(&tile_file, &bytes).await {
                                        logger.log(format!("[Fetch Worker {}] Error writing tile file: {}", worker_id + 1, e));
                                        sleep(Duration::from_millis(1000)).await;
                                        break; // Exit retry loop, continue to next tile
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
                                        if let Err(e) = cache_guard.mark_cached(x as i32, y as i32, 200, &center_url) {
                                            logger.log(format!("[Fetch Worker {}] Error caching tile: {}", worker_id + 1, e));
                                        }
                                    }
                                    
                                    logger.log(format!("[Fetch Worker {}] ✓ Saved tile ({}, {})", 
                                        worker_id + 1, x, y));
                                    break; // Success, exit retry loop
                                }
                                _ => {
                                    // Empty response, cache as 200 but don't save file
                                    let cache_guard = cache.lock().unwrap();
                                    if let Err(e) = cache_guard.mark_cached(x as i32, y as i32, 200, &center_url) {
                                        logger.log(format!("[Fetch Worker {}] Error caching tile: {}", worker_id + 1, e));
                                    }
                                    
                                    {
                                        let mut stats_guard = stats.status_200.lock().unwrap();
                                        *stats_guard += 1;
                                    }
                                    break; // Success (empty response), exit retry loop
                                }
                            }
                        }
                        404 => {
                            // Cache 404s - no retry needed
                            let cache_guard = cache.lock().unwrap();
                            if let Err(e) = cache_guard.mark_cached(x as i32, y as i32, 404, &center_url) {
                                logger.log(format!("[Fetch Worker {}] Error caching tile: {}", worker_id + 1, e));
                            }
                            
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
                            
                            logger.log(format!("[Fetch Worker {}] Tile ({}, {}) not found (404) - cached", 
                                worker_id + 1, x, y));
                            break; // Success (404 is expected), exit retry loop
                        }
                        429 => {
                            logger.log(format!("[Fetch Worker {}] HTTP 429: Rate limit exceeded. Sleeping for 30 seconds...", 
                                worker_id + 1));
                            sleep(Duration::from_secs(30)).await;
                            // Retry after sleep
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                logger.log(format!("[Fetch Worker {}] Max retries reached for tile ({}, {}) after HTTP 429", 
                                    worker_id + 1, x, y));
                                {
                                    let mut stats_guard = stats.errors.lock().unwrap();
                                    *stats_guard += 1;
                                }
                                break; // Max retries reached
                            }
                            continue; // Retry after sleep
                        }
                        _ => {
                            // Other HTTP errors - retry if we haven't exceeded max retries
                            retry_count += 1;
                            if retry_count >= config.fetch_retry_count {
                                let cache_guard = cache.lock().unwrap();
                                if let Err(e) = cache_guard.mark_cached(x as i32, y as i32, status.as_u16() as i32, &center_url) {
                                    logger.log(format!("[Fetch Worker {}] Error caching tile: {}", worker_id + 1, e));
                                }
                                
                                {
                                    let mut stats_guard = stats.errors.lock().unwrap();
                                    *stats_guard += 1;
                                }
                                
                                logger.log(format!("[Fetch Worker {}] Error fetching tile ({}, {}): HTTP {} (max retries reached)", 
                                    worker_id + 1, x, y, status));
                                break; // Max retries reached
                            } else {
                                logger.log(format!("[Fetch Worker {}] Error fetching tile ({}, {}): HTTP {} (retry {}/{})", 
                                    worker_id + 1, x, y, status, retry_count, config.fetch_retry_count));
                                sleep(Duration::from_millis(1000 * retry_count as u64)).await; // Exponential backoff
                                continue; // Retry
                            }
                        }
                    }
                }
                Err(e) => {
                    // Network/connection errors - retry if we haven't exceeded max retries
                    retry_count += 1;
                    if retry_count >= config.fetch_retry_count {
                        let center_url = generate_url(
                            x as f64 + 0.5,
                            y as f64 + 0.5,
                            &config.zoom_level,
                            true,
                        );
                        
                        let cache_guard = cache.lock().unwrap();
                        if let Err(cache_err) = cache_guard.mark_cached(x as i32, y as i32, 0, &center_url) {
                            logger.log(format!("[Fetch Worker {}] Error caching tile: {}", worker_id + 1, cache_err));
                        }
                        
                        {
                            let mut stats_guard = stats.errors.lock().unwrap();
                            *stats_guard += 1;
                        }
                        
                        logger.log(format!("[Fetch Worker {}] Error fetching tile ({}, {}): {} (max retries reached)", 
                            worker_id + 1, x, y, e));
                        break; // Max retries reached
                    } else {
                        logger.log(format!("[Fetch Worker {}] Error fetching tile ({}, {}): {} (retry {}/{})", 
                            worker_id + 1, x, y, e, retry_count, config.fetch_retry_count));
                        sleep(Duration::from_millis(1000 * retry_count as u64)).await; // Exponential backoff
                        continue; // Retry
                    }
                }
            }
        }
    }
    
    // Note: This code is unreachable as the loop runs indefinitely
    // Workers are cancelled when the process exits
    Ok(())
}

