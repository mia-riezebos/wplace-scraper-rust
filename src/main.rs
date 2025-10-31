mod config;
mod coordinate;
mod db;
mod proxy;
mod matcher;
mod fetch_worker;
mod match_worker;
mod tui;
mod logger;

use std::sync::Arc;
use std::path::PathBuf;
use tokio::signal;
use tokio::fs;

// Helper function to get current hour in HH00 format (e.g., "0800", "1400")
fn get_current_hour_dir() -> String {
    let now = chrono::Local::now();
    now.format("%H00").to_string()
}

async fn scan_tiles_directory(dir: &str) -> Result<Vec<(i32, i32)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tiles = Vec::new();
    
    fn parse_tile_file_name(file_path: &std::path::Path) -> Result<(i32, i32), Box<dyn std::error::Error + Send + Sync>> {
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
    
    async fn scan_recursive(dir: &str, tiles: &mut Vec<(i32, i32)>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(mut entries) = fs::read_dir(dir).await {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                
                if path.is_dir() {
                    Box::pin(scan_recursive(path.to_str().unwrap(), tiles)).await?;
                } else if path.extension().and_then(|s| s.to_str()) == Some("png") {
                    if let Ok((x, y)) = parse_tile_file_name(&path) {
                        tiles.push((x, y));
                    }
                }
            }
        }
        Ok(())
    }
    
    scan_recursive(dir, &mut tiles).await?;
    Ok(tiles)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load configuration
    let config = Arc::new(config::Config::from_env()?);

    // Initialize database
    std::fs::create_dir_all("outputs")?;
    let cache = Arc::new(std::sync::Mutex::new(db::Cache::new("outputs/cache.db", config.cache_ttl_ms, config.cache_expire_on_hour)?));

    // Load proxies based on mode
    let proxy_manager = match config.proxy_mode {
        config::ProxyMode::File => {
            Arc::new(proxy::ProxyManager::from_file("assets/proxies.json", config.rate_limit)?)
        }
        config::ProxyMode::Rotating => {
            let endpoint = config.proxy_endpoint.as_ref()
                .ok_or("PROXY_ENDPOINT is required for rotating mode")?;
            let proxies_count = config.proxies_count
                .ok_or("PROXY_COUNT is required for rotating mode")?;
            Arc::new(proxy::ProxyManager::from_rotating_endpoint(
                endpoint,
                proxies_count,
                config.rate_limit,
            )?)
        }
    };
    let proxy_count = proxy_manager.count();

    // Calculate worker count and rate
    let worker_count = config.get_worker_count(proxy_count);
    let rate_per_worker = config.calculate_worker_rate(proxy_count);

    // Load template matcher
    let matcher = Arc::new(matcher::TemplateMatcher::from_file("assets/template.png")?);

    // Create stats
    let fetch_stats = Arc::new(fetch_worker::FetchStats::new());

    let match_stats = Arc::new(match_worker::MatchStats::new());

    // Track processed tiles to avoid reprocessing
    let mut processed_tiles_set = std::collections::HashSet::new();
    
    // Load processed tiles from database
    {
        let cache_guard = cache.lock().unwrap();
        let processed = cache_guard.get_all_processed_tiles().unwrap_or_else(|_| {
            Vec::new()
        });
        for (x, y) in processed {
            processed_tiles_set.insert((x, y));
        }
    }
    
    // Scan tiles directory and check which tiles are already processed
    let existing_tiles = scan_tiles_directory("outputs/tiles").await?;
    let mut already_processed_count = 0;
    {
        let cache_guard = cache.lock().unwrap();
        for (x, y) in &existing_tiles {
            // Only add to processed set if already marked as processed in database
            if cache_guard.is_processed(*x, *y).unwrap_or(false) {
                processed_tiles_set.insert((*x, *y));
                already_processed_count += 1;
            }
        }
    }
    
    let processed_tiles_set_len = processed_tiles_set.len();
    let processed_tiles = Arc::new(std::sync::Mutex::new(processed_tiles_set));

    // Ensure tiles directory exists
    tokio::fs::create_dir_all("outputs/tiles").await?;

    let fetch_running = Arc::new(std::sync::Mutex::new(false)); // Start stopped
    let match_running = Arc::new(std::sync::Mutex::new(false)); // Start stopped
    let shutdown_requested = Arc::new(std::sync::Mutex::new(false));
    
    // Initialize TUI
    let mut tui = tui::Tui::new(
        fetch_running.clone(),
        match_running.clone(),
        shutdown_requested.clone(),
        cache.clone(),
    );
    let tui_logs = tui.logs();
    let logger = Arc::new(logger::Logger::new(tui_logs.clone()));
    
    // Log initial messages
    tui.add_log("Initializing wplace scraper...".to_string());
    tui.add_log(format!("API_ENDPOINT: {}", config.api_endpoint));
    tui.add_log(format!("TILE_PATH: {}", config.tile_path));
    tui.add_log(format!("Rate limit: {} tiles/sec", config.rate_limit));
    tui.add_log(format!("Zoom level: {}", config.zoom_level));
    tui.add_log(format!("Match threshold: {:.1}%", config.match_threshold * 100.0));
    tui.add_log(format!("Cache TTL: {} ms ({:.1} hours)", config.cache_ttl_ms, config.cache_ttl_ms as f64 / 3600000.0));
    if config.cache_expire_on_hour {
        tui.add_log("Cache expiration: On the hour (entries expire at top of each hour)".to_string());
    }
    tui.add_log(format!("Fetch retry count: {} (retries failed tile fetches)", config.fetch_retry_count));
    if config.auto_cycle_mode {
        tui.add_log(format!("Auto-cycle mode: ENABLED (fetch {} min, match {} min)", 
            config.fetch_cycle_minutes, config.match_cycle_minutes));
    }
    tui.add_log(format!("Proxy mode: {:?}", config.proxy_mode));
    if config.proxy_mode == config::ProxyMode::Rotating {
        tui.add_log(format!("Rotating endpoint: {} ({} proxies)", 
            config.proxy_endpoint.as_ref().unwrap(), 
            config.proxies_count.unwrap_or(0)));
    }
    if let Some(max_concurrency) = config.max_concurrency {
        tui.add_log(format!("Max concurrency per worker: {}", max_concurrency));
    }
    tui.add_log(format!("Loaded {} proxies", proxy_count));
    tui.add_log(format!("Worker count: {} (capped at {} proxies)", worker_count, proxy_count));
    tui.add_log(format!("Match worker count: {} (auto-calculated)", config.match_worker_count));
    tui.add_log(format!("Tile selection mode: {:?}", config.tile_selection_mode));
    tui.add_log(format!("Rate per worker: {:.3} tiles/sec", rate_per_worker));
    tui.add_log(format!("Loaded {} processed tiles from database", processed_tiles_set_len));
    tui.add_log(format!("Found {} existing tiles in directory ({} already processed, {} need processing)", 
        existing_tiles.len(), already_processed_count, existing_tiles.len() - already_processed_count));
    
    // Load fetch progress from cache
    {
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get_fetch_progress() {
            Ok((total_fetched, total_200s, max_coords, last_index)) => {
                tui.add_log(format!("Cache analysis: {} tiles fetched ({} successful)", total_fetched, total_200s));
                if let Some((max_x, max_y)) = max_coords {
                    tui.add_log(format!("Highest coordinates reached: ({}, {})", max_x, max_y));
                    if let Some(idx) = last_index {
                        tui.add_log(format!("Consecutive mode resume point: index {} (tile {}, {})", 
                            idx, idx % 2048, idx / 2048));
                    }
                } else {
                    tui.add_log("No tiles found in cache".to_string());
                }
            }
            Err(e) => {
                tui.add_log(format!("Warning: Could not analyze cache: {}", e));
            }
        }
    }
    
    // If X_TILE and Y_TILE are configured, fetch and process immediately
    if let (Some(x_tile), Some(y_tile)) = (config.x_tile, config.y_tile) {
        tui.add_log(format!("Fetching and processing tile ({}, {}) immediately...", x_tile, y_tile));
        
        // Fetch the tile - wrap in error handling to prevent early exit
        let fetch_result: Result<(), Box<dyn std::error::Error + Send + Sync>> = (|| async {
            let proxy = proxy_manager.get_next_available();
            let proxy_url = format!("http://{}:{}", proxy.hostname, proxy.port);
            
            let mut client_builder = reqwest::Client::builder()
                .connect_timeout(tokio::time::Duration::from_secs(15))  // Connection timeout
                .timeout(tokio::time::Duration::from_secs(45))        // Read timeout
                .no_proxy();
            
            if let (Some(username), Some(password)) = (&proxy.username, &proxy.password) {
                let http_proxy = reqwest::Proxy::http(&proxy_url)?
                    .basic_auth(username, password);
                client_builder = client_builder.proxy(http_proxy);
                
                let https_proxy = reqwest::Proxy::https(&proxy_url)?
                    .basic_auth(username, password);
                client_builder = client_builder.proxy(https_proxy);
            } else {
                let http_proxy = reqwest::Proxy::http(&proxy_url)?;
                client_builder = client_builder.proxy(http_proxy);
                
                let https_proxy = reqwest::Proxy::https(&proxy_url)?;
                client_builder = client_builder.proxy(https_proxy);
            }
            
            let client = client_builder.build()?;
            let tile_path = if config.tile_path.ends_with('/') {
                config.tile_path.clone()
            } else {
                format!("{}/", config.tile_path)
            };
            
            // Construct URL properly using Url::parse to ensure valid URL format
            let tile_url_str = format!("{}{}{}/{}.png", config.api_endpoint, tile_path, x_tile, y_tile);
            let tile_url = url::Url::parse(&tile_url_str)
                .map_err(|e| format!("Invalid URL format: {} - {}", tile_url_str, e))?;
            
            match client.get(tile_url.clone()).send().await {
                Ok(response) => {
                    let status = response.status();
                    let center_url = coordinate::generate_url(
                        x_tile as f64 + 0.5,
                        y_tile as f64 + 0.5,
                        &config.zoom_level,
                        true,
                    );
                    
                    match status.as_u16() {
                        200 => {
                            match response.bytes().await {
                                Ok(bytes) if bytes.len() > 0 => {
                                    let tile_dir = if config.save_to_hour_dir {
                                        let hour_dir = get_current_hour_dir();
                                        PathBuf::from("outputs/tiles")
                                            .join(&hour_dir)
                                            .join(x_tile.to_string())
                                    } else {
                                        PathBuf::from("outputs/tiles")
                                            .join(x_tile.to_string())
                                    };
                                    tokio::fs::create_dir_all(&tile_dir).await?;
                                    
                                    let tile_file = tile_dir.join(format!("{}.png", y_tile));
                                    tokio::fs::write(&tile_file, &bytes).await?;
                                    
                                    {
                                        let mut stats_guard = fetch_stats.tiles_fetched.lock().unwrap();
                                        *stats_guard += 1;
                                    }
                                    
                                    {
                                        let cache_guard = cache.lock().unwrap();
                                        cache_guard.mark_cached(x_tile, y_tile, 200, &center_url)?;
                                    }
                                    
                                    logger.log(format!("✓ Fetched tile ({}, {})", x_tile, y_tile));
                                    
                                    // Process the tile with the matcher (using the bytes we just fetched)
                                    // This happens AFTER the fetch is completely finished
                                    match matcher.find_match(&bytes, config.match_threshold) {
                                        Ok(mr) => {
                                            {
                                                let mut stats_guard = match_stats.tiles_matched.lock().unwrap();
                                                *stats_guard += 1;
                                            }
                                            
                                            {
                                                let cache_guard = cache.lock().unwrap();
                                                cache_guard.mark_processed(x_tile, y_tile).unwrap_or_else(|e| {
                                                    logger.log(format!("Error marking tile ({}, {}) as processed: {}", x_tile, y_tile, e));
                                                });
                                            }
                                            
                                            if mr.matched {
                                                let x_grid = x_tile as f64 + mr.center_x / 1000.0;
                                                let y_grid = y_tile as f64 + mr.center_y / 1000.0;
                                                let match_url = coordinate::generate_url(
                                                    x_grid,
                                                    y_grid,
                                                    &config.zoom_level,
                                                    true,
                                                );
                                                
                                                        let should_add = {
                                                            let cache_guard = cache.lock().unwrap();
                                                            !cache_guard.is_match_exists(&match_url).unwrap_or(false)
                                                        };
                                                        
                                                        if should_add {
                                                            {
                                                                let mut cache_guard = cache.lock().unwrap();
                                                                cache_guard.mark_match(x_tile, y_tile, 200, &match_url)?;
                                                            }
                                                    
                                                    // Append to matches file
                                                    let matches_file = PathBuf::from("outputs/matches.txt");
                                                    if let Some(parent) = matches_file.parent() {
                                                        tokio::fs::create_dir_all(parent).await?;
                                                    }
                                                    let mut file = tokio::fs::OpenOptions::new()
                                                        .create(true)
                                                        .append(true)
                                                        .open(&matches_file)
                                                        .await?;
                                                    use tokio::io::AsyncWriteExt;
                                                    file.write_all(format!("{}\n", match_url).as_bytes()).await?;
                                                    
                                                    {
                                                        let mut stats_guard = match_stats.matches_found.lock().unwrap();
                                                        *stats_guard += 1;
                                                    }
                                                    
                                                    logger.log(format!("✓ Match found at tile ({}, {}) - {:.1}% match", x_tile, y_tile, mr.match_percent * 100.0));
                                                    logger.log(format!("  URL: {}", match_url));
                                                } else {
                                                    logger.log(format!("Match found at tile ({}, {}) but already exists - skipping", x_tile, y_tile));
                                                }
                                            } else {
                                                logger.log(format!("No match found at tile ({}, {}) - {:.1}% match", x_tile, y_tile, mr.match_percent * 100.0));
                                            }
                                        }
                                        Err(e) => {
                                            logger.log(format!("Error matching tile ({}, {}): {}", x_tile, y_tile, e));
                                            {
                                                let mut stats_guard = match_stats.errors.lock().unwrap();
                                                *stats_guard += 1;
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    let cache_guard = cache.lock().unwrap();
                                    cache_guard.mark_cached(x_tile, y_tile, 200, &center_url)?;
                                    logger.log(format!("Tile ({}, {}) fetched but empty response", x_tile, y_tile));
                                }
                            }
                        }
                        404 => {
                            let cache_guard = cache.lock().unwrap();
                            cache_guard.mark_cached(x_tile, y_tile, 404, &center_url)?;
                            {
                                let mut stats_guard = fetch_stats.tiles_fetched.lock().unwrap();
                                *stats_guard += 1;
                            }
                            logger.log(format!("Tile ({}, {}) not found (404) - cached", x_tile, y_tile));
                        }
                        _ => {
                            let cache_guard = cache.lock().unwrap();
                            cache_guard.mark_cached(x_tile, y_tile, status.as_u16() as i32, &center_url)?;
                            {
                                let mut stats_guard = fetch_stats.errors.lock().unwrap();
                                *stats_guard += 1;
                            }
                            logger.log(format!("Error fetching tile ({}, {}): HTTP {}", x_tile, y_tile, status));
                        }
                    }
                }
                Err(e) => {
                    let center_url = coordinate::generate_url(
                        x_tile as f64 + 0.5,
                        y_tile as f64 + 0.5,
                        &config.zoom_level,
                        true,
                    );
                    
                    let cache_guard = cache.lock().unwrap();
                    cache_guard.mark_cached(x_tile, y_tile, 0, &center_url)?;
                    
                    {
                        let mut stats_guard = fetch_stats.errors.lock().unwrap();
                        *stats_guard += 1;
                    }
                    
                    logger.log(format!("Error fetching tile ({}, {}): {}", x_tile, y_tile, e));
                }
            }
            Ok(())
        })().await;
        
        if let Err(e) = fetch_result {
            logger.log(format!("Failed to fetch/process immediate tile ({}, {}): {}", x_tile, y_tile, e));
        }
    }
    
    if !config.auto_cycle_mode {
        tui.add_log("Workers are currently STOPPED. Press 'f' to start fetch workers, 'm' to start match workers.".to_string());
    }

    // Handle SIGINT/SIGHUP for force stop
    let shutdown_requested_signal = shutdown_requested.clone();
    let fetch_running_signal = fetch_running.clone();
    let match_running_signal = match_running.clone();
    let fetch_stats_clone = fetch_stats.clone();
    let match_stats_clone = match_stats.clone();
    let tui_logs_signal = tui_logs.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        {
            let mut logs = tui_logs_signal.lock().unwrap();
            logs.push_back("---".to_string());
            logs.push_back("Force stopping scraper...".to_string());
        }
        *fetch_running_signal.lock().unwrap() = false;
        *match_running_signal.lock().unwrap() = false;
        *shutdown_requested_signal.lock().unwrap() = true;
        
        // Log final stats
        let tiles_fetched = *fetch_stats_clone.tiles_fetched.lock().unwrap();
        let fetch_errors = *fetch_stats_clone.errors.lock().unwrap();
        let tiles_matched = *match_stats_clone.tiles_matched.lock().unwrap();
        let matches_found = *match_stats_clone.matches_found.lock().unwrap();
        let match_errors = *match_stats_clone.errors.lock().unwrap();
        
        let stats_msg = format!(
            "Final stats: {} tiles fetched, {} matched, {} matches found, {} fetch errors, {} match errors",
            tiles_fetched, tiles_matched, matches_found, fetch_errors, match_errors
        );
        tui_logs_signal.lock().unwrap().push_back(stats_msg);
    });

    // Auto-cycle mode: automatically switch between fetching and matching
    if config.auto_cycle_mode {
        let fetch_running_cycle = fetch_running.clone();
        let match_running_cycle = match_running.clone();
        let shutdown_requested_cycle = shutdown_requested.clone();
        let tui_logs_cycle = tui_logs.clone();
        let fetch_cycle_duration = tokio::time::Duration::from_secs(config.fetch_cycle_minutes * 60);
        let match_cycle_duration = tokio::time::Duration::from_secs(config.match_cycle_minutes * 60);
        
        tokio::spawn(async move {
            loop {
                // Check if shutdown requested
                if *shutdown_requested_cycle.lock().unwrap() {
                    break;
                }
                
                // Phase 1: Fetch for fetch_cycle_minutes
                {
                    let mut logs = tui_logs_cycle.lock().unwrap();
                    logs.push_back(format!("[Auto-Cycle] Starting fetch phase ({} minutes)", 
                        fetch_cycle_duration.as_secs() / 60));
                }
                *fetch_running_cycle.lock().unwrap() = true;
                *match_running_cycle.lock().unwrap() = false;
                
                tokio::time::sleep(fetch_cycle_duration).await;
                
                // Check if shutdown requested before switching
                if *shutdown_requested_cycle.lock().unwrap() {
                    break;
                }
                
                // Phase 2: Stop fetching, start matching for match_cycle_minutes
                {
                    let mut logs = tui_logs_cycle.lock().unwrap();
                    logs.push_back(format!("[Auto-Cycle] Switching to match phase ({} minutes)", 
                        match_cycle_duration.as_secs() / 60));
                }
                *fetch_running_cycle.lock().unwrap() = false;
                *match_running_cycle.lock().unwrap() = true;
                
                tokio::time::sleep(match_cycle_duration).await;
                
                // Cycle repeats...
            }
        });
        
        // Start with fetching phase
        *fetch_running.lock().unwrap() = true;
        tui.add_log(format!("[Auto-Cycle] Auto-cycle mode started - beginning with fetch phase"));
    }

    // Spawn fetch workers
    let mut fetch_handles = Vec::new();
    
    // Create shared tile counter for consecutive mode
    let tile_counter: Option<Arc<std::sync::atomic::AtomicU64>> = if config.tile_selection_mode == crate::config::TileSelectionMode::Consecutive {
        // Try to resume from cache
        let initial_value = {
            let cache_guard = cache.lock().unwrap();
            match cache_guard.get_fetch_progress() {
                Ok((_, _, _, Some(last_index))) => {
                    tui.add_log(format!("Resuming consecutive mode from index {}", last_index));
                    last_index
                }
                _ => {
                    tui.add_log("Starting consecutive mode from index 0".to_string());
                    0
                }
            }
        };
        Some(Arc::new(std::sync::atomic::AtomicU64::new(initial_value)))
    } else {
        None
    };
    
    // Create shared grid progress for grid mode
    let grid_progress: Option<Arc<fetch_worker::GridProgress>> = if config.tile_selection_mode == crate::config::TileSelectionMode::Grid {
        let grid_prog = fetch_worker::GridProgress::new(proxy_count);
        let (grid_size, tiles_per_grid) = grid_prog.get_grid_info();
        tui.add_log(format!("Grid mode: {}x{} grid ({} tiles per square)", grid_size, grid_size, tiles_per_grid * tiles_per_grid));
        
        // Initialize grid progress from cache
        {
            let cache_guard = cache.lock().unwrap();
            match grid_prog.initialize_from_cache(&cache_guard, proxy_count) {
                Ok(_) => {
                    tui.add_log("Grid mode: Initialized progress from cache".to_string());
                }
                Err(e) => {
                    tui.add_log(format!("Grid mode: Warning - could not initialize from cache: {}", e));
                }
            }
        }
        
        Some(Arc::new(grid_prog))
    } else {
        None
    };
    
    for i in 0..worker_count {
        let handle = tokio::spawn(fetch_worker::fetch_tile_worker(
            i,
            config.clone(),
            proxy_manager.clone(),
            cache.clone(),
            fetch_stats.clone(),
            rate_per_worker,
            fetch_running.clone(),
            logger.clone(),
            tile_counter.clone(),
            grid_progress.clone(),
        ));
        fetch_handles.push(handle);
    }

    // Spawn match workers (configurable via MATCH_WORKER_COUNT env var, default: 4)
    let match_worker_count = config.match_worker_count;
    let mut match_handles = Vec::new();
    for i in 0..match_worker_count {
        let handle = tokio::spawn(match_worker::match_tile_worker(
            i,
            config.clone(),
            cache.clone(),
            matcher.clone(),
            match_stats.clone(),
            match_running.clone(),
            processed_tiles.clone(),
            logger.clone(),
        ));
        match_handles.push(handle);
    }
    
    // Run TUI - this will handle keyboard input and display
    tui.run(fetch_stats.clone(), match_stats.clone()).await?;
    
    // After TUI exits (shutdown requested), clean up
    let match_running_state = *match_running.lock().unwrap();
    if match_running_state {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
    
    // Print final stats (after TUI exits, restore terminal)
    let tiles_fetched = *fetch_stats.tiles_fetched.lock().unwrap();
    let fetch_errors = *fetch_stats.errors.lock().unwrap();
    let tiles_matched = *match_stats.tiles_matched.lock().unwrap();
    let matches_found = *match_stats.matches_found.lock().unwrap();
    let match_errors = *match_stats.errors.lock().unwrap();
    
    println!(
        "\nFinal stats: {} tiles fetched, {} matched, {} matches found, {} fetch errors, {} match errors",
        tiles_fetched, tiles_matched, matches_found, fetch_errors, match_errors
    );

    Ok(())
}
