use std::env;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TileSelectionMode {
    Random,
    Consecutive,
    Grid,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProxyMode {
    File,      // Load proxies from file (assets/proxies.json)
    Rotating,  // Single rotating endpoint that rotates remotely
}

pub struct Config {
    // WPLACE_* - Main application config
    pub api_endpoint: String,
    pub tile_path: String,
    pub rate_limit: f64,
    pub zoom_level: String,
    pub match_threshold: f64,
    pub tile_selection_mode: TileSelectionMode,
    pub x_tile: Option<i32>,
    pub y_tile: Option<i32>,
    
    // PROXY_* - Proxy configuration
    pub proxy_mode: ProxyMode,
    pub proxy_endpoint: Option<String>,  // For rotating mode
    pub proxies_count: Option<usize>,   // For rotating mode - number of proxies to simulate
    
    // FETCHER_* - Fetcher configuration
    pub worker_count: usize,
    pub match_worker_count: usize,
    pub max_concurrency: Option<usize>, // Max concurrent requests per worker
    pub fetch_retry_count: usize,
    pub save_to_hour_dir: bool,  // Save tiles to hour-based subdirectories
    
    // CACHE_* - Cache configuration
    pub cache_ttl_ms: i64,
    pub cache_expire_on_hour: bool,
    
    // Auto-cycle mode
    pub auto_cycle_mode: bool,
    pub fetch_cycle_minutes: u64,
    pub match_cycle_minutes: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        dotenv::dotenv().ok();

        // WPLACE_* - Main application config
        let api_endpoint = env::var("WPLACE_API_ENDPOINT")
            .or_else(|_| env::var("API_ENDPOINT"))
            .map_err(|_| "WPLACE_API_ENDPOINT (or API_ENDPOINT) environment variable is required".to_string())?;
        
        let tile_path = env::var("WPLACE_TILE_PATH")
            .or_else(|_| env::var("TILE_PATH"))
            .map_err(|_| "WPLACE_TILE_PATH (or TILE_PATH) environment variable is required".to_string())?;
        
        let rate_limit = env::var("WPLACE_RATE_LIMIT")
            .or_else(|_| env::var("RATE_LIMIT"))
            .unwrap_or_else(|_| "1.8".to_string())
            .parse::<f64>()
            .map_err(|e| format!("WPLACE_RATE_LIMIT must be a valid number: {}", e))?;
        
        let zoom_level = env::var("WPLACE_ZOOM_LEVEL")
            .or_else(|_| env::var("ZOOM_LEVEL"))
            .unwrap_or_else(|_| "15".to_string());
        
        let match_threshold = env::var("MATCHER_THRESHOLD")
            .or_else(|_| env::var("WPLACE_MATCH_THRESHOLD"))
            .or_else(|_| env::var("MATCH_THRESHOLD"))
            .unwrap_or_else(|_| "0.25".to_string())
            .parse::<f64>()
            .map_err(|e| format!("MATCHER_THRESHOLD must be a valid number between 0 and 1: {}", e))?;
        
        if match_threshold < 0.0 || match_threshold > 1.0 {
            return Err("MATCHER_THRESHOLD must be between 0 and 1".to_string().into());
        }

        let tile_selection_mode = match env::var("FETCHER_TILE_SELECTION_MODE")
            .or_else(|_| env::var("WPLACE_TILE_SELECTION_MODE"))
            .or_else(|_| env::var("TILE_SELECTION_MODE"))
            .unwrap_or_else(|_| "random".to_string())
            .to_lowercase()
            .as_str()
        {
            "consecutive" | "sequential" => TileSelectionMode::Consecutive,
            "grid" => TileSelectionMode::Grid,
            "random" | _ => TileSelectionMode::Random,
        };

        let x_tile = env::var("FETCHER_TILE_X")
            .or_else(|_| env::var("WPLACE_X_TILE"))
            .or_else(|_| env::var("X_TILE"))
            .ok()
            .and_then(|v| v.parse::<i32>().ok());
        
        let y_tile = env::var("FETCHER_TILE_Y")
            .or_else(|_| env::var("WPLACE_Y_TILE"))
            .or_else(|_| env::var("Y_TILE"))
            .ok()
            .and_then(|v| v.parse::<i32>().ok());

        // PROXY_* - Proxy configuration
        let proxy_mode = match env::var("PROXY_MODE")
            .unwrap_or_else(|_| "file".to_string())
            .to_lowercase()
            .as_str()
        {
            "rotating" => ProxyMode::Rotating,
            "file" | _ => ProxyMode::File,
        };

        let proxy_endpoint = env::var("PROXY_ENDPOINT").ok();
        let proxies_count = env::var("PROXY_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());

        let max_concurrency = env::var("PROXY_MAX_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok());

        // Validate rotating mode requires endpoint and count
        if proxy_mode == ProxyMode::Rotating {
            if proxy_endpoint.is_none() {
                return Err("PROXY_ENDPOINT is required when PROXY_MODE=rotating".to_string().into());
            }
            if proxies_count.is_none() {
                return Err("PROXY_COUNT is required when PROXY_MODE=rotating".to_string().into());
            }
        }

        // FETCHER_* - Fetcher configuration
        let worker_count = if let Ok(wc) = env::var("FETCHER_WORKER_COUNT")
            .or_else(|_| env::var("WORKER_FETCHER_COUNT"))
            .or_else(|_| env::var("WORKER_COUNT")) {
            wc.parse::<usize>()
                .map_err(|e| format!("FETCHER_WORKER_COUNT must be a valid integer: {}", e))?
        } else {
            // Auto-calculate: max(1, floor(cpu_threads * 0.25))
            let cpu_threads = num_cpus::get();
            (cpu_threads * 25 / 100).max(1)
        };

        let fetch_retry_count = env::var("PROXY_RETRY_COUNT")
            .or_else(|_| env::var("WORKER_FETCH_RETRY_COUNT"))
            .unwrap_or_else(|_| "3".to_string())
            .parse::<usize>()
            .map_err(|e| format!("PROXY_RETRY_COUNT must be a valid integer: {}", e))?;

        let save_to_hour_dir = env::var("FETCHER_SAVE_TO_HOUR_DIR")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "true";

        // MATCHER_* - Matcher configuration
        let match_worker_count = if let Ok(mwc) = env::var("MATCHER_WORKER_COUNT")
            .or_else(|_| env::var("WORKER_MATCHER_COUNT"))
            .or_else(|_| env::var("WORKER_MATCH_COUNT"))
            .or_else(|_| env::var("MATCH_WORKER_COUNT")) {
            mwc.parse::<usize>()
                .map_err(|e| format!("MATCHER_WORKER_COUNT must be a valid integer: {}", e))?
        } else {
            // Auto-calculate: max(1, floor(cpu_threads * 0.25))
            let cpu_threads = num_cpus::get();
            (cpu_threads * 25 / 100).max(1)
        };

        // CACHE_* - Cache configuration
        let cache_ttl_ms = env::var("CACHE_TTL_MS")
            .unwrap_or_else(|_| "3600000".to_string())
            .parse::<i64>()
            .map_err(|e| format!("CACHE_TTL_MS must be a valid integer: {}", e))?;

        let cache_expire_on_hour = env::var("CACHE_EXPIRE_ON_HOUR")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        // Auto-cycle mode
        let auto_cycle_mode = env::var("WPLACE_AUTO_CYCLE_MODE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        let fetch_cycle_minutes = env::var("WPLACE_FETCH_CYCLE_MINUTES")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .map_err(|e| format!("WPLACE_FETCH_CYCLE_MINUTES must be a valid integer: {}", e))?;

        let match_cycle_minutes = env::var("WPLACE_MATCH_CYCLE_MINUTES")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .map_err(|e| format!("WPLACE_MATCH_CYCLE_MINUTES must be a valid integer: {}", e))?;

        Ok(Config {
            api_endpoint,
            tile_path,
            rate_limit,
            zoom_level,
            match_threshold,
            tile_selection_mode,
            x_tile,
            y_tile,
            proxy_mode,
            proxy_endpoint,
            proxies_count,
            worker_count,
            match_worker_count,
            max_concurrency,
            fetch_retry_count,
            save_to_hour_dir,
            cache_ttl_ms,
            cache_expire_on_hour,
            auto_cycle_mode,
            fetch_cycle_minutes,
            match_cycle_minutes,
        })
    }

    pub fn calculate_worker_rate(&self, proxy_count: usize) -> f64 {
        let worker_count = self.worker_count.min(proxy_count);
        // Formula: (RATE_LIMIT / workerCount) * proxyCount
        (self.rate_limit / worker_count as f64) * proxy_count as f64
    }

    pub fn get_worker_count(&self, proxy_count: usize) -> usize {
        self.worker_count.min(proxy_count).max(1)
    }
}