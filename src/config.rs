use std::env;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TileSelectionMode {
    Random,
    Consecutive,
    Grid,
}

pub struct Config {
    pub api_endpoint: String,
    pub tile_path: String,
    pub rate_limit: f64,
    pub zoom_level: String,
    pub worker_count: usize,
    pub match_worker_count: usize,
    pub match_threshold: f64,
    pub cache_ttl_ms: i64,
    pub cache_expire_on_hour: bool,
    pub tile_selection_mode: TileSelectionMode,
    pub fetch_retry_count: usize,
    pub auto_cycle_mode: bool,
    pub fetch_cycle_minutes: u64,
    pub match_cycle_minutes: u64,
    pub x_tile: Option<i32>,
    pub y_tile: Option<i32>,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        dotenv::dotenv().ok();

        let api_endpoint = env::var("API_ENDPOINT")
            .map_err(|_| "API_ENDPOINT environment variable is required".to_string())?;
        
        let tile_path = env::var("TILE_PATH")
            .map_err(|_| "TILE_PATH environment variable is required".to_string())?;
        
        let rate_limit = env::var("RATE_LIMIT")
            .unwrap_or_else(|_| "1.8".to_string())
            .parse::<f64>()
            .map_err(|e| format!("RATE_LIMIT must be a valid number: {}", e))?;
        
        let zoom_level = env::var("ZOOM_LEVEL")
            .unwrap_or_else(|_| "15".to_string());
        
        let match_threshold = env::var("MATCH_THRESHOLD")
            .unwrap_or_else(|_| "0.25".to_string())
            .parse::<f64>()
            .map_err(|e| format!("MATCH_THRESHOLD must be a valid number between 0 and 1: {}", e))?;
        
        if match_threshold < 0.0 || match_threshold > 1.0 {
            return Err("MATCH_THRESHOLD must be between 0 and 1".to_string().into());
        }

        let worker_count = if let Ok(wc) = env::var("WORKER_COUNT") {
            wc.parse::<usize>()
                .map_err(|e| format!("WORKER_COUNT must be a valid integer: {}", e))?
        } else {
            // Auto-calculate: max(1, floor(cpu_threads * 0.25))
            let cpu_threads = num_cpus::get();
            (cpu_threads * 25 / 100).max(1)
        };

        // Cache TTL in milliseconds (default: 1 hour = 3600000ms)
        let cache_ttl_ms = env::var("CACHE_TTL_MS")
            .unwrap_or_else(|_| "3600000".to_string())
            .parse::<i64>()
            .map_err(|e| format!("CACHE_TTL_MS must be a valid integer: {}", e))?;

        // Cache expire on hour (default: false)
        // If true, cache entries expire at the top of each hour (e.g., 1:00, 2:00, 3:00)
        let cache_expire_on_hour = env::var("CACHE_EXPIRE_ON_HOUR")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        let match_worker_count = if let Ok(mwc) = env::var("MATCH_WORKER_COUNT") {
            mwc.parse::<usize>()
                .map_err(|e| format!("MATCH_WORKER_COUNT must be a valid integer: {}", e))?
        } else {
            // Auto-calculate: max(1, floor(cpu_threads * 0.25))
            let cpu_threads = num_cpus::get();
            (cpu_threads * 25 / 100).max(1)
        };

        let tile_selection_mode = match env::var("TILE_SELECTION_MODE")
            .unwrap_or_else(|_| "random".to_string())
            .to_lowercase()
            .as_str()
        {
            "consecutive" | "sequential" => TileSelectionMode::Consecutive,
            "grid" => TileSelectionMode::Grid,
            "random" | _ => TileSelectionMode::Random,
        };

        // Fetch retry count (default: 3)
        let fetch_retry_count = env::var("FETCH_RETRY_COUNT")
            .unwrap_or_else(|_| "3".to_string())
            .parse::<usize>()
            .map_err(|e| format!("FETCH_RETRY_COUNT must be a valid integer: {}", e))?;

        // Auto-cycle mode (default: false)
        // If enabled, automatically cycles between fetching and matching
        let auto_cycle_mode = env::var("AUTO_CYCLE_MODE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        // Fetch cycle duration in minutes (default: 30)
        let fetch_cycle_minutes = env::var("FETCH_CYCLE_MINUTES")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .map_err(|e| format!("FETCH_CYCLE_MINUTES must be a valid integer: {}", e))?;

        // Match cycle duration in minutes (default: 30)
        let match_cycle_minutes = env::var("MATCH_CYCLE_MINUTES")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .map_err(|e| format!("MATCH_CYCLE_MINUTES must be a valid integer: {}", e))?;

        let x_tile = env::var("X_TILE")
            .ok()
            .and_then(|v| v.parse::<i32>().ok());
        
        let y_tile = env::var("Y_TILE")
            .ok()
            .and_then(|v| v.parse::<i32>().ok());

        Ok(Config {
            api_endpoint,
            tile_path,
            rate_limit,
            zoom_level,
            worker_count,
            match_worker_count,
            match_threshold,
            cache_ttl_ms,
            cache_expire_on_hour,
            tile_selection_mode,
            fetch_retry_count,
            auto_cycle_mode,
            fetch_cycle_minutes,
            match_cycle_minutes,
            x_tile,
            y_tile,
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

