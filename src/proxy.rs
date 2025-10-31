use std::sync::{Arc, Mutex};
use std::time::Instant;
use url::Url;

#[derive(Clone, Debug)]
pub struct ProxyInfo {
    pub url: String,
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

struct ProxyUsage {
    last_used: Instant,
}

pub struct ProxyManager {
    proxies: Vec<ProxyInfo>,
    usage: Arc<Mutex<Vec<ProxyUsage>>>,
    current_index: Arc<Mutex<usize>>,
    rate_limit_interval_ms: u64,
}

impl ProxyManager {
    pub fn from_file(path: &str, rate_limit_per_second: f64) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let proxy_urls: Vec<String> = serde_json::from_str(&content)?;

        let proxies: Vec<ProxyInfo> = proxy_urls
            .into_iter()
            .map(|url_str| {
                let url = Url::parse(&url_str)?;
                let hostname = url.host_str()
                    .ok_or("Missing hostname")?
                    .to_string();
                let port = url.port().unwrap_or(8080);
                let (username, password) = if !url.username().is_empty() {
                    let user_pass = url.username();
                    if let Some(colon_idx) = user_pass.find(':') {
                        (
                            Some(user_pass[..colon_idx].to_string()),
                            Some(user_pass[colon_idx + 1..].to_string()),
                        )
                    } else {
                        (Some(user_pass.to_string()), None)
                    }
                } else {
                    (None, None)
                };

                Ok(ProxyInfo {
                    url: url_str,
                    hostname,
                    port,
                    username,
                    password,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

        if proxies.is_empty() {
            return Err("No proxies loaded".into());
        }

        let proxy_count = proxies.len();
        let usage: Vec<ProxyUsage> = (0..proxy_count)
            .map(|_| ProxyUsage {
                last_used: Instant::now(), // Start fresh - all proxies available immediately
            })
            .collect();

        // Calculate minimum interval between uses of the same proxy
        // Each proxy should be used at most rate_limit_per_second times per second
        let rate_limit_interval_ms = (1000.0 / rate_limit_per_second) as u64;

        Ok(ProxyManager {
            proxies,
            usage: Arc::new(Mutex::new(usage)),
            current_index: Arc::new(Mutex::new(0)),
            rate_limit_interval_ms,
        })
    }

    pub fn get_next_available(&self) -> ProxyInfo {
        let mut usage_guard = self.usage.lock().unwrap();
        let mut index_guard = self.current_index.lock().unwrap();
        
        let proxy_count = self.proxies.len();
        let now = Instant::now();
        let min_interval = std::time::Duration::from_millis(self.rate_limit_interval_ms);
        
        // Try to find an available proxy starting from current_index
        for _ in 0..proxy_count {
            let idx = *index_guard % proxy_count;
            let time_since_last_use = now.saturating_duration_since(usage_guard[idx].last_used);
            
            if time_since_last_use >= min_interval {
                // This proxy is available
                usage_guard[idx].last_used = now;
                *index_guard = (idx + 1) % proxy_count;
                return self.proxies[idx].clone();
            }
            
            // Try next proxy
            *index_guard = (*index_guard + 1) % proxy_count;
        }
        
        // If no proxy is immediately available, use the oldest one
        let mut oldest_idx = 0;
        let mut oldest_time = usage_guard[0].last_used;
        
        for (idx, usage) in usage_guard.iter().enumerate() {
            if usage.last_used < oldest_time {
                oldest_time = usage.last_used;
                oldest_idx = idx;
            }
        }
        
        usage_guard[oldest_idx].last_used = now;
        *index_guard = (oldest_idx + 1) % proxy_count;
        self.proxies[oldest_idx].clone()
    }

    pub fn get_next(&self) -> ProxyInfo {
        let mut index = self.current_index.lock().unwrap();
        let proxy = self.proxies[*index].clone();
        *index = (*index + 1) % self.proxies.len();
        proxy
    }

    pub fn count(&self) -> usize {
        self.proxies.len()
    }

    pub fn get_by_index(&self, index: usize) -> ProxyInfo {
        self.proxies[index % self.proxies.len()].clone()
    }

    pub fn get_proxy_index(&self, proxy: &ProxyInfo) -> Option<usize> {
        self.proxies.iter().position(|p| p.url == proxy.url)
    }
}

