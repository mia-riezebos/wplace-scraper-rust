use crate::config::ProxyMode;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use url::Url;

// Helper function to extract port from URL string before Url parser normalizes it
fn extract_port_from_url_string(url_str: &str) -> Option<u16> {
    // Find the scheme separator
    if let Some(scheme_end) = url_str.find("://") {
        let after_scheme = &url_str[scheme_end + 3..];

        // Skip username/password if present (format: user:pass@host:port)
        let after_auth = if let Some(at_pos) = after_scheme.find('@') {
            &after_scheme[at_pos + 1..]
        } else {
            after_scheme
        };

        // Find the port separator (colon before the port)
        // We need to find the last colon that's part of the host:port pattern
        // This is tricky because IPv6 addresses have colons too
        // For simplicity, we'll look for :port pattern (where port is digits)
        if let Some(colon_pos) = after_auth.rfind(':') {
            let port_str = &after_auth[colon_pos + 1..];
            // Check if there's a path separator after the port
            let port_end = port_str.find('/').unwrap_or(port_str.len());
            let port_str = &port_str[..port_end];

            // Check if it's all digits (not an IPv6 address segment)
            if port_str.chars().all(|c| c.is_ascii_digit()) && !port_str.is_empty() {
                return port_str.parse::<u16>().ok();
            }
        }
    }
    None
}

// Helper function to extract username and password from URL string
// Format: http://username:password@host:port/
fn extract_auth_from_url_string(url_str: &str) -> (Option<String>, Option<String>) {
    if let Some(scheme_end) = url_str.find("://") {
        let after_scheme = &url_str[scheme_end + 3..];

        // Find the @ separator (end of auth section)
        if let Some(at_pos) = after_scheme.find('@') {
            let auth_part = &after_scheme[..at_pos];

            // Split on colon to get username and password
            if let Some(colon_pos) = auth_part.find(':') {
                let username = &auth_part[..colon_pos];
                let password = &auth_part[colon_pos + 1..];

                // URL decode if needed (basic - handle % encoded characters)
                // For now, return as-is since url crate should handle encoding
                (
                    if username.is_empty() {
                        None
                    } else {
                        Some(username.to_string())
                    },
                    if password.is_empty() {
                        None
                    } else {
                        Some(password.to_string())
                    },
                )
            } else {
                // No colon means username only, no password
                (
                    if auth_part.is_empty() {
                        None
                    } else {
                        Some(auth_part.to_string())
                    },
                    None,
                )
            }
        } else {
            // No @ found, no auth
            (None, None)
        }
    } else {
        (None, None)
    }
}

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
    #[allow(dead_code)]
    mode: ProxyMode,
    #[allow(dead_code)]
    rotating_endpoint: Option<String>, // For rotating mode
}

impl ProxyManager {
    pub fn from_file(
        path: &str,
        rate_limit_per_second: f64,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let proxy_urls: Vec<String> = serde_json::from_str(&content)?;

        let proxies: Vec<ProxyInfo> = proxy_urls
            .into_iter()
            .map(|url_str| {
                // Parse port from original URL string before Url parser normalizes it
                // The Url crate returns None for default ports even if explicitly specified
                let explicit_port = extract_port_from_url_string(&url_str);

                let url = Url::parse(&url_str)?;
                let hostname = url.host_str().ok_or("Missing hostname")?.to_string();

                // Use explicit port if found, otherwise use Url's port() or scheme-based default
                let port = explicit_port.or_else(|| url.port()).unwrap_or_else(|| {
                    match url.scheme() {
                        "http" => 80,
                        "https" => 443,
                        _ => 8080, // Default fallback
                    }
                });

                // Extract username and password from original URL string
                // This ensures we get credentials even if url crate's password() method fails
                let (url_username, url_password) = extract_auth_from_url_string(&url_str);

                // Use url crate methods as primary, fall back to manual extraction
                let username = if !url.username().is_empty() {
                    Some(url.username().to_string())
                } else {
                    url_username
                };

                let password = url.password().map(|p| p.to_string()).or(url_password);

                // Debug: verify extraction worked
                if username.is_some() && password.is_none() {
                    eprintln!(
                        "Warning: Username found but password is None for URL: {}",
                        url_str
                    );
                }

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
            mode: ProxyMode::File,
            rotating_endpoint: None,
        })
    }

    pub fn from_rotating_endpoint(
        endpoint: &str,
        proxies_count: usize,
        rate_limit_per_second: f64,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the rotating endpoint URL
        // Extract port from original string before Url parser normalizes it
        let explicit_port = extract_port_from_url_string(endpoint);

        let endpoint_url = Url::parse(endpoint)?;
        let hostname = endpoint_url
            .host_str()
            .ok_or("Missing hostname in PROXY_ENDPOINT")?
            .to_string();
        // Use explicit port if found, otherwise use Url's port() or scheme-based default
        let port = explicit_port
            .or_else(|| endpoint_url.port())
            .unwrap_or_else(|| {
                match endpoint_url.scheme() {
                    "http" => 80,
                    "https" => 443,
                    _ => 8080, // Default fallback
                }
            });

        // Extract username/password from original URL string
        let (url_username, url_password) = extract_auth_from_url_string(endpoint);

        // Use url crate methods as primary, fall back to manual extraction
        let username = if !endpoint_url.username().is_empty() {
            Some(endpoint_url.username().to_string())
        } else {
            url_username
        };

        let password = endpoint_url
            .password()
            .map(|p| p.to_string())
            .or(url_password);

        // Create virtual proxies for the rotating endpoint
        // Each "proxy" represents one slot in the rotating pool
        let proxies: Vec<ProxyInfo> = (0..proxies_count)
            .map(|i| ProxyInfo {
                url: format!("{}#{}", endpoint, i), // Add index to differentiate
                hostname: hostname.clone(),
                port,
                username: username.clone(),
                password: password.clone(),
            })
            .collect();

        let usage: Vec<ProxyUsage> = (0..proxies_count)
            .map(|_| ProxyUsage {
                last_used: Instant::now(),
            })
            .collect();

        let rate_limit_interval_ms = (1000.0 / rate_limit_per_second) as u64;

        Ok(ProxyManager {
            proxies,
            usage: Arc::new(Mutex::new(usage)),
            current_index: Arc::new(Mutex::new(0)),
            rate_limit_interval_ms,
            mode: ProxyMode::Rotating,
            rotating_endpoint: Some(endpoint.to_string()),
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

    #[allow(dead_code)]
    pub fn get_next(&self) -> ProxyInfo {
        let mut index = self.current_index.lock().unwrap();
        let proxy = self.proxies[*index].clone();
        *index = (*index + 1) % self.proxies.len();
        proxy
    }

    pub fn count(&self) -> usize {
        self.proxies.len()
    }

    #[allow(dead_code)]
    pub fn get_by_index(&self, index: usize) -> ProxyInfo {
        self.proxies[index % self.proxies.len()].clone()
    }

    pub fn get_proxy_index(&self, proxy: &ProxyInfo) -> Option<usize> {
        self.proxies.iter().position(|p| p.url == proxy.url)
    }
}
