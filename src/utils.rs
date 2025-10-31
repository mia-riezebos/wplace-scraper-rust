use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Write debug messages to outputs/debug.txt
pub fn write_debug(msg: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("outputs/debug.txt")
    {
        let _ = file.write_all(msg.as_bytes());
        let _ = file.flush();
    }
}

/// Get current hour in HH00 format (e.g., "0800", "1400")
pub fn get_current_hour_dir() -> String {
    let now = chrono::Local::now();
    now.format("%H00").to_string()
}

/// Parse tile coordinates from file path
/// Supports both formats: tiles/HH00/x/y.png and tiles/x/y.png
pub fn parse_tile_file_name(
    file_path: &Path,
) -> Result<(i32, i32), Box<dyn std::error::Error + Send + Sync>> {
    let components: Vec<_> = file_path.components().collect();

    // Find 'tiles' in path
    let tiles_idx = components
        .iter()
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

    let x_str = components[x_idx]
        .as_os_str()
        .to_str()
        .ok_or("Invalid tile X coordinate")?;
    let y_str = components[x_idx + 1]
        .as_os_str()
        .to_str()
        .ok_or("Invalid tile Y coordinate")?
        .replace(".png", "");

    let x = x_str.parse::<i32>()?;
    let y = y_str.parse::<i32>()?;

    Ok((x, y))
}

/// Build tile file path based on configuration
pub fn build_tile_path(x: i32, y: i32, save_to_hour_dir: bool) -> PathBuf {
    if save_to_hour_dir {
        let hour_dir = get_current_hour_dir();
        PathBuf::from("outputs/tiles")
            .join(&hour_dir)
            .join(x.to_string())
            .join(format!("{}.png", y))
    } else {
        PathBuf::from("outputs/tiles")
            .join(x.to_string())
            .join(format!("{}.png", y))
    }
}

/// Build HTTP client with proxy configuration
pub fn build_proxy_client(
    proxy: &crate::proxy::ProxyInfo,
) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
    let mut client_builder = reqwest::Client::builder()
        .connect_timeout(tokio::time::Duration::from_secs(15))
        .timeout(tokio::time::Duration::from_secs(45))
        .no_proxy();

    let proxy_url = format!("http://{}:{}", proxy.hostname, proxy.port);

    if let (Some(username), Some(password)) = (&proxy.username, &proxy.password) {
        let http_proxy = reqwest::Proxy::http(&proxy_url)?
            .basic_auth(username, password);
        client_builder = client_builder.proxy(http_proxy.clone());

        let https_proxy = reqwest::Proxy::https(&proxy_url)?
            .basic_auth(username, password);
        client_builder = client_builder.proxy(https_proxy);
    } else {
        let http_proxy = reqwest::Proxy::http(&proxy_url)?;
        client_builder = client_builder.proxy(http_proxy.clone());

        let https_proxy = reqwest::Proxy::https(&proxy_url)?;
        client_builder = client_builder.proxy(https_proxy);
    }

    Ok(client_builder.build()?)
}

/// Scan tiles directory recursively and return list of tile coordinates
pub async fn scan_tiles_directory(
    dir: &str,
) -> Result<Vec<(i32, i32)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tiles = Vec::new();

    async fn scan_recursive(
        dir: &str,
        tiles: &mut Vec<(i32, i32)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
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

