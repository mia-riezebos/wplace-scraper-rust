use rusqlite::{Connection, Result};
use std::fs;
use std::path::PathBuf;

pub struct Cache {
    conn: Connection,
    ttl_ms: i64,
    expire_on_hour: bool,
    last_cleared_hour: Option<i64>, // Track last hour when matches.txt was cleared
}

impl Cache {
    pub fn new(path: &str, ttl_ms: i64, expire_on_hour: bool) -> Result<Self> {
        let conn = Connection::open(path)?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tiles (
                tile_x INTEGER NOT NULL,
                tile_y INTEGER NOT NULL,
                response_code INTEGER NOT NULL,
                url TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (tile_x, tile_y)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS matches (
                tile_x INTEGER NOT NULL,
                tile_y INTEGER NOT NULL,
                response_code INTEGER NOT NULL,
                url TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (tile_x, tile_y)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_tiles (
                tile_x INTEGER NOT NULL,
                tile_y INTEGER NOT NULL,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (tile_x, tile_y)
            )",
            [],
        )?;

        Ok(Cache { 
            conn, 
            ttl_ms, 
            expire_on_hour,
            last_cleared_hour: None,
        })
    }

    fn get_current_hour_timestamp(&self) -> i64 {
        let now = chrono::Utc::now();
        let timestamp_secs = now.timestamp();
        let seconds_in_hour = timestamp_secs % 3600;
        let hour_start_timestamp = timestamp_secs - seconds_in_hour;
        hour_start_timestamp * 1000 // Convert to milliseconds
    }

    fn clear_matches_file_if_new_hour(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Only clear if expire_on_hour is enabled
        if !self.expire_on_hour {
            return Ok(());
        }

        let current_hour = self.get_current_hour_timestamp();
        
        // Check if we've crossed into a new hour
        if let Some(last_hour) = self.last_cleared_hour {
            if current_hour <= last_hour {
                // Still in the same hour, don't clear
                return Ok(());
            }
        }
        
        // New hour detected, empty matches.txt (truncate to empty)
        let matches_file = PathBuf::from("outputs/matches.txt");
        if matches_file.exists() {
            // Truncate the file to empty instead of deleting it
            fs::File::create(&matches_file)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        
        // Update last cleared hour timestamp
        self.last_cleared_hour = Some(current_hour);
        
        Ok(())
    }

    pub fn get_cutoff_time(&self) -> i64 {
        let now = chrono::Utc::now();
        
        if self.expire_on_hour {
            // Round down to the top of the current hour
            // Calculate milliseconds since epoch for the start of current hour
            let timestamp_secs = now.timestamp();
            let seconds_in_hour = timestamp_secs % 3600;
            let hour_start_timestamp = timestamp_secs - seconds_in_hour;
            hour_start_timestamp * 1000 // Convert to milliseconds
        } else {
            // Standard TTL-based expiration
            now.timestamp_millis() - self.ttl_ms
        }
    }

    pub fn is_cached(&self, x: i32, y: i32) -> Result<bool> {
        let cutoff_time = self.get_cutoff_time();
        
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM tiles WHERE tile_x = ? AND tile_y = ? AND created_at > ?"
        )?;
        
        let exists = stmt.exists(rusqlite::params![x, y, cutoff_time])?;
        Ok(exists)
    }

    pub fn mark_cached(
        &self,
        x: i32,
        y: i32,
        response_code: i32,
        url: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO tiles (tile_x, tile_y, response_code, url, created_at) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![x, y, response_code, url, chrono::Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn update_url(&self, x: i32, y: i32, url: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE tiles SET url = ? WHERE tile_x = ? AND tile_y = ?",
            rusqlite::params![url, x, y],
        )?;
        Ok(())
    }

    pub fn mark_match(
        &mut self,
        x: i32,
        y: i32,
        response_code: i32,
        url: &str,
    ) -> Result<()> {
        // Check if we need to clear matches.txt (on the hour) before adding new match
        let _ = self.clear_matches_file_if_new_hour();
        
        self.conn.execute(
            "INSERT OR REPLACE INTO matches (tile_x, tile_y, response_code, url, created_at) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![x, y, response_code, url, chrono::Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    pub fn is_match_exists(&self, url: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM matches WHERE url = ?"
        )?;
        let exists = stmt.exists([url])?;
        Ok(exists)
    }

    pub fn mark_processed(&self, x: i32, y: i32) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO processed_tiles (tile_x, tile_y, processed_at) 
             VALUES (?1, ?2, ?3)",
            rusqlite::params![x, y, chrono::Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    pub fn is_processed(&self, x: i32, y: i32) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM processed_tiles WHERE tile_x = ? AND tile_y = ?"
        )?;
        let exists = stmt.exists(rusqlite::params![x, y])?;
        Ok(exists)
    }

    pub fn get_all_processed_tiles(&self) -> Result<Vec<(i32, i32)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tile_x, tile_y FROM processed_tiles"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        
        let mut tiles = Vec::new();
        for row in rows {
            tiles.push(row?);
        }
        Ok(tiles)
    }

    pub fn get_status_code_counts(&self) -> Result<(u64, u64, u64)> {
        // Returns (status_200_count, status_404_count, total_tiles)
        // Only counts non-expired entries based on cache TTL
        let cutoff_time = self.get_cutoff_time();
        
        let mut stmt = self.conn.prepare(
            "SELECT response_code, COUNT(*) FROM tiles WHERE created_at > ? GROUP BY response_code"
        )?;
        
        let mut status_200 = 0u64;
        let mut status_404 = 0u64;
        let mut total = 0u64;
        
        let rows = stmt.query_map([cutoff_time], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, i64>(1)?))
        })?;
        
        for row in rows {
            let (code, count) = row?;
            total += count as u64;
            match code {
                200 => status_200 = count as u64,
                404 => status_404 = count as u64,
                _ => {}
            }
        }
        
        Ok((status_200, status_404, total))
    }

    pub fn cleanup_expired_entries(&mut self) -> Result<(u64, u64)> {
        // Clean up expired entries from tiles and matches tables
        // Returns (tiles_deleted, matches_deleted)
        let cutoff_time = self.get_cutoff_time();
        
        // Check if we need to clear matches.txt (on the hour)
        // Ignore errors - if file clearing fails, continue with cleanup
        let _ = self.clear_matches_file_if_new_hour();
        
        // Delete expired tiles
        let tiles_deleted = self.conn.execute(
            "DELETE FROM tiles WHERE created_at <= ?",
            [cutoff_time],
        )? as u64;
        
        // Delete expired matches (matches don't expire, but we'll use same TTL for consistency)
        let matches_deleted = self.conn.execute(
            "DELETE FROM matches WHERE created_at <= ?",
            [cutoff_time],
        )? as u64;
        
        Ok((tiles_deleted, matches_deleted))
    }

    pub fn clear_all(&self) -> Result<()> {
        // Clear all cache tables
        self.conn.execute("DELETE FROM tiles", [])?;
        self.conn.execute("DELETE FROM matches", [])?;
        self.conn.execute("DELETE FROM processed_tiles", [])?;
        Ok(())
    }

    pub fn get_all_cached_tiles(&self) -> Result<Vec<(i32, i32)>> {
        // Get all cached tiles (non-expired)
        let cutoff_time = self.get_cutoff_time();
        
        let mut stmt = self.conn.prepare(
            "SELECT tile_x, tile_y FROM tiles WHERE created_at > ?"
        )?;
        
        let rows = stmt.query_map([cutoff_time], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        
        let mut tiles = Vec::new();
        for row in rows {
            tiles.push(row?);
        }
        Ok(tiles)
    }

    pub fn get_fetch_progress(&self) -> Result<(u64, u64, Option<(i32, i32)>, Option<u64>)> {
        // Returns: (total_fetched, total_200s, max_coords, last_index_for_consecutive)
        // Only counts non-expired entries
        let cutoff_time = self.get_cutoff_time();
        
        // Get total counts
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*), SUM(CASE WHEN response_code = 200 THEN 1 ELSE 0 END) 
             FROM tiles WHERE created_at > ?"
        )?;
        
        let mut total_fetched = 0u64;
        let mut total_200s = 0u64;
        
        let rows = stmt.query_map([cutoff_time], |row| {
            Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?))
        })?;
        
        for row in rows {
            let (total, twos) = row?;
            total_fetched = total.unwrap_or(0) as u64;
            total_200s = twos.unwrap_or(0) as u64;
        }
        
        // Get max coordinates (for consecutive mode)
        let mut stmt = self.conn.prepare(
            "SELECT MAX(tile_x), MAX(tile_y) FROM tiles WHERE created_at > ?"
        )?;
        
        let mut max_coords: Option<(i32, i32)> = None;
        let rows = stmt.query_map([cutoff_time], |row| {
            Ok((row.get::<_, Option<i32>>(0)?, row.get::<_, Option<i32>>(1)?))
        })?;
        
        for row in rows {
            let (max_x, max_y) = row?;
            if let (Some(x), Some(y)) = (max_x, max_y) {
                max_coords = Some((x, y));
            }
        }
        
        // Calculate last index for consecutive mode
        // Convert max coords to index: index = y * 2048 + x
        let last_index = max_coords.map(|(x, y)| {
            (y as u64 * 2048) + x as u64 + 1 // +1 to start from next tile
        });
        
        Ok((total_fetched, total_200s, max_coords, last_index))
    }
}

