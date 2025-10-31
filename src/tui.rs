use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use crate::utils;

pub struct Tui {
    logs: Arc<Mutex<VecDeque<String>>>,
    fetch_running: Arc<std::sync::Mutex<bool>>,
    match_running: Arc<std::sync::Mutex<bool>>,
    shutdown_requested: Arc<std::sync::Mutex<bool>>,
    cache: Arc<std::sync::Mutex<crate::db::Cache>>,
    #[allow(dead_code)]
    config: Arc<crate::config::Config>,
    rate_limit: Arc<std::sync::Mutex<f64>>,
}

impl Tui {
    pub fn new(
        fetch_running: Arc<std::sync::Mutex<bool>>,
        match_running: Arc<std::sync::Mutex<bool>>,
        shutdown_requested: Arc<std::sync::Mutex<bool>>,
        cache: Arc<std::sync::Mutex<crate::db::Cache>>,
        config: Arc<crate::config::Config>,
        rate_limit: Arc<std::sync::Mutex<f64>>,
    ) -> Self {
        Self {
            logs: Arc::new(Mutex::new(VecDeque::new())),
            fetch_running,
            match_running,
            shutdown_requested,
            cache,
            config,
            rate_limit,
        }
    }

    pub fn logs(&self) -> Arc<Mutex<VecDeque<String>>> {
        self.logs.clone()
    }

    pub fn add_log(&self, message: String) {
        let mut logs = self.logs.lock().unwrap();
        logs.push_back(message);
        if logs.len() > 1000 {
            logs.pop_front();
        }
    }

    pub async fn run(
        &mut self,
        fetch_stats: Arc<crate::fetch_worker::FetchStats>,
        match_stats: Arc<crate::match_worker::MatchStats>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
        crossterm::terminal::enable_raw_mode()?;
        crossterm::execute!(std::io::stdout(), crossterm::cursor::Hide)?;

        loop {
            terminal.draw(|f| self.draw(f, fetch_stats.clone(), match_stats.clone()))?;

            // Check for key events
            if event::poll(std::time::Duration::from_millis(50))? {
                if let Event::Key(key_event) = event::read()? {
                    if key_event.kind == KeyEventKind::Press {
                        match key_event.code {
                            KeyCode::Char('f') | KeyCode::Char('F') => {
                                let mut state = self.fetch_running.lock().unwrap();
                                *state = !*state;
                                let status = if *state { "STARTED" } else { "STOPPED" };
                                self.add_log(format!("[Control] Fetch workers {}", status));
                            }
                            KeyCode::Char('m') | KeyCode::Char('M') => {
                                let mut state = self.match_running.lock().unwrap();
                                *state = !*state;
                                let status = if *state { "STARTED" } else { "STOPPED" };
                                self.add_log(format!("[Control] Match workers {}", status));
                            }
                            KeyCode::Char('l') | KeyCode::Char('L') => {
                                // Check tile processing status asynchronously
                                let tui_logs = self.logs.clone();
                                let cache = self.cache.clone();
                                let fetch_stats = fetch_stats.clone();
                                tokio::spawn(async move {
                                    // Create a temporary Tui instance just for the status check
                                    // We only need logs and cache for this operation
                                    struct TempTui {
                                        logs: Arc<Mutex<VecDeque<String>>>,
                                        cache: Arc<std::sync::Mutex<crate::db::Cache>>,
                                        fetch_stats: Arc<crate::fetch_worker::FetchStats>,
                                    }

                                    impl TempTui {
                                        fn add_log(&self, message: String) {
                                            let mut logs = self.logs.lock().unwrap();
                                            logs.push_back(message);
                                            if logs.len() > 1000 {
                                                logs.pop_front();
                                            }
                                        }

                                        async fn check_status(&self) {
                                            self.add_log(
                                                "[Status] Checking tile processing status..."
                                                    .to_string(),
                                            );

                                            // Clean up expired cache entries first
                                            let (tiles_deleted, matches_deleted) = {
                                                let mut cache_guard = self.cache.lock().unwrap();
                                                match cache_guard.cleanup_expired_entries() {
                                                    Ok(counts) => counts,
                                                    Err(e) => {
                                                        self.add_log(format!("[Status] Error cleaning expired entries: {}", e));
                                                        (0, 0)
                                                    }
                                                }
                                            };

                                            if tiles_deleted > 0 || matches_deleted > 0 {
                                                self.add_log(format!("[Status] Cleaned up {} expired tiles and {} expired matches", 
                                                    tiles_deleted, matches_deleted));
                                            }

                                            // Update fetch stats from database (only non-expired entries)
                                            let (status_200_count, status_404_count, total_fetched) = {
                                                let cache_guard = self.cache.lock().unwrap();
                                                match cache_guard.get_status_code_counts() {
                                                    Ok(counts) => counts,
                                                    Err(e) => {
                                                        self.add_log(format!("[Status] Error getting status code counts: {}", e));
                                                        (0, 0, 0)
                                                    }
                                                }
                                            };

                                            // Update fetch stats counters
                                            self.fetch_stats.update_status_counts(
                                                status_200_count,
                                                status_404_count,
                                            );

                                            if total_fetched > 0 {
                                                self.add_log(format!("[Status] Updated fetch stats from database: {} 200s, {} 404s (total: {})", 
                                                    status_200_count, status_404_count, total_fetched));
                                            }

                                            let tile_files = match utils::scan_tiles_directory("outputs/tiles").await {
                                                Ok(files) => files,
                                                Err(e) => {
                                                    self.add_log(format!("[Status] Error scanning tiles directory: {}", e));
                                                    return;
                                                }
                                            };

                                            let total_tiles = tile_files.len();

                                            if total_tiles == 0 {
                                                self.add_log(
                                                    "[Status] No tiles found on disk".to_string(),
                                                );
                                                return;
                                            }

                                            let mut processed_count = 0;
                                            let mut unprocessed_tiles = Vec::new();

                                            {
                                                let cache_guard = self.cache.lock().unwrap();
                                                for (x, y) in &tile_files {
                                                    match cache_guard.is_processed(*x, *y) {
                                                        Ok(true) => processed_count += 1,
                                                        Ok(false) => {
                                                            unprocessed_tiles.push((*x, *y))
                                                        }
                                                        Err(e) => {
                                                            self.add_log(format!("[Status] Error checking tile ({}, {}): {}", x, y, e));
                                                        }
                                                    }
                                                }
                                            }

                                            let unprocessed_count = total_tiles - processed_count;
                                            let percentage = if total_tiles > 0 {
                                                (processed_count as f64 / total_tiles as f64)
                                                    * 100.0
                                            } else {
                                                0.0
                                            };

                                            self.add_log(format!(
                                                "[Status] Total tiles on disk: {}",
                                                total_tiles
                                            ));
                                            self.add_log(format!(
                                                "[Status] Processed: {} ({:.1}%)",
                                                processed_count, percentage
                                            ));
                                            self.add_log(format!(
                                                "[Status] Unprocessed: {}",
                                                unprocessed_count
                                            ));

                                            if unprocessed_count == 0 {
                                                self.add_log(
                                                    "[Status] ✓ All tiles have been processed!"
                                                        .to_string(),
                                                );
                                            } else {
                                                self.add_log(format!(
                                                    "[Status] {} tiles still need processing",
                                                    unprocessed_count
                                                ));
                                                let show_count = unprocessed_tiles.len().min(10);
                                                if show_count > 0 {
                                                    let sample: Vec<String> = unprocessed_tiles
                                                        [..show_count]
                                                        .iter()
                                                        .map(|(x, y)| format!("({}, {})", x, y))
                                                        .collect();
                                                    self.add_log(format!(
                                                        "[Status] Sample unprocessed tiles: {}",
                                                        sample.join(", ")
                                                    ));
                                                }
                                            }
                                        }
                                    }

                                    let temp_tui = TempTui {
                                        logs: tui_logs,
                                        cache,
                                        fetch_stats,
                                    };
                                    temp_tui.check_status().await;
                                });
                            }
                            KeyCode::Char('x') | KeyCode::Char('X') => {
                                // Clear all cache
                                let tui_logs = self.logs.clone();
                                let cache = self.cache.clone();
                                let fetch_stats = fetch_stats.clone();
                                tokio::spawn(async move {
                                    {
                                        let cache_guard = cache.lock().unwrap();
                                        match cache_guard.clear_all() {
                                            Ok(_) => {
                                                let mut logs = tui_logs.lock().unwrap();
                                                logs.push_back(
                                                    "[Control] Cache cleared successfully"
                                                        .to_string(),
                                                );
                                                logs.push_back("[Control] Status counters reset (main stats preserved)".to_string());
                                            }
                                            Err(e) => {
                                                let mut logs = tui_logs.lock().unwrap();
                                                logs.push_back(format!(
                                                    "[Control] Error clearing cache: {}",
                                                    e
                                                ));
                                            }
                                        }
                                    }

                                    // Reset only status counters (200/404), not main counters
                                    fetch_stats.update_status_counts(0, 0);
                                });
                            }
                            KeyCode::Char('d') | KeyCode::Char('D') => {
                                // Delete all saved tiles
                                let tui_logs = self.logs.clone();
                                tokio::spawn(async move {
                                    match tokio::fs::remove_dir_all("outputs/tiles").await {
                                        Ok(_) => {
                                            // Recreate the directory
                                            if let Err(e) =
                                                tokio::fs::create_dir_all("outputs/tiles").await
                                            {
                                                let mut logs = tui_logs.lock().unwrap();
                                                logs.push_back(format!("[Control] Error recreating tiles directory: {}", e));
                                                return;
                                            }

                                            let mut logs = tui_logs.lock().unwrap();
                                            logs.push_back(
                                                "[Control] All saved tiles deleted".to_string(),
                                            );
                                        }
                                        Err(e) => {
                                            let mut logs = tui_logs.lock().unwrap();
                                            logs.push_back(format!(
                                                "[Control] Error deleting tiles: {}",
                                                e
                                            ));
                                        }
                                    }
                                });
                            }
                            KeyCode::Up => {
                                // Increase rate limit by 0.1
                                let mut current_rate = self.rate_limit.lock().unwrap();
                                *current_rate += 0.1;
                                let new_rate = *current_rate;
                                self.add_log(format!("[Control] Rate limit increased to {:.1} tiles/sec", new_rate));
                            }
                            KeyCode::Down => {
                                // Decrease rate limit by 0.1 (minimum 0.1)
                                let mut current_rate = self.rate_limit.lock().unwrap();
                                if *current_rate > 0.1 {
                                    *current_rate -= 0.1;
                                    let new_rate = *current_rate;
                                    self.add_log(format!("[Control] Rate limit decreased to {:.1} tiles/sec", new_rate));
                                } else {
                                    self.add_log("[Control] Rate limit already at minimum (0.1 tiles/sec)".to_string());
                                }
                            }
                            KeyCode::Char('c')
                            | KeyCode::Char('C')
                            | KeyCode::Char('q')
                            | KeyCode::Char('Q') => {
                                self.add_log("[Control] Shutdown requested".to_string());
                                *self.shutdown_requested.lock().unwrap() = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Check if shutdown was requested
            if *self.shutdown_requested.lock().unwrap() {
                break;
            }
        }

        crossterm::execute!(std::io::stdout(), crossterm::cursor::Show)?;
        crossterm::terminal::disable_raw_mode()?;
        Ok(())
    }

    fn draw(
        &self,
        f: &mut Frame,
        fetch_stats: Arc<crate::fetch_worker::FetchStats>,
        match_stats: Arc<crate::match_worker::MatchStats>,
    ) {
        let size = f.size();

        // Split into header (controls) and body (logs)
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(12), Constraint::Min(0)])
            .split(size);

        // Draw header with controls
        self.draw_header(f, chunks[0], fetch_stats, match_stats);

        // Draw logs
        self.draw_logs(f, chunks[1]);
    }

    fn draw_header(
        &self,
        f: &mut Frame,
        area: Rect,
        fetch_stats: Arc<crate::fetch_worker::FetchStats>,
        match_stats: Arc<crate::match_worker::MatchStats>,
    ) {
        let fetch_running = *self.fetch_running.lock().unwrap();
        let match_running = *self.match_running.lock().unwrap();

        let fetch_status = if fetch_running { "RUNNING" } else { "STOPPED" };
        let match_status = if match_running { "RUNNING" } else { "STOPPED" };
        let fetch_symbol = if fetch_running { "[ON]" } else { "[OFF]" };
        let match_symbol = if match_running { "[ON]" } else { "[OFF]" };

        let tiles_fetched = *fetch_stats.tiles_fetched.lock().unwrap();
        let fetch_errors = *fetch_stats.errors.lock().unwrap();
        let status_200 = *fetch_stats.status_200.lock().unwrap();
        let status_404 = *fetch_stats.status_404.lock().unwrap();
        let tiles_matched = *match_stats.tiles_matched.lock().unwrap();
        let matches_found = *match_stats.matches_found.lock().unwrap();
        let match_errors = *match_stats.errors.lock().unwrap();

        let fetch_rate = fetch_stats.tiles_per_second();
        let match_rate = match_stats.tiles_per_second();
        let current_rate_limit = *self.rate_limit.lock().unwrap();

        // Get tile counts from cache
        let (indexed_tiles, processed_tiles) = {
            let cache_guard = self.cache.lock().unwrap();
            cache_guard.get_tile_counts().unwrap_or((0, 0))
        };
        const TOTAL_TILES: u64 = 2048 * 2048; // 4,194,304
        let remaining_to_index = TOTAL_TILES.saturating_sub(indexed_tiles);
        let remaining_to_process = indexed_tiles.saturating_sub(processed_tiles);

        let width = area.width as usize;
        let box_width = width.min(62);

        let header_text = vec![
            Line::from(format!("{:=<width$}", "", width = box_width)),
            Line::from(format!("{:^width$}", "CONTROLS", width = box_width)),
            Line::from(format!("{:=<width$}", "", width = box_width)),
            Line::from(format!(
                "Press 'f' - Toggle fetch workers {} {}",
                fetch_symbol, fetch_status
            )),
            Line::from(format!(
                "Press 'm' - Toggle match workers {} {}",
                match_symbol, match_status
            )),
            Line::from(format!("Press 'l' - Check tile processing status")),
            Line::from(format!(
                "Press 'x' - Clear cache | Press 'd' - Delete all tiles"
            )),
            Line::from(format!("Press '↑'/'↓' - Adjust rate limit (current: {:.1} tiles/sec)", current_rate_limit)),
            Line::from(format!("Press 'c' - Clean shutdown")),
            Line::from(format!(
                "Stats: F:{} ({:.1}/s) [{}/{}] M:{} ({:.1}/s) Matches:{} E:{}|{}",
                tiles_fetched,
                fetch_rate,
                status_200,
                status_404,
                tiles_matched,
                match_rate,
                matches_found,
                fetch_errors,
                match_errors
            )),
            Line::from(format!(
                "Indexed: {} / {} (remaining: {})",
                indexed_tiles,
                TOTAL_TILES,
                remaining_to_index
            )),
            Line::from(format!(
                "Processed: {} / {} (remaining: {})",
                processed_tiles,
                indexed_tiles,
                remaining_to_process
            )),
            Line::from(format!("{:=<width$}", "", width = box_width)),
        ];

        let header = Paragraph::new(header_text)
            .block(Block::default().borders(Borders::NONE))
            .style(Style::default().fg(Color::Cyan));
        f.render_widget(header, area);
    }

    fn draw_logs(&self, f: &mut Frame, area: Rect) {
        let logs = self.logs.lock().unwrap();
        let items: Vec<ListItem> = logs
            .iter()
            .rev()
            .take((area.height as usize).saturating_sub(2))
            .map(|log| {
                let style = if log.contains("✓") {
                    Style::default().fg(Color::Green)
                } else if log.contains("✗") || log.contains("Error") || log.contains("ERROR") {
                    Style::default().fg(Color::Red)
                } else if log.contains("[Control]") {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                };
                ListItem::new(log.as_str()).style(style)
            })
            .collect();

        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Logs"))
            .style(Style::default().fg(Color::White));
        f.render_widget(list, area);
    }
}
