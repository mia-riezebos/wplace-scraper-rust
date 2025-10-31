use std::sync::Arc;
use std::sync::Mutex;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

pub struct Logger {
    tui_logs: Arc<Mutex<VecDeque<String>>>,
    errors_file: PathBuf,
}

impl Logger {
    pub fn new(tui_logs: Arc<Mutex<VecDeque<String>>>) -> Self {
        Self {
            tui_logs,
            errors_file: PathBuf::from("outputs/errors.txt"),
        }
    }

    fn write_error_to_file(&self, message: &str) {
        // Ensure output directory exists
        if let Some(parent) = self.errors_file.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                // Silently fail - don't want to create a loop of errors
                eprintln!("Failed to create errors directory: {}", e);
                return;
            }
        }

        // Append error to file
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.errors_file)
        {
            let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
            if let Err(_) = writeln!(file, "[{}] {}", timestamp, message) {
                // Silently fail - don't want to create a loop of errors
            }
        }
    }

    pub fn log(&self, message: String) {
        let mut logs = self.tui_logs.lock().unwrap();
        logs.push_back(message.clone());
        if logs.len() > 1000 {
            logs.pop_front();
        }
        // Don't print to stdout - TUI handles display
        
        // If message contains "Error", also write to errors.txt
        if message.contains("Error") || message.contains("ERROR") || message.contains("error") {
            self.write_error_to_file(&message);
        }
    }

    pub fn log_error(&self, message: String) {
        // Explicitly log an error - both to TUI and errors.txt
        self.log(message.clone());
        // Also write to errors.txt even if "Error" isn't in the message
        self.write_error_to_file(&message);
    }
}

