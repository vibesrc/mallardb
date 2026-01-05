//! Filesystem watcher for jobs directory with debouncing.

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Events emitted when job directories change.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A new job directory was created or an existing one was modified.
    JobChanged { name: String, path: PathBuf },

    /// A job directory was deleted.
    JobRemoved { name: String },
}

/// Watches the jobs directory for changes with debouncing.
pub struct FileWatcher {
    jobs_dir: PathBuf,
    debounce_duration: Duration,
}

impl FileWatcher {
    /// Create a new file watcher.
    pub fn new(jobs_dir: PathBuf) -> Self {
        Self {
            jobs_dir,
            debounce_duration: Duration::from_secs(1),
        }
    }

    /// Create a new file watcher with custom debounce duration.
    pub fn with_debounce(jobs_dir: PathBuf, debounce_duration: Duration) -> Self {
        Self {
            jobs_dir,
            debounce_duration,
        }
    }

    /// Scan the jobs directory for existing jobs.
    ///
    /// Returns a list of (job_name, job_path) tuples for valid job directories.
    pub fn scan_jobs(&self) -> Vec<(String, PathBuf)> {
        let mut jobs = Vec::new();

        let entries = match std::fs::read_dir(&self.jobs_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to scan jobs directory: {}", e);
                return jobs;
            }
        };

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if self.is_valid_job_dir(&path)
                && let Some(name) = path.file_name()
            {
                let name = name.to_string_lossy().to_string();
                debug!("Found job directory: {}", name);
                jobs.push((name, path));
            }
        }

        info!("Scanned {} jobs from {:?}", jobs.len(), self.jobs_dir);
        jobs
    }

    /// Start watching for changes, sending events through the provided channel.
    ///
    /// This function runs indefinitely until the sender is dropped.
    pub async fn watch(&self, tx: mpsc::Sender<WatchEvent>) -> notify::Result<()> {
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();

        let mut watcher: RecommendedWatcher =
            notify::recommended_watcher(move |res: notify::Result<Event>| {
                if let Ok(event) = res {
                    let _ = notify_tx.send(event);
                }
            })?;

        watcher.watch(&self.jobs_dir, RecursiveMode::Recursive)?;
        info!("Started watching {:?}", self.jobs_dir);

        // Track pending events for debouncing
        let mut pending: HashMap<String, (WatchEvent, tokio::time::Instant)> = HashMap::new();
        let debounce = self.debounce_duration;

        loop {
            // Check for new notify events (non-blocking)
            while let Ok(event) = notify_rx.try_recv() {
                if let Some(watch_event) = self.classify_event(&event) {
                    let key = match &watch_event {
                        WatchEvent::JobChanged { name, .. } => name.clone(),
                        WatchEvent::JobRemoved { name } => name.clone(),
                    };
                    debug!("Pending event for {}: {:?}", key, watch_event);
                    pending.insert(key, (watch_event, tokio::time::Instant::now()));
                }
            }

            // Emit debounced events
            let now = tokio::time::Instant::now();
            let mut to_emit = Vec::new();

            pending.retain(|key, (event, timestamp)| {
                if now.duration_since(*timestamp) >= debounce {
                    to_emit.push((key.clone(), event.clone()));
                    false
                } else {
                    true
                }
            });

            for (name, event) in to_emit {
                debug!("Emitting debounced event for {}: {:?}", name, event);
                if tx.send(event).await.is_err() {
                    // Receiver dropped, stop watching
                    return Ok(());
                }
            }

            // Sleep briefly to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Check if a path is a valid job directory (has job.toml).
    fn is_valid_job_dir(&self, path: &Path) -> bool {
        path.is_dir() && path.join("job.toml").exists()
    }

    /// Classify a notify event into a WatchEvent.
    fn classify_event(&self, event: &Event) -> Option<WatchEvent> {
        let path = event.paths.first()?;

        // Extract job name from path (first directory component under jobs/)
        let relative = path.strip_prefix(&self.jobs_dir).ok()?;
        let job_name = relative
            .components()
            .next()?
            .as_os_str()
            .to_string_lossy()
            .to_string();

        // Skip hidden directories
        if job_name.starts_with('.') {
            return None;
        }

        let job_path = self.jobs_dir.join(&job_name);

        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                // Only emit if it's a valid job directory
                if self.is_valid_job_dir(&job_path) {
                    Some(WatchEvent::JobChanged {
                        name: job_name,
                        path: job_path,
                    })
                } else {
                    None
                }
            }
            EventKind::Remove(_) => {
                // If the directory no longer exists, it was removed
                if !job_path.exists() {
                    Some(WatchEvent::JobRemoved { name: job_name })
                } else {
                    // Maybe just a file was removed, check if still valid
                    if !self.is_valid_job_dir(&job_path) {
                        // job.toml was removed, treat as job removal
                        Some(WatchEvent::JobRemoved { name: job_name })
                    } else {
                        // Something else was removed, treat as modification
                        Some(WatchEvent::JobChanged {
                            name: job_name,
                            path: job_path,
                        })
                    }
                }
            }
            _ => None,
        }
    }
}

/// Find all SQL files in a job directory, sorted lexically.
pub fn find_sql_files(job_path: &Path) -> Vec<PathBuf> {
    let mut sql_files: Vec<PathBuf> = std::fs::read_dir(job_path)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();

    sql_files.sort();
    sql_files
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_job(dir: &TempDir, name: &str) -> PathBuf {
        let job_dir = dir.path().join(name);
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(job_dir.join("job.toml"), "schedule = \"0 * * * * *\"").unwrap();
        job_dir
    }

    #[test]
    fn test_scan_jobs() {
        let temp_dir = TempDir::new().unwrap();
        setup_test_job(&temp_dir, "job1");
        setup_test_job(&temp_dir, "job2");

        // Create an invalid directory (no job.toml)
        std::fs::create_dir_all(temp_dir.path().join("invalid")).unwrap();

        let watcher = FileWatcher::new(temp_dir.path().to_path_buf());
        let jobs = watcher.scan_jobs();

        assert_eq!(jobs.len(), 2);
        let names: Vec<&str> = jobs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"job1"));
        assert!(names.contains(&"job2"));
    }

    #[test]
    fn test_find_sql_files() {
        let temp_dir = TempDir::new().unwrap();
        let job_dir = setup_test_job(&temp_dir, "test_job");

        // Create SQL files in non-lexical order
        std::fs::write(job_dir.join("03.sql"), "SELECT 3").unwrap();
        std::fs::write(job_dir.join("01.sql"), "SELECT 1").unwrap();
        std::fs::write(job_dir.join("02.sql"), "SELECT 2").unwrap();
        std::fs::write(job_dir.join("not_sql.txt"), "not sql").unwrap();

        let sql_files = find_sql_files(&job_dir);

        assert_eq!(sql_files.len(), 3);
        assert!(sql_files[0].ends_with("01.sql"));
        assert!(sql_files[1].ends_with("02.sql"));
        assert!(sql_files[2].ends_with("03.sql"));
    }

    #[test]
    fn test_is_valid_job_dir() {
        let temp_dir = TempDir::new().unwrap();
        let valid_job = setup_test_job(&temp_dir, "valid");
        let invalid_dir = temp_dir.path().join("invalid");
        std::fs::create_dir_all(&invalid_dir).unwrap();

        let watcher = FileWatcher::new(temp_dir.path().to_path_buf());

        assert!(watcher.is_valid_job_dir(&valid_job));
        assert!(!watcher.is_valid_job_dir(&invalid_dir));
        assert!(!watcher.is_valid_job_dir(&temp_dir.path().join("nonexistent")));
    }
}
