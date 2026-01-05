//! SQL job scheduler for MallardDB.
//!
//! Provides filesystem-based cron scheduling of SQL scripts with:
//! - Per-job FIFO queues with optional max size
//! - Environment variable interpolation in SQL
//! - Dynamic reloading via file watching
//! - SQL introspection via _mallardb schema

pub mod catalog;
pub mod config;
pub mod executor;
pub mod queue;
pub mod registry;
pub mod watcher;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use cron::Schedule;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::backend::DuckDbConnection;

pub use catalog::{
    detect_mallardb_table, generate_job_queue_sql, generate_job_queue_table, generate_jobs_sql,
    generate_jobs_table, MallardbTable, VirtualTableSql, CREATE_JOB_RUNS_VIEW,
};
pub use config::JobConfig;
pub use executor::{ExecutorError, JobExecutor, JobRunRecord};
pub use queue::{JobQueueManager, QueuedRun};
pub use registry::{JobMetadata, JobRegistry};
pub use watcher::{FileWatcher, WatchEvent};

/// Main coordinator for the jobs scheduler.
///
/// Manages job registration, scheduling, execution, and file watching.
pub struct JobsCoordinator {
    /// Path to the DuckDB database.
    db_path: PathBuf,

    /// Path to the jobs directory.
    jobs_dir: PathBuf,

    /// Registry of all registered jobs.
    registry: Arc<JobRegistry>,

    /// Queue manager for all jobs.
    queue_manager: Arc<JobQueueManager>,

    /// SQL executor.
    executor: Arc<JobExecutor>,

    /// Shutdown flag.
    shutdown: Arc<RwLock<bool>>,

    /// Per-job scheduler handles.
    scheduler_handles: RwLock<HashMap<String, JoinHandle<()>>>,
}

impl JobsCoordinator {
    /// Create a new jobs coordinator.
    pub fn new(db_path: PathBuf, jobs_dir: PathBuf) -> Self {
        let registry = JobRegistry::new_shared();
        let queue_manager = JobQueueManager::new_shared();
        let executor = JobExecutor::new_shared(db_path.clone());

        Self {
            db_path,
            jobs_dir,
            registry,
            queue_manager,
            executor,
            shutdown: Arc::new(RwLock::new(false)),
            scheduler_handles: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new coordinator wrapped in an Arc.
    pub fn new_shared(db_path: PathBuf, jobs_dir: PathBuf) -> Arc<Self> {
        Arc::new(Self::new(db_path, jobs_dir))
    }

    /// Start the jobs coordinator.
    ///
    /// This will:
    /// 1. Create placeholder views for virtual tables
    /// 2. Scan and register existing jobs
    /// 3. Start the file watcher for dynamic reloading
    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        // Check if jobs directory exists
        if !self.jobs_dir.exists() {
            info!(
                "Jobs directory {:?} does not exist, job scheduler disabled",
                self.jobs_dir
            );
            return Ok(());
        }

        // Initialize the job runs table
        self.init_job_runs_table()
            .map_err(|e| format!("Failed to initialize job_runs table: {}", e))?;

        // Initial scan of jobs directory
        let file_watcher = FileWatcher::new(self.jobs_dir.clone());
        for (name, path) in file_watcher.scan_jobs() {
            if let Err(e) = self.register_job(&name, &path).await {
                warn!("Failed to register job {}: {}", name, e);
            }
        }

        // Start file watcher for dynamic reloading
        let (tx, rx) = mpsc::channel(100);
        let coordinator = Arc::clone(self);

        // Spawn watcher task
        let jobs_dir = self.jobs_dir.clone();
        tokio::spawn(async move {
            let watcher = FileWatcher::new(jobs_dir);
            if let Err(e) = watcher.watch(tx).await {
                error!("File watcher error: {}", e);
            }
        });

        // Spawn event handler task
        tokio::spawn(async move {
            coordinator.handle_watch_events(rx).await;
        });

        info!(
            "Jobs coordinator started, watching {:?}",
            self.jobs_dir
        );
        Ok(())
    }

    /// Initialize placeholder views for virtual tables.
    /// The actual _mallardb.job_runs table is created by the Backend.
    fn init_job_runs_table(&self) -> Result<(), String> {
        let mut conn =
            DuckDbConnection::new(&self.db_path).map_err(|e| format!("Connection error: {}", e))?;

        // Create placeholder views for virtual tables (for schema introspection)
        for stmt in CREATE_JOB_RUNS_VIEW.split(';').filter(|s| !s.trim().is_empty()) {
            if let Err(e) = conn.execute(stmt) {
                debug!("Note: Could not create view (may already exist): {}", e);
            }
        }

        info!("Initialized _mallardb virtual table views");
        Ok(())
    }

    /// Handle file watcher events.
    async fn handle_watch_events(self: Arc<Self>, mut rx: mpsc::Receiver<WatchEvent>) {
        while let Some(event) = rx.recv().await {
            if *self.shutdown.read().await {
                break;
            }

            match event {
                WatchEvent::JobChanged { name, path } => {
                    info!("Job changed: {}", name);
                    if let Err(e) = self.register_job(&name, &path).await {
                        warn!("Failed to register/update job {}: {}", name, e);
                    }
                }
                WatchEvent::JobRemoved { name } => {
                    info!("Job removed: {}", name);
                    self.unregister_job(&name).await;
                }
            }
        }
    }

    /// Register or update a job.
    async fn register_job(&self, name: &str, path: &Path) -> Result<(), String> {
        // Parse config
        let config_path = path.join("job.toml");
        let config = JobConfig::from_file(&config_path)
            .map_err(|e| format!("Invalid job.toml: {}", e))?;

        // Parse schedule
        let schedule = Schedule::from_str(&config.schedule)
            .map_err(|e| format!("Invalid cron expression: {}", e))?;

        // Find SQL files
        let sql_files = watcher::find_sql_files(path);
        if sql_files.is_empty() {
            return Err(format!("Job {} has no SQL files", name));
        }

        // Set queue max
        self.queue_manager
            .set_max_size(name, config.queue_max)
            .await;

        let metadata = JobMetadata {
            name: name.to_string(),
            path: path.to_path_buf(),
            config: config.clone(),
            schedule: schedule.clone(),
            sql_files,
            running: false,
            queue_depth: 0,
        };

        // Cancel existing scheduler if updating
        self.cancel_scheduler(name).await;

        // Register in registry
        self.registry.register(name.to_string(), metadata).await;

        // Start scheduler if enabled
        if config.enabled {
            self.start_job_scheduler(name.to_string(), schedule, config.timeout_duration())
                .await;
        }

        info!(
            "Registered job: {} (schedule: {}, enabled: {})",
            name, config.schedule, config.enabled
        );
        Ok(())
    }

    /// Unregister a job.
    async fn unregister_job(&self, name: &str) {
        self.cancel_scheduler(name).await;
        self.registry.unregister(name).await;
        self.queue_manager.remove_job(name).await;
        info!("Unregistered job: {}", name);
    }

    /// Cancel a job's scheduler.
    async fn cancel_scheduler(&self, name: &str) {
        if let Some(handle) = self.scheduler_handles.write().await.remove(name) {
            handle.abort();
            debug!("Cancelled scheduler for job: {}", name);
        }
    }

    /// Start the scheduler for a job.
    async fn start_job_scheduler(
        &self,
        job_name: String,
        schedule: Schedule,
        timeout: Option<Duration>,
    ) {
        let registry = Arc::clone(&self.registry);
        let queue_manager = Arc::clone(&self.queue_manager);
        let executor = Arc::clone(&self.executor);
        let db_path = self.db_path.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let job_name_clone = job_name.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Find next scheduled time
                let next = schedule.upcoming(chrono::Utc).next();
                let Some(next_time) = next else {
                    warn!("No more scheduled times for job: {}", job_name_clone);
                    break;
                };

                // Calculate delay
                let now = chrono::Utc::now();
                let delay = (next_time - now).to_std().unwrap_or(Duration::ZERO);

                debug!(
                    "Job {} next run at {} (in {:?})",
                    job_name_clone, next_time, delay
                );

                tokio::time::sleep(delay).await;

                if *shutdown.read().await {
                    break;
                }

                // Enqueue run
                if queue_manager.enqueue(&job_name_clone).await {
                    let depth = queue_manager.queue_depth(&job_name_clone).await;
                    registry.update_queue_depth(&job_name_clone, depth).await;

                    // Execute if not already running
                    if !registry.is_running(&job_name_clone).await {
                        Self::execute_queued_runs(
                            job_name_clone.clone(),
                            registry.clone(),
                            queue_manager.clone(),
                            executor.clone(),
                            db_path.clone(),
                            timeout,
                        )
                        .await;
                    }
                } else {
                    warn!(
                        "Job {} queue full, dropping scheduled run",
                        job_name_clone
                    );
                }
            }
        });

        self.scheduler_handles
            .write()
            .await
            .insert(job_name, handle);
    }

    /// Execute queued runs for a job.
    async fn execute_queued_runs(
        job_name: String,
        registry: Arc<JobRegistry>,
        queue_manager: Arc<JobQueueManager>,
        executor: Arc<JobExecutor>,
        db_path: PathBuf,
        timeout: Option<Duration>,
    ) {
        while let Some(_run) = queue_manager.pop(&job_name).await {
            // Mark as running
            registry.set_running(&job_name, true).await;
            let depth = queue_manager.queue_depth(&job_name).await;
            registry.update_queue_depth(&job_name, depth).await;

            // Get job metadata
            let Some(job) = registry.get(&job_name).await else {
                error!("Job {} not found in registry", job_name);
                break;
            };

            // Execute
            match executor.execute_job(&job_name, &job.sql_files, timeout).await {
                Ok(records) => {
                    // Log to job_runs table
                    if let Err(e) = Self::log_run_records(&db_path, &records) {
                        error!("Failed to log job run records: {}", e);
                    }
                }
                Err(e) => {
                    error!("Job {} execution failed: {}", job_name, e);
                }
            }

            // Mark as not running
            registry.set_running(&job_name, false).await;
        }
    }

    /// Log execution records to the database.
    fn log_run_records(db_path: &Path, records: &[JobRunRecord]) -> Result<(), String> {
        debug!("Logging {} job run records to {:?}", records.len(), db_path);
        let db_path_buf = db_path.to_path_buf();
        let mut conn =
            DuckDbConnection::new(&db_path_buf).map_err(|e| format!("Connection error: {}", e))?;

        let executor = JobExecutor::new(db_path_buf);
        executor
            .log_records(&mut conn, records)
            .map_err(|e| format!("Failed to log records: {}", e))
    }

    /// Shutdown the coordinator gracefully.
    pub async fn shutdown(&self) {
        info!("Shutting down jobs coordinator");
        *self.shutdown.write().await = true;

        // Cancel all schedulers
        let mut handles = self.scheduler_handles.write().await;
        for (name, handle) in handles.drain() {
            debug!("Cancelling scheduler for job: {}", name);
            handle.abort();
        }

        info!("Jobs coordinator shutdown complete");
    }

    // Public accessors for catalog queries

    /// Get the job registry.
    pub fn registry(&self) -> &Arc<JobRegistry> {
        &self.registry
    }

    /// Get the queue manager.
    pub fn queue_manager(&self) -> &Arc<JobQueueManager> {
        &self.queue_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_env() -> (TempDir, PathBuf, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let jobs_dir = temp_dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();
        (temp_dir, db_path, jobs_dir)
    }

    fn create_test_job(jobs_dir: &Path, name: &str, schedule: &str) {
        let job_dir = jobs_dir.join(name);
        std::fs::create_dir_all(&job_dir).unwrap();
        std::fs::write(
            job_dir.join("job.toml"),
            format!("schedule = \"{}\"", schedule),
        )
        .unwrap();
        std::fs::write(job_dir.join("01.sql"), "SELECT 1").unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_creation() {
        let (_temp, db_path, jobs_dir) = setup_test_env();
        let coordinator = JobsCoordinator::new(db_path, jobs_dir);

        assert!(coordinator.registry.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_register_job() {
        let (_temp, db_path, jobs_dir) = setup_test_env();
        create_test_job(&jobs_dir, "test_job", "0 * * * * *");

        let coordinator = JobsCoordinator::new(db_path, jobs_dir);
        let job_path = coordinator.jobs_dir.join("test_job");

        coordinator
            .register_job("test_job", &job_path)
            .await
            .unwrap();

        let job = coordinator.registry.get("test_job").await;
        assert!(job.is_some());
        assert_eq!(job.unwrap().name, "test_job");
    }

    #[tokio::test]
    async fn test_unregister_job() {
        let (_temp, db_path, jobs_dir) = setup_test_env();
        create_test_job(&jobs_dir, "test_job", "0 * * * * *");

        let coordinator = JobsCoordinator::new(db_path, jobs_dir);
        let job_path = coordinator.jobs_dir.join("test_job");

        coordinator
            .register_job("test_job", &job_path)
            .await
            .unwrap();
        coordinator.unregister_job("test_job").await;

        assert!(coordinator.registry.get("test_job").await.is_none());
    }

    #[tokio::test]
    async fn test_start_with_existing_jobs() {
        let (_temp, db_path, jobs_dir) = setup_test_env();
        create_test_job(&jobs_dir, "job1", "0 * * * * *");
        create_test_job(&jobs_dir, "job2", "0 */5 * * * *");

        let coordinator = JobsCoordinator::new_shared(db_path, jobs_dir);
        coordinator.start().await.unwrap();

        // Give it a moment to scan
        tokio::time::sleep(Duration::from_millis(100)).await;

        let jobs = coordinator.registry.list().await;
        assert_eq!(jobs.len(), 2);

        coordinator.shutdown().await;
    }

    #[tokio::test]
    async fn test_start_without_jobs_dir() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let jobs_dir = temp_dir.path().join("nonexistent");

        let coordinator = JobsCoordinator::new_shared(db_path, jobs_dir);
        // Should not error, just log and return
        coordinator.start().await.unwrap();
    }
}
