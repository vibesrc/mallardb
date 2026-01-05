//! In-memory job registry for tracking registered jobs and their state.

use cron::Schedule;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::config::JobConfig;

/// Metadata for a registered job.
#[derive(Debug, Clone)]
pub struct JobMetadata {
    /// Job name (directory name).
    pub name: String,

    /// Path to the job directory.
    pub path: PathBuf,

    /// Parsed job configuration.
    pub config: JobConfig,

    /// Parsed cron schedule.
    pub schedule: Schedule,

    /// SQL files to execute, sorted lexically.
    pub sql_files: Vec<PathBuf>,

    /// Whether the job is currently running.
    pub running: bool,

    /// Current queue depth.
    pub queue_depth: usize,
}

/// Thread-safe registry of all registered jobs.
#[derive(Debug, Default)]
pub struct JobRegistry {
    jobs: RwLock<HashMap<String, JobMetadata>>,
}

impl JobRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new registry wrapped in an Arc.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Register or update a job.
    pub async fn register(&self, name: String, metadata: JobMetadata) {
        self.jobs.write().await.insert(name, metadata);
    }

    /// Unregister a job by name, returning the removed metadata if it existed.
    pub async fn unregister(&self, name: &str) -> Option<JobMetadata> {
        self.jobs.write().await.remove(name)
    }

    /// Get a job by name.
    pub async fn get(&self, name: &str) -> Option<JobMetadata> {
        self.jobs.read().await.get(name).cloned()
    }

    /// List all registered jobs.
    pub async fn list(&self) -> Vec<JobMetadata> {
        self.jobs.read().await.values().cloned().collect()
    }

    /// Get the names of all registered jobs.
    pub async fn names(&self) -> Vec<String> {
        self.jobs.read().await.keys().cloned().collect()
    }

    /// Check if a job exists.
    pub async fn exists(&self, name: &str) -> bool {
        self.jobs.read().await.contains_key(name)
    }

    /// Set the running state of a job.
    pub async fn set_running(&self, name: &str, running: bool) {
        if let Some(job) = self.jobs.write().await.get_mut(name) {
            job.running = running;
        }
    }

    /// Update the queue depth of a job.
    pub async fn update_queue_depth(&self, name: &str, depth: usize) {
        if let Some(job) = self.jobs.write().await.get_mut(name) {
            job.queue_depth = depth;
        }
    }

    /// Get the current running state of a job.
    pub async fn is_running(&self, name: &str) -> bool {
        self.jobs
            .read()
            .await
            .get(name)
            .map(|j| j.running)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn create_test_metadata(name: &str) -> JobMetadata {
        JobMetadata {
            name: name.to_string(),
            path: PathBuf::from(format!("/jobs/{}", name)),
            config: JobConfig {
                schedule: "0 * * * * *".to_string(),
                enabled: true,
                queue_max: None,
                timeout: None,
            },
            schedule: Schedule::from_str("0 * * * * *").unwrap(),
            sql_files: vec![PathBuf::from("01.sql")],
            running: false,
            queue_depth: 0,
        }
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let registry = JobRegistry::new();
        let metadata = create_test_metadata("test_job");

        registry.register("test_job".to_string(), metadata).await;

        let retrieved = registry.get("test_job").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "test_job");
    }

    #[tokio::test]
    async fn test_unregister() {
        let registry = JobRegistry::new();
        let metadata = create_test_metadata("test_job");

        registry.register("test_job".to_string(), metadata).await;
        let removed = registry.unregister("test_job").await;

        assert!(removed.is_some());
        assert!(registry.get("test_job").await.is_none());
    }

    #[tokio::test]
    async fn test_list_jobs() {
        let registry = JobRegistry::new();
        registry
            .register("job1".to_string(), create_test_metadata("job1"))
            .await;
        registry
            .register("job2".to_string(), create_test_metadata("job2"))
            .await;

        let jobs = registry.list().await;
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn test_set_running() {
        let registry = JobRegistry::new();
        registry
            .register("test_job".to_string(), create_test_metadata("test_job"))
            .await;

        assert!(!registry.is_running("test_job").await);

        registry.set_running("test_job", true).await;
        assert!(registry.is_running("test_job").await);

        registry.set_running("test_job", false).await;
        assert!(!registry.is_running("test_job").await);
    }

    #[tokio::test]
    async fn test_update_queue_depth() {
        let registry = JobRegistry::new();
        registry
            .register("test_job".to_string(), create_test_metadata("test_job"))
            .await;

        registry.update_queue_depth("test_job", 5).await;

        let job = registry.get("test_job").await.unwrap();
        assert_eq!(job.queue_depth, 5);
    }
}
