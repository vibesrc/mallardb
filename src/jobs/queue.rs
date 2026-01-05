//! Per-job FIFO queue management for scheduled job runs.

use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A queued job run waiting to be executed.
#[derive(Debug, Clone)]
pub struct QueuedRun {
    /// Name of the job.
    pub job_name: String,

    /// When this run was queued.
    pub queued_at: DateTime<Utc>,

    /// When this run started executing (None if still waiting).
    pub started_at: Option<DateTime<Utc>>,
}

/// Manages per-job FIFO queues for scheduled runs.
#[derive(Debug, Default)]
pub struct JobQueueManager {
    /// Per-job queues.
    queues: RwLock<HashMap<String, VecDeque<QueuedRun>>>,

    /// Maximum queue size per job (None means unlimited).
    max_sizes: RwLock<HashMap<String, Option<usize>>>,
}

impl JobQueueManager {
    /// Create a new queue manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new queue manager wrapped in an Arc.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Set the maximum queue size for a job.
    pub async fn set_max_size(&self, job_name: &str, max: Option<usize>) {
        self.max_sizes
            .write()
            .await
            .insert(job_name.to_string(), max);
    }

    /// Enqueue a new run for a job. Returns false if the queue is full.
    pub async fn enqueue(&self, job_name: &str) -> bool {
        let mut queues = self.queues.write().await;
        let max_sizes = self.max_sizes.read().await;

        let queue = queues.entry(job_name.to_string()).or_default();

        // Check queue max
        if let Some(Some(max)) = max_sizes.get(job_name)
            && queue.len() >= *max
        {
            return false;
        }

        queue.push_back(QueuedRun {
            job_name: job_name.to_string(),
            queued_at: Utc::now(),
            started_at: None,
        });
        true
    }

    /// Pop the next run from a job's queue.
    pub async fn pop(&self, job_name: &str) -> Option<QueuedRun> {
        let mut queues = self.queues.write().await;
        if let Some(queue) = queues.get_mut(job_name)
            && let Some(mut run) = queue.pop_front()
        {
            run.started_at = Some(Utc::now());
            return Some(run);
        }
        None
    }

    /// Peek at the next run without removing it.
    pub async fn peek(&self, job_name: &str) -> Option<QueuedRun> {
        self.queues
            .read()
            .await
            .get(job_name)
            .and_then(|q| q.front().cloned())
    }

    /// Get the current queue depth for a job.
    pub async fn queue_depth(&self, job_name: &str) -> usize {
        self.queues
            .read()
            .await
            .get(job_name)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Get all queued runs across all jobs.
    pub async fn all_queued(&self) -> Vec<QueuedRun> {
        self.queues
            .read()
            .await
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    /// Get all queued runs for a specific job.
    pub async fn queued_for_job(&self, job_name: &str) -> Vec<QueuedRun> {
        self.queues
            .read()
            .await
            .get(job_name)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove all queued runs for a job (used when unregistering).
    pub async fn remove_job(&self, job_name: &str) {
        self.queues.write().await.remove(job_name);
        self.max_sizes.write().await.remove(job_name);
    }

    /// Clear all queues.
    pub async fn clear(&self) {
        self.queues.write().await.clear();
        self.max_sizes.write().await.clear();
    }

    /// Check if a job has any queued runs.
    pub async fn has_queued(&self, job_name: &str) -> bool {
        self.queues
            .read()
            .await
            .get(job_name)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_enqueue_and_pop() {
        let manager = JobQueueManager::new();

        assert!(manager.enqueue("test_job").await);
        assert_eq!(manager.queue_depth("test_job").await, 1);

        let run = manager.pop("test_job").await;
        assert!(run.is_some());
        assert_eq!(run.unwrap().job_name, "test_job");
        assert_eq!(manager.queue_depth("test_job").await, 0);
    }

    #[tokio::test]
    async fn test_queue_max_enforcement() {
        let manager = JobQueueManager::new();
        manager.set_max_size("test_job", Some(2)).await;

        assert!(manager.enqueue("test_job").await);
        assert!(manager.enqueue("test_job").await);
        assert!(!manager.enqueue("test_job").await); // Should fail

        assert_eq!(manager.queue_depth("test_job").await, 2);
    }

    #[tokio::test]
    async fn test_fifo_order() {
        let manager = JobQueueManager::new();

        manager.enqueue("test_job").await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        manager.enqueue("test_job").await;

        let first = manager.pop("test_job").await.unwrap();
        let second = manager.pop("test_job").await.unwrap();

        assert!(first.queued_at < second.queued_at);
    }

    #[tokio::test]
    async fn test_all_queued() {
        let manager = JobQueueManager::new();

        manager.enqueue("job1").await;
        manager.enqueue("job2").await;
        manager.enqueue("job1").await;

        let all = manager.all_queued().await;
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_remove_job() {
        let manager = JobQueueManager::new();
        manager.set_max_size("test_job", Some(5)).await;
        manager.enqueue("test_job").await;

        manager.remove_job("test_job").await;

        assert_eq!(manager.queue_depth("test_job").await, 0);
    }

    #[tokio::test]
    async fn test_unlimited_queue() {
        let manager = JobQueueManager::new();
        // No max set, should allow unlimited

        for _ in 0..100 {
            assert!(manager.enqueue("test_job").await);
        }

        assert_eq!(manager.queue_depth("test_job").await, 100);
    }

    #[tokio::test]
    async fn test_pop_sets_started_at() {
        let manager = JobQueueManager::new();
        manager.enqueue("test_job").await;

        let run = manager.pop("test_job").await.unwrap();
        assert!(run.started_at.is_some());
    }
}
