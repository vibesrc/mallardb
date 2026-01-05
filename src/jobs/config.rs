//! Job configuration parsing from job.toml files.

use cron::Schedule;
use serde::Deserialize;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

/// Errors that can occur when parsing job configuration.
#[derive(Error, Debug)]
pub enum JobConfigError {
    #[error("Failed to read job.toml: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse job.toml: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),

    #[error("Invalid timeout format: {0}")]
    InvalidTimeout(String),
}

/// Configuration for a single job, parsed from job.toml.
#[derive(Debug, Clone, Deserialize)]
pub struct JobConfig {
    /// Cron expression for scheduling (required).
    pub schedule: String,

    /// Whether the job is enabled (default: true).
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum number of queued runs (optional).
    pub queue_max: Option<usize>,

    /// Job timeout as a human-readable string like "30m" or "1h" (optional).
    pub timeout: Option<String>,
}

fn default_true() -> bool {
    true
}

impl JobConfig {
    /// Parse a job configuration from a file path.
    pub fn from_file(path: &Path) -> Result<Self, JobConfigError> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse a job configuration from a TOML string.
    pub fn parse(content: &str) -> Result<Self, JobConfigError> {
        let config: JobConfig = toml::from_str(content)?;

        // Validate cron expression
        Schedule::from_str(&config.schedule)
            .map_err(|e| JobConfigError::InvalidCron(e.to_string()))?;

        // Validate timeout if present
        if let Some(ref timeout) = config.timeout {
            humantime::parse_duration(timeout)
                .map_err(|e| JobConfigError::InvalidTimeout(e.to_string()))?;
        }

        Ok(config)
    }

    /// Parse the cron schedule.
    pub fn parse_schedule(&self) -> Result<Schedule, JobConfigError> {
        Schedule::from_str(&self.schedule).map_err(|e| JobConfigError::InvalidCron(e.to_string()))
    }

    /// Get the timeout as a Duration, if configured.
    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
            .as_ref()
            .and_then(|s| humantime::parse_duration(s).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"schedule = "0 */15 * * * *""#;
        let config = JobConfig::parse(toml).unwrap();
        assert_eq!(config.schedule, "0 */15 * * * *");
        assert!(config.enabled);
        assert!(config.queue_max.is_none());
        assert!(config.timeout.is_none());
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
schedule = "0 0 * * * *"
enabled = false
queue_max = 3
timeout = "30m"
"#;
        let config = JobConfig::parse(toml).unwrap();
        assert_eq!(config.schedule, "0 0 * * * *");
        assert!(!config.enabled);
        assert_eq!(config.queue_max, Some(3));
        assert_eq!(config.timeout, Some("30m".to_string()));
        assert_eq!(
            config.timeout_duration(),
            Some(Duration::from_secs(30 * 60))
        );
    }

    #[test]
    fn test_invalid_cron() {
        let toml = r#"schedule = "invalid cron""#;
        let result = JobConfig::parse(toml);
        assert!(matches!(result, Err(JobConfigError::InvalidCron(_))));
    }

    #[test]
    fn test_invalid_timeout() {
        let toml = r#"
schedule = "0 0 * * * *"
timeout = "invalid"
"#;
        let result = JobConfig::parse(toml);
        assert!(matches!(result, Err(JobConfigError::InvalidTimeout(_))));
    }

    #[test]
    fn test_missing_schedule() {
        let toml = r#"enabled = true"#;
        let result = JobConfig::parse(toml);
        assert!(matches!(result, Err(JobConfigError::Parse(_))));
    }
}
