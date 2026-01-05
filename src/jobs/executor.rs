//! Job executor for running SQL files with environment variable interpolation.

use chrono::{DateTime, Utc};
use regex::Regex;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, trace};
use uuid::Uuid;

use crate::backend::DuckDbConnection;

/// Errors that can occur during job execution.
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Failed to read SQL file {path}: {reason}")]
    FileRead { path: String, reason: String },

    #[error("Missing environment variable: ${name}")]
    MissingEnvVar { name: String },

    #[error("SQL execution failed: {0}")]
    SqlError(String),

    #[error("Job timed out after {0:?}")]
    Timeout(Duration),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Record of a job or file execution for logging to _mallardb.job_runs.
#[derive(Debug, Clone)]
pub struct JobRunRecord {
    /// Unique identifier for this job run (shared across job and file records).
    pub run_id: String,

    /// Name of the job.
    pub job_name: String,

    /// Record type: "job" for overall job summary, "file" for individual file execution.
    pub record_type: String,

    /// Name of the SQL file (None for job-level record).
    pub file_name: Option<String>,

    /// When execution started.
    pub started_at: DateTime<Utc>,

    /// When execution finished.
    pub finished_at: DateTime<Utc>,

    /// Status: "running", "success", or "failed".
    pub status: String,

    /// Error message if failed.
    pub error_message: Option<String>,
}

/// Executes jobs by running their SQL files in order.
pub struct JobExecutor {
    db_path: PathBuf,
}

impl JobExecutor {
    /// Create a new executor.
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
    }

    /// Create a new executor wrapped in an Arc.
    pub fn new_shared(db_path: PathBuf) -> Arc<Self> {
        Arc::new(Self::new(db_path))
    }

    /// Execute all SQL files for a job.
    ///
    /// Returns records of each file execution plus the overall job execution.
    /// On first failure, stops and returns records up to that point.
    pub async fn execute_job(
        &self,
        job_name: &str,
        sql_files: &[PathBuf],
        timeout: Option<Duration>,
    ) -> Result<Vec<JobRunRecord>, ExecutorError> {
        let mut records = Vec::new();
        let run_id = Uuid::new_v4().to_string();
        let job_start = Utc::now();

        info!(
            "Starting job: {} (run_id: {}, {} files)",
            job_name,
            run_id,
            sql_files.len()
        );

        // Load all SQL files at job start (RFC requirement: mid-run edits don't affect current execution)
        let sql_contents = self.load_sql_files(sql_files)?;

        // Interpolate environment variables
        let interpolated = self.interpolate_all(&sql_contents)?;

        // Create connection for this job execution
        let mut conn = DuckDbConnection::new(&self.db_path)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?;

        // Execute each SQL file in its own transaction
        for (file_name, sql) in interpolated {
            let file_start = Utc::now();
            trace!("Executing file: {} for job: {}", file_name, job_name);

            match self.execute_file(&mut conn, &sql, timeout).await {
                Ok(()) => {
                    let finished = Utc::now();
                    info!(
                        "File {} completed in {:?}",
                        file_name,
                        finished - file_start
                    );
                    records.push(JobRunRecord {
                        run_id: run_id.clone(),
                        job_name: job_name.to_string(),
                        record_type: "file".to_string(),
                        file_name: Some(file_name),
                        started_at: file_start,
                        finished_at: finished,
                        status: "success".to_string(),
                        error_message: None,
                    });
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!("Job {} failed on {}: {}", job_name, file_name, error_msg);

                    records.push(JobRunRecord {
                        run_id: run_id.clone(),
                        job_name: job_name.to_string(),
                        record_type: "file".to_string(),
                        file_name: Some(file_name),
                        started_at: file_start,
                        finished_at: Utc::now(),
                        status: "failed".to_string(),
                        error_message: Some(error_msg.clone()),
                    });

                    // Add job-level failure record
                    records.insert(
                        0,
                        JobRunRecord {
                            run_id: run_id.clone(),
                            job_name: job_name.to_string(),
                            record_type: "job".to_string(),
                            file_name: None,
                            started_at: job_start,
                            finished_at: Utc::now(),
                            status: "failed".to_string(),
                            error_message: Some(error_msg),
                        },
                    );

                    return Ok(records);
                }
            }
        }

        // Add job-level success record at the beginning
        let job_finished = Utc::now();
        info!(
            "Job {} (run_id: {}) completed successfully in {:?}",
            job_name,
            run_id,
            job_finished - job_start
        );
        records.insert(
            0,
            JobRunRecord {
                run_id,
                job_name: job_name.to_string(),
                record_type: "job".to_string(),
                file_name: None,
                started_at: job_start,
                finished_at: job_finished,
                status: "success".to_string(),
                error_message: None,
            },
        );

        Ok(records)
    }

    /// Load SQL files from disk.
    fn load_sql_files(
        &self,
        sql_files: &[PathBuf],
    ) -> Result<Vec<(String, String)>, ExecutorError> {
        sql_files
            .iter()
            .map(|path| {
                let content =
                    std::fs::read_to_string(path).map_err(|e| ExecutorError::FileRead {
                        path: path.display().to_string(),
                        reason: e.to_string(),
                    })?;
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.display().to_string());
                Ok((name, content))
            })
            .collect()
    }

    /// Interpolate environment variables in all SQL files.
    fn interpolate_all(
        &self,
        sql_contents: &[(String, String)],
    ) -> Result<Vec<(String, String)>, ExecutorError> {
        sql_contents
            .iter()
            .map(|(name, content)| {
                let interpolated = interpolate_env_vars(content)?;
                Ok((name.clone(), interpolated))
            })
            .collect()
    }

    /// Execute a single SQL file within a transaction.
    async fn execute_file(
        &self,
        conn: &mut DuckDbConnection,
        sql: &str,
        timeout: Option<Duration>,
    ) -> Result<(), ExecutorError> {
        // Each file runs in its own transaction
        conn.execute("BEGIN")
            .map_err(|e| ExecutorError::SqlError(e.to_string()))?;

        let result = if let Some(duration) = timeout {
            // Execute with timeout
            match tokio::time::timeout(duration, async { conn.execute(sql) }).await {
                Ok(result) => result,
                Err(_) => {
                    // Timeout - rollback and return error
                    let _ = conn.execute("ROLLBACK");
                    return Err(ExecutorError::Timeout(duration));
                }
            }
        } else {
            conn.execute(sql)
        };

        match result {
            Ok(_) => {
                conn.execute("COMMIT")
                    .map_err(|e| ExecutorError::SqlError(e.to_string()))?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK");
                Err(ExecutorError::SqlError(e.to_string()))
            }
        }
    }

    /// Log execution records to the _mallardb.job_runs table.
    pub fn log_records(
        &self,
        conn: &mut DuckDbConnection,
        records: &[JobRunRecord],
    ) -> Result<(), ExecutorError> {
        for record in records {
            let error_value = record
                .error_message
                .as_ref()
                .map(|s| format!("'{}'", s.replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string());
            let file_value = record
                .file_name
                .as_ref()
                .map(|s| format!("'{}'", s.replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string());

            let sql = format!(
                "INSERT INTO _mallardb.job_runs (run_id, job_name, record_type, file_name, started_at, finished_at, status, error_message) \
                 VALUES ('{}', '{}', '{}', {}, '{}', '{}', '{}', {})",
                record.run_id,
                record.job_name.replace('\'', "''"),
                record.record_type,
                file_value,
                record.started_at.format("%Y-%m-%d %H:%M:%S%.6f"),
                record.finished_at.format("%Y-%m-%d %H:%M:%S%.6f"),
                record.status,
                error_value
            );

            conn.execute(&sql)
                .map_err(|e| ExecutorError::SqlError(e.to_string()))?;
        }

        Ok(())
    }
}

/// Interpolate ${VAR} patterns with environment variables.
///
/// Returns an error if any referenced variable is not set.
pub fn interpolate_env_vars(sql: &str) -> Result<String, ExecutorError> {
    let re = Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)\}").unwrap();
    let mut result = sql.to_string();
    let mut missing = Vec::new();

    // Find all variable references first
    for cap in re.captures_iter(sql) {
        let var_name = &cap[1];
        match std::env::var(var_name) {
            Ok(value) => {
                result = result.replace(&cap[0], &value);
            }
            Err(_) => {
                if !missing.contains(&var_name.to_string()) {
                    missing.push(var_name.to_string());
                }
            }
        }
    }

    if !missing.is_empty() {
        return Err(ExecutorError::MissingEnvVar {
            name: missing.join(", "),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpolate_single_var() {
        // SAFETY: Test runs in isolation
        unsafe {
            std::env::set_var("TEST_VAR_1", "hello");
        }
        let result = interpolate_env_vars("SELECT '${TEST_VAR_1}'").unwrap();
        assert_eq!(result, "SELECT 'hello'");
        unsafe {
            std::env::remove_var("TEST_VAR_1");
        }
    }

    #[test]
    fn test_interpolate_multiple_vars() {
        // SAFETY: Test runs in isolation
        unsafe {
            std::env::set_var("TEST_VAR_A", "foo");
            std::env::set_var("TEST_VAR_B", "bar");
        }
        let result = interpolate_env_vars("${TEST_VAR_A} and ${TEST_VAR_B}").unwrap();
        assert_eq!(result, "foo and bar");
        unsafe {
            std::env::remove_var("TEST_VAR_A");
            std::env::remove_var("TEST_VAR_B");
        }
    }

    #[test]
    fn test_interpolate_missing_var() {
        let result = interpolate_env_vars("${DEFINITELY_NOT_SET_12345}");
        assert!(matches!(result, Err(ExecutorError::MissingEnvVar { .. })));
    }

    #[test]
    fn test_interpolate_no_vars() {
        let sql = "SELECT * FROM users";
        let result = interpolate_env_vars(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_interpolate_same_var_twice() {
        // SAFETY: Test runs in isolation
        unsafe {
            std::env::set_var("TEST_DOUBLE", "value");
        }
        let result = interpolate_env_vars("${TEST_DOUBLE} ${TEST_DOUBLE}").unwrap();
        assert_eq!(result, "value value");
        unsafe {
            std::env::remove_var("TEST_DOUBLE");
        }
    }

    #[test]
    fn test_interpolate_realistic_sql() {
        // SAFETY: Test runs in isolation
        unsafe {
            std::env::set_var("PG_CONNSTR", "postgresql://localhost/db");
            std::env::set_var("S3_BUCKET", "s3://my-bucket");
        }
        let sql = r#"
ATTACH '${PG_CONNSTR}' AS source (TYPE postgres);
COPY source.public.orders TO '${S3_BUCKET}/orders.parquet';
"#;
        let result = interpolate_env_vars(sql).unwrap();
        assert!(result.contains("postgresql://localhost/db"));
        assert!(result.contains("s3://my-bucket"));
        unsafe {
            std::env::remove_var("PG_CONNSTR");
            std::env::remove_var("S3_BUCKET");
        }
    }

    use tempfile::TempDir;

    fn setup_test_db() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        (temp_dir, db_path)
    }

    #[tokio::test]
    async fn test_execute_job_creates_job_level_record() {
        let (_temp, db_path) = setup_test_db();
        let executor = JobExecutor::new(db_path);

        // Create a temp SQL file
        let sql_dir = _temp.path().join("test_job");
        std::fs::create_dir_all(&sql_dir).unwrap();
        let sql_file = sql_dir.join("01.sql");
        std::fs::write(&sql_file, "SELECT 1").unwrap();

        let sql_files = vec![sql_file];
        let records = executor
            .execute_job("test_job", &sql_files, None)
            .await
            .unwrap();

        // Should have 2 records: 1 job-level + 1 file-level
        assert_eq!(records.len(), 2);

        // First record should be job-level
        let job_record = &records[0];
        assert_eq!(job_record.job_name, "test_job");
        assert_eq!(job_record.record_type, "job");
        assert!(job_record.file_name.is_none());
        assert_eq!(job_record.status, "success");
    }

    #[tokio::test]
    async fn test_execute_job_creates_file_level_records() {
        let (_temp, db_path) = setup_test_db();
        let executor = JobExecutor::new(db_path);

        // Create temp SQL files
        let sql_dir = _temp.path().join("test_job");
        std::fs::create_dir_all(&sql_dir).unwrap();
        let sql_file1 = sql_dir.join("01_first.sql");
        let sql_file2 = sql_dir.join("02_second.sql");
        std::fs::write(&sql_file1, "SELECT 1").unwrap();
        std::fs::write(&sql_file2, "SELECT 2").unwrap();

        let sql_files = vec![sql_file1, sql_file2];
        let records = executor
            .execute_job("test_job", &sql_files, None)
            .await
            .unwrap();

        // Should have 3 records: 1 job-level + 2 file-level
        assert_eq!(records.len(), 3);

        // First record is job-level
        assert_eq!(records[0].record_type, "job");

        // Remaining records are file-level with file names
        let file_record1 = &records[1];
        assert_eq!(file_record1.job_name, "test_job");
        assert_eq!(file_record1.record_type, "file");
        assert_eq!(file_record1.file_name.as_deref(), Some("01_first.sql"));
        assert_eq!(file_record1.status, "success");

        let file_record2 = &records[2];
        assert_eq!(file_record2.job_name, "test_job");
        assert_eq!(file_record2.record_type, "file");
        assert_eq!(file_record2.file_name.as_deref(), Some("02_second.sql"));
        assert_eq!(file_record2.status, "success");
    }

    #[tokio::test]
    async fn test_execute_job_failure_creates_both_record_types() {
        let (_temp, db_path) = setup_test_db();
        let executor = JobExecutor::new(db_path);

        // Create SQL files - second one will fail
        let sql_dir = _temp.path().join("test_job");
        std::fs::create_dir_all(&sql_dir).unwrap();
        let sql_file1 = sql_dir.join("01_good.sql");
        let sql_file2 = sql_dir.join("02_bad.sql");
        std::fs::write(&sql_file1, "SELECT 1").unwrap();
        std::fs::write(&sql_file2, "INVALID SQL SYNTAX HERE").unwrap();

        let sql_files = vec![sql_file1, sql_file2];
        let records = executor
            .execute_job("test_job", &sql_files, None)
            .await
            .unwrap();

        // Should have 3 records: 1 job-level (failed) + 1 success file + 1 failed file
        assert_eq!(records.len(), 3);

        // Job-level record should be first and show failure
        let job_record = &records[0];
        assert_eq!(job_record.record_type, "job");
        assert_eq!(job_record.status, "failed");
        assert!(job_record.error_message.is_some());

        // First file succeeded
        let file_record1 = &records[1];
        assert_eq!(file_record1.record_type, "file");
        assert_eq!(file_record1.file_name.as_deref(), Some("01_good.sql"));
        assert_eq!(file_record1.status, "success");

        // Second file failed
        let file_record2 = &records[2];
        assert_eq!(file_record2.record_type, "file");
        assert_eq!(file_record2.file_name.as_deref(), Some("02_bad.sql"));
        assert_eq!(file_record2.status, "failed");
        assert!(file_record2.error_message.is_some());
    }

    #[tokio::test]
    async fn test_run_id_groups_job_and_file_records() {
        // This test verifies that run_id correctly groups job and file records
        let (_temp, db_path) = setup_test_db();
        let executor = JobExecutor::new(db_path);

        let sql_dir = _temp.path().join("test_job");
        std::fs::create_dir_all(&sql_dir).unwrap();
        let sql_file1 = sql_dir.join("01.sql");
        let sql_file2 = sql_dir.join("02.sql");
        std::fs::write(&sql_file1, "SELECT 1").unwrap();
        std::fs::write(&sql_file2, "SELECT 2").unwrap();

        let records = executor
            .execute_job("test_job", &[sql_file1, sql_file2], None)
            .await
            .unwrap();

        // All records should have the same run_id
        let run_id = &records[0].run_id;
        assert!(!run_id.is_empty(), "run_id should not be empty");
        for record in &records {
            assert_eq!(
                &record.run_id, run_id,
                "All records should share the same run_id"
            );
        }

        // Query patterns users would use:
        // SELECT * FROM _mallardb.job_runs WHERE record_type = 'job'
        let job_level: Vec<_> = records.iter().filter(|r| r.record_type == "job").collect();
        assert_eq!(
            job_level.len(),
            1,
            "Should have exactly one job-level record"
        );

        // SELECT * FROM _mallardb.job_runs WHERE record_type = 'file'
        let file_level: Vec<_> = records.iter().filter(|r| r.record_type == "file").collect();
        assert_eq!(file_level.len(), 2, "Should have two file-level records");

        // SELECT * FROM _mallardb.job_runs WHERE run_id = '...'
        // Groups all records from a single execution
        let by_run_id: Vec<_> = records.iter().filter(|r| r.run_id == *run_id).collect();
        assert_eq!(
            by_run_id.len(),
            3,
            "run_id should group all records from one execution"
        );
    }
}
