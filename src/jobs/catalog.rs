//! Virtual table generation for _mallardb schema introspection.

use crate::backend::{ColumnInfo, QueryOutput, Value};
use crate::sql_rewriter::TableRef;

use super::queue::JobQueueManager;
use super::registry::JobRegistry;

/// Virtual table data for SQL execution.
/// Contains the CREATE TEMP TABLE statement and INSERT statements.
pub struct VirtualTableSql {
    /// Name of the temp table (e.g., "_vt_jobs")
    pub temp_table_name: &'static str,
    /// SQL to create and populate the temp table
    pub setup_sql: String,
    /// SQL to drop the temp table
    pub cleanup_sql: String,
}

/// Convert a Value to a SQL literal string
#[allow(dead_code)]
fn value_to_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float32(n) => n.to_string(),
        Value::Float64(n) => n.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!("'\\x{}'", hex::encode(b)),
        Value::Date(d) => format!("'{}'::DATE", d),
        Value::Time(t) => format!("'{}'::TIME", t),
        Value::Timestamp(ts) => format!("'{}'::TIMESTAMP", ts),
        Value::TimestampTz(ts) => format!("'{}'::TIMESTAMPTZ", ts),
    }
}

/// Which _mallardb table is being queried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MallardbTable {
    /// _mallardb.jobs - virtual table of registered jobs
    Jobs,
    /// _mallardb.job_queue - virtual table of queued runs
    JobQueue,
    /// _mallardb.job_runs - real table (pass through to DuckDB)
    JobRuns,
}

/// Check if a query targets the _mallardb schema and which table.
/// Uses parsed AST table references for accurate matching.
pub fn detect_mallardb_table(table_refs: &[TableRef]) -> Option<MallardbTable> {
    for table_ref in table_refs {
        if table_ref.schema.as_deref() == Some("_mallardb") {
            match table_ref.table.as_str() {
                "jobs" => return Some(MallardbTable::Jobs),
                "job_queue" => return Some(MallardbTable::JobQueue),
                "job_runs" => return Some(MallardbTable::JobRuns),
                _ => {}
            }
        }
    }
    None
}

/// Generate virtual table response for _mallardb.jobs.
///
/// Schema:
/// - name TEXT: job directory name
/// - schedule TEXT: cron expression
/// - enabled BOOLEAN: whether job is enabled
/// - queue_max INTEGER: max queued runs (NULL if unlimited)
/// - timeout TEXT: job timeout (NULL if not set)
/// - file_count INTEGER: number of SQL files
/// - queued INTEGER: current queue depth
/// - running BOOLEAN: whether job is currently executing
pub async fn generate_jobs_table(registry: &JobRegistry) -> QueryOutput {
    let columns = vec![
        ColumnInfo {
            name: "name".to_string(),
            type_name: "TEXT".to_string(),
        },
        ColumnInfo {
            name: "schedule".to_string(),
            type_name: "TEXT".to_string(),
        },
        ColumnInfo {
            name: "enabled".to_string(),
            type_name: "BOOLEAN".to_string(),
        },
        ColumnInfo {
            name: "queue_max".to_string(),
            type_name: "INTEGER".to_string(),
        },
        ColumnInfo {
            name: "timeout".to_string(),
            type_name: "TEXT".to_string(),
        },
        ColumnInfo {
            name: "file_count".to_string(),
            type_name: "INTEGER".to_string(),
        },
        ColumnInfo {
            name: "queued".to_string(),
            type_name: "INTEGER".to_string(),
        },
        ColumnInfo {
            name: "running".to_string(),
            type_name: "BOOLEAN".to_string(),
        },
    ];

    let jobs = registry.list().await;
    let rows: Vec<Vec<Value>> = jobs
        .iter()
        .map(|job| {
            vec![
                Value::Text(job.name.clone()),
                Value::Text(job.config.schedule.clone()),
                Value::Boolean(job.config.enabled),
                job.config
                    .queue_max
                    .map(|n| Value::Int32(n as i32))
                    .unwrap_or(Value::Null),
                job.config
                    .timeout
                    .as_ref()
                    .map(|s| Value::Text(s.clone()))
                    .unwrap_or(Value::Null),
                Value::Int32(job.sql_files.len() as i32),
                Value::Int32(job.queue_depth as i32),
                Value::Boolean(job.running),
            ]
        })
        .collect();

    QueryOutput::Rows { columns, rows }
}

/// Generate SQL to create a temp table with jobs data.
/// This allows full SQL support (WHERE, ORDER BY, column selection).
pub async fn generate_jobs_sql(registry: &JobRegistry) -> VirtualTableSql {
    let jobs = registry.list().await;

    let create_sql = r#"
CREATE TEMP TABLE IF NOT EXISTS _vt_jobs (
    name TEXT,
    schedule TEXT,
    enabled BOOLEAN,
    queue_max INTEGER,
    timeout TEXT,
    file_count INTEGER,
    queued INTEGER,
    running BOOLEAN
)"#;

    let mut setup_parts = vec![create_sql.to_string()];
    setup_parts.push("DELETE FROM _vt_jobs".to_string());

    if !jobs.is_empty() {
        let values: Vec<String> = jobs
            .iter()
            .map(|job| {
                let queue_max = job.config.queue_max
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                let timeout = job.config.timeout
                    .as_ref()
                    .map(|s| format!("'{}'", s.replace('\'', "''")))
                    .unwrap_or_else(|| "NULL".to_string());
                format!(
                    "('{}', '{}', {}, {}, {}, {}, {}, {})",
                    job.name.replace('\'', "''"),
                    job.config.schedule.replace('\'', "''"),
                    job.config.enabled,
                    queue_max,
                    timeout,
                    job.sql_files.len(),
                    job.queue_depth,
                    job.running
                )
            })
            .collect();
        setup_parts.push(format!(
            "INSERT INTO _vt_jobs VALUES {}",
            values.join(", ")
        ));
    }

    VirtualTableSql {
        temp_table_name: "_vt_jobs",
        setup_sql: setup_parts.join("; "),
        cleanup_sql: "DROP TABLE IF EXISTS _vt_jobs".to_string(),
    }
}

/// Generate SQL to create a temp table with job_queue data.
pub async fn generate_job_queue_sql(queue_manager: &JobQueueManager) -> VirtualTableSql {
    let queued = queue_manager.all_queued().await;

    let create_sql = r#"
CREATE TEMP TABLE IF NOT EXISTS _vt_job_queue (
    job_name TEXT,
    queued_at TIMESTAMP,
    started_at TIMESTAMP
)"#;

    let mut setup_parts = vec![create_sql.to_string()];
    setup_parts.push("DELETE FROM _vt_job_queue".to_string());

    if !queued.is_empty() {
        let values: Vec<String> = queued
            .iter()
            .map(|run| {
                let started = run.started_at
                    .map(|t| format!("'{}'::TIMESTAMP", t.naive_utc()))
                    .unwrap_or_else(|| "NULL".to_string());
                format!(
                    "('{}', '{}'::TIMESTAMP, {})",
                    run.job_name.replace('\'', "''"),
                    run.queued_at.naive_utc(),
                    started
                )
            })
            .collect();
        setup_parts.push(format!(
            "INSERT INTO _vt_job_queue VALUES {}",
            values.join(", ")
        ));
    }

    VirtualTableSql {
        temp_table_name: "_vt_job_queue",
        setup_sql: setup_parts.join("; "),
        cleanup_sql: "DROP TABLE IF EXISTS _vt_job_queue".to_string(),
    }
}

/// Generate virtual table response for _mallardb.job_queue.
///
/// Schema:
/// - job_name TEXT: name of the job
/// - queued_at TIMESTAMP: when the run was queued
/// - started_at TIMESTAMP: when execution started (NULL if waiting)
pub async fn generate_job_queue_table(queue_manager: &JobQueueManager) -> QueryOutput {
    let columns = vec![
        ColumnInfo {
            name: "job_name".to_string(),
            type_name: "TEXT".to_string(),
        },
        ColumnInfo {
            name: "queued_at".to_string(),
            type_name: "TIMESTAMP".to_string(),
        },
        ColumnInfo {
            name: "started_at".to_string(),
            type_name: "TIMESTAMP".to_string(),
        },
    ];

    let queued = queue_manager.all_queued().await;
    let rows: Vec<Vec<Value>> = queued
        .iter()
        .map(|run| {
            vec![
                Value::Text(run.job_name.clone()),
                Value::Timestamp(run.queued_at.naive_utc()),
                run.started_at
                    .map(|t| Value::Timestamp(t.naive_utc()))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect();

    QueryOutput::Rows { columns, rows }
}

/// SQL to create placeholder views for virtual tables (for schema introspection).
/// The actual _mallardb.job_runs table is created by Backend::init_mallardb_schema().
pub const CREATE_JOB_RUNS_VIEW: &str = r#"
CREATE OR REPLACE VIEW _mallardb.jobs AS SELECT NULL::TEXT as name, NULL::TEXT as schedule, NULL::BOOLEAN as enabled, NULL::INTEGER as queue_max, NULL::TEXT as timeout, NULL::INTEGER as file_count, NULL::INTEGER as queued, NULL::BOOLEAN as running WHERE false;
CREATE OR REPLACE VIEW _mallardb.job_queue AS SELECT NULL::TEXT as job_name, NULL::TIMESTAMP as queued_at, NULL::TIMESTAMP as started_at WHERE false;
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn table_ref(schema: Option<&str>, table: &str) -> TableRef {
        TableRef {
            schema: schema.map(|s| s.to_string()),
            table: table.to_string(),
        }
    }

    #[test]
    fn test_detect_mallardb_jobs() {
        let refs = vec![table_ref(Some("_mallardb"), "jobs")];
        assert_eq!(detect_mallardb_table(&refs), Some(MallardbTable::Jobs));
    }

    #[test]
    fn test_detect_mallardb_job_queue() {
        let refs = vec![table_ref(Some("_mallardb"), "job_queue")];
        assert_eq!(detect_mallardb_table(&refs), Some(MallardbTable::JobQueue));
    }

    #[test]
    fn test_detect_mallardb_job_runs() {
        let refs = vec![table_ref(Some("_mallardb"), "job_runs")];
        assert_eq!(detect_mallardb_table(&refs), Some(MallardbTable::JobRuns));
    }

    #[test]
    fn test_detect_non_mallardb() {
        let refs = vec![table_ref(None, "users")];
        assert_eq!(detect_mallardb_table(&refs), None);

        let refs = vec![table_ref(Some("pg_catalog"), "pg_class")];
        assert_eq!(detect_mallardb_table(&refs), None);
    }

    #[test]
    fn test_jobs_not_confused_with_job_runs() {
        // Exact table name matching - no confusion possible with AST
        let refs = vec![table_ref(Some("_mallardb"), "job_runs")];
        assert_eq!(detect_mallardb_table(&refs), Some(MallardbTable::JobRuns));

        let refs = vec![table_ref(Some("_mallardb"), "jobs")];
        assert_eq!(detect_mallardb_table(&refs), Some(MallardbTable::Jobs));
    }

    #[test]
    fn test_empty_refs() {
        let refs: Vec<TableRef> = vec![];
        assert_eq!(detect_mallardb_table(&refs), None);
    }

    #[tokio::test]
    async fn test_generate_jobs_table_empty() {
        let registry = JobRegistry::new();
        let output = generate_jobs_table(&registry).await;

        match output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 8);
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[tokio::test]
    async fn test_generate_job_queue_table_empty() {
        let queue_manager = JobQueueManager::new();
        let output = generate_job_queue_table(&queue_manager).await;

        match output {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Rows output"),
        }
    }
}
