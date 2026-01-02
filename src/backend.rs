//! DuckDB backend with per-connection concurrency
//!
//! Each client gets its own DuckDB connection. DuckDB handles write
//! serialization internally via its WAL and locking mechanisms.

use std::path::PathBuf;
use std::sync::Arc;

use duckdb::Connection;
use tracing::{debug, info};

use crate::config::Config;
use crate::error::MallardbError;

/// Result type for query execution
pub type QueryResult = Result<QueryOutput, MallardbError>;

/// Output from a query execution
#[derive(Debug)]
pub enum QueryOutput {
    /// Rows returned from a SELECT query
    Rows {
        columns: Vec<ColumnInfo>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// Affected rows from INSERT/UPDATE/DELETE
    Execute { affected_rows: usize, command: String },
    /// DDL completion
    Command { tag: String },
}

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
}

/// A DuckDB connection for a client session
pub struct DuckDbConnection {
    conn: Connection,
}

impl DuckDbConnection {
    /// Create a new read-write connection
    pub fn new(db_path: &PathBuf) -> Result<Self, MallardbError> {
        // Create parent directory if needed
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MallardbError::Internal(format!("Failed to create data directory: {}", e))
            })?;
        }

        let conn = Connection::open(db_path).map_err(MallardbError::from_duckdb)?;
        Ok(DuckDbConnection { conn })
    }

    /// Create a new read-only connection
    pub fn new_readonly(db_path: &PathBuf) -> Result<Self, MallardbError> {
        let conn = Connection::open_with_flags(
            db_path,
            duckdb::Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)
                .map_err(|e| MallardbError::Internal(e.to_string()))?,
        )
        .map_err(MallardbError::from_duckdb)?;

        Ok(DuckDbConnection { conn })
    }

    /// Execute a query
    pub fn execute(&self, sql: &str) -> QueryResult {
        let result = execute_query(&self.conn, sql);

        // Auto-rollback on error to prevent "transaction aborted" deadlock
        if let Err(ref e) = result {
            debug!("Query error: {:?}", e);
            debug!("Auto-rolling back after error");
            let _ = self.conn.execute("ROLLBACK", []);
        }

        result
    }

    /// Describe a query to get its column schema without returning data
    /// For SELECT queries, wraps in LIMIT 0 subquery to get schema without actual execution
    pub fn describe(&self, sql: &str) -> Result<Vec<ColumnInfo>, MallardbError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();

        // For SELECT-like queries, wrap in LIMIT 0 subquery to get schema
        let describe_sql = if upper.starts_with("SELECT")
            || upper.starts_with("SHOW")
            || upper.starts_with("WITH")
            || upper.starts_with("TABLE")
            || upper.starts_with("FROM")
        {
            format!("SELECT * FROM ({}) AS _describe_subquery LIMIT 0", trimmed)
        } else {
            // For non-SELECT queries (DDL, DML), return empty - they don't return rows
            return Ok(vec![]);
        };

        // Prepare and execute with LIMIT 0 - this gets schema without data
        let mut stmt = self.conn.prepare(&describe_sql).map_err(MallardbError::from_duckdb)?;
        let rows = stmt.query([]).map_err(MallardbError::from_duckdb)?;

        // Get column info from the Arrow schema
        let columns: Vec<ColumnInfo> = match rows.as_ref() {
            Some(stmt) => {
                let schema = stmt.schema();
                schema
                    .fields()
                    .iter()
                    .map(|field| ColumnInfo {
                        name: field.name().clone(),
                        type_name: arrow_type_to_duckdb_name(field.data_type()),
                    })
                    .collect()
            }
            None => vec![],
        };
        Ok(columns)
    }
}

/// Execute a query and return the result
fn execute_query(conn: &Connection, sql: &str) -> QueryResult {
    debug!("Executing: {}", sql);

    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Determine query type
    if upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("WITH")
        || upper.starts_with("TABLE")
        || upper.starts_with("FROM")
    {
        execute_select(conn, sql)
    } else if upper.starts_with("INSERT") {
        execute_dml(conn, sql, "INSERT")
    } else if upper.starts_with("UPDATE") {
        execute_dml(conn, sql, "UPDATE")
    } else if upper.starts_with("DELETE") {
        execute_dml(conn, sql, "DELETE")
    } else if upper.starts_with("CREATE") {
        execute_ddl(conn, sql, extract_ddl_tag(&upper))
    } else if upper.starts_with("DROP") {
        execute_ddl(conn, sql, extract_ddl_tag(&upper))
    } else if upper.starts_with("ALTER") {
        execute_ddl(conn, sql, extract_ddl_tag(&upper))
    } else if upper.starts_with("BEGIN")
        || upper.starts_with("START TRANSACTION")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
    {
        execute_transaction(conn, sql, &upper)
    } else if upper.starts_with("SET") {
        execute_set(conn, sql)
    } else {
        // Try as a generic statement
        execute_generic(conn, sql)
    }
}

/// Convert Arrow DataType to a DuckDB-style type name for PostgreSQL mapping
fn arrow_type_to_duckdb_name(dt: &duckdb::arrow::datatypes::DataType) -> String {
    use duckdb::arrow::datatypes::DataType;
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "UTINYINT".to_string(),
        DataType::UInt16 => "USMALLINT".to_string(),
        DataType::UInt32 => "UINTEGER".to_string(),
        DataType::UInt64 => "UBIGINT".to_string(),
        DataType::Float16 => "FLOAT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
        DataType::Binary | DataType::LargeBinary => "BLOB".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL".to_string(),
        DataType::List(_) | DataType::LargeList(_) => "LIST".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Map(_, _) => "MAP".to_string(),
        DataType::Null => "NULL".to_string(),
        _ => "VARCHAR".to_string(),
    }
}

fn execute_select(conn: &Connection, sql: &str) -> QueryResult {
    let mut stmt = conn.prepare(sql).map_err(MallardbError::from_duckdb)?;

    // Execute query first - this is required before accessing column metadata
    let mut rows_result = stmt.query([]).map_err(MallardbError::from_duckdb)?;

    // Get column info from the Arrow schema (has proper types)
    let columns: Vec<ColumnInfo> = match rows_result.as_ref() {
        Some(stmt) => {
            let schema = stmt.schema();
            schema
                .fields()
                .iter()
                .map(|field| ColumnInfo {
                    name: field.name().clone(),
                    type_name: arrow_type_to_duckdb_name(field.data_type()),
                })
                .collect()
        }
        None => vec![],
    };
    let col_count = columns.len();

    // Collect rows
    let mut rows = Vec::new();

    while let Some(row) = rows_result.next().map_err(MallardbError::from_duckdb)? {
        let mut row_data = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let value = get_value_as_string(&row, i);
            row_data.push(value);
        }
        rows.push(row_data);
    }

    Ok(QueryOutput::Rows { columns, rows })
}

/// Helper to get any DuckDB value as a string
fn get_value_as_string(row: &duckdb::Row, idx: usize) -> Option<String> {
    use duckdb::types::ValueRef;

    let value_ref = match row.get_ref(idx) {
        Ok(v) => v,
        Err(_) => return None,
    };

    match value_ref {
        ValueRef::Null => None,
        ValueRef::Boolean(b) => Some(if b { "t".to_string() } else { "f".to_string() }),
        ValueRef::TinyInt(i) => Some(i.to_string()),
        ValueRef::SmallInt(i) => Some(i.to_string()),
        ValueRef::Int(i) => Some(i.to_string()),
        ValueRef::BigInt(i) => Some(i.to_string()),
        ValueRef::HugeInt(i) => Some(i.to_string()),
        ValueRef::UTinyInt(i) => Some(i.to_string()),
        ValueRef::USmallInt(i) => Some(i.to_string()),
        ValueRef::UInt(i) => Some(i.to_string()),
        ValueRef::UBigInt(i) => Some(i.to_string()),
        ValueRef::Float(f) => Some(f.to_string()),
        ValueRef::Double(d) => Some(d.to_string()),
        ValueRef::Decimal(d) => Some(d.to_string()),
        ValueRef::Text(s) => String::from_utf8(s.to_vec()).ok(),
        ValueRef::Blob(b) => Some(format!("\\x{}", hex::encode(b))),
        ValueRef::Date32(d) => Some(format_date(d)),
        ValueRef::Time64(unit, t) => Some(format_time(unit, t)),
        ValueRef::Timestamp(unit, ts) => Some(format_timestamp(unit, ts)),
        ValueRef::Interval { months, days, nanos } => Some(format_interval(months, days, nanos)),
        ValueRef::Enum(enum_type, idx) => {
            use arrow::array::StringArray;
            let dict_values = match enum_type {
                duckdb::types::EnumType::UInt8(res) => res.values(),
                duckdb::types::EnumType::UInt16(res) => res.values(),
                duckdb::types::EnumType::UInt32(res) => res.values(),
            };
            let dict_key = match enum_type {
                duckdb::types::EnumType::UInt8(res) => res.key(idx),
                duckdb::types::EnumType::UInt16(res) => res.key(idx),
                duckdb::types::EnumType::UInt32(res) => res.key(idx),
            };
            dict_key.and_then(|k| {
                dict_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|arr| arr.value(k).to_string())
            })
        }
        _ => Some("".to_string()),
    }
}

fn format_date(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = epoch + chrono::Duration::days(days as i64);
    date.format("%Y-%m-%d").to_string()
}

fn format_time(unit: duckdb::types::TimeUnit, value: i64) -> String {
    let micros = match unit {
        duckdb::types::TimeUnit::Second => value * 1_000_000,
        duckdb::types::TimeUnit::Millisecond => value * 1_000,
        duckdb::types::TimeUnit::Microsecond => value,
        duckdb::types::TimeUnit::Nanosecond => value / 1_000,
    };
    let secs = (micros / 1_000_000) as u32;
    let micro_part = (micros % 1_000_000) as u32;
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, micro_part * 1000);
    time.map(|t| t.format("%H:%M:%S%.6f").to_string())
        .unwrap_or_else(|| "00:00:00".to_string())
}

fn format_timestamp(unit: duckdb::types::TimeUnit, value: i64) -> String {
    let micros = match unit {
        duckdb::types::TimeUnit::Second => value * 1_000_000,
        duckdb::types::TimeUnit::Millisecond => value * 1_000,
        duckdb::types::TimeUnit::Microsecond => value,
        duckdb::types::TimeUnit::Nanosecond => value / 1_000,
    };
    let secs = micros / 1_000_000;
    let nsecs = ((micros % 1_000_000) * 1000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00:00".to_string())
}

fn format_interval(months: i32, days: i32, nanos: i64) -> String {
    let mut parts = Vec::new();
    if months != 0 {
        let years = months / 12;
        let mons = months % 12;
        if years != 0 {
            parts.push(format!(
                "{} year{}",
                years,
                if years.abs() != 1 { "s" } else { "" }
            ));
        }
        if mons != 0 {
            parts.push(format!(
                "{} mon{}",
                mons,
                if mons.abs() != 1 { "s" } else { "" }
            ));
        }
    }
    if days != 0 {
        parts.push(format!(
            "{} day{}",
            days,
            if days.abs() != 1 { "s" } else { "" }
        ));
    }
    if nanos != 0 {
        let secs = nanos / 1_000_000_000;
        let remaining_nanos = nanos % 1_000_000_000;
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let s = secs % 60;
        parts.push(format!(
            "{:02}:{:02}:{:02}.{:09}",
            hours, mins, s, remaining_nanos
        ));
    }
    if parts.is_empty() {
        "00:00:00".to_string()
    } else {
        parts.join(" ")
    }
}

fn execute_dml(conn: &Connection, sql: &str, command: &str) -> QueryResult {
    let affected_rows = conn.execute(sql, []).map_err(MallardbError::from_duckdb)?;

    Ok(QueryOutput::Execute {
        affected_rows,
        command: command.to_string(),
    })
}

fn execute_ddl(conn: &Connection, sql: &str, tag: String) -> QueryResult {
    conn.execute(sql, []).map_err(MallardbError::from_duckdb)?;

    Ok(QueryOutput::Command { tag })
}

fn execute_transaction(conn: &Connection, sql: &str, upper: &str) -> QueryResult {
    conn.execute(sql, []).map_err(MallardbError::from_duckdb)?;

    let tag = if upper.starts_with("BEGIN") || upper.starts_with("START") {
        "BEGIN"
    } else if upper.starts_with("COMMIT") {
        "COMMIT"
    } else {
        "ROLLBACK"
    };

    Ok(QueryOutput::Command {
        tag: tag.to_string(),
    })
}

fn execute_set(conn: &Connection, sql: &str) -> QueryResult {
    conn.execute(sql, []).map_err(MallardbError::from_duckdb)?;

    Ok(QueryOutput::Command {
        tag: "SET".to_string(),
    })
}

fn execute_generic(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql, []) {
        Ok(rows) => Ok(QueryOutput::Execute {
            affected_rows: rows,
            command: "OK".to_string(),
        }),
        Err(e) => {
            // Maybe it's a query that returns rows?
            match execute_select(conn, sql) {
                Ok(result) => Ok(result),
                Err(_) => Err(MallardbError::from_duckdb(e)),
            }
        }
    }
}

fn extract_ddl_tag(upper: &str) -> String {
    let words: Vec<&str> = upper.split_whitespace().take(3).collect();
    match words.as_slice() {
        ["CREATE", "TABLE", ..] => "CREATE TABLE".to_string(),
        ["CREATE", "INDEX", ..] => "CREATE INDEX".to_string(),
        ["CREATE", "VIEW", ..] => "CREATE VIEW".to_string(),
        ["CREATE", "SCHEMA", ..] => "CREATE SCHEMA".to_string(),
        ["CREATE", "SEQUENCE", ..] => "CREATE SEQUENCE".to_string(),
        ["CREATE", "TYPE", ..] => "CREATE TYPE".to_string(),
        ["DROP", "TABLE", ..] => "DROP TABLE".to_string(),
        ["DROP", "INDEX", ..] => "DROP INDEX".to_string(),
        ["DROP", "VIEW", ..] => "DROP VIEW".to_string(),
        ["DROP", "SCHEMA", ..] => "DROP SCHEMA".to_string(),
        ["ALTER", "TABLE", ..] => "ALTER TABLE".to_string(),
        _ => words.join(" "),
    }
}

/// The backend manages database connections
/// Uses a base connection that gets cloned for each client to avoid race conditions
pub struct Backend {
    /// Base connection - used as template for cloning new connections
    base_conn: Connection,
    db_path: PathBuf,
}

impl Backend {
    /// Create a new backend
    pub fn new(config: Arc<Config>) -> Self {
        let db_path = config.db_path();

        // Ensure data directory exists
        if let Some(parent) = db_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!("Failed to create data directory: {}", e);
            }
        }

        // Open the database once - all connections will be cloned from this
        let base_conn = Connection::open(&db_path).expect("Failed to open database");

        // Create PostgreSQL compatibility macros
        Self::init_pg_compat_macros(&base_conn);

        info!("Backend initialized with database at {:?}", db_path);

        Backend { base_conn, db_path }
    }

    /// Initialize PostgreSQL compatibility macros
    /// These allow DuckDB to handle PostgreSQL-specific functions natively
    fn init_pg_compat_macros(conn: &Connection) {
        // Scalar macros for PostgreSQL compatibility
        let scalar_macros = [
            // array_lower: PostgreSQL arrays are 1-indexed, first dimension
            "CREATE OR REPLACE MACRO array_lower(arr, dim) AS 1",
            // array_upper: Return the length of the array (upper bound of 1-indexed array)
            "CREATE OR REPLACE MACRO array_upper(arr, dim) AS len(arr)",
            // current_setting: Handle PostgreSQL-specific settings
            "CREATE OR REPLACE MACRO current_setting(name) AS CASE
                WHEN name = 'search_path' THEN 'main'
                WHEN name = 'server_version' THEN '15.0'
                WHEN name = 'server_version_num' THEN '150000'
                ELSE NULL
            END",
            // quote_ident: PostgreSQL identifier quoting - passthrough for DuckDB
            "CREATE OR REPLACE MACRO quote_ident(ident) AS ident",
        ];

        // Table macros for PostgreSQL compatibility (return rows, usable in FROM clause)
        let table_macros = [
            // string_to_array as table function - PostgreSQL uses this in FROM clause
            "CREATE OR REPLACE MACRO string_to_array(str, delim) AS TABLE
                SELECT unnest(string_split(str, delim)) AS unnest",
        ];

        let macros: Vec<&str> = scalar_macros.iter().chain(table_macros.iter()).copied().collect();

        for macro_sql in macros {
            if let Err(e) = conn.execute(macro_sql, []) {
                debug!("Failed to create macro (may already exist): {} - {}", macro_sql, e);
            }
        }
    }

    /// Create a new read-write connection for a client
    pub fn create_connection(&self) -> Result<DuckDbConnection, MallardbError> {
        // Clone from the base connection - this shares the underlying database
        let conn = self.base_conn.try_clone().map_err(MallardbError::from_duckdb)?;
        Ok(DuckDbConnection { conn })
    }

    /// Create a new read-only connection for a client
    pub fn create_readonly_connection(&self) -> Result<DuckDbConnection, MallardbError> {
        // For read-only, we still need to open separately with flags
        DuckDbConnection::new_readonly(&self.db_path)
    }

    /// Get the database path
    pub fn db_path(&self) -> &PathBuf {
        &self.db_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        (dir, db_path)
    }

    #[test]
    fn test_create_connection() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();
        assert!(db_path.exists());
        drop(conn);
    }

    #[test]
    fn test_create_readonly_connection() {
        let (_dir, db_path) = create_test_db();
        // First create DB with a writable connection
        {
            let conn = DuckDbConnection::new(&db_path).unwrap();
            conn.execute("CREATE TABLE test (id INTEGER)").unwrap();
        }
        // Now open read-only
        let conn = DuckDbConnection::new_readonly(&db_path).unwrap();
        let result = conn.execute("SELECT * FROM test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_select() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT 1 AS num, 'hello' AS msg").unwrap();
        match result {
            QueryOutput::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "num");
                assert_eq!(columns[1].name, "msg");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Some("1".to_string()));
                assert_eq!(rows[0][1], Some("hello".to_string()));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_execute_create_table() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        match result {
            QueryOutput::Command { tag } => {
                assert_eq!(tag, "CREATE TABLE");
            }
            _ => panic!("Expected Command output"),
        }
    }

    #[test]
    fn test_execute_insert() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        let result = conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

        match result {
            QueryOutput::Execute { affected_rows, command } => {
                assert_eq!(affected_rows, 2);
                assert_eq!(command, "INSERT");
            }
            _ => panic!("Expected Execute output"),
        }
    }

    #[test]
    fn test_execute_update() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        let result = conn.execute("UPDATE users SET name = 'Charlie' WHERE id = 1").unwrap();

        match result {
            QueryOutput::Execute { affected_rows, command } => {
                assert_eq!(affected_rows, 1);
                assert_eq!(command, "UPDATE");
            }
            _ => panic!("Expected Execute output"),
        }
    }

    #[test]
    fn test_execute_delete() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        let result = conn.execute("DELETE FROM users WHERE id = 1").unwrap();

        match result {
            QueryOutput::Execute { affected_rows, command } => {
                assert_eq!(affected_rows, 1);
                assert_eq!(command, "DELETE");
            }
            _ => panic!("Expected Execute output"),
        }
    }

    #[test]
    fn test_execute_transaction() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("BEGIN").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "BEGIN"));

        conn.execute("CREATE TABLE test (id INTEGER)").unwrap();

        let result = conn.execute("COMMIT").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "COMMIT"));
    }

    #[test]
    fn test_execute_rollback() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER)").unwrap();
        let result = conn.execute("ROLLBACK").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "ROLLBACK"));

        // Table should not exist after rollback
        let result = conn.execute("SELECT * FROM test");
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_rollback_on_error() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        // Start transaction
        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER)").unwrap();

        // Force an error (syntax error)
        let result = conn.execute("INVALID SQL SYNTAX HERE");
        assert!(result.is_err());

        // After auto-rollback, table shouldn't exist
        let result = conn.execute("SELECT * FROM test");
        assert!(result.is_err());

        // Should be able to start a new transaction
        let result = conn.execute("BEGIN");
        assert!(result.is_ok());
    }

    #[test]
    fn test_null_values() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT NULL AS null_val").unwrap();
        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], None);
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_boolean_values() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT true AS t, false AS f").unwrap();
        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Some("t".to_string()));
                assert_eq!(rows[0][1], Some("f".to_string()));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_numeric_types() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute(
            "SELECT 42::INTEGER AS i, 3.14::DOUBLE AS d, 100::BIGINT AS b"
        ).unwrap();

        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Some("42".to_string()));
                assert!(rows[0][1].as_ref().unwrap().starts_with("3.14"));
                assert_eq!(rows[0][2], Some("100".to_string()));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_date_time_types() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT DATE '2024-01-15' AS d").unwrap();
        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Some("2024-01-15".to_string()));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_backend_new() {
        let dir = TempDir::new().unwrap();
        let config = Arc::new(crate::config::Config {
            postgres_user: "test".to_string(),
            postgres_password: "test".to_string(),
            postgres_readonly_user: None,
            postgres_readonly_password: None,
            postgres_db: "test".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            data_dir: dir.path().to_path_buf(),
            db_file: "data.db".to_string(),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
        });

        let backend = Backend::new(config.clone());
        assert_eq!(backend.db_path(), &dir.path().join("data.db"));
    }

    #[test]
    fn test_backend_create_connection() {
        let dir = TempDir::new().unwrap();
        let config = Arc::new(crate::config::Config {
            postgres_user: "test".to_string(),
            postgres_password: "test".to_string(),
            postgres_readonly_user: None,
            postgres_readonly_password: None,
            postgres_db: "test".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            data_dir: dir.path().to_path_buf(),
            db_file: "data.db".to_string(),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
        });

        let backend = Backend::new(config);
        let conn = backend.create_connection().unwrap();

        // Test that the connection works
        let result = conn.execute("SELECT 1").unwrap();
        assert!(matches!(result, QueryOutput::Rows { .. }));
    }

    #[test]
    fn test_backend_create_readonly_connection() {
        let dir = TempDir::new().unwrap();
        let config = Arc::new(crate::config::Config {
            postgres_user: "test".to_string(),
            postgres_password: "test".to_string(),
            postgres_readonly_user: None,
            postgres_readonly_password: None,
            postgres_db: "test".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            data_dir: dir.path().to_path_buf(),
            db_file: "data.db".to_string(),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
        });

        let backend = Backend::new(config);

        // Create DB first with a writable connection
        {
            let conn = backend.create_connection().unwrap();
            conn.execute("CREATE TABLE test (id INTEGER)").unwrap();
        }

        // Now create readonly connection
        let conn = backend.create_readonly_connection().unwrap();
        let result = conn.execute("SELECT * FROM test").unwrap();
        assert!(matches!(result, QueryOutput::Rows { .. }));

        // Write should fail on readonly connection
        let result = conn.execute("INSERT INTO test VALUES (1)");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_statement() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SET threads = 4").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "SET"));
    }

    #[test]
    fn test_show_statement() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SHOW TABLES").unwrap();
        assert!(matches!(result, QueryOutput::Rows { .. }));
    }

    #[test]
    fn test_explain_statement() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("EXPLAIN SELECT 1").unwrap();
        assert!(matches!(result, QueryOutput::Rows { .. }));
    }

    #[test]
    fn test_with_statement() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("WITH cte AS (SELECT 1 AS num) SELECT * FROM cte").unwrap();
        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_column_info() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT 1 AS first, 2 AS second, 3 AS third").unwrap();
        match result {
            QueryOutput::Rows { columns, .. } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "first");
                assert_eq!(columns[1].name, "second");
                assert_eq!(columns[2].name, "third");
            }
            _ => panic!("Expected Rows output"),
        }
    }
}
