//! DuckDB backend with per-connection concurrency
//!
//! Each client gets its own DuckDB connection. DuckDB handles write
//! serialization internally via its WAL and locking mechanisms.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime};
use duckdb::Connection;
use rust_decimal::Decimal;
use tracing::{debug, info};

use crate::config::Config;
use crate::error::MallardbError;
use crate::sql_rewriter::{
    get_ddl_tag_from_parsed, get_transaction_tag_from_parsed, kind_returns_rows, parse_sql,
    StatementKind,
};

/// Result type for query execution
pub type QueryResult = Result<QueryOutput, MallardbError>;

/// A typed value from DuckDB - preserves native types for efficient encoding
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(rust_decimal::Decimal),
    Text(String),
    Bytes(Vec<u8>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
}

impl Value {
    /// Get the text value if this is a Text variant
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
}

/// Output from a query execution
#[derive(Debug)]
pub enum QueryOutput {
    /// Rows returned from a SELECT query
    Rows {
        columns: Vec<ColumnInfo>,
        rows: Vec<Vec<Value>>,
    },
    /// Affected rows from INSERT/UPDATE/DELETE
    Execute {
        affected_rows: usize,
        command: String,
    },
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
        execute_query(&self.conn, sql)
    }

    /// Describe a query to get its column schema without returning data
    /// For SELECT queries, wraps in LIMIT 0 subquery to get schema without actual execution
    pub fn describe(&self, sql: &str) -> Result<Vec<ColumnInfo>, MallardbError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let parsed = parse_sql(trimmed);

        // For SELECT-like queries, wrap in LIMIT 0 subquery to get schema
        let describe_sql = if kind_returns_rows(parsed.kind) {
            format!("SELECT * FROM ({}) AS _describe_subquery LIMIT 0", trimmed)
        } else {
            // For non-SELECT queries (DDL, DML), return empty - they don't return rows
            return Ok(vec![]);
        };

        // Prepare and execute with LIMIT 0 - this gets schema without data
        let mut stmt = self
            .conn
            .prepare(&describe_sql)
            .map_err(MallardbError::from_duckdb)?;
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
    let parsed = parse_sql(trimmed);

    // Route based on statement kind (single parse)
    match parsed.kind {
        StatementKind::Select | StatementKind::Show => execute_select(conn, sql),
        StatementKind::Insert => execute_dml(conn, sql, "INSERT"),
        StatementKind::Update => execute_dml(conn, sql, "UPDATE"),
        StatementKind::Delete => execute_dml(conn, sql, "DELETE"),
        StatementKind::Ddl => execute_ddl(conn, sql, get_ddl_tag_from_parsed(&parsed, trimmed)),
        StatementKind::Transaction => {
            execute_transaction(conn, sql, get_transaction_tag_from_parsed(&parsed, trimmed))
        }
        StatementKind::Set => execute_set(conn, sql),
        StatementKind::Copy | StatementKind::Other => execute_generic(conn, sql),
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
            let value = get_typed_value(row, i);
            row_data.push(value);
        }
        rows.push(row_data);
    }

    Ok(QueryOutput::Rows { columns, rows })
}

/// Helper to get a typed Value from DuckDB - preserves native types
fn get_typed_value(row: &duckdb::Row, idx: usize) -> Value {
    use duckdb::types::ValueRef;

    let value_ref = match row.get_ref(idx) {
        Ok(v) => v,
        Err(_) => return Value::Null,
    };

    match value_ref {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Boolean(b),
        ValueRef::TinyInt(i) => Value::Int16(i as i16),
        ValueRef::SmallInt(i) => Value::Int16(i),
        ValueRef::Int(i) => Value::Int32(i),
        ValueRef::BigInt(i) => Value::Int64(i),
        ValueRef::HugeInt(i) => {
            // Try to fit in Decimal, fallback to text
            Decimal::from_str(&i.to_string())
                .map(Value::Decimal)
                .unwrap_or_else(|_| Value::Text(i.to_string()))
        }
        ValueRef::UTinyInt(i) => Value::Int16(i as i16),
        ValueRef::USmallInt(i) => Value::Int32(i as i32),
        ValueRef::UInt(i) => Value::Int64(i as i64),
        ValueRef::UBigInt(i) => {
            // u64 might not fit in i64, use Decimal
            Decimal::from_str(&i.to_string())
                .map(Value::Decimal)
                .unwrap_or_else(|_| Value::Text(i.to_string()))
        }
        ValueRef::Float(f) => Value::Float32(f),
        ValueRef::Double(d) => Value::Float64(d),
        ValueRef::Decimal(d) => Decimal::from_str(&d.to_string())
            .map(Value::Decimal)
            .unwrap_or_else(|_| Value::Text(d.to_string())),
        ValueRef::Text(s) => String::from_utf8(s.to_vec())
            .map(Value::Text)
            .unwrap_or_else(|_| Value::Bytes(s.to_vec())),
        ValueRef::Blob(b) => Value::Bytes(b.to_vec()),
        ValueRef::Date32(days) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Value::Date(epoch + chrono::Duration::days(days as i64))
        }
        ValueRef::Time64(unit, t) => {
            let micros = match unit {
                duckdb::types::TimeUnit::Second => t * 1_000_000,
                duckdb::types::TimeUnit::Millisecond => t * 1_000,
                duckdb::types::TimeUnit::Microsecond => t,
                duckdb::types::TimeUnit::Nanosecond => t / 1_000,
            };
            let secs = (micros / 1_000_000) as u32;
            let micro_part = (micros % 1_000_000) as u32;
            NaiveTime::from_num_seconds_from_midnight_opt(secs, micro_part * 1000)
                .map(Value::Time)
                .unwrap_or_else(|| {
                    Value::Text(format!(
                        "{:02}:{:02}:{:02}",
                        secs / 3600,
                        (secs % 3600) / 60,
                        secs % 60
                    ))
                })
        }
        ValueRef::Timestamp(unit, ts) => {
            let micros = match unit {
                duckdb::types::TimeUnit::Second => ts * 1_000_000,
                duckdb::types::TimeUnit::Millisecond => ts * 1_000,
                duckdb::types::TimeUnit::Microsecond => ts,
                duckdb::types::TimeUnit::Nanosecond => ts / 1_000,
            };
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| Value::Timestamp(dt.naive_utc()))
                .unwrap_or_else(|| Value::Text("1970-01-01 00:00:00".to_string()))
        }
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => Value::Text(format_interval(months, days, nanos)),
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
            dict_key
                .and_then(|k| {
                    dict_values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|arr| Value::Text(arr.value(k).to_string()))
                })
                .unwrap_or(Value::Null)
        }
        ValueRef::List(list_type, row_idx) => Value::Text(format_list(list_type, row_idx)),
        ValueRef::Map(map_array, row_idx) => Value::Text(format_map(map_array, row_idx)),
        ValueRef::Struct(struct_array, row_idx) => {
            Value::Text(format_struct(struct_array, row_idx))
        }
        ValueRef::Array(array, row_idx) => Value::Text(format_fixed_list(array, row_idx)),
        ValueRef::Union(union_array, row_idx) => Value::Text(format_union(union_array, row_idx)),
    }
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

/// Format a List/Array value from Arrow
fn format_list(list_type: duckdb::types::ListType, row_idx: usize) -> String {
    match list_type {
        duckdb::types::ListType::Regular(list_array) => {
            let value = list_array.value(row_idx);
            format_arrow_array(&value)
        }
        duckdb::types::ListType::Large(list_array) => {
            let value = list_array.value(row_idx);
            format_arrow_array(&value)
        }
    }
}

/// Format a fixed-size array value
fn format_fixed_list(array: &duckdb::arrow::array::FixedSizeListArray, row_idx: usize) -> String {
    let value = array.value(row_idx);
    format_arrow_array(&value)
}

/// Format a Map value from Arrow
fn format_map(map_array: &duckdb::arrow::array::MapArray, row_idx: usize) -> String {
    let entries = map_array.value(row_idx);
    // Map entries are stored as a struct array with "key" and "value" fields
    format!("{{{}}}", format_arrow_array(&entries))
}

/// Format a Struct value from Arrow
fn format_struct(struct_array: &duckdb::arrow::array::StructArray, row_idx: usize) -> String {
    let fields = struct_array.fields();
    let mut parts = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let col = struct_array.column(i);
        let value = format_arrow_value(col, row_idx);
        parts.push(format!("'{}': {}", field.name(), value));
    }
    format!("{{{}}}", parts.join(", "))
}

/// Format a Union value from Arrow
fn format_union(union_array: &duckdb::arrow::array::ArrayRef, row_idx: usize) -> String {
    format_arrow_value(union_array, row_idx)
}

/// Format an Arrow array as a string (for list contents)
fn format_arrow_array(array: &dyn duckdb::arrow::array::Array) -> String {
    let mut values = Vec::new();
    for i in 0..array.len() {
        values.push(format_arrow_value_dyn(array, i));
    }
    format!("[{}]", values.join(", "))
}

/// Format a single Arrow value at an index
fn format_arrow_value(array: &duckdb::arrow::array::ArrayRef, idx: usize) -> String {
    format_arrow_value_dyn(array.as_ref(), idx)
}

/// Format a single Arrow value dynamically
fn format_arrow_value_dyn(array: &dyn duckdb::arrow::array::Array, idx: usize) -> String {
    use duckdb::arrow::array::*;

    if array.is_null(idx) {
        return "NULL".to_string();
    }

    // Try common types
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return format!("'{}'", arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return format!("'{}'", arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return if arr.value(idx) { "true" } else { "false" }.to_string();
    }

    // Fallback: use Arrow's display formatting
    use duckdb::arrow::util::display::ArrayFormatter;
    let options = duckdb::arrow::util::display::FormatOptions::default();
    match ArrayFormatter::try_new(array, &options) {
        Ok(formatter) => formatter.value(idx).to_string(),
        Err(_) => "<complex>".to_string(),
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

fn execute_transaction(_conn: &Connection, _sql: &str, tag: &'static str) -> QueryResult {
    // For PostgreSQL client compatibility, completely ignore transaction commands.
    // DuckDB works in autocommit mode - each statement commits immediately.
    // Calling actual transaction commands can interfere with the connection state.
    debug!("Ignoring transaction command: {}", tag);
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
        let db_path = config.db_path().clone();

        // Ensure data directory exists
        if let Some(parent) = db_path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            tracing::error!("Failed to create data directory: {}", e);
        }

        // Open the database once - all connections will be cloned from this
        let base_conn = Connection::open(&db_path).expect("Failed to open database");

        // Set extension directory
        let ext_dir = &config.extension_directory;
        if let Err(e) = std::fs::create_dir_all(ext_dir) {
            tracing::error!("Failed to create extension directory: {}", e);
        }
        let set_sql = format!("SET extension_directory = '{}'", ext_dir.display());
        if let Err(e) = base_conn.execute(&set_sql, []) {
            tracing::error!("Failed to set extension_directory: {}", e);
        } else {
            info!("Extension directory set to {:?}", ext_dir);
        }

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

        let macros: Vec<&str> = scalar_macros
            .iter()
            .chain(table_macros.iter())
            .copied()
            .collect();

        for macro_sql in macros {
            if let Err(e) = conn.execute(macro_sql, []) {
                debug!(
                    "Failed to create macro (may already exist): {} - {}",
                    macro_sql, e
                );
            }
        }
    }

    /// Create a new read-write connection for a client
    pub fn create_connection(&self) -> Result<DuckDbConnection, MallardbError> {
        // Clone from the base connection - this shares the underlying database
        let conn = self
            .base_conn
            .try_clone()
            .map_err(MallardbError::from_duckdb)?;
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
                assert_eq!(rows[0][0], Value::Int32(1));
                assert_eq!(rows[0][1], Value::Text("hello".to_string()));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_execute_create_table() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn
            .execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .unwrap();
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

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .unwrap();
        let result = conn
            .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();

        match result {
            QueryOutput::Execute {
                affected_rows,
                command,
            } => {
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

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        let result = conn
            .execute("UPDATE users SET name = 'Charlie' WHERE id = 1")
            .unwrap();

        match result {
            QueryOutput::Execute {
                affected_rows,
                command,
            } => {
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

        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        let result = conn.execute("DELETE FROM users WHERE id = 1").unwrap();

        match result {
            QueryOutput::Execute {
                affected_rows,
                command,
            } => {
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
    fn test_transaction_commands_ignored() {
        // Transaction commands are ignored for compatibility with PostgreSQL clients
        // DuckDB runs in autocommit mode - each statement commits immediately
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        // BEGIN returns success but doesn't start a transaction
        let result = conn.execute("BEGIN").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "BEGIN"));

        // Create a table - this auto-commits immediately
        conn.execute("CREATE TABLE test (id INTEGER)").unwrap();

        // ROLLBACK returns success but doesn't actually rollback
        let result = conn.execute("ROLLBACK").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "ROLLBACK"));

        // Table still exists because we're in autocommit mode
        let result = conn.execute("SELECT * FROM test");
        assert!(result.is_ok());

        // COMMIT also returns success
        let result = conn.execute("COMMIT").unwrap();
        assert!(matches!(result, QueryOutput::Command { tag } if tag == "COMMIT"));

        // Multiple BEGIN/COMMIT work fine (all ignored)
        conn.execute("BEGIN").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    #[test]
    fn test_null_values() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn.execute("SELECT NULL AS null_val").unwrap();
        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
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
                assert_eq!(rows[0][0], Value::Boolean(true));
                assert_eq!(rows[0][1], Value::Boolean(false));
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_numeric_types() {
        let (_dir, db_path) = create_test_db();
        let conn = DuckDbConnection::new(&db_path).unwrap();

        let result = conn
            .execute("SELECT 42::INTEGER AS i, 3.25::DOUBLE AS d, 100::BIGINT AS b")
            .unwrap();

        match result {
            QueryOutput::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int32(42));
                if let Value::Float64(d) = rows[0][1] {
                    assert!((d - 3.25).abs() < 0.001);
                } else {
                    panic!("Expected Float64");
                }
                assert_eq!(rows[0][2], Value::Int64(100));
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
                assert_eq!(
                    rows[0][0],
                    Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
                );
            }
            _ => panic!("Expected Rows output"),
        }
    }

    #[test]
    fn test_backend_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let config = Arc::new(crate::config::Config {
            postgres_user: "test".to_string(),
            postgres_password: "test".to_string(),
            postgres_readonly_user: None,
            postgres_readonly_password: None,
            postgres_db: "test".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: db_path.clone(),
            extension_directory: std::path::PathBuf::from("./extensions"),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
            tls_cert_path: None,
            tls_key_path: None,
        });

        let backend = Backend::new(config.clone());
        assert_eq!(backend.db_path(), &db_path);
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
            database: dir.path().join("test.db"),
            extension_directory: std::path::PathBuf::from("./extensions"),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
            tls_cert_path: None,
            tls_key_path: None,
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
            database: dir.path().join("test.db"),
            extension_directory: std::path::PathBuf::from("./extensions"),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
            tls_cert_path: None,
            tls_key_path: None,
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

        let result = conn
            .execute("WITH cte AS (SELECT 1 AS num) SELECT * FROM cte")
            .unwrap();
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

        let result = conn
            .execute("SELECT 1 AS first, 2 AS second, 3 AS third")
            .unwrap();
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
