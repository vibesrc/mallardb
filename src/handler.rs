//! pgwire protocol handlers for mallardb
//!
//! Implements SimpleQueryHandler and ExtendedQueryHandler for PostgreSQL protocol compatibility.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{Stream, stream};
use tracing::debug;

use bytes::Bytes;
use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use crate::auth::MallardbAuthSource;
use crate::backend::{Backend, DuckDbConnection, QueryOutput, Value};
use crate::catalog::{handle_catalog_query, handle_ignored_set};
use crate::config::Config;
use crate::jobs::{
    JobQueueManager, JobRegistry, MallardbTable, detect_mallardb_table, generate_job_queue_sql,
    generate_jobs_sql,
};
use crate::query_parser::{MallardbQueryParser, MallardbStatement};
use crate::types::duckdb_type_to_pgwire;

/// The main handler for a single client connection
/// Each client gets their own handler with a persistent DuckDB connection
/// The connection is wrapped in Mutex since DuckDB Connection is not Sync
pub struct MallardbHandler {
    conn: Mutex<DuckDbConnection>,
    config: Arc<Config>,
    query_parser: Arc<MallardbQueryParser>,
    /// Optional jobs registry for _mallardb.jobs queries
    jobs_registry: Option<Arc<JobRegistry>>,
    /// Optional jobs queue manager for _mallardb.job_queue queries
    jobs_queue_manager: Option<Arc<JobQueueManager>>,
}

impl MallardbHandler {
    /// Create a new handler with its own DuckDB connection
    pub fn new(
        backend: &Backend,
        config: Arc<Config>,
        readonly: bool,
        jobs_registry: Option<Arc<JobRegistry>>,
        jobs_queue_manager: Option<Arc<JobQueueManager>>,
    ) -> Result<Self, crate::error::MallardbError> {
        let conn = if readonly {
            backend.create_readonly_connection()?
        } else {
            backend.create_connection()?
        };

        Ok(MallardbHandler {
            conn: Mutex::new(conn),
            config: config.clone(),
            query_parser: Arc::new(MallardbQueryParser::new(config)),
            jobs_registry,
            jobs_queue_manager,
        })
    }

    /// Execute a query using the persistent connection
    /// Takes a pre-parsed MallardbStatement to avoid redundant parsing
    fn execute_statement(
        &self,
        stmt: &MallardbStatement,
        username: &str,
    ) -> PgWireResult<QueryOutput> {
        debug!("Executing query: {}", stmt.sql);

        // Handle PostgreSQL-specific SET commands that DuckDB doesn't support
        if stmt.is_pg_ignored_set {
            debug!("Ignoring PostgreSQL-specific SET command");
            return Ok(handle_ignored_set());
        }

        // Check for _mallardb.* queries (jobs introspection)
        if let Some(table) = detect_mallardb_table(&stmt.table_refs) {
            match table {
                MallardbTable::Jobs => {
                    if let Some(ref registry) = self.jobs_registry {
                        debug!("Handling _mallardb.jobs query with temp table");
                        let vt = tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(generate_jobs_sql(registry))
                        });
                        return self.execute_virtual_table_query(
                            &stmt.rewritten_sql,
                            "_mallardb.jobs",
                            vt.temp_table_name,
                            &vt.setup_sql,
                        );
                    }
                }
                MallardbTable::JobQueue => {
                    if let Some(ref queue_manager) = self.jobs_queue_manager {
                        debug!("Handling _mallardb.job_queue query with temp table");
                        let vt = tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current()
                                .block_on(generate_job_queue_sql(queue_manager))
                        });
                        return self.execute_virtual_table_query(
                            &stmt.rewritten_sql,
                            "_mallardb.job_queue",
                            vt.temp_table_name,
                            &vt.setup_sql,
                        );
                    }
                }
                MallardbTable::JobRuns => {
                    // Pass through to DuckDB - it's a real table
                    debug!("Passing _mallardb.job_runs query to DuckDB");
                }
            }
        }

        // Check for catalog queries
        if stmt.is_catalog_query
            && let Some(result) = handle_catalog_query(
                &stmt.sql,
                &self.config.postgres_db,
                username,
                &self.config.pg_version,
            )
        {
            return Ok(result);
        }
        // pg_namespace falls through to DuckDB's built-in pg_catalog.pg_namespace

        debug!("Rewritten SQL: {}", stmt.rewritten_sql);

        // Execute on the persistent connection (lock for thread safety)
        let mut conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        conn.execute(&stmt.rewritten_sql)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::from(e))))
    }

    /// Execute a query against a virtual table by:
    /// 1. Setting up a temp table with the virtual data
    /// 2. Rewriting the query to use the temp table
    /// 3. Executing the rewritten query
    fn execute_virtual_table_query(
        &self,
        original_sql: &str,
        virtual_table: &str,
        temp_table: &str,
        setup_sql: &str,
    ) -> PgWireResult<QueryOutput> {
        let mut conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        // Set up the temp table with virtual data
        debug!("Setting up temp table: {}", temp_table);
        conn.execute(setup_sql)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::from(e))))?;

        // Rewrite the query to use the temp table
        // Case-insensitive replacement of the virtual table name
        let rewritten = replace_table_name(original_sql, virtual_table, temp_table);
        debug!("Rewritten virtual table query: {}", rewritten);

        // Execute the rewritten query
        conn.execute(&rewritten)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::from(e))))
    }
}

/// Replace a table name in SQL with another name (case-insensitive)
fn replace_table_name(sql: &str, from: &str, to: &str) -> String {
    // Use case-insensitive replacement
    let lower_sql = sql.to_lowercase();
    let lower_from = from.to_lowercase();

    let mut result = String::new();
    let mut last_end = 0;

    for (start, _) in lower_sql.match_indices(&lower_from) {
        // Check that this is a complete identifier (not part of a larger word)
        let before_ok = start == 0 || !is_ident_char(sql.chars().nth(start - 1).unwrap_or(' '));
        let after_pos = start + from.len();
        let after_ok =
            after_pos >= sql.len() || !is_ident_char(sql.chars().nth(after_pos).unwrap_or(' '));

        if before_ok && after_ok {
            result.push_str(&sql[last_end..start]);
            result.push_str(to);
            last_end = after_pos;
        }
    }

    result.push_str(&sql[last_end..]);
    result
}

/// Check if a character can be part of an identifier
fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Check if we can properly encode a type in binary format
fn can_encode_type_as_binary(type_name: &str) -> bool {
    let upper = type_name.to_uppercase();
    let base_type = if upper.contains('(') {
        upper.split('(').next().unwrap_or(&upper)
    } else {
        &upper
    };

    matches!(
        base_type.trim(),
        // Numeric types
        "BOOLEAN" | "BOOL"
            | "TINYINT"
            | "SMALLINT"
            | "INT2"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "SIGNED"
            | "BIGINT"
            | "INT8"
            | "LONG"
            | "FLOAT"
            | "FLOAT4"
            | "REAL"
            | "DOUBLE"
            | "FLOAT8"
            | "DECIMAL"
            | "NUMERIC"
            // Date/time types
            | "DATE"
            | "TIME"
            | "TIMESTAMP"
            | "TIMESTAMPTZ"
    )
    // Types we still cannot encode in binary:
    // HUGEINT (too large), INTERVAL (complex), etc.
}

/// Convert QueryOutput to pgwire Response with specified format
fn query_output_to_response(output: QueryOutput, format: &Format) -> PgWireResult<Vec<Response>> {
    match output {
        QueryOutput::Rows { columns, rows } => {
            // Extract type names for encoding
            let type_names: Vec<String> = columns.iter().map(|c| c.type_name.clone()).collect();

            // Determine per-column format based on client request
            let column_formats: Vec<bool> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    // Check what the client requested for this column
                    let client_wants_binary = match format {
                        Format::UnifiedBinary => true,
                        Format::UnifiedText => false,
                        Format::Individual(formats) => formats.get(i).map(|&f| f == 1).unwrap_or(false),
                    };
                    // Only use binary if client wants it AND we can encode this type in binary
                    let can_encode_binary = can_encode_type_as_binary(&col.type_name);
                    let use_binary = client_wants_binary && can_encode_binary;

                    debug!(
                        "Column '{}' type='{}' client_wants_binary={} can_encode={} -> use_binary={}",
                        col.name, col.type_name, client_wants_binary, can_encode_binary, use_binary
                    );
                    use_binary
                })
                .collect();

            let field_infos: Vec<FieldInfo> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let pg_type = duckdb_type_to_pgwire(&col.type_name);
                    // FieldInfo format must match what we'll actually encode
                    let field_format = if column_formats[i] {
                        FieldFormat::Binary
                    } else {
                        FieldFormat::Text
                    };
                    FieldInfo::new(col.name.clone(), None, None, pg_type, field_format)
                })
                .collect();

            let header = Arc::new(field_infos);
            let data_rows =
                encode_rows_per_column_format(rows, header.clone(), type_names, column_formats);

            Ok(vec![Response::Query(QueryResponse::new(header, data_rows))])
        }
        QueryOutput::Execute {
            affected_rows,
            command,
        } => {
            let tag = match command.as_str() {
                "INSERT" => Tag::new("INSERT").with_oid(0).with_rows(affected_rows),
                "UPDATE" => Tag::new("UPDATE").with_rows(affected_rows),
                "DELETE" => Tag::new("DELETE").with_rows(affected_rows),
                _ => Tag::new(&command).with_rows(affected_rows),
            };
            Ok(vec![Response::Execution(tag)])
        }
        QueryOutput::Command { tag } => Ok(vec![Response::Execution(Tag::new(&tag))]),
    }
}

/// Simple query version (always text format)
fn query_output_to_response_text(output: QueryOutput) -> PgWireResult<Vec<Response>> {
    query_output_to_response(output, &Format::UnifiedText)
}

/// Encode rows into DataRow stream with per-column format
fn encode_rows_per_column_format(
    rows: Vec<Vec<Value>>,
    schema: Arc<Vec<FieldInfo>>,
    _type_names: Vec<String>,  // No longer needed - we have typed values
    column_formats: Vec<bool>, // true = binary, false = text
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();

    for row in rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let mut success = true;

        for (i, value) in row.iter().enumerate() {
            let use_binary = column_formats.get(i).copied().unwrap_or(false);
            let encode_result = encode_typed_value(&mut encoder, value, use_binary);
            if encode_result.is_err() {
                success = false;
                break;
            }
        }

        if success {
            results.push(Ok(encoder.take_row()));
        }
    }

    stream::iter(results)
}

/// Encode a typed Value - no parsing needed, direct encoding
fn encode_typed_value(
    encoder: &mut DataRowEncoder,
    value: &Value,
    use_binary: bool,
) -> PgWireResult<()> {
    match value {
        Value::Null => encoder.encode_field(&None::<i32>),
        Value::Boolean(b) => encoder.encode_field(b),
        Value::Int16(n) => encoder.encode_field(n),
        Value::Int32(n) => encoder.encode_field(n),
        Value::Int64(n) => encoder.encode_field(n),
        Value::Float32(n) => encoder.encode_field(n),
        Value::Float64(n) => encoder.encode_field(n),
        Value::Decimal(d) => encoder.encode_field(d),
        Value::Text(s) => encoder.encode_field(s),
        Value::Bytes(b) => encoder.encode_field(b),
        Value::Date(d) => {
            if use_binary {
                encoder.encode_field(d)
            } else {
                encoder.encode_field(&d.format("%Y-%m-%d").to_string())
            }
        }
        Value::Time(t) => {
            if use_binary {
                encoder.encode_field(t)
            } else {
                encoder.encode_field(&t.format("%H:%M:%S%.6f").to_string())
            }
        }
        Value::Timestamp(ts) => {
            if use_binary {
                encoder.encode_field(ts)
            } else {
                encoder.encode_field(&ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
        }
        Value::TimestampTz(ts) => {
            if use_binary {
                encoder.encode_field(ts)
            } else {
                encoder.encode_field(&ts.format("%Y-%m-%d %H:%M:%S%.6f%z").to_string())
            }
        }
    }
}

use pgwire::api::METADATA_USER;

/// Substitute parameter placeholders ($1, $2, etc.) with actual values
fn substitute_parameters(sql: &str, parameters: &[Option<Bytes>], format: &Format) -> String {
    if parameters.is_empty() {
        return sql.to_string();
    }

    let mut result = sql.to_string();

    // Replace from highest to lowest to avoid $1 matching in $10
    for (i, param) in parameters.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let is_binary = match format {
            Format::UnifiedBinary => true,
            Format::UnifiedText => false,
            Format::Individual(formats) => formats.get(i).map(|&f| f == 1).unwrap_or(false),
        };

        let value = match param {
            Some(bytes) => {
                if is_binary {
                    // Binary format - decode based on length
                    decode_binary_param(bytes)
                } else {
                    // Text format
                    decode_text_param(bytes)
                }
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &value);
    }

    result
}

/// Decode a binary format parameter
fn decode_binary_param(bytes: &Bytes) -> String {
    match bytes.len() {
        0 => "''".to_string(),
        1 => {
            // bool or int1
            let v = bytes[0];
            if v == 0 || v == 1 {
                if v == 1 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            } else {
                (v as i8).to_string()
            }
        }
        2 => {
            // int2 (big-endian)
            let v = i16::from_be_bytes([bytes[0], bytes[1]]);
            v.to_string()
        }
        4 => {
            // int4 or oid (big-endian)
            let v = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            v.to_string()
        }
        8 => {
            // int8 (big-endian)
            let v = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            v.to_string()
        }
        _ => {
            // Unknown binary format - try as string, strip nulls
            let s = String::from_utf8_lossy(bytes);
            format!("'{}'", s.replace('\'', "''").replace('\0', ""))
        }
    }
}

/// Decode a text format parameter
fn decode_text_param(bytes: &Bytes) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.is_empty() {
        "''".to_string()
    } else if s.parse::<i64>().is_ok()
        || s.parse::<f64>().is_ok()
        || s.eq_ignore_ascii_case("true")
        || s.eq_ignore_ascii_case("false")
    {
        s.to_string()
    } else if s.eq_ignore_ascii_case("null") {
        "NULL".to_string()
    } else {
        // String value - escape and quote
        format!("'{}'", s.replace('\'', "''"))
    }
}

#[async_trait]
impl SimpleQueryHandler for MallardbHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("Simple query: {}", query);

        // Get username from client metadata
        let username = client
            .metadata()
            .get(METADATA_USER)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.postgres_user);

        // Parse and execute - simple queries don't go through QueryParser
        let sql = query.replace('\0', "");
        let stmt = MallardbStatement::new(&sql, &self.config);
        let result = self.execute_statement(&stmt, username)?;
        query_output_to_response_text(result)
    }
}

#[async_trait]
impl ExtendedQueryHandler for MallardbHandler {
    type Statement = MallardbStatement;
    type QueryParser = MallardbQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = &portal.statement.statement;

        // Substitute parameters into the query
        let query = substitute_parameters(&stmt.sql, &portal.parameters, &portal.parameter_format);
        debug!("Extended query: {}", query);

        // Get username from client metadata
        let username = client
            .metadata()
            .get(METADATA_USER)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.postgres_user);

        // Re-create statement with substituted parameters for execution
        let stmt = MallardbStatement::new(&query, &self.config);
        let result = self.execute_statement(&stmt, username)?;
        debug!(
            "Query result obtained, converting to response with format {:?}",
            portal.result_column_format
        );
        let responses = query_output_to_response(result, &portal.result_column_format)?;
        let response = responses
            .into_iter()
            .next()
            .unwrap_or(Response::Execution(Tag::new("OK")));
        debug!("Response created successfully");
        Ok(response)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parsed = &stmt.statement;
        debug!("Describe statement: {}", parsed.sql);

        // Check for catalog queries first using pre-parsed flag
        if parsed.is_catalog_query
            && let Some(result) = handle_catalog_query(
                &parsed.sql,
                &self.config.postgres_db,
                &self.config.postgres_user,
                &self.config.pg_version,
            )
            && let QueryOutput::Rows { columns, .. } = result
        {
            // pg_namespace falls through to DuckDB's built-in pg_catalog.pg_namespace
            let field_infos: Vec<FieldInfo> = columns
                .iter()
                .map(|col| {
                    let pg_type = duckdb_type_to_pgwire(&col.type_name);
                    FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
                })
                .collect();
            return Ok(DescribeStatementResponse::new(vec![], field_infos));
        }

        // Use pre-rewritten SQL
        let sql = &parsed.rewritten_sql;

        // Describe the query to get schema
        let conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        match conn.describe(sql) {
            Ok(columns) => {
                let field_infos: Vec<FieldInfo> = columns
                    .iter()
                    .map(|col| {
                        let pg_type = duckdb_type_to_pgwire(&col.type_name);
                        FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
                    })
                    .collect();
                // First vec is parameter types, second vec is result column types
                Ok(DescribeStatementResponse::new(vec![], field_infos))
            }
            Err(_) => {
                // If we can't describe, return empty (for DDL statements etc)
                Ok(DescribeStatementResponse::new(vec![], vec![]))
            }
        }
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let stmt = &portal.statement.statement;

        // Substitute parameters so we can describe the actual query
        let query = substitute_parameters(&stmt.sql, &portal.parameters, &portal.parameter_format);
        debug!("Describe portal: {}", query);

        // Re-create statement with substituted parameters
        let stmt = MallardbStatement::new(&query, &self.config);

        // Get username from client metadata
        let username = client
            .metadata()
            .get(METADATA_USER)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.postgres_user);

        // Check for catalog queries first using pre-parsed flag - return their schema
        if stmt.is_catalog_query
            && let Some(result) = handle_catalog_query(
                &stmt.sql,
                &self.config.postgres_db,
                username,
                &self.config.pg_version,
            )
            && let QueryOutput::Rows { columns, .. } = result
        {
            let field_infos: Vec<FieldInfo> = columns
                .iter()
                .map(|col| {
                    let pg_type = duckdb_type_to_pgwire(&col.type_name);
                    FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
                })
                .collect();
            return Ok(DescribePortalResponse::new(field_infos));
        }

        // Use pre-rewritten SQL
        let sql = &stmt.rewritten_sql;

        // Describe the query to get schema
        let conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        match conn.describe(sql) {
            Ok(columns) => {
                let field_infos: Vec<FieldInfo> = columns
                    .iter()
                    .map(|col| {
                        let pg_type = duckdb_type_to_pgwire(&col.type_name);
                        FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
                    })
                    .collect();
                Ok(DescribePortalResponse::new(field_infos))
            }
            Err(_) => {
                // If we can't describe, return empty (for DDL statements etc)
                Ok(DescribePortalResponse::new(vec![]))
            }
        }
    }
}

/// Factory for creating per-connection handler wrappers
/// This is the shared state across all connections
pub struct MallardbHandlerFactory {
    backend: Arc<Backend>,
    config: Arc<Config>,
    auth_source: Arc<MallardbAuthSource>,
    /// Optional jobs registry for _mallardb.jobs queries
    jobs_registry: Option<Arc<JobRegistry>>,
    /// Optional jobs queue manager for _mallardb.job_queue queries
    jobs_queue_manager: Option<Arc<JobQueueManager>>,
}

impl MallardbHandlerFactory {
    pub fn new(backend: Arc<Backend>, config: Arc<Config>) -> Self {
        let auth_source = Arc::new(MallardbAuthSource::new(config.clone()));
        MallardbHandlerFactory {
            backend,
            config,
            auth_source,
            jobs_registry: None,
            jobs_queue_manager: None,
        }
    }

    /// Create a new factory with jobs coordinator integration
    pub fn with_jobs(
        backend: Arc<Backend>,
        config: Arc<Config>,
        jobs_registry: Arc<JobRegistry>,
        jobs_queue_manager: Arc<JobQueueManager>,
    ) -> Self {
        let auth_source = Arc::new(MallardbAuthSource::new(config.clone()));
        MallardbHandlerFactory {
            backend,
            config,
            auth_source,
            jobs_registry: Some(jobs_registry),
            jobs_queue_manager: Some(jobs_queue_manager),
        }
    }

    /// Create a per-connection handler wrapper
    pub fn create_connection_handler(
        &self,
    ) -> Result<MallardbConnectionHandler, crate::error::MallardbError> {
        // Create a single handler that will be shared for both simple and extended queries
        let handler = Arc::new(MallardbHandler::new(
            &self.backend,
            self.config.clone(),
            false,
            self.jobs_registry.clone(),
            self.jobs_queue_manager.clone(),
        )?);

        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = format!(
            "PostgreSQL {} (mallardb 0.1.0, DuckDB 1.0.0)",
            self.config.pg_version
        );

        let startup = Arc::new(Md5PasswordAuthStartupHandler::new(
            self.auth_source.clone(),
            Arc::new(parameters),
        ));

        Ok(MallardbConnectionHandler { handler, startup })
    }
}

/// Per-connection handler wrapper
/// This ensures the SAME handler (and thus same DuckDB connection) is used
/// for both simple and extended query protocols within a single client session
pub struct MallardbConnectionHandler {
    handler: Arc<MallardbHandler>,
    startup: Arc<Md5PasswordAuthStartupHandler<MallardbAuthSource, DefaultServerParameterProvider>>,
}

impl PgWireServerHandlers for MallardbConnectionHandler {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone() // Return the SAME handler
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone() // Return the SAME handler
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }
}
