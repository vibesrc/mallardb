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
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use crate::auth::MallardbAuthSource;
use crate::backend::{Backend, DuckDbConnection, QueryOutput, Value};
use crate::catalog::{
    handle_catalog_query, handle_ignored_set, is_catalog_query, is_pg_ignored_set, rewrite_sql,
};
use crate::config::Config;
use crate::types::duckdb_type_to_pgwire;

/// The main handler for a single client connection
/// Each client gets their own handler with a persistent DuckDB connection
/// The connection is wrapped in Mutex since DuckDB Connection is not Sync
pub struct MallardbHandler {
    conn: Mutex<DuckDbConnection>,
    config: Arc<Config>,
    query_parser: Arc<NoopQueryParser>,
}

impl MallardbHandler {
    /// Create a new handler with its own DuckDB connection
    pub fn new(
        backend: &Backend,
        config: Arc<Config>,
        readonly: bool,
    ) -> Result<Self, crate::error::MallardbError> {
        let conn = if readonly {
            backend.create_readonly_connection()?
        } else {
            backend.create_connection()?
        };

        Ok(MallardbHandler {
            conn: Mutex::new(conn),
            config,
            query_parser: Arc::new(NoopQueryParser::new()),
        })
    }

    /// Execute a query using the persistent connection
    fn execute_query(&self, sql: &str, username: &str) -> PgWireResult<QueryOutput> {
        // Strip any nul bytes from the SQL (can come from binary protocol)
        let sql = &sql.replace('\0', "");
        debug!("Executing query: {}", sql);

        // Handle PostgreSQL-specific SET commands that DuckDB doesn't support
        if is_pg_ignored_set(sql) {
            debug!("Ignoring PostgreSQL-specific SET command");
            return Ok(handle_ignored_set());
        }

        // Check for catalog queries first
        if is_catalog_query(sql)
            && let Some(result) = handle_catalog_query(
                sql,
                &self.config.postgres_db,
                username,
                &self.config.pg_version,
            )
        {
            return Ok(result);
        }
        // pg_namespace falls through to DuckDB's built-in pg_catalog.pg_namespace

        // Rewrite SQL for DuckDB compatibility (e.g., 'public' -> 'main', catalog -> 'data')
        let sql = rewrite_sql(sql, &self.config.postgres_db);
        debug!("Rewritten SQL: {}", sql);

        // Execute on the persistent connection (lock for thread safety)
        let conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        conn.execute(&sql)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::from(e))))
    }
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

        let result = self.execute_query(query, username)?;
        query_output_to_response_text(result)
    }
}

#[async_trait]
impl ExtendedQueryHandler for MallardbHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

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
        let query = &portal.statement.statement;

        // Substitute parameters into the query
        let query = substitute_parameters(query, &portal.parameters, &portal.parameter_format);
        debug!("Extended query: {}", query);

        // Get username from client metadata
        let username = client
            .metadata()
            .get(METADATA_USER)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.postgres_user);

        let result = self.execute_query(&query, username)?;
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
        let query = &stmt.statement;
        debug!("Describe statement: {}", query);

        // Check for catalog queries first
        if is_catalog_query(query) {
            // pg_namespace falls through to DuckDB's built-in pg_catalog.pg_namespace

            if let Some(result) = handle_catalog_query(
                query,
                &self.config.postgres_db,
                &self.config.postgres_user,
                &self.config.pg_version,
            ) && let QueryOutput::Rows { columns, .. } = result
            {
                let field_infos: Vec<FieldInfo> = columns
                    .iter()
                    .map(|col| {
                        let pg_type = duckdb_type_to_pgwire(&col.type_name);
                        FieldInfo::new(col.name.clone(), None, None, pg_type, FieldFormat::Text)
                    })
                    .collect();
                return Ok(DescribeStatementResponse::new(vec![], field_infos));
            }
        }

        // Rewrite SQL for DuckDB compatibility
        let sql = rewrite_sql(query, &self.config.postgres_db);

        // Describe the query to get schema
        let conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        match conn.describe(&sql) {
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
        let query = &portal.statement.statement;

        // Substitute parameters so we can describe the actual query
        let query = substitute_parameters(query, &portal.parameters, &portal.parameter_format);
        debug!("Describe portal: {}", query);

        // Get username from client metadata
        let username = client
            .metadata()
            .get(METADATA_USER)
            .map(|s| s.as_str())
            .unwrap_or(&self.config.postgres_user);

        // Check for catalog queries first - return their schema
        if is_catalog_query(&query)
            && let Some(result) = handle_catalog_query(
                &query,
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

        // Rewrite SQL for DuckDB compatibility
        let sql = rewrite_sql(&query, &self.config.postgres_db);

        // Describe the query to get schema
        let conn = self.conn.lock().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "Connection lock poisoned".to_string(),
            )))
        })?;

        match conn.describe(&sql) {
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
}

impl MallardbHandlerFactory {
    pub fn new(backend: Arc<Backend>, config: Arc<Config>) -> Self {
        let auth_source = Arc::new(MallardbAuthSource::new(config.clone()));
        MallardbHandlerFactory {
            backend,
            config,
            auth_source,
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
