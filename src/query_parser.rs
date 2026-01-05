//! QueryParser implementation for mallardb
//!
//! Parses SQL once upfront and stores the parsed statement for reuse
//! throughout the query lifecycle.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::Type;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, portal::Format};
use pgwire::error::PgWireResult;

use crate::catalog::is_pg_ignored_set_from_kind;
use crate::config::Config;
use crate::sql_rewriter::{
    StatementKind, TableRef, contains_catalog_reference, extract_table_refs, kind_returns_rows,
    parse_sql, rewrite_sql,
};

/// A parsed and analyzed SQL statement
#[derive(Debug, Clone)]
pub struct MallardbStatement {
    /// Original SQL string
    pub sql: String,
    /// Rewritten SQL for DuckDB (after public->main, etc.)
    pub rewritten_sql: String,
    /// Statement classification
    pub kind: StatementKind,
    /// Whether this is a PG-specific SET that should be ignored
    pub is_pg_ignored_set: bool,
    /// Whether this needs catalog emulation
    pub is_catalog_query: bool,
    /// Table references extracted from AST
    pub table_refs: Vec<TableRef>,
}

impl MallardbStatement {
    /// Create a new parsed statement
    pub fn new(sql: &str, config: &Config) -> Self {
        let parsed = parse_sql(sql);
        let kind = parsed.kind;

        // Extract table references from AST
        let table_refs = parsed
            .statement
            .as_ref()
            .map(extract_table_refs)
            .unwrap_or_default();

        // Check if this is a PG-specific SET to ignore
        let is_pg_ignored_set = is_pg_ignored_set_from_kind(sql, kind);

        // Check if this needs catalog emulation
        let is_catalog_query = kind == StatementKind::Show || contains_catalog_reference(sql);

        // Rewrite SQL for DuckDB compatibility
        let rewritten_sql = rewrite_sql(sql, &config.postgres_db);

        MallardbStatement {
            sql: sql.to_string(),
            rewritten_sql,
            kind,
            is_pg_ignored_set,
            is_catalog_query,
            table_refs,
        }
    }

    /// Check if this statement returns rows
    pub fn returns_rows(&self) -> bool {
        kind_returns_rows(self.kind)
    }
}

/// Query parser that parses SQL once and caches metadata
pub struct MallardbQueryParser {
    config: Arc<Config>,
}

impl MallardbQueryParser {
    /// Create a new query parser
    pub fn new(config: Arc<Config>) -> Self {
        MallardbQueryParser { config }
    }
}

#[async_trait]
impl QueryParser for MallardbQueryParser {
    type Statement = MallardbStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(MallardbStatement::new(sql, &self.config))
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // We don't do parameter type inference - let DuckDB handle it
        Ok(vec![])
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // For statements that don't return rows, return empty schema
        if !stmt.returns_rows() {
            return Ok(vec![]);
        }

        // For now, return empty and let describe fill it in
        // TODO: Could cache schema from describe here
        Ok(vec![])
    }
}
