//! mallardb - PostgreSQL wire protocol compatible database powered by DuckDB
//!
//! mallardb presents a fully PostgreSQL-compatible interface to clients while
//! internally executing all queries against DuckDB.

pub mod auth;
pub mod backend;
pub mod catalog;
pub mod config;
pub mod error;
pub mod handler;
pub mod query_parser;
pub mod sql_rewriter;
pub mod types;
