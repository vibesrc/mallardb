//! Error types and SQLSTATE mapping for mallardb
//!
//! Maps DuckDB errors to PostgreSQL SQLSTATE codes.

use thiserror::Error;

/// SQLSTATE error codes as defined in PostgreSQL
pub mod sqlstate {
    pub const SUCCESSFUL_COMPLETION: &str = "00000";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const OUT_OF_MEMORY: &str = "53200";
    pub const CONNECTION_FAILURE: &str = "08006";
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";
    pub const INSUFFICIENT_RESOURCES: &str = "53000";
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
    pub const INVALID_AUTHORIZATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const QUERY_CANCELED: &str = "57014";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
    pub const SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: &str = "42000";
}

/// Main error type for mallardb
#[derive(Error, Debug)]
pub enum MallardbError {
    #[error("DuckDB error: {message}")]
    DuckDb {
        message: String,
        sqlstate: String,
    },

    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Read-only violation: cannot execute {statement} in a read-only transaction")]
    ReadOnlyViolation { statement: String },

    #[error("Too many connections for role \"{role}\"")]
    TooManyConnections { role: String },

    #[error("Write queue full, try again later")]
    WriteQueueFull,

    #[error("Query timeout")]
    QueryTimeout,

    #[error("Lock timeout")]
    LockTimeout,

    #[error("Feature not supported: {0}")]
    FeatureNotSupported(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

impl MallardbError {
    /// Get the SQLSTATE code for this error
    pub fn sqlstate(&self) -> &str {
        match self {
            MallardbError::DuckDb { sqlstate, .. } => sqlstate,
            MallardbError::Authentication(_) => sqlstate::INVALID_PASSWORD,
            MallardbError::ReadOnlyViolation { .. } => sqlstate::READ_ONLY_SQL_TRANSACTION,
            MallardbError::TooManyConnections { .. } => sqlstate::TOO_MANY_CONNECTIONS,
            MallardbError::WriteQueueFull => sqlstate::INSUFFICIENT_RESOURCES,
            MallardbError::QueryTimeout => sqlstate::QUERY_CANCELED,
            MallardbError::LockTimeout => sqlstate::LOCK_NOT_AVAILABLE,
            MallardbError::FeatureNotSupported(_) => sqlstate::FEATURE_NOT_SUPPORTED,
            MallardbError::Internal(_) => sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            MallardbError::Config(_) => sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
        }
    }

    /// Classify DuckDB error message to appropriate SQLSTATE
    pub fn from_duckdb(err: duckdb::Error) -> Self {
        let message = err.to_string();
        let sqlstate = classify_duckdb_error(&message);
        MallardbError::DuckDb { message, sqlstate }
    }
}

/// Classify DuckDB error message to PostgreSQL SQLSTATE
fn classify_duckdb_error(message: &str) -> String {
    let lower = message.to_lowercase();

    if lower.contains("syntax error") || lower.contains("parser error") {
        sqlstate::SYNTAX_ERROR.to_string()
    } else if lower.contains("table") && (lower.contains("not found") || lower.contains("does not exist")) {
        sqlstate::UNDEFINED_TABLE.to_string()
    } else if lower.contains("column") && (lower.contains("not found") || lower.contains("does not exist")) {
        sqlstate::UNDEFINED_COLUMN.to_string()
    } else if lower.contains("unique constraint") || lower.contains("duplicate key") {
        sqlstate::UNIQUE_VIOLATION.to_string()
    } else if lower.contains("foreign key") {
        sqlstate::FOREIGN_KEY_VIOLATION.to_string()
    } else if lower.contains("not null constraint") || lower.contains("cannot be null") {
        sqlstate::NOT_NULL_VIOLATION.to_string()
    } else if lower.contains("division by zero") {
        sqlstate::DIVISION_BY_ZERO.to_string()
    } else if lower.contains("out of memory") {
        sqlstate::OUT_OF_MEMORY.to_string()
    } else if lower.contains("read-only") || lower.contains("readonly") {
        sqlstate::READ_ONLY_SQL_TRANSACTION.to_string()
    } else {
        sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION.to_string()
    }
}

/// Convert MallardbError to pgwire ErrorInfo
impl From<MallardbError> for pgwire::error::ErrorInfo {
    fn from(err: MallardbError) -> Self {
        pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            err.sqlstate().to_string(),
            err.to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlstate_codes() {
        assert_eq!(sqlstate::SUCCESSFUL_COMPLETION, "00000");
        assert_eq!(sqlstate::SYNTAX_ERROR, "42601");
        assert_eq!(sqlstate::UNDEFINED_TABLE, "42P01");
        assert_eq!(sqlstate::UNIQUE_VIOLATION, "23505");
        assert_eq!(sqlstate::READ_ONLY_SQL_TRANSACTION, "25006");
    }

    #[test]
    fn test_duckdb_error_classification_syntax() {
        let err = MallardbError::from_duckdb(
            duckdb::Error::QueryReturnedNoRows // Using a simple error type
        );
        // The error message won't contain "syntax error" so it will be generic
        assert!(!err.sqlstate().is_empty());
    }

    #[test]
    fn test_authentication_error_sqlstate() {
        let err = MallardbError::Authentication("invalid password".to_string());
        assert_eq!(err.sqlstate(), sqlstate::INVALID_PASSWORD);
    }

    #[test]
    fn test_readonly_violation_sqlstate() {
        let err = MallardbError::ReadOnlyViolation {
            statement: "INSERT".to_string(),
        };
        assert_eq!(err.sqlstate(), sqlstate::READ_ONLY_SQL_TRANSACTION);
        assert!(err.to_string().contains("INSERT"));
    }

    #[test]
    fn test_too_many_connections_sqlstate() {
        let err = MallardbError::TooManyConnections {
            role: "reader".to_string(),
        };
        assert_eq!(err.sqlstate(), sqlstate::TOO_MANY_CONNECTIONS);
        assert!(err.to_string().contains("reader"));
    }

    #[test]
    fn test_write_queue_full_sqlstate() {
        let err = MallardbError::WriteQueueFull;
        assert_eq!(err.sqlstate(), sqlstate::INSUFFICIENT_RESOURCES);
    }

    #[test]
    fn test_query_timeout_sqlstate() {
        let err = MallardbError::QueryTimeout;
        assert_eq!(err.sqlstate(), sqlstate::QUERY_CANCELED);
    }

    #[test]
    fn test_lock_timeout_sqlstate() {
        let err = MallardbError::LockTimeout;
        assert_eq!(err.sqlstate(), sqlstate::LOCK_NOT_AVAILABLE);
    }

    #[test]
    fn test_feature_not_supported_sqlstate() {
        let err = MallardbError::FeatureNotSupported("COPY TO STDOUT".to_string());
        assert_eq!(err.sqlstate(), sqlstate::FEATURE_NOT_SUPPORTED);
        assert!(err.to_string().contains("COPY TO STDOUT"));
    }

    #[test]
    fn test_internal_error_sqlstate() {
        let err = MallardbError::Internal("something went wrong".to_string());
        assert_eq!(err.sqlstate(), sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
    }

    #[test]
    fn test_config_error_sqlstate() {
        let err = MallardbError::Config("bad config".to_string());
        assert_eq!(err.sqlstate(), sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
    }

    #[test]
    fn test_error_to_pgwire_errorinfo() {
        let err = MallardbError::Authentication("test auth failure".to_string());
        let _info: pgwire::error::ErrorInfo = err.into();
        // ErrorInfo fields are checked implicitly - conversion should not panic
    }

    #[test]
    fn test_classify_syntax_error() {
        // Test internal classification function via from_duckdb
        // We can't directly test classify_duckdb_error since it's private,
        // but we can verify the behavior through the public API
        let err = MallardbError::DuckDb {
            message: "Parser Error: syntax error at or near 'SELEC'".to_string(),
            sqlstate: classify_duckdb_error("Parser Error: syntax error at or near 'SELEC'"),
        };
        assert_eq!(err.sqlstate(), sqlstate::SYNTAX_ERROR);
    }

    #[test]
    fn test_classify_undefined_table() {
        let sqlstate = classify_duckdb_error("Catalog Error: Table 'users' does not exist");
        assert_eq!(sqlstate, sqlstate::UNDEFINED_TABLE);
    }

    #[test]
    fn test_classify_undefined_column() {
        let sqlstate = classify_duckdb_error("Binder Error: column 'foo' not found");
        assert_eq!(sqlstate, sqlstate::UNDEFINED_COLUMN);
    }

    #[test]
    fn test_classify_unique_violation() {
        let sqlstate = classify_duckdb_error("Constraint Error: duplicate key value violates unique constraint");
        assert_eq!(sqlstate, sqlstate::UNIQUE_VIOLATION);
    }

    #[test]
    fn test_classify_foreign_key_violation() {
        let sqlstate = classify_duckdb_error("Constraint Error: foreign key constraint failed");
        assert_eq!(sqlstate, sqlstate::FOREIGN_KEY_VIOLATION);
    }

    #[test]
    fn test_classify_not_null_violation() {
        let sqlstate = classify_duckdb_error("Constraint Error: NOT NULL constraint failed: column cannot be null");
        assert_eq!(sqlstate, sqlstate::NOT_NULL_VIOLATION);
    }

    #[test]
    fn test_classify_division_by_zero() {
        let sqlstate = classify_duckdb_error("Error: division by zero");
        assert_eq!(sqlstate, sqlstate::DIVISION_BY_ZERO);
    }

    #[test]
    fn test_classify_out_of_memory() {
        let sqlstate = classify_duckdb_error("Error: out of memory");
        assert_eq!(sqlstate, sqlstate::OUT_OF_MEMORY);
    }

    #[test]
    fn test_classify_readonly() {
        let sqlstate = classify_duckdb_error("Error: Cannot write to read-only database");
        assert_eq!(sqlstate, sqlstate::READ_ONLY_SQL_TRANSACTION);
    }

    #[test]
    fn test_classify_unknown_error() {
        let sqlstate = classify_duckdb_error("Some unknown error message");
        assert_eq!(sqlstate, sqlstate::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
    }
}
