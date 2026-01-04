//! Catalog system emulation for PostgreSQL compatibility
//!
//! Intercepts queries to pg_catalog and information_schema and either
//! translates them to DuckDB equivalents or synthesizes responses.

use crate::backend::{ColumnInfo, QueryOutput, Value};
use crate::sql_rewriter::{contains_catalog_reference, get_set_variable, is_show_statement};

// Re-export rewrite_sql from sql_rewriter module
pub use crate::sql_rewriter::rewrite_sql;

/// PostgreSQL-specific SET parameters that DuckDB doesn't support.
/// These are silently accepted and ignored for compatibility.
const PG_IGNORED_SET_PARAMS: &[&str] = &[
    "extra_float_digits",
    "application_name",
    "datestyle",
    "intervalstyle",
    "timezone",
    "client_encoding",
    "standard_conforming_strings",
    "statement_timeout",
    "lock_timeout",
    "idle_in_transaction_session_timeout",
    "row_security",
    "default_transaction_isolation",
    "transaction_isolation",
    "bytea_output",
    "xmloption",
    "client_min_messages",
    "search_path",
    "geqo",
    "jit",
];

/// Check if this is a SET command for a PostgreSQL-specific parameter
/// that should be silently ignored for compatibility
pub fn is_pg_ignored_set(sql: &str) -> bool {
    if let Some(var_name) = get_set_variable(sql) {
        return PG_IGNORED_SET_PARAMS.iter().any(|&p| var_name == p);
    }
    false
}

/// Handle an ignored SET command by returning a synthetic SET response
pub fn handle_ignored_set() -> QueryOutput {
    QueryOutput::Command {
        tag: "SET".to_string(),
    }
}

/// Check if a query targets the catalog system that we need to intercept
/// NOTE: Many pg_catalog tables are handled natively by DuckDB - only intercept
/// what requires PostgreSQL-specific emulation (auth, sessions, version info)
pub fn is_catalog_query(sql: &str) -> bool {
    is_show_statement(sql) || contains_catalog_reference(sql)
}

/// Handle a catalog query and return synthetic results
pub fn handle_catalog_query(
    sql: &str,
    database: &str,
    username: &str,
    pg_version: &str,
) -> Option<QueryOutput> {
    let lower = sql.to_lowercase();

    // Handle SHOW search_path specifically
    if is_show_statement(sql) && lower.contains("search_path") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "search_path".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text("main".to_string())]],
        });
    }
    // Let other SHOW commands pass through to DuckDB

    // Handle common catalog queries
    if lower.contains("select version()") || (lower.contains("version()") && lower.len() < 50) {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "version".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text(format!(
                "PostgreSQL {} (mallardb 0.1.0, DuckDB 1.0.0)",
                pg_version
            ))]],
        });
    }

    if lower.contains("current_database()") && !lower.contains("from") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "current_database".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text(database.to_string())]],
        });
    }

    if lower.contains("current_schema()") && !lower.contains("from") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "current_schema".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text("main".to_string())]],
        });
    }

    if lower.contains("current_user") && !lower.contains("from") && lower.len() < 30 {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "current_user".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text(username.to_string())]],
        });
    }

    if lower.contains("session_user") && !lower.contains("from") && lower.len() < 30 {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "session_user".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text(username.to_string())]],
        });
    }

    if lower.contains("pg_backend_pid()") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "pg_backend_pid".to_string(),
                type_name: "INT4".to_string(),
            }],
            rows: vec![vec![Value::Text(std::process::id().to_string())]],
        });
    }

    // pg_database queries (DuckDB doesn't have this concept)
    if lower.contains("pg_database") {
        return Some(handle_pg_database_query(database));
    }

    // pg_namespace queries - let DuckDB handle via its built-in pg_catalog.pg_namespace

    // pg_roles queries
    if lower.contains("pg_roles") || lower.contains("pg_user") {
        return Some(handle_pg_roles_query(username));
    }

    // pg_settings queries
    if lower.contains("pg_settings") {
        return Some(handle_pg_settings_query(pg_version));
    }

    // pg_stat_activity
    if lower.contains("pg_stat_activity") {
        return Some(handle_pg_stat_activity_query(database, username));
    }

    // pg_proc, pg_class, pg_attribute, pg_type, pg_namespace, pg_index, pg_constraint
    // are all handled natively by DuckDB's built-in pg_catalog tables

    // obj_description/col_description function calls - return NULL
    // (table JOINs to pg_description pass through to DuckDB)
    if (lower.contains("obj_description(") || lower.contains("col_description("))
        && !lower.contains("join")
        && !lower.contains("from")
    {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "description".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Null]],
        });
    }

    // has_*_privilege functions - return true (permissive, DuckDB has no auth)
    if lower.contains("has_table_privilege")
        || lower.contains("has_schema_privilege")
        || lower.contains("has_database_privilege")
    {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "has_privilege".to_string(),
                type_name: "BOOL".to_string(),
            }],
            rows: vec![vec![Value::Text("t".to_string())]],
        });
    }

    // pg_encoding_to_char - PostgreSQL encoding function
    if lower.contains("pg_encoding_to_char") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "pg_encoding_to_char".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text("UTF8".to_string())]],
        });
    }

    // pg_get_userbyid - DuckDB has no auth system
    if lower.contains("pg_get_userbyid") {
        return Some(QueryOutput::Rows {
            columns: vec![ColumnInfo {
                name: "username".to_string(),
                type_name: "TEXT".to_string(),
            }],
            rows: vec![vec![Value::Text(username.to_string())]],
        });
    }

    None
}

fn handle_pg_database_query(database: &str) -> QueryOutput {
    // SQLTools fetchDatabases expects: db.*, db.datname as label, db.datname as database,
    // 'connection.database' as type, 'database' as detail
    QueryOutput::Rows {
        columns: vec![
            ColumnInfo {
                name: "oid".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "datname".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "datdba".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "encoding".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "datcollate".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "datctype".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "datistemplate".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "datallowconn".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "datconnlimit".to_string(),
                type_name: "INT4".to_string(),
            },
            // Aliases expected by SQLTools
            ColumnInfo {
                name: "label".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "database".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "type".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "detail".to_string(),
                type_name: "TEXT".to_string(),
            },
        ],
        rows: vec![vec![
            Value::Text("1".to_string()),
            Value::Text(database.to_string()),
            Value::Text("10".to_string()),
            Value::Text("6".to_string()), // UTF8
            Value::Text("C".to_string()),
            Value::Text("C".to_string()),
            Value::Text("f".to_string()),
            Value::Text("t".to_string()),
            Value::Text("-1".to_string()),
            // Alias values
            Value::Text(database.to_string()),              // label
            Value::Text(database.to_string()),              // database
            Value::Text("connection.database".to_string()), // type
            Value::Text("database".to_string()),            // detail
        ]],
    }
}

fn handle_pg_roles_query(username: &str) -> QueryOutput {
    QueryOutput::Rows {
        columns: vec![
            ColumnInfo {
                name: "oid".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "rolname".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "rolsuper".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "rolinherit".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "rolcreaterole".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "rolcreatedb".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "rolcanlogin".to_string(),
                type_name: "BOOL".to_string(),
            },
            ColumnInfo {
                name: "rolconnlimit".to_string(),
                type_name: "INT4".to_string(),
            },
        ],
        rows: vec![vec![
            Value::Text("10".to_string()),
            Value::Text(username.to_string()),
            Value::Text("t".to_string()),
            Value::Text("t".to_string()),
            Value::Text("t".to_string()),
            Value::Text("t".to_string()),
            Value::Text("t".to_string()),
            Value::Text("-1".to_string()),
        ]],
    }
}

fn handle_pg_settings_query(pg_version: &str) -> QueryOutput {
    let settings = [
        ("server_version", pg_version),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("max_connections", "100"),
    ];

    let rows: Vec<Vec<Value>> = settings
        .iter()
        .map(|(name, setting)| {
            vec![
                Value::Text(name.to_string()),
                Value::Text(setting.to_string()),
            ]
        })
        .collect();

    QueryOutput::Rows {
        columns: vec![
            ColumnInfo {
                name: "name".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "setting".to_string(),
                type_name: "TEXT".to_string(),
            },
        ],
        rows,
    }
}

fn handle_pg_stat_activity_query(database: &str, username: &str) -> QueryOutput {
    QueryOutput::Rows {
        columns: vec![
            ColumnInfo {
                name: "datid".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "datname".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "pid".to_string(),
                type_name: "INT4".to_string(),
            },
            ColumnInfo {
                name: "usename".to_string(),
                type_name: "TEXT".to_string(),
            },
            ColumnInfo {
                name: "state".to_string(),
                type_name: "TEXT".to_string(),
            },
        ],
        rows: vec![vec![
            Value::Text("1".to_string()),
            Value::Text(database.to_string()),
            Value::Text(std::process::id().to_string()),
            Value::Text(username.to_string()),
            Value::Text("active".to_string()),
        ]],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== rewrite_sql tests =====

    #[test]
    fn test_rewrite_sql_public_to_main() {
        let sql = "SELECT * FROM t WHERE table_schema = 'public'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("table_schema = 'main'"));
        assert!(!result.contains("table_schema = 'public'"));
    }

    #[test]
    fn test_rewrite_sql_no_spaces() {
        let sql = "SELECT * FROM t WHERE table_schema='public'";
        let result = rewrite_sql(sql, "mydb");
        // AST-based rewriter normalizes to 'main' (may add spaces)
        assert!(result.to_lowercase().contains("'main'"));
        assert!(!result.to_lowercase().contains("'public'"));
    }

    #[test]
    fn test_rewrite_sql_nspname() {
        let sql = "SELECT * FROM pg_namespace WHERE nspname = 'public'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("nspname = 'main'"));
    }

    #[test]
    fn test_rewrite_sql_catalog_preserved() {
        // Catalog name should be preserved (matches DuckDB catalog from filename)
        let sql = "SELECT * FROM t WHERE table_catalog = 'mydb'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("mydb"));
    }

    #[test]
    fn test_rewrite_sql_case_insensitive() {
        let sql = "SELECT * FROM t WHERE TABLE_SCHEMA = 'public'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("'main'"));
    }

    #[test]
    fn test_rewrite_sql_no_change_when_not_matching() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let result = rewrite_sql(sql, "mydb");
        assert_eq!(result, sql);
    }

    // ===== is_catalog_query tests =====
    // NOTE: Many queries now pass through to DuckDB's built-in pg_catalog

    #[test]
    fn test_is_catalog_query_passes_through_pg_catalog_tables() {
        // These should NOT trigger interception - DuckDB handles them natively
        assert!(!is_catalog_query("SELECT * FROM pg_catalog.pg_tables"));
        assert!(!is_catalog_query("SELECT * FROM pg_type"));
        assert!(!is_catalog_query("SELECT * FROM pg_class"));
        assert!(!is_catalog_query("SELECT * FROM pg_namespace"));
        assert!(!is_catalog_query(
            "SELECT * FROM information_schema.schemata"
        ));
    }

    #[test]
    fn test_is_catalog_query_intercepts_pg_specific_functions() {
        // These need interception because DuckDB doesn't have them
        assert!(is_catalog_query("SELECT current_database()"));
        assert!(is_catalog_query("SELECT version()"));
        assert!(is_catalog_query("SELECT pg_backend_pid()"));
        assert!(is_catalog_query("SELECT current_user"));
        assert!(is_catalog_query("SELECT session_user"));
    }

    #[test]
    fn test_is_catalog_query_has_privilege() {
        // Auth functions need interception (DuckDB has no auth)
        assert!(is_catalog_query(
            "SELECT has_table_privilege('users', 'SELECT')"
        ));
        assert!(is_catalog_query(
            "SELECT has_schema_privilege('public', 'USAGE')"
        ));
        assert!(is_catalog_query(
            "SELECT has_database_privilege('mydb', 'CONNECT')"
        ));
    }

    #[test]
    fn test_is_catalog_query_passes_through_size_functions() {
        // Size functions pass through to DuckDB
        assert!(!is_catalog_query("SELECT pg_total_relation_size('users')"));
        assert!(!is_catalog_query("SELECT pg_table_size('users')"));
        assert!(!is_catalog_query("SELECT pg_indexes_size('users')"));
        assert!(!is_catalog_query("SELECT pg_size_pretty(1024)"));
    }

    #[test]
    fn test_is_catalog_query_false_for_regular_queries() {
        assert!(!is_catalog_query("SELECT * FROM users"));
        assert!(!is_catalog_query("INSERT INTO logs VALUES (1)"));
        assert!(!is_catalog_query("UPDATE users SET name = 'test'"));
    }

    // ===== handle_catalog_query tests =====

    #[test]
    fn test_handle_catalog_query_version() {
        let result = handle_catalog_query("SELECT version()", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        let output = result.unwrap();
        if let QueryOutput::Rows { columns, rows } = output {
            assert_eq!(columns[0].name, "version");
            assert!(rows[0][0].as_text().unwrap().contains("PostgreSQL 15.0"));
            assert!(rows[0][0].as_text().unwrap().contains("mallardb"));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_current_database() {
        let result = handle_catalog_query("SELECT current_database()", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("mydb".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_current_schema() {
        let result = handle_catalog_query("SELECT current_schema()", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("main".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_current_user() {
        let result = handle_catalog_query("SELECT current_user", "mydb", "admin", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("admin".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_session_user() {
        let result = handle_catalog_query("SELECT session_user", "mydb", "admin", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("admin".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_backend_pid() {
        let result = handle_catalog_query("SELECT pg_backend_pid()", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            let pid: u32 = rows[0][0].as_text().unwrap().parse().unwrap();
            assert!(pid > 0);
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_type() {
        // pg_type now passes through to DuckDB's built-in pg_catalog.pg_type
        let result = handle_catalog_query("SELECT * FROM pg_type", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_database() {
        let result =
            handle_catalog_query("SELECT * FROM pg_database", "testdb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { columns, rows } = result.unwrap() {
            assert!(columns.iter().any(|c| c.name == "datname"));
            assert_eq!(rows[0][1], Value::Text("testdb".to_string())); // datname
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_namespace() {
        // pg_namespace is handled by handler.rs to query real schemas from DuckDB
        let result = handle_catalog_query("SELECT * FROM pg_namespace", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_roles() {
        let result = handle_catalog_query("SELECT * FROM pg_roles", "mydb", "admin", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][1], Value::Text("admin".to_string())); // rolname
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_user() {
        let result = handle_catalog_query("SELECT * FROM pg_user", "mydb", "admin", "15.0");
        assert!(result.is_some()); // pg_user maps to same as pg_roles
    }

    #[test]
    fn test_handle_catalog_query_pg_settings() {
        let result = handle_catalog_query("SELECT * FROM pg_settings", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { columns, rows } = result.unwrap() {
            assert!(columns.iter().any(|c| c.name == "name"));
            assert!(columns.iter().any(|c| c.name == "setting"));
            // Check for some expected settings
            let names: Vec<&str> = rows.iter().filter_map(|r| r[0].as_text()).collect();
            assert!(names.contains(&"server_version"));
            assert!(names.contains(&"server_encoding"));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_stat_activity() {
        let result =
            handle_catalog_query("SELECT * FROM pg_stat_activity", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { columns, rows } = result.unwrap() {
            assert!(columns.iter().any(|c| c.name == "datname"));
            assert!(columns.iter().any(|c| c.name == "usename"));
            assert!(columns.iter().any(|c| c.name == "state"));
            assert_eq!(rows[0][1], Value::Text("mydb".to_string())); // datname
            assert_eq!(rows[0][3], Value::Text("testuser".to_string())); // usename
            assert_eq!(rows[0][4], Value::Text("active".to_string())); // state
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_proc() {
        // pg_proc now passes through to DuckDB's built-in pg_catalog.pg_proc
        let result = handle_catalog_query("SELECT * FROM pg_proc", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_description() {
        // pg_description now passes through to DuckDB's built-in pg_catalog.pg_description
        let result =
            handle_catalog_query("SELECT * FROM pg_description", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_has_privilege() {
        let result = handle_catalog_query(
            "SELECT has_table_privilege('users', 'SELECT')",
            "mydb",
            "testuser",
            "15.0",
        );
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("t".to_string())); // Always returns true
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_pg_encoding_to_char() {
        let result =
            handle_catalog_query("SELECT pg_encoding_to_char(6)", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("UTF8".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_size_functions() {
        // Size functions now pass through to DuckDB natively
        let result = handle_catalog_query(
            "SELECT pg_total_relation_size('users')",
            "mydb",
            "testuser",
            "15.0",
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_size_pretty() {
        // pg_size_pretty now passes through to DuckDB
        let result =
            handle_catalog_query("SELECT pg_size_pretty(1024)", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_format_type() {
        // format_type now passes through to DuckDB's built-in implementation
        let result = handle_catalog_query("SELECT format_type(23, -1)", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_table_is_visible() {
        // pg_table_is_visible now passes through to DuckDB
        let result = handle_catalog_query(
            "SELECT pg_table_is_visible(12345)",
            "mydb",
            "testuser",
            "15.0",
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_catalog_query_pg_get_userbyid() {
        let result = handle_catalog_query("SELECT pg_get_userbyid(10)", "mydb", "testuser", "15.0");
        assert!(result.is_some());
        if let QueryOutput::Rows { rows, .. } = result.unwrap() {
            assert_eq!(rows[0][0], Value::Text("testuser".to_string()));
        } else {
            panic!("Expected Rows output");
        }
    }

    #[test]
    fn test_handle_catalog_query_returns_none_for_regular_queries() {
        let result = handle_catalog_query("SELECT * FROM users", "mydb", "testuser", "15.0");
        assert!(result.is_none());
    }

    // NOTE: current_setting is now handled by DuckDB macros in backend.rs

    // NOTE: SQL rewriting tests are now in sql_rewriter module

    // ===== is_pg_ignored_set tests =====

    #[test]
    fn test_is_pg_ignored_set_extra_float_digits() {
        assert!(is_pg_ignored_set("SET extra_float_digits = 3"));
        assert!(is_pg_ignored_set("SET extra_float_digits TO 3"));
        assert!(is_pg_ignored_set("set EXTRA_FLOAT_DIGITS = 2"));
    }

    #[test]
    fn test_is_pg_ignored_set_common_params() {
        assert!(is_pg_ignored_set("SET application_name = 'psql'"));
        assert!(is_pg_ignored_set("SET datestyle = 'ISO, MDY'"));
        assert!(is_pg_ignored_set("SET timezone = 'UTC'"));
        assert!(is_pg_ignored_set("SET client_encoding = 'UTF8'"));
        assert!(is_pg_ignored_set("SET search_path = 'public'"));
        assert!(is_pg_ignored_set("SET statement_timeout = 0"));
    }

    #[test]
    fn test_is_pg_ignored_set_false_for_duckdb_params() {
        // DuckDB-supported params should NOT be ignored
        assert!(!is_pg_ignored_set("SET threads = 4"));
        assert!(!is_pg_ignored_set("SET memory_limit = '4GB'"));
        assert!(!is_pg_ignored_set("SET enable_progress_bar = true"));
    }

    #[test]
    fn test_is_pg_ignored_set_false_for_non_set() {
        assert!(!is_pg_ignored_set("SELECT extra_float_digits"));
        assert!(!is_pg_ignored_set("SHOW extra_float_digits"));
        assert!(!is_pg_ignored_set(
            "INSERT INTO settings VALUES ('extra_float_digits')"
        ));
    }
}
