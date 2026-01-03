//! SQL rewriting for PostgreSQL to DuckDB compatibility
//!
//! Uses sqlparser-rs to parse SQL, transform the AST, and re-emit compatible SQL.
//! This provides robust handling compared to string-based replacements.

use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value, ValueWithSpan,
    visit_expressions_mut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::warn;

/// Rewrite SQL for DuckDB compatibility
/// Falls back to minimal string-based rewriting if parsing fails
pub fn rewrite_sql(sql: &str, database: &str) -> String {
    // Try AST-based rewriting first
    match rewrite_sql_ast(sql, database) {
        Ok(rewritten) => rewritten,
        Err(e) => {
            // Log warning and fall back to string-based rewriting
            warn!("SQL parser failed, using string fallback: {}", e);
            rewrite_sql_string(sql, database)
        }
    }
}

/// AST-based SQL rewriting
fn rewrite_sql_ast(sql: &str, database: &str) -> Result<String, sqlparser::parser::ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    for statement in &mut statements {
        // Check if statement contains PostgreSQL patterns that need full replacement
        if let Some(replacement) = get_statement_replacement(statement) {
            *statement = replacement;
            continue;
        }
        rewrite_statement(statement, database);
    }

    // Convert back to SQL
    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

/// Check if a statement needs complete replacement due to PostgreSQL-specific patterns
/// Returns a DuckDB-compatible replacement statement if needed
fn get_statement_replacement(statement: &Statement) -> Option<Statement> {
    // Only handle SELECT statements
    let Statement::Query(query) = statement else {
        return None;
    };

    // Check if this query uses PostgreSQL-specific table function patterns
    let pattern = analyze_query_pattern(query);

    if pattern.has_string_to_array_table_func && pattern.has_generate_series_table_func {
        // Grafana-style table discovery query - needs replacement
        if pattern.queries_information_schema_tables {
            return build_table_list_query();
        }
        if pattern.queries_information_schema_columns {
            return build_column_list_query();
        }
    }

    None
}

/// Pattern analysis result for a query
struct QueryPattern {
    has_string_to_array_table_func: bool,
    has_generate_series_table_func: bool,
    queries_information_schema_tables: bool,
    queries_information_schema_columns: bool,
}

/// Analyze a query to detect PostgreSQL-specific patterns
fn analyze_query_pattern(query: &Query) -> QueryPattern {
    let mut pattern = QueryPattern {
        has_string_to_array_table_func: false,
        has_generate_series_table_func: false,
        queries_information_schema_tables: false,
        queries_information_schema_columns: false,
    };

    if let SetExpr::Select(select) = query.body.as_ref() {
        analyze_select(&mut pattern, select);
    }

    pattern
}

/// Analyze a SELECT statement for patterns
fn analyze_select(pattern: &mut QueryPattern, select: &Select) {
    // Check FROM clause for table functions and tables
    for table_with_joins in &select.from {
        analyze_table_factor(pattern, &table_with_joins.relation);
        for join in &table_with_joins.joins {
            analyze_table_factor(pattern, &join.relation);
        }
    }

    // Also check subqueries in projection (for IN subqueries)
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            analyze_expr_for_subqueries(pattern, expr);
        }
    }

    // Check WHERE clause for subqueries
    if let Some(selection) = &select.selection {
        analyze_expr_for_subqueries(pattern, selection);
    }
}

/// Analyze a table factor (FROM clause item)
fn analyze_table_factor(pattern: &mut QueryPattern, factor: &TableFactor) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            let table_name = name.to_string().to_lowercase();

            // Check if it's a table function call
            if args.is_some() {
                if table_name == "string_to_array" {
                    pattern.has_string_to_array_table_func = true;
                } else if table_name == "generate_series" {
                    pattern.has_generate_series_table_func = true;
                }
            }

            // Check if querying information_schema
            if table_name == "information_schema.tables" {
                pattern.queries_information_schema_tables = true;
            } else if table_name == "information_schema.columns" {
                pattern.queries_information_schema_columns = true;
            }
        }
        TableFactor::Derived { subquery, .. } => {
            let sub_pattern = analyze_query_pattern(subquery);
            pattern.has_string_to_array_table_func |= sub_pattern.has_string_to_array_table_func;
            pattern.has_generate_series_table_func |= sub_pattern.has_generate_series_table_func;
            pattern.queries_information_schema_tables |=
                sub_pattern.queries_information_schema_tables;
            pattern.queries_information_schema_columns |=
                sub_pattern.queries_information_schema_columns;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            analyze_table_factor(pattern, &table_with_joins.relation);
            for join in &table_with_joins.joins {
                analyze_table_factor(pattern, &join.relation);
            }
        }
        _ => {}
    }
}

/// Analyze expressions for subqueries that might contain the patterns
fn analyze_expr_for_subqueries(pattern: &mut QueryPattern, expr: &Expr) {
    match expr {
        Expr::Subquery(query)
        | Expr::InSubquery {
            subquery: query, ..
        } => {
            let sub_pattern = analyze_query_pattern(query);
            pattern.has_string_to_array_table_func |= sub_pattern.has_string_to_array_table_func;
            pattern.has_generate_series_table_func |= sub_pattern.has_generate_series_table_func;
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                analyze_expr_for_subqueries(pattern, op);
            }
            for cond in conditions {
                analyze_expr_for_subqueries(pattern, &cond.condition);
                analyze_expr_for_subqueries(pattern, &cond.result);
            }
            if let Some(els) = else_result {
                analyze_expr_for_subqueries(pattern, els);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            analyze_expr_for_subqueries(pattern, left);
            analyze_expr_for_subqueries(pattern, right);
        }
        Expr::Nested(inner) => {
            analyze_expr_for_subqueries(pattern, inner);
        }
        _ => {}
    }
}

/// Build a DuckDB-compatible table list query
/// Uses parser to generate AST from SQL - cleaner than manual AST construction
fn build_table_list_query() -> Option<Statement> {
    Parser::parse_sql(
        &PostgreSqlDialect {},
        r#"SELECT
            CASE WHEN table_schema = 'main'
                THEN table_name
                ELSE table_schema || '.' || table_name
            END AS "table"
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY CASE WHEN table_schema = 'main' THEN 0 ELSE 1 END, table_name"#,
    )
    .ok()
    .and_then(|mut stmts| stmts.pop())
}

/// Build a DuckDB-compatible column list query
fn build_column_list_query() -> Option<Statement> {
    Parser::parse_sql(
        &PostgreSqlDialect {},
        r#"SELECT column_name AS "column", data_type AS "type"
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"#,
    )
    .ok()
    .and_then(|mut stmts| stmts.pop())
}

/// Rewrite a single statement
fn rewrite_statement(statement: &mut Statement, database: &str) {
    // Use the visitor pattern to traverse and modify expressions
    let database_owned = database.to_string();

    let _ = visit_expressions_mut(statement, |expr| {
        rewrite_expr(expr, &database_owned);
        std::ops::ControlFlow::<()>::Continue(())
    });
}

/// Rewrite an expression in place
fn rewrite_expr(expr: &mut Expr, database: &str) {
    // Check for function calls that need modification
    if let Expr::Function(func) = expr {
        let func_name = func.name.to_string().to_lowercase();

        // List of PostgreSQL functions that don't exist in DuckDB - replace with NULL
        let unsupported_functions = [
            "pg_get_partkeydef",
            "shobj_description",
            "pg_get_serial_sequence",
            "pg_get_function_identity_arguments",
            "pg_get_functiondef",
            "pg_get_function_result",
            "pg_get_triggerdef",
            "pg_get_ruledef",
            "pg_get_statisticsobjdef",
            "pg_tablespace_location",
            "pg_relation_filenode",
            "pg_filenode_relation",
        ];

        let base_name = func_name.strip_prefix("pg_catalog.").unwrap_or(&func_name);

        if unsupported_functions.contains(&base_name) {
            *expr = Expr::Value(Value::Null.into());
            return;
        }

        // pg_get_expr: DuckDB only supports 2 args, PostgreSQL has 3-arg version
        // pg_get_expr(expr, relid, pretty) -> pg_get_expr(expr, relid)
        if base_name == "pg_get_expr" {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut func.args
                && arg_list.args.len() == 3
            {
                arg_list.args.truncate(2);
            }
            return;
        }
    }

    match expr {
        // Handle type casts (including ::regclass)
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            // Check if it's a ::regclass cast - strip it
            if format!("{}", data_type).to_lowercase() == "regclass" {
                // Replace the cast with just the inner expression
                *expr = *inner.clone();
            }
        }
        // Handle string comparisons for schema/catalog mapping
        Expr::BinaryOp { left, right, .. } => {
            // Check for schema = 'public' patterns
            rewrite_schema_comparison(left, right);
            rewrite_schema_comparison(right, left);

            // Check for catalog = 'database' patterns
            rewrite_catalog_comparison(left, right, database);
            rewrite_catalog_comparison(right, left, database);
        }
        _ => {}
    }
}

/// Rewrite schema comparisons: 'public' -> 'main'
fn rewrite_schema_comparison(col_expr: &mut Expr, val_expr: &mut Expr) {
    // Check if column is a schema-related column
    let is_schema_col = match col_expr {
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            matches!(
                name.as_str(),
                "table_schema" | "schema_name" | "nspname" | "schemaname"
            )
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                let name = last.value.to_lowercase();
                matches!(
                    name.as_str(),
                    "table_schema" | "schema_name" | "nspname" | "schemaname"
                )
            } else {
                false
            }
        }
        _ => false,
    };

    if is_schema_col {
        // Replace 'public' with 'main'
        if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) = val_expr
            && s.to_lowercase() == "public"
        {
            *s = "main".to_string();
        }
    }
}

/// Rewrite catalog comparisons: database name -> 'data'
fn rewrite_catalog_comparison(col_expr: &mut Expr, val_expr: &mut Expr, database: &str) {
    // Check if column is a catalog-related column
    let is_catalog_col = match col_expr {
        Expr::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            matches!(name.as_str(), "table_catalog" | "catalog_name")
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                let name = last.value.to_lowercase();
                matches!(name.as_str(), "table_catalog" | "catalog_name")
            } else {
                false
            }
        }
        _ => false,
    };

    if is_catalog_col {
        // Replace database name with 'data'
        if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) = val_expr
            && s == database
        {
            *s = "data".to_string();
        }
    }
}

/// String-based SQL rewriting as fallback for unparseable SQL
///
/// This is only called when sqlparser fails to parse the SQL.
/// It provides basic transformations for edge cases but is not as robust as AST-based rewriting.
///
/// NOTE: If this function is being called frequently, consider fixing the parser or
/// handling those SQL patterns in the AST-based rewriter.
fn rewrite_sql_string(sql: &str, database: &str) -> String {
    let mut result = sql.to_string();

    // Replace 'public' schema references with 'main'
    let schema_replacements = [
        ("table_schema = 'public'", "table_schema = 'main'"),
        ("table_schema='public'", "table_schema='main'"),
        ("schema_name = 'public'", "schema_name = 'main'"),
        ("schema_name='public'", "schema_name='main'"),
        ("nspname = 'public'", "nspname = 'main'"),
        ("nspname='public'", "nspname='main'"),
        ("schemaname = 'public'", "schemaname = 'main'"),
        ("schemaname='public'", "schemaname='main'"),
        ("n.nspname = 'public'", "n.nspname = 'main'"),
    ];

    for (from, to) in schema_replacements {
        result = case_insensitive_replace(&result, from, to);
    }

    // Replace database/catalog name references with 'data'
    let catalog_replacements = [
        (
            format!("table_catalog = '{}'", database),
            "table_catalog = 'data'".to_string(),
        ),
        (
            format!("table_catalog='{}'", database),
            "table_catalog='data'".to_string(),
        ),
        (
            format!("catalog_name = '{}'", database),
            "catalog_name = 'data'".to_string(),
        ),
        (
            format!("catalog_name='{}'", database),
            "catalog_name='data'".to_string(),
        ),
    ];

    for (from, to) in catalog_replacements {
        result = case_insensitive_replace(&result, &from, &to);
    }

    // Remove ::regclass casts
    while let Some(pos) = result.find("::regclass") {
        result = format!("{}{}", &result[..pos], &result[pos + 10..]);
    }

    // Replace unsupported functions with NULL
    result = replace_function_with_null(&result, "pg_catalog.pg_get_partkeydef");
    result = replace_function_with_null(&result, "pg_get_partkeydef");
    result = replace_function_with_null(&result, "pg_catalog.shobj_description");
    result = replace_function_with_null(&result, "shobj_description");

    result
}

fn case_insensitive_replace(input: &str, from: &str, to: &str) -> String {
    let lower_input = input.to_lowercase();
    let lower_from = from.to_lowercase();

    if let Some(pos) = lower_input.find(&lower_from) {
        let mut result = String::new();
        result.push_str(&input[..pos]);
        result.push_str(to);
        result.push_str(&input[pos + from.len()..]);
        case_insensitive_replace(&result, from, to)
    } else {
        input.to_string()
    }
}

fn replace_function_with_null(sql: &str, func_name: &str) -> String {
    let func_lower = func_name.to_lowercase();
    let mut result = sql.to_string();

    loop {
        let lower = result.to_lowercase();
        if let Some(func_pos) = lower.find(&func_lower)
            && let Some(paren_start) = result[func_pos..].find('(')
        {
            let paren_abs = func_pos + paren_start;
            if let Some(paren_end) = find_matching_paren(&result[paren_abs..]) {
                let end_abs = paren_abs + paren_end + 1;
                result = format!("{}NULL{}", &result[..func_pos], &result[end_abs..]);
                continue;
            }
        }
        break;
    }

    result
}

fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_public_to_main() {
        let sql = "SELECT * FROM t WHERE table_schema = 'public'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.to_lowercase().contains("'main'"));
        assert!(!result.to_lowercase().contains("'public'"));
    }

    #[test]
    fn test_rewrite_regclass_cast() {
        let sql = "SELECT 'pg_class'::regclass";
        let result = rewrite_sql(sql, "mydb");
        assert!(!result.contains("::regclass"));
        assert!(result.contains("'pg_class'"));
    }

    #[test]
    fn test_rewrite_pg_get_partkeydef() {
        let sql = "SELECT pg_get_partkeydef(c.oid) FROM pg_class c";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.to_lowercase().contains("null"));
        assert!(!result.to_lowercase().contains("pg_get_partkeydef"));
    }

    #[test]
    fn test_rewrite_catalog_qualified_function() {
        let sql = "SELECT pg_catalog.pg_get_partkeydef(c.oid) FROM pg_class c";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.to_lowercase().contains("null"));
    }

    #[test]
    fn test_rewrite_catalog_name() {
        let sql = "SELECT * FROM t WHERE table_catalog = 'mydb'";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("'data'"));
    }

    #[test]
    fn test_rewrite_preserves_valid_sql() {
        let sql = "SELECT id, name FROM users WHERE active = true";
        let result = rewrite_sql(sql, "mydb");
        // Should be essentially unchanged
        assert!(result.to_lowercase().contains("select"));
        assert!(result.to_lowercase().contains("users"));
    }

    #[test]
    fn test_string_fallback_regclass() {
        // Test the string-based fallback directly
        let sql = "SELECT 'foo'::regclass";
        let result = rewrite_sql_string(sql, "mydb");
        assert!(!result.contains("::regclass"));
    }

    #[test]
    fn test_string_fallback_function() {
        let sql = "SELECT pg_get_partkeydef(123)";
        let result = rewrite_sql_string(sql, "mydb");
        assert!(result.contains("NULL"));
    }

    #[test]
    fn test_rewrite_pg_get_expr_three_args() {
        // DuckDB only supports 2 args, PostgreSQL has 3-arg version with pretty print bool
        let sql = "SELECT pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) AS def_value FROM pg_attrdef ad";
        let result = rewrite_sql(sql, "mydb");
        // Should have removed the third argument
        assert!(result.contains("pg_get_expr(ad.adbin, ad.adrelid)"));
        assert!(!result.contains("true"));
    }

    #[test]
    fn test_rewrite_pg_get_expr_two_args_unchanged() {
        // 2-arg version should be unchanged
        let sql = "SELECT pg_get_expr(ad.adbin, ad.adrelid) FROM pg_attrdef ad";
        let result = rewrite_sql(sql, "mydb");
        assert!(result.contains("pg_get_expr(ad.adbin, ad.adrelid)"));
    }
}
