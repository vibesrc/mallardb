//! Integration tests for mallardb
//!
//! These tests verify the full server functionality using a real PostgreSQL client.
//! They require the server to be started externally or use a test harness.

use std::sync::Once;
use std::time::Duration;
use tokio_postgres::{Config, NoTls};

static INIT: Once = Once::new();

/// Load .env file once
fn init_env() {
    INIT.call_once(|| {
        let _ = dotenvy::dotenv();
    });
}

/// Get test configuration from environment variables
fn get_test_config() -> (String, String, String, u16) {
    init_env();
    let user = std::env::var("MALLARDB_USER")
        .or_else(|_| std::env::var("POSTGRES_USER"))
        .unwrap_or_else(|_| "mallard".to_string());
    let password = std::env::var("MALLARDB_PASSWORD")
        .or_else(|_| std::env::var("POSTGRES_PASSWORD"))
        .expect("MALLARDB_PASSWORD or POSTGRES_PASSWORD must be set for integration tests");
    let database = std::env::var("MALLARDB_DB")
        .or_else(|_| std::env::var("POSTGRES_DB"))
        .unwrap_or_else(|_| user.clone());
    let port = std::env::var("MALLARDB_PORT")
        .unwrap_or_else(|_| "5432".to_string())
        .parse()
        .unwrap_or(5432);
    (user, password, database, port)
}

/// Helper to create a client configuration
fn create_client_config(user: &str, password: &str, database: &str, port: u16) -> Config {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.user(user);
    config.password(password);
    config.dbname(database);
    config.connect_timeout(Duration::from_secs(5));
    config
}

/// Helper to create client config from environment
fn create_test_client_config() -> Config {
    let (user, password, database, port) = get_test_config();
    create_client_config(&user, &password, &database, port)
}

/// These tests are marked as ignored by default because they require
/// a running mallardb server. Run them with:
///
/// ```bash
/// MALLARDB_PASSWORD=yourpassword cargo run &
/// MALLARDB_PASSWORD=yourpassword cargo test --test integration_test -- --ignored
/// ```
mod server_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_basic_connection() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Simple query
        let rows = client
            .query("SELECT 1 as num", &[])
            .await
            .expect("Query failed");
        assert_eq!(rows.len(), 1);
        let num: i32 = rows[0].get("num");
        assert_eq!(num, 1);
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_version_query() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let rows = client
            .query("SELECT version()", &[])
            .await
            .expect("Query failed");
        assert_eq!(rows.len(), 1);
        let version: &str = rows[0].get(0);
        assert!(version.contains("PostgreSQL"));
        assert!(version.contains("mallardb"));
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_current_database() {
        let (_, _, expected_db, _) = get_test_config();
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let rows = client
            .query("SELECT current_database()", &[])
            .await
            .expect("Query failed");
        assert_eq!(rows.len(), 1);
        let db: &str = rows[0].get(0);
        assert_eq!(db, expected_db);
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_create_table_and_insert() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Clean up from any previous runs
        let _ = client.execute("DROP TABLE IF EXISTS test_users", &[]).await;

        // Create table
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name VARCHAR)",
                &[],
            )
            .await
            .expect("Create table failed");

        // Insert data
        client
            .execute("INSERT INTO test_users VALUES (1, 'Alice')", &[])
            .await
            .expect("Insert failed");

        // Query data
        let rows = client
            .query("SELECT * FROM test_users WHERE id = 1", &[])
            .await
            .expect("Query failed");

        assert_eq!(rows.len(), 1);
        let id: i32 = rows[0].get("id");
        let name: &str = rows[0].get("name");
        assert_eq!(id, 1);
        assert_eq!(name, "Alice");

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS test_users", &[])
            .await
            .expect("Drop table failed");
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_transaction() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Clean up from any previous runs
        let _ = client.execute("DROP TABLE IF EXISTS tx_test", &[]).await;

        // Create table
        client
            .execute("CREATE TABLE IF NOT EXISTS tx_test (id INTEGER)", &[])
            .await
            .expect("Create table failed");

        // Start transaction
        client.execute("BEGIN", &[]).await.expect("BEGIN failed");
        client
            .execute("INSERT INTO tx_test VALUES (1)", &[])
            .await
            .expect("Insert failed");
        client.execute("COMMIT", &[]).await.expect("COMMIT failed");

        // Verify data
        let rows = client
            .query("SELECT * FROM tx_test", &[])
            .await
            .expect("Query failed");
        assert_eq!(rows.len(), 1);

        // Test rollback
        client.execute("BEGIN", &[]).await.expect("BEGIN failed");
        client
            .execute("INSERT INTO tx_test VALUES (2)", &[])
            .await
            .expect("Insert failed");
        client
            .execute("ROLLBACK", &[])
            .await
            .expect("ROLLBACK failed");

        // Verify rollback worked
        let rows = client
            .query("SELECT * FROM tx_test", &[])
            .await
            .expect("Query failed");
        assert_eq!(rows.len(), 1); // Still only 1 row

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS tx_test", &[])
            .await
            .expect("Drop table failed");
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_data_types() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Test various data types
        let rows = client
            .query(
                "SELECT
                    42::INTEGER as int_val,
                    1.5::DOUBLE as float_val,
                    true as bool_val,
                    'hello' as text_val",
                &[],
            )
            .await
            .expect("Query failed");

        assert_eq!(rows.len(), 1);
        let int_val: i32 = rows[0].get("int_val");
        let float_val: f64 = rows[0].get("float_val");
        let bool_val: bool = rows[0].get("bool_val");
        let text_val: &str = rows[0].get("text_val");

        assert_eq!(int_val, 42);
        assert!((float_val - 1.5).abs() < 0.001);
        assert!(bool_val);
        assert_eq!(text_val, "hello");
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_pg_type_catalog() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let rows = client
            .query("SELECT oid, typname FROM pg_type LIMIT 5", &[])
            .await
            .expect("Query failed");

        assert!(!rows.is_empty());
        // Verify we get type information
        for row in rows {
            let _oid: i32 = row.get("oid");
            let _typname: &str = row.get("typname");
        }
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_information_schema() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Clean up from any previous runs
        let _ = client
            .execute("DROP TABLE IF EXISTS info_schema_test", &[])
            .await;

        // Create a test table first
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS info_schema_test (id INTEGER)",
                &[],
            )
            .await
            .expect("Create table failed");

        // Query information_schema
        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'info_schema_test'",
                &[],
            )
            .await
            .expect("Query failed");

        assert!(!rows.is_empty());

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS info_schema_test", &[])
            .await
            .expect("Drop table failed");
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_error_handling() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Test syntax error
        let result = client.query("SELEC * FROM nonexistent", &[]).await;
        assert!(result.is_err());

        // Test undefined table
        let result = client
            .query("SELECT * FROM nonexistent_table_xyz", &[])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_auto_rollback_after_error() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Clean up from any previous runs
        let _ = client
            .execute("DROP TABLE IF EXISTS rollback_test", &[])
            .await;

        // Create table
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS rollback_test (id INTEGER PRIMARY KEY)",
                &[],
            )
            .await
            .expect("Create table failed");

        // Start transaction
        client.execute("BEGIN", &[]).await.expect("BEGIN failed");

        // Cause an error (syntax error)
        let _ = client.query("SELEC invalid", &[]).await;

        // The auto-rollback should have happened, so we should be able to continue
        // without "current transaction is aborted" errors
        let result = client.query("SELECT 1 as num", &[]).await;
        assert!(
            result.is_ok(),
            "Should be able to query after auto-rollback"
        );

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS rollback_test", &[])
            .await
            .expect("Drop table failed");
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_multiple_statements() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Run multiple queries in sequence
        for i in 1..=10 {
            let rows = client
                .query(&format!("SELECT {} as num", i), &[])
                .await
                .expect("Query failed");
            let num: i32 = rows[0].get("num");
            assert_eq!(num, i);
        }
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_null_values() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let rows = client
            .query("SELECT NULL as null_val, 1 as not_null", &[])
            .await
            .expect("Query failed");

        assert_eq!(rows.len(), 1);
        let null_val: Option<i32> = rows[0].get("null_val");
        let not_null: i32 = rows[0].get("not_null");
        assert!(null_val.is_none());
        assert_eq!(not_null, 1);
    }

    #[tokio::test]
    #[ignore = "requires running server"]
    async fn test_aggregate_functions() {
        let config = create_test_client_config();
        let (client, connection) = config.connect(NoTls).await.expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Clean up from any previous runs
        let _ = client.execute("DROP TABLE IF EXISTS agg_test", &[]).await;

        // Create and populate test table
        client
            .execute("CREATE TABLE IF NOT EXISTS agg_test (value INTEGER)", &[])
            .await
            .expect("Create table failed");

        client
            .execute("INSERT INTO agg_test VALUES (1), (2), (3), (4), (5)", &[])
            .await
            .expect("Insert failed");

        // Test aggregates
        let rows = client
            .query(
                "SELECT COUNT(*) as cnt, SUM(value) as total, AVG(value) as avg_val FROM agg_test",
                &[],
            )
            .await
            .expect("Query failed");

        assert_eq!(rows.len(), 1);
        let cnt: i64 = rows[0].get("cnt");
        assert_eq!(cnt, 5);

        // Cleanup
        client
            .execute("DROP TABLE IF EXISTS agg_test", &[])
            .await
            .expect("Drop table failed");
    }
}

/// Tests that don't require a running server
mod unit_like_tests {
    use super::*;

    #[test]
    fn test_client_config_creation() {
        let config = create_client_config("user", "pass", "db", 5432);
        // Just verify it doesn't panic
        let _ = format!("{:?}", config);
    }

    #[test]
    fn test_client_config_custom_port() {
        let config = create_client_config("user", "pass", "db", 5433);
        let _ = format!("{:?}", config);
    }
}
