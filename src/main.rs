//! mallardb - PostgreSQL wire protocol compatible database powered by DuckDB
//!
//! mallardb presents a fully PostgreSQL-compatible interface to clients while
//! internally executing all queries against DuckDB.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use mallardb::backend::Backend;
use mallardb::config::Config;
use mallardb::handler::MallardbHandlerFactory;

use pgwire::tokio::process_socket;

/// Run SQL scripts from a directory in sorted order
async fn run_scripts(backend: &Backend, dir: &Path, description: &str) {
    if !dir.exists() {
        return;
    }

    let mut scripts: Vec<_> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "sql").unwrap_or(false))
            .collect(),
        Err(e) => {
            warn!("Failed to read {} directory {:?}: {}", description, dir, e);
            return;
        }
    };

    scripts.sort();

    for script in scripts {
        info!("Running {} script: {:?}", description, script.file_name().unwrap_or_default());

        let sql = match std::fs::read_to_string(&script) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to read script {:?}: {}", script, e);
                continue;
            }
        };

        // Execute the script - DuckDB handles multiple statements
        match backend.create_connection() {
            Ok(conn) => {
                if let Err(e) = conn.execute(&sql) {
                    error!("Failed to execute script {:?}: {}", script, e);
                }
            }
            Err(e) => {
                error!("Failed to create connection for script {:?}: {}", script, e);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env if present
    let _ = dotenvy::dotenv();

    // Initialize logging - respects RUST_LOG env var, defaults to info
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    // Load configuration
    let config = match Config::from_env() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            eprintln!("");
            eprintln!("Required environment variables:");
            eprintln!("  MALLARDB_PASSWORD (or POSTGRES_PASSWORD)");
            eprintln!("");
            eprintln!("Optional environment variables:");
            eprintln!("  MALLARDB_USER          - Username (default: mallard)");
            eprintln!("  MALLARDB_DB            - Database name (default: $MALLARDB_USER)");
            eprintln!("  MALLARDB_READONLY_USER - Username for read-only access");
            eprintln!("  MALLARDB_READONLY_PASSWORD - Password for read-only user");
            eprintln!("  MALLARDB_HOST          - Listen address (default: 0.0.0.0)");
            eprintln!("  MALLARDB_PORT          - Listen port (default: 5432)");
            eprintln!("  MALLARDB_DATA_DIR      - Data directory (default: ./data)");
            eprintln!("");
            eprintln!("Note: POSTGRES_* variants are also accepted for compatibility.");
            std::process::exit(1);
        }
    };

    info!("Starting mallardb v0.1.0");
    info!("Database: {}", config.postgres_db);
    info!("Data path: {:?}", config.db_path());
    info!("Read-only role: {}", if config.has_readonly_role() { "enabled" } else { "disabled" });

    // Check if this is first start (database doesn't exist)
    let is_first_start = !config.db_path().exists();

    // Create backend
    let backend = Arc::new(Backend::new(config.clone()));

    // Run init scripts (following PostgreSQL Docker conventions)
    let initdb_dir = std::env::var("MALLARDB_INITDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/docker-entrypoint-initdb.d"));
    let startdb_dir = std::env::var("MALLARDB_STARTDB_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/docker-entrypoint-startdb.d"));

    if is_first_start {
        run_scripts(&backend, &initdb_dir, "initdb").await;
    }
    run_scripts(&backend, &startdb_dir, "startdb").await;

    // Create handler factory
    let factory = Arc::new(MallardbHandlerFactory::new(backend, config.clone()));

    // Start listening
    let listen_addr = config.listen_addr();
    let listener = match TcpListener::bind(&listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", listen_addr, e);
            std::process::exit(1);
        }
    };

    info!("Listening on {}", listen_addr);
    info!("Ready to accept connections (Ctrl+C to shutdown)");

    // Shutdown flag
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        info!("Shutdown signal received, stopping...");
        shutdown_clone.store(true, Ordering::SeqCst);
    });

    // Accept connections until shutdown
    while !shutdown.load(Ordering::SeqCst) {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!("New connection from {}", addr);
                        // Create a per-connection handler that shares the same DuckDB connection
                        // for both simple and extended query protocols
                        match factory.create_connection_handler() {
                            Ok(connection_handler) => {
                                let connection_handler = Arc::new(connection_handler);
                                tokio::spawn(async move {
                                    if let Err(e) = process_socket(socket, None, connection_handler).await {
                                        error!("Connection error from {}: {:?}", addr, e);
                                    }
                                    info!("Connection closed from {}", addr);
                                });
                            }
                            Err(e) => {
                                error!("Failed to create handler for {}: {}", addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Check shutdown flag periodically
            }
        }
    }

    info!("mallardb shutdown complete");
}
