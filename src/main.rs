//! mallardb - PostgreSQL wire protocol compatible database powered by DuckDB
//!
//! mallardb presents a fully PostgreSQL-compatible interface to clients while
//! internally executing all queries against DuckDB.

// DuckDB Connection is not Send+Sync, but we create per-connection handlers
// that don't share state across threads, so this is safe.
#![allow(clippy::arc_with_non_send_sync)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use mallardb::backend::Backend;
use mallardb::config::Config;
use mallardb::handler::MallardbHandlerFactory;

use pgwire::tokio::process_socket;

/// mallardb - PostgreSQL-compatible interface over DuckDB
#[derive(Parser, Debug)]
#[command(name = "mallardb")]
#[command(about = "PostgreSQL-compatible interface over DuckDB", long_about = None)]
#[command(version)]
struct Args {
    /// Database file path (overrides MALLARDB_DATABASE)
    #[arg(short = 'd', long, value_name = "PATH")]
    database: Option<PathBuf>,

    /// Listen port (overrides MALLARDB_PORT)
    #[arg(short = 'p', long, value_name = "PORT")]
    port: Option<u16>,

    /// Listen host/address (overrides MALLARDB_HOST)
    #[arg(short = 'H', long, value_name = "HOST")]
    host: Option<String>,
}

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
        info!(
            "Running {} script: {:?}",
            description,
            script.file_name().unwrap_or_default()
        );

        let sql = match std::fs::read_to_string(&script) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to read script {:?}: {}", script, e);
                continue;
            }
        };

        // Execute the script - DuckDB handles multiple statements
        match backend.create_connection() {
            Ok(mut conn) => {
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

/// Load TLS certificates and create a TlsAcceptor
fn load_tls_config(config: &Config) -> Option<TlsAcceptor> {
    let cert_path = config.tls_cert_path.as_ref()?;
    let key_path = config.tls_key_path.as_ref()?;

    // Load certificate chain
    let cert_file = match std::fs::File::open(cert_path) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open TLS certificate file {:?}: {}", cert_path, e);
            return None;
        }
    };
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(|r| r.ok())
        .collect();

    if certs.is_empty() {
        error!("No valid certificates found in {:?}", cert_path);
        return None;
    }

    // Load private key
    let key_file = match std::fs::File::open(key_path) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open TLS key file {:?}: {}", key_path, e);
            return None;
        }
    };
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = match rustls_pemfile::private_key(&mut key_reader) {
        Ok(Some(key)) => key,
        Ok(None) => {
            error!("No private key found in {:?}", key_path);
            return None;
        }
        Err(e) => {
            error!("Failed to read private key from {:?}: {}", key_path, e);
            return None;
        }
    };

    // Build rustls ServerConfig
    let server_config = match rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
    {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to build TLS config: {}", e);
            return None;
        }
    };

    Some(TlsAcceptor::from(Arc::new(server_config)))
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env if present
    let _ = dotenvy::dotenv();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging - respects RUST_LOG env var, defaults to info
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    // Load configuration from env, then apply CLI overrides
    let config = match Config::from_env() {
        Ok(c) => Arc::new(c.with_cli_overrides(args.database, args.port, args.host)),
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            eprintln!();
            eprintln!("Required environment variables:");
            eprintln!("  MALLARDB_PASSWORD (or POSTGRES_PASSWORD)");
            eprintln!();
            eprintln!("Optional environment variables:");
            eprintln!("  MALLARDB_USER          - Username (default: mallard)");
            eprintln!("  MALLARDB_DB            - Database name (default: $MALLARDB_USER)");
            eprintln!("  MALLARDB_READONLY_USER - Username for read-only access");
            eprintln!("  MALLARDB_READONLY_PASSWORD - Password for read-only user");
            eprintln!("  MALLARDB_HOST          - Listen address (default: 0.0.0.0)");
            eprintln!("  MALLARDB_PORT          - Listen port (default: 5432)");
            eprintln!("  MALLARDB_DATABASE      - Database file path (default: ./data/mallard.db)");
            eprintln!();
            eprintln!("Note: POSTGRES_* variants are also accepted for compatibility.");
            std::process::exit(1);
        }
    };

    info!("Starting mallardb v0.1.0");
    info!("Database: {}", config.postgres_db);
    info!("Data path: {:?}", config.db_path());
    info!(
        "Read-only role: {}",
        if config.has_readonly_role() {
            "enabled"
        } else {
            "disabled"
        }
    );

    // Load TLS configuration if enabled
    let tls_acceptor = if config.tls_enabled() {
        match load_tls_config(&config) {
            Some(acceptor) => {
                info!("TLS enabled");
                Some(acceptor)
            }
            None => {
                error!("TLS configuration failed, exiting");
                std::process::exit(1);
            }
        }
    } else {
        info!("TLS disabled (set MALLARDB_TLS_CERT and MALLARDB_TLS_KEY to enable)");
        None
    };

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
                                let tls = tls_acceptor.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = process_socket(socket, tls, connection_handler).await {
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
