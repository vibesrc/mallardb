//! Configuration management for mallardb
//!
//! Reads configuration from environment variables following PostgreSQL Docker conventions.

use std::path::PathBuf;

/// Server configuration
#[derive(Debug, Clone)]
pub struct Config {
    // Authentication
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_readonly_user: Option<String>,
    pub postgres_readonly_password: Option<String>,
    pub postgres_db: String,

    // Network
    pub host: String,
    pub port: u16,

    // Data storage
    pub database: PathBuf,
    pub extension_directory: PathBuf,

    // Performance
    pub max_readers: usize,
    pub writer_queue_size: usize,
    pub batch_size: usize,
    pub query_timeout_ms: u64,

    // Compatibility
    pub pg_version: String,

    // Logging
    pub log_level: String,
    pub log_queries: bool,

    // TLS (optional - enabled when both cert and key are provided)
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,

    // Jobs scheduler
    pub jobs_dir: PathBuf,
}

impl Config {
    /// Load configuration from environment variables
    /// Supports both POSTGRES_* and MALLARDB_* prefixes (MALLARDB_* takes precedence)
    pub fn from_env() -> Result<Self, ConfigError> {
        let postgres_user = std::env::var("MALLARDB_USER")
            .or_else(|_| std::env::var("POSTGRES_USER"))
            .unwrap_or_else(|_| "mallard".to_string());
        let postgres_password = std::env::var("MALLARDB_PASSWORD")
            .or_else(|_| std::env::var("POSTGRES_PASSWORD"))
            .map_err(|_| ConfigError::MissingRequired("POSTGRES_PASSWORD or MALLARDB_PASSWORD"))?;

        let postgres_db = std::env::var("MALLARDB_DB")
            .or_else(|_| std::env::var("POSTGRES_DB"))
            .unwrap_or_else(|_| postgres_user.clone());

        let postgres_readonly_user = std::env::var("MALLARDB_READONLY_USER")
            .or_else(|_| std::env::var("POSTGRES_READONLY_USER"))
            .ok();
        let postgres_readonly_password = std::env::var("MALLARDB_READONLY_PASSWORD")
            .or_else(|_| std::env::var("POSTGRES_READONLY_PASSWORD"))
            .ok();

        let host = std::env::var("MALLARDB_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("MALLARDB_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse()
            .unwrap_or(5432);

        let database = std::env::var("MALLARDB_DATABASE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/mallard.db"));

        let extension_directory = std::env::var("MALLARDB_EXTENSION_DIRECTORY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./extensions"));

        let max_readers = std::env::var("MALLARDB_MAX_READERS")
            .unwrap_or_else(|_| "64".to_string())
            .parse()
            .unwrap_or(64);
        let writer_queue_size = std::env::var("MALLARDB_WRITER_QUEUE_SIZE")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000);
        let batch_size = std::env::var("MALLARDB_BATCH_SIZE")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000);
        let query_timeout_ms = std::env::var("MALLARDB_QUERY_TIMEOUT_MS")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .unwrap_or(0);

        let pg_version =
            std::env::var("MALLARDB_PG_VERSION").unwrap_or_else(|_| "15.0".to_string());

        let log_level = std::env::var("MALLARDB_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
        let log_queries = std::env::var("MALLARDB_LOG_QUERIES")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let tls_cert_path = std::env::var("MALLARDB_TLS_CERT").ok().map(PathBuf::from);
        let tls_key_path = std::env::var("MALLARDB_TLS_KEY").ok().map(PathBuf::from);

        let jobs_dir = std::env::var("MALLARDB_JOBS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./jobs"));

        Ok(Config {
            postgres_user,
            postgres_password,
            postgres_readonly_user,
            postgres_readonly_password,
            postgres_db,
            host,
            port,
            database,
            extension_directory,
            max_readers,
            writer_queue_size,
            batch_size,
            query_timeout_ms,
            pg_version,
            log_level,
            log_queries,
            tls_cert_path,
            tls_key_path,
            jobs_dir,
        })
    }

    /// Get the full path to the database file
    pub fn db_path(&self) -> &PathBuf {
        &self.database
    }

    /// Get the server listen address
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Check if readonly role is configured
    pub fn has_readonly_role(&self) -> bool {
        self.postgres_readonly_user.is_some() && self.postgres_readonly_password.is_some()
    }

    /// Check if TLS is enabled (both cert and key paths are provided)
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }

    /// Apply CLI overrides to the configuration
    pub fn with_cli_overrides(
        mut self,
        database: Option<std::path::PathBuf>,
        port: Option<u16>,
        host: Option<String>,
    ) -> Self {
        if let Some(db) = database {
            self.database = db;
        }
        if let Some(p) = port {
            self.port = p;
        }
        if let Some(h) = host {
            self.host = h;
        }
        self
    }
}

#[derive(Debug)]
pub enum ConfigError {
    MissingRequired(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::MissingRequired(var) => {
                write!(f, "Required environment variable {} is not set", var)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    // SAFETY: These tests run serially due to env var manipulation.
    // In Rust 2024, set_var/remove_var are unsafe due to potential
    // race conditions with other threads reading env vars.

    unsafe fn clear_env_vars() {
        // SAFETY: Caller ensures no concurrent access to environment variables
        unsafe {
            for var in [
                "MALLARDB_USER",
                "MALLARDB_PASSWORD",
                "MALLARDB_DB",
                "MALLARDB_READONLY_USER",
                "MALLARDB_READONLY_PASSWORD",
                "MALLARDB_HOST",
                "MALLARDB_PORT",
                "MALLARDB_DATABASE",
                "POSTGRES_USER",
                "POSTGRES_PASSWORD",
                "POSTGRES_DB",
                "POSTGRES_READONLY_USER",
                "POSTGRES_READONLY_PASSWORD",
            ] {
                env::remove_var(var);
            }
        }
    }

    #[test]
    fn test_config_requires_password() {
        unsafe {
            clear_env_vars();
        }
        let result = Config::from_env();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::MissingRequired(_)
        ));
    }

    #[test]
    fn test_config_with_postgres_password() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "secret123");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_password, "secret123");
        assert_eq!(config.postgres_user, "mallard");
        assert_eq!(config.postgres_db, "mallard");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_config_with_mallardb_password() {
        unsafe {
            clear_env_vars();
            env::set_var("MALLARDB_PASSWORD", "mallardsecret");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_password, "mallardsecret");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_mallardb_prefix_takes_precedence() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "pg_pass");
            env::set_var("MALLARDB_PASSWORD", "mallard_pass");
            env::set_var("POSTGRES_USER", "pg_user");
            env::set_var("MALLARDB_USER", "mallard_user");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_password, "mallard_pass");
        assert_eq!(config.postgres_user, "mallard_user");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_config_defaults() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_user, "mallard");
        assert_eq!(config.postgres_db, "mallard");
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, PathBuf::from("./data/mallard.db"));
        assert_eq!(config.max_readers, 64);
        assert_eq!(config.pg_version, "15.0");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_config_custom_user_and_db() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
            env::set_var("POSTGRES_USER", "myuser");
            env::set_var("POSTGRES_DB", "mydb");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_user, "myuser");
        assert_eq!(config.postgres_db, "mydb");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_config_db_defaults_to_user() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
            env::set_var("POSTGRES_USER", "customuser");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres_user, "customuser");
        assert_eq!(config.postgres_db, "customuser");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_readonly_role_disabled_by_default() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
        }

        let config = Config::from_env().unwrap();
        assert!(!config.has_readonly_role());
        assert!(config.postgres_readonly_user.is_none());
        assert!(config.postgres_readonly_password.is_none());

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_readonly_role_enabled() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
            env::set_var("POSTGRES_READONLY_USER", "reader");
            env::set_var("POSTGRES_READONLY_PASSWORD", "readerpass");
        }

        let config = Config::from_env().unwrap();
        assert!(config.has_readonly_role());
        assert_eq!(config.postgres_readonly_user, Some("reader".to_string()));
        assert_eq!(
            config.postgres_readonly_password,
            Some("readerpass".to_string())
        );

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_db_path() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
            env::set_var("MALLARDB_DATABASE", "/custom/path/my.db");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.db_path(), &PathBuf::from("/custom/path/my.db"));

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_listen_addr() {
        unsafe {
            clear_env_vars();
            env::set_var("POSTGRES_PASSWORD", "test");
            env::set_var("MALLARDB_HOST", "127.0.0.1");
            env::set_var("MALLARDB_PORT", "5433");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.listen_addr(), "127.0.0.1:5433");

        unsafe {
            clear_env_vars();
        }
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::MissingRequired("TEST_VAR");
        assert!(err.to_string().contains("TEST_VAR"));
    }
}
