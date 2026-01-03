//! Authentication handling for mallardb
//!
//! Implements PostgreSQL-compatible authentication with cleartext password.

use async_trait::async_trait;
use std::sync::Arc;

use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;

use crate::config::Config;

/// Role type for routing decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleType {
    /// Read-write role - queries go through writer queue
    Write,
    /// Read-only role - gets dedicated DuckDB reader
    Read,
}

/// Authentication source backed by environment configuration
#[derive(Debug)]
pub struct MallardbAuthSource {
    config: Arc<Config>,
}

impl MallardbAuthSource {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Validate user and return their role type
    pub fn validate_user(&self, username: &str) -> Option<RoleType> {
        if username == self.config.postgres_user {
            return Some(RoleType::Write);
        }

        if let Some(ref readonly_user) = self.config.postgres_readonly_user
            && username == readonly_user
        {
            return Some(RoleType::Read);
        }

        None
    }

    /// Get the password for a user
    pub fn get_password_for_user(&self, username: &str) -> Option<&str> {
        if username == self.config.postgres_user {
            return Some(&self.config.postgres_password);
        }

        if let Some(ref readonly_user) = self.config.postgres_readonly_user
            && username == readonly_user
        {
            return self.config.postgres_readonly_password.as_deref();
        }

        None
    }
}

#[async_trait]
impl AuthSource for MallardbAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        use rand::Rng;

        let username = login_info.user().unwrap_or_default();

        tracing::debug!("Auth request for user: {}", username);

        // Check if user exists
        let password = self.get_password_for_user(username).unwrap_or("");

        // Generate random 4-byte salt
        let mut rng = rand::rng();
        let salt: Vec<u8> = (0..4).map(|_| rng.random::<u8>()).collect();

        // Compute the expected MD5 hash that client will send
        // Format: md5(md5(password + username) + salt)
        let hash = hash_md5_password(username, password, &salt);

        tracing::debug!(
            "Auth: user={}, salt={:?}, hash_len={}",
            username,
            salt,
            hash.len()
        );

        Ok(Password::new(Some(salt), hash.as_bytes().to_vec()))
    }
}

/// Session information after successful authentication
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub username: String,
    pub role: RoleType,
    pub database: String,
    pub application_name: Option<String>,
    pub process_id: i32,
    pub secret_key: i32,
}

impl SessionInfo {
    pub fn new(username: String, role: RoleType, database: String) -> Self {
        use rand::Rng;
        let mut rng = rand::rng();

        Self {
            username,
            role,
            database,
            application_name: None,
            process_id: rng.random::<i32>().abs(),
            secret_key: rng.random::<i32>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn create_test_config(
        user: &str,
        password: &str,
        readonly_user: Option<&str>,
        readonly_password: Option<&str>,
    ) -> Config {
        Config {
            postgres_user: user.to_string(),
            postgres_password: password.to_string(),
            postgres_readonly_user: readonly_user.map(|s| s.to_string()),
            postgres_readonly_password: readonly_password.map(|s| s.to_string()),
            postgres_db: "testdb".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: PathBuf::from("./data/mallard.db"),
            extension_directory: PathBuf::from("./extensions"),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
        }
    }

    // ===== RoleType tests =====

    #[test]
    fn test_role_type_equality() {
        assert_eq!(RoleType::Write, RoleType::Write);
        assert_eq!(RoleType::Read, RoleType::Read);
        assert_ne!(RoleType::Write, RoleType::Read);
    }

    #[test]
    fn test_role_type_clone() {
        let role = RoleType::Write;
        let cloned = role;
        assert_eq!(role, cloned);
    }

    #[test]
    fn test_role_type_copy() {
        let role = RoleType::Read;
        let copied: RoleType = role; // Copy trait
        assert_eq!(role, copied);
    }

    #[test]
    fn test_role_type_debug() {
        assert_eq!(format!("{:?}", RoleType::Write), "Write");
        assert_eq!(format!("{:?}", RoleType::Read), "Read");
    }

    // ===== MallardbAuthSource tests =====

    #[test]
    fn test_auth_source_new() {
        let config = Arc::new(create_test_config("admin", "secret", None, None));
        let auth = MallardbAuthSource::new(config.clone());
        assert!(format!("{:?}", auth).contains("MallardbAuthSource"));
    }

    #[test]
    fn test_validate_user_write_role() {
        let config = Arc::new(create_test_config("admin", "secret", None, None));
        let auth = MallardbAuthSource::new(config);

        let role = auth.validate_user("admin");
        assert_eq!(role, Some(RoleType::Write));
    }

    #[test]
    fn test_validate_user_read_role() {
        let config = Arc::new(create_test_config(
            "admin",
            "secret",
            Some("reader"),
            Some("readerpass"),
        ));
        let auth = MallardbAuthSource::new(config);

        let role = auth.validate_user("reader");
        assert_eq!(role, Some(RoleType::Read));
    }

    #[test]
    fn test_validate_user_unknown() {
        let config = Arc::new(create_test_config("admin", "secret", None, None));
        let auth = MallardbAuthSource::new(config);

        let role = auth.validate_user("unknown");
        assert_eq!(role, None);
    }

    #[test]
    fn test_validate_user_case_sensitive() {
        let config = Arc::new(create_test_config("Admin", "secret", None, None));
        let auth = MallardbAuthSource::new(config);

        // Usernames are case-sensitive
        assert_eq!(auth.validate_user("Admin"), Some(RoleType::Write));
        assert_eq!(auth.validate_user("admin"), None);
        assert_eq!(auth.validate_user("ADMIN"), None);
    }

    #[test]
    fn test_get_password_for_write_user() {
        let config = Arc::new(create_test_config("admin", "secret123", None, None));
        let auth = MallardbAuthSource::new(config);

        let password = auth.get_password_for_user("admin");
        assert_eq!(password, Some("secret123"));
    }

    #[test]
    fn test_get_password_for_read_user() {
        let config = Arc::new(create_test_config(
            "admin",
            "secret",
            Some("reader"),
            Some("readerpass"),
        ));
        let auth = MallardbAuthSource::new(config);

        let password = auth.get_password_for_user("reader");
        assert_eq!(password, Some("readerpass"));
    }

    #[test]
    fn test_get_password_for_unknown_user() {
        let config = Arc::new(create_test_config("admin", "secret", None, None));
        let auth = MallardbAuthSource::new(config);

        let password = auth.get_password_for_user("unknown");
        assert_eq!(password, None);
    }

    #[test]
    fn test_readonly_user_without_password() {
        // If readonly user exists but no password, get_password_for_user returns None
        let config = Arc::new(Config {
            postgres_user: "admin".to_string(),
            postgres_password: "secret".to_string(),
            postgres_readonly_user: Some("reader".to_string()),
            postgres_readonly_password: None, // No password set
            postgres_db: "testdb".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: PathBuf::from("./data/mallard.db"),
            extension_directory: PathBuf::from("./extensions"),
            max_readers: 64,
            writer_queue_size: 1000,
            batch_size: 1000,
            query_timeout_ms: 0,
            pg_version: "15.0".to_string(),
            log_level: "info".to_string(),
            log_queries: false,
        });
        let auth = MallardbAuthSource::new(config);

        // User exists for validation
        assert_eq!(auth.validate_user("reader"), Some(RoleType::Read));
        // But password is None
        assert_eq!(auth.get_password_for_user("reader"), None);
    }

    // ===== SessionInfo tests =====

    #[test]
    fn test_session_info_new() {
        let session = SessionInfo::new("testuser".to_string(), RoleType::Write, "mydb".to_string());

        assert_eq!(session.username, "testuser");
        assert_eq!(session.role, RoleType::Write);
        assert_eq!(session.database, "mydb");
        assert!(session.application_name.is_none());
        assert!(session.process_id >= 0);
    }

    #[test]
    fn test_session_info_unique_ids() {
        let session1 = SessionInfo::new("user1".to_string(), RoleType::Write, "db".to_string());
        let session2 = SessionInfo::new("user2".to_string(), RoleType::Read, "db".to_string());

        // Process IDs and secret keys should be different (random)
        // Note: There's a tiny chance they could be the same, but it's extremely unlikely
        assert!(
            session1.process_id != session2.process_id
                || session1.secret_key != session2.secret_key,
            "Sessions should have different random identifiers"
        );
    }

    #[test]
    fn test_session_info_clone() {
        let session = SessionInfo::new("user".to_string(), RoleType::Read, "db".to_string());
        let cloned = session.clone();

        assert_eq!(session.username, cloned.username);
        assert_eq!(session.role, cloned.role);
        assert_eq!(session.database, cloned.database);
        assert_eq!(session.process_id, cloned.process_id);
        assert_eq!(session.secret_key, cloned.secret_key);
    }

    #[test]
    fn test_session_info_debug() {
        let session = SessionInfo::new("user".to_string(), RoleType::Write, "db".to_string());
        let debug = format!("{:?}", session);

        assert!(debug.contains("SessionInfo"));
        assert!(debug.contains("user"));
        assert!(debug.contains("Write"));
        assert!(debug.contains("db"));
    }

    #[test]
    fn test_session_info_with_application_name() {
        let mut session = SessionInfo::new("user".to_string(), RoleType::Write, "db".to_string());
        session.application_name = Some("psql".to_string());

        assert_eq!(session.application_name, Some("psql".to_string()));
    }

    #[test]
    fn test_session_info_process_id_positive() {
        // process_id uses .abs() so should always be non-negative
        for _ in 0..100 {
            let session = SessionInfo::new("user".to_string(), RoleType::Write, "db".to_string());
            assert!(session.process_id >= 0);
        }
    }
}
