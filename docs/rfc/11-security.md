# Section 11: Security Considerations

## 11.1 Threat Model

### 11.1.1 Assumed Attackers

mallardb's security model considers:

1. **Network attackers**: Can observe and inject network traffic
2. **Unauthorized users**: Have network access but no valid credentials
3. **Malicious read users**: Have read-only credentials, attempt privilege escalation
4. **Resource exhaustion**: Attempt denial of service via query load

### 11.1.2 Trust Boundaries

```
┌─────────────────────────────────────────────────┐
│                   Untrusted                      │
│                                                  │
│    ┌────────────┐        ┌────────────┐         │
│    │  Client A  │        │  Client B  │         │
│    └──────┬─────┘        └──────┬─────┘         │
│           │                     │                │
└───────────┼─────────────────────┼────────────────┘
            │     Network         │
┌───────────┼─────────────────────┼────────────────┐
│           │     Trusted         │                │
│    ┌──────▼─────────────────────▼──────┐        │
│    │           mallardb                 │        │
│    │  ┌─────────────────────────────┐  │        │
│    │  │      Authentication         │  │        │
│    │  └─────────────────────────────┘  │        │
│    │  ┌─────────────────────────────┐  │        │
│    │  │      Query Execution        │  │        │
│    │  └─────────────────────────────┘  │        │
│    │  ┌─────────────────────────────┐  │        │
│    │  │         DuckDB              │  │        │
│    │  └─────────────────────────────┘  │        │
│    └────────────────────────────────────┘        │
│                                                  │
│    ┌────────────────────────────────────┐        │
│    │           Filesystem               │        │
│    └────────────────────────────────────┘        │
└──────────────────────────────────────────────────┘
```

## 11.2 Authentication Security

### 11.2.1 Credential Storage

Credentials are provided via environment variables:
- Environment variables are NOT persisted to disk by mallardb
- Container orchestrators SHOULD use secrets management
- Credentials MUST NOT be logged

### 11.2.2 Password Handling

- Cleartext passwords are compared in constant time to prevent timing attacks
- Passwords are not stored after initial configuration load
- Failed authentication attempts are logged (without password)

### 11.2.3 Authentication Bypass Prevention

- ALL connections MUST authenticate before executing queries
- Catalog queries require authentication
- No "trust" authentication mode (PostgreSQL pg_hba.conf equivalent)

## 11.3 Authorization

### 11.3.1 Role-Based Access

Current implementation provides two access levels:

| Role | Capabilities |
|------|--------------|
| Write role | Full DDL, DML, and query access |
| Read role | Query access only |

### 11.3.2 Read Role Enforcement

Read role restrictions are enforced at two levels:

1. **DuckDB level**: Reader connections opened with read-only flag
2. **mallardb level**: Write detection before DuckDB execution (defense in depth)

### 11.3.3 Future Authorization

Future versions SHOULD support:
- Object-level permissions (GRANT/REVOKE)
- Row-level security
- Column-level permissions

## 11.4 Data Protection

### 11.4.1 TLS/SSL

When TLS is enabled:
- All data in transit is encrypted
- Server authenticates via certificate
- Client certificate authentication MAY be required

TLS configuration requirements:
- Minimum TLS 1.2
- Strong cipher suites only
- Certificate chain validation

### 11.4.2 Data at Rest

mallardb does NOT provide native data-at-rest encryption. For encrypted storage:
- Use filesystem-level encryption (LUKS, dm-crypt)
- Use cloud provider encrypted volumes
- Use DuckDB's encryption extension (if available)

### 11.4.3 Sensitive Data in Logs

mallardb MUST NOT log:
- Passwords or credentials
- Query results
- Full query text at INFO level (only at DEBUG)

mallardb MAY log (at appropriate levels):
- Query fingerprints/hashes
- Error messages (sanitized)
- Connection metadata

## 11.5 Denial of Service

### 11.5.1 Connection Limits

| Protection | Configuration |
|------------|---------------|
| Max connections | `mallardb_MAX_READERS` |
| Writer queue depth | `mallardb_WRITER_QUEUE_SIZE` |
| Connection timeout | Configurable per client |

### 11.5.2 Query Resource Limits

| Protection | Mechanism |
|------------|-----------|
| Query timeout | `mallardb_QUERY_TIMEOUT_MS` |
| Memory limit | DuckDB `memory_limit` setting |
| Result size | Streaming prevents memory exhaustion |

### 11.5.3 Rate Limiting

Current implementation does NOT include rate limiting. Deployments SHOULD use:
- Load balancer rate limiting
- Connection pooler limits (e.g., PgBouncer in front)
- Network-level rate limiting

## 11.6 SQL Injection

### 11.6.1 Prepared Statements

mallardb supports prepared statements with parameter binding, which prevents SQL injection when used correctly:

```sql
-- Safe: parameters are bound, not interpolated
PREPARE stmt AS SELECT * FROM users WHERE id = $1;
EXECUTE stmt(123);
```

### 11.6.2 Catalog Query Injection

The catalog emulator MUST sanitize inputs when constructing queries:
- Table/column names MUST be identifier-quoted
- User inputs MUST NOT be directly interpolated

## 11.7 File System Security

### 11.7.1 DuckDB File Operations

DuckDB supports file operations (COPY, read_parquet, etc.). Security implications:

| Operation | Risk | Mitigation |
|-----------|------|------------|
| `COPY TO` | Write arbitrary files | Restrict to data directory |
| `read_parquet()` | Read arbitrary files | Use DuckDB's allowed paths |
| Extensions | Load native code | Disable if untrusted users |

### 11.7.2 Recommended Restrictions

For untrusted environments:

```sql
-- Disable external file access
SET enable_external_access = false;

-- Restrict allowed paths
SET allowed_paths = '/var/lib/mallardb/data';
```

## 11.8 Extension Security

DuckDB extensions are native code. In multi-tenant or untrusted environments:
- Disable extension loading
- Pre-load only required extensions
- Use container isolation

## 11.9 Audit Logging

### 11.9.1 Security Events

mallardb SHOULD log these security-relevant events:

| Event | Log Level | Information |
|-------|-----------|-------------|
| Authentication success | INFO | User, source IP |
| Authentication failure | WARN | User (attempted), source IP |
| Authorization failure | WARN | User, attempted operation |
| Connection close | DEBUG | User, duration |

### 11.9.2 Audit Log Format

Security events include:
- Timestamp (ISO 8601)
- Event type
- Source IP address
- Username (if known)
- Success/failure
- Additional context

## 11.10 Security Recommendations

### 11.10.1 Production Deployment Checklist

- [ ] Enable TLS
- [ ] Use strong passwords (20+ characters)
- [ ] Run as non-root user
- [ ] Limit network exposure (firewall, VPC)
- [ ] Mount data volume with appropriate permissions
- [ ] Enable query logging for audit
- [ ] Set resource limits
- [ ] Use secrets management for credentials
- [ ] Regular security updates

### 11.10.2 Container Security

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop:
      - ALL
```
