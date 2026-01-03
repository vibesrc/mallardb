# Section 10: Configuration

## 10.1 Overview

mallardb is configured primarily through environment variables, following PostgreSQL Docker image conventions for compatibility. This section specifies all configuration options and their behaviors.

## 10.2 Environment Variables

### 10.2.1 Authentication (Required)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTGRES_USER` | Yes | - | Username for read-write role |
| `POSTGRES_PASSWORD` | Yes | - | Password for read-write role |
| `POSTGRES_READONLY_USER` | No | - | Username for read-only role |
| `POSTGRES_READONLY_PASSWORD` | No | - | Password for read-only role |
| `POSTGRES_DB` | No | `$POSTGRES_USER` | Database name |

### 10.2.2 Network

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MALLARDB_HOST` | No | `0.0.0.0` | Listen address |
| `MALLARDB_PORT` | No | `5432` | Listen port |

### 10.2.3 TLS

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MALLARDB_TLS_ENABLED` | No | `false` | Enable TLS |
| `MALLARDB_TLS_CERT` | If TLS | - | Path to certificate file |
| `MALLARDB_TLS_KEY` | If TLS | - | Path to private key file |
| `MALLARDB_TLS_CA` | No | - | Path to CA certificate (client auth) |

### 10.2.4 Data Storage

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MALLARDB_DATABASE` | No | `./data/mallard.db` | Database file path |

### 10.2.5 Performance

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MALLARDB_MAX_READERS` | No | `64` | Maximum reader connections |
| `MALLARDB_WRITER_QUEUE_SIZE` | No | `1000` | Writer queue capacity |
| `MALLARDB_BATCH_SIZE` | No | `1000` | Result batch size |
| `MALLARDB_QUERY_TIMEOUT_MS` | No | `0` | Query timeout (0=disabled) |

### 10.2.6 Compatibility

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MALLARDB_PG_VERSION` | No | `15.0` | Reported PostgreSQL version |

## 10.3 Docker Image

### 10.3.1 Image Structure

```dockerfile
FROM rust:1.75-slim as builder
# Build steps...

FROM debian:bookworm-slim
COPY --from=builder /app/mallardb /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/

ENV MALLARDB_DATABASE=/var/lib/mallardb/data/mallard.db
VOLUME /var/lib/mallardb/data

EXPOSE 5432

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["mallardb"]
```

### 10.3.2 Docker Compose Example

```yaml
version: '3.8'

services:
  mallardb:
    image: ghcr.io/vibesrc/mallardb:latest
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: secretpassword
      POSTGRES_DB: mydb
      POSTGRES_READONLY_USER: reader
      POSTGRES_READONLY_PASSWORD: readerpass
    ports:
      - "5432:5432"
    volumes:
      - mallardb_data:/var/lib/mallardb/data

volumes:
  mallardb_data:
```

### 10.3.3 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mallardb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mallardb
  template:
    metadata:
      labels:
        app: mallardb
    spec:
      containers:
      - name: mallardb
        image: ghcr.io/vibesrc/mallardb:latest
        env:
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: mallardb-secrets
              key: username
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: mallardb-secrets
              key: password
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: data
          mountPath: /var/lib/mallardb/data
        livenessProbe:
          tcpSocket:
            port: 5432
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - mallardb-isready
            - -U
            - $(POSTGRES_USER)
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: mallardb-pvc
```

## 10.4 Initialization

### 10.4.1 First Start Behavior

On first start with empty data directory:

1. Create data directory if not exists
2. Initialize empty DuckDB database file
3. Create default schema (`public`)
4. Start accepting connections

### 10.4.2 Init Scripts

mallardb supports two script directories for initialization (following PostgreSQL Docker conventions):

#### initdb.d/ - First Start Only

Scripts in `/docker-entrypoint-initdb.d/` run once on first database creation:

```
/docker-entrypoint-initdb.d/
├── 01-schema.sql
├── 02-seed-data.sql
└── 03-indexes.sql
```

Override with `MALLARDB_INITDB_DIR` environment variable.

#### startdb.d/ - Every Startup

Scripts in `/docker-entrypoint-startdb.d/` run on every startup (for idempotent operations):

```
/docker-entrypoint-startdb.d/
├── 01-ensure-tables.sql    # CREATE TABLE IF NOT EXISTS ...
├── 02-ensure-indexes.sql   # CREATE INDEX IF NOT EXISTS ...
└── 03-refresh-views.sql    # CREATE OR REPLACE VIEW ...
```

Override with `MALLARDB_STARTDB_DIR` environment variable.

**Script Execution:**
- Only `.sql` files are executed
- Scripts run in alphabetical order
- Errors are logged but don't prevent startup
- Scripts execute through the writer queue (serialized)

## 10.5 Health Checks

### 10.5.1 TCP Health Check

Basic liveness: TCP connection to port 5432 succeeds.

### 10.5.2 mallardb-isready

Command-line health check utility:

```bash
mallardb-isready [OPTIONS]

Options:
  -h, --host <HOST>      Server host [default: localhost]
  -p, --port <PORT>      Server port [default: 5432]
  -U, --username <USER>  Username for connection test
  -d, --dbname <DB>      Database name [default: $POSTGRES_DB]
  -t, --timeout <SECS>   Connection timeout [default: 3]
```

Exit codes:
- `0`: Server accepting connections
- `1`: Server rejecting connections
- `2`: No response / timeout

### 10.5.3 pg_isready Compatibility

`mallardb-isready` is designed to be invoked as `pg_isready` alias:

```bash
ln -s /usr/local/bin/mallardb-isready /usr/local/bin/pg_isready
```

## 10.6 Logging

### 10.6.1 Log Levels

| Variable | Default | Options |
|----------|---------|---------|
| `MALLARDB_LOG_LEVEL` | `info` | error, warn, info, debug, trace |
| `MALLARDB_LOG_FORMAT` | `text` | text, json |

### 10.6.2 Log Output

Logs write to stdout/stderr following 12-factor app principles:

```
2024-01-15T10:30:00.000Z INFO mallardb::server: Listening on 0.0.0.0:5432
2024-01-15T10:30:05.123Z INFO mallardb::auth: Connection from 10.0.0.5:54321, user=admin
2024-01-15T10:30:05.456Z DEBUG mallardb::query: Executing: SELECT * FROM users
```

JSON format:

```json
{"timestamp":"2024-01-15T10:30:00.000Z","level":"INFO","target":"mallardb::server","message":"Listening on 0.0.0.0:5432"}
```

### 10.6.3 Query Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `MALLARDB_LOG_QUERIES` | `false` | Log all queries |
| `MALLARDB_LOG_SLOW_QUERIES_MS` | `0` | Log queries slower than N ms (0=disabled) |

## 10.7 Connection String

mallardb accepts standard PostgreSQL connection strings:

```
postgresql://user:password@host:port/database?param=value
postgres://user:password@host:port/database
```

Supported parameters:

| Parameter | Description |
|-----------|-------------|
| `sslmode` | disable, allow, prefer, require |
| `application_name` | Client application name |
| `connect_timeout` | Connection timeout in seconds |
| `options` | Runtime parameters |

## 10.8 Runtime Parameters

These PostgreSQL parameters are accepted via `SET`:

| Parameter | Behavior |
|-----------|----------|
| `search_path` | Functional - affects schema resolution |
| `timezone` | Functional - affects timestamp display |
| `client_encoding` | Accepted (must be UTF8) |
| `application_name` | Stored in session |
| `statement_timeout` | Functional if query timeout enabled |
| Others | Accepted, ignored (logged at debug level) |

## 10.9 Signals

| Signal | Behavior |
|--------|----------|
| `SIGTERM` | Graceful shutdown (finish active queries) |
| `SIGINT` | Graceful shutdown |
| `SIGQUIT` | Immediate shutdown (abort queries) |
| `SIGHUP` | Reload configuration (future) |

## 10.10 Resource Limits

### 10.10.1 Memory

DuckDB manages its own memory. mallardb adds minimal overhead:

| Component | Typical Memory |
|-----------|----------------|
| Per connection | ~10 KB |
| Writer queue | ~1 MB (depends on queue size) |
| Catalog cache | ~5 MB |

### 10.10.2 File Descriptors

Each connection requires:
- 1 TCP socket
- 1 DuckDB reader (for read role)

Recommended ulimit: `nofile >= 65536`

### 10.10.3 DuckDB Configuration

DuckDB settings can be configured via SQL after connection:

```sql
SET memory_limit = '4GB';
SET threads = 4;
SET temp_directory = '/tmp/duckdb';
```

Future versions MAY support DuckDB configuration via environment variables.
