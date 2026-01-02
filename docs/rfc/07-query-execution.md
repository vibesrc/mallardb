# Section 7: Query Execution

## 7.1 Overview

Query execution in mallardb uses a per-connection concurrency model where each client gets its own DuckDB connection. DuckDB handles write serialization internally via its WAL and locking mechanisms. This section specifies the routing logic, concurrency model, and execution semantics.

## 7.2 Query Routing

### 7.2.1 Routing Decision

Upon receiving a query, mallardb routes based solely on the connection's authenticated role:

| Role | Connection Type | Concurrency |
|------|-----------------|-------------|
| Write role (`POSTGRES_USER`) | Read-write connection | DuckDB-managed serialization |
| Read role (`POSTGRES_READONLY_USER`) | Read-only connection | Fully concurrent |

```
                    ┌─────────────────┐
                    │  Incoming Query │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Check Role     │
                    └────────┬────────┘
                             │
            ┌────────────────┴────────────────┐
            │                                  │
   ┌────────▼────────┐              ┌─────────▼─────────┐
   │  Write Role     │              │   Read Role       │
   │  Create R/W     │              │   Create R/O      │
   │  Connection     │              │   Connection      │
   └────────┬────────┘              └─────────┬─────────┘
            │                                  │
   ┌────────▼────────┐              ┌─────────▼─────────┐
   │  Execute Query  │              │  Execute Query    │
   │  (DuckDB locks) │              │  (Concurrent)     │
   └─────────────────┘              └───────────────────┘
```

### 7.2.2 No Query Parsing for Routing

mallardb does NOT parse queries to determine read vs write intent. The routing decision is made entirely based on the authenticated role. This design:

- Simplifies implementation
- Avoids parsing overhead
- Provides predictable behavior
- Matches PostgreSQL connection pooler semantics (e.g., pgBouncer)

Consequence: A write role connection sending `SELECT 1` uses a read-write connection. A read role connection sending `INSERT` receives an error from DuckDB.

## 7.3 Connection Model

### 7.3.1 Per-Query Connections

Each query creates a fresh DuckDB connection:

```rust
// Conceptual structure
pub struct DuckDbConnection {
    conn: duckdb::Connection,
}

impl DuckDbConnection {
    pub fn new(db_path: &Path) -> Result<Self, Error> {
        let conn = Connection::open(db_path)?;
        Ok(DuckDbConnection { conn })
    }

    pub fn new_readonly(db_path: &Path) -> Result<Self, Error> {
        let conn = Connection::open_with_flags(
            db_path,
            Config::default().access_mode(AccessMode::ReadOnly)?,
        )?;
        Ok(DuckDbConnection { conn })
    }
}
```

### 7.3.2 DuckDB Concurrency Model

DuckDB manages write serialization internally:

- **Multiple connections**: Each client can have its own connection
- **Write serialization**: DuckDB uses `wait_for_write_lock` to serialize writes
- **Read concurrency**: Multiple readers can execute simultaneously
- **MVCC**: Readers see consistent snapshots without blocking writers

This matches DuckDB's documented concurrency model where:
> Within a single process, DuckDB supports multiple writer connections using a combination of MVCC and optimistic concurrency control.

### 7.3.3 Auto-Rollback on Error

To prevent "transaction aborted" deadlock states, mallardb automatically rolls back after any error:

```rust
pub fn execute(&self, sql: &str) -> QueryResult {
    let result = execute_query(&self.conn, sql);

    // Auto-rollback on error to prevent "transaction aborted" deadlock
    if result.is_err() {
        let _ = self.conn.execute("ROLLBACK", []);
    }

    result
}
```

## 7.4 Read Path

### 7.4.1 Read-Only Connections

Read role connections use DuckDB's read-only mode:

- DuckDB `AccessMode::ReadOnly` flag
- No write operations permitted at database level
- Fully concurrent with other readers and writers

If a read role attempts a write operation:
1. DuckDB returns an error
2. mallardb translates to SQLSTATE `25006`
3. Message: "cannot execute {statement} in a read-only transaction"

## 7.5 Catalog Query Handling

Before routing to DuckDB, mallardb checks if the query targets system catalogs:

### 7.5.1 Detection

Catalog queries are detected by:
1. Quick regex scan for `pg_catalog`, `information_schema`
2. If matched, parse query to confirm table references
3. Route to catalog emulator if confirmed

### 7.5.2 Emulator Execution

The catalog emulator:
1. May execute translated queries against DuckDB (via same routing path)
2. May synthesize results entirely
3. May combine DuckDB results with synthetic data

### 7.5.3 Passthrough

Queries that reference catalog schemas but can be handled by DuckDB's native information_schema are passed through with minimal modification.

## 7.6 Query Timeout

mallardb SHOULD support query timeouts:

| Setting | Default | Description |
|---------|---------|-------------|
| `MALLARDB_QUERY_TIMEOUT_MS` | 0 | Query timeout (0 = disabled) |

When a query times out:
- Cancel execution if possible (DuckDB interrupt)
- Return SQLSTATE `57014` (query_canceled)
- Message: "canceling statement due to statement timeout"

## 7.7 DuckDB Feature Access

All DuckDB features are accessible through normal queries:

### 7.7.1 File Operations

```sql
-- Read Parquet
SELECT * FROM read_parquet('data/*.parquet');

-- Write Parquet
COPY table TO 'output.parquet' (FORMAT PARQUET);

-- Read CSV
SELECT * FROM read_csv_auto('data.csv');
```

### 7.7.2 Extensions

```sql
-- Load extension
INSTALL httpfs;
LOAD httpfs;

-- Use extension
SELECT * FROM read_parquet('s3://bucket/file.parquet');
```

### 7.7.3 DuckDB-Specific Syntax

DuckDB syntax passes through unchanged:

```sql
-- Exclude columns
SELECT * EXCLUDE (internal_id) FROM users;

-- Replace columns
SELECT * REPLACE (upper(name) AS name) FROM users;

-- Positional joins
SELECT * FROM t1 POSITIONAL JOIN t2;
```

## 7.8 Result Streaming

For large result sets, mallardb SHOULD stream results:

1. Execute query against DuckDB
2. Fetch rows in batches (configurable batch size)
3. Send `DataRow` messages as batches complete
4. Send `CommandComplete` when exhausted

| Setting | Default | Description |
|---------|---------|-------------|
| `MALLARDB_BATCH_SIZE` | 1000 | Rows per batch |

This prevents memory exhaustion for queries returning millions of rows.

## 7.9 Error Translation

DuckDB errors are translated to PostgreSQL format:

1. Catch DuckDB exception
2. Map to appropriate SQLSTATE (see [Section 4.6.2](./04-wire-protocol.md#462-sqlstate-mapping))
3. Format as `ErrorResponse` message
4. Include original DuckDB message in Message field
5. Include position information if available
