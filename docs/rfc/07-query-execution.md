# Section 7: Query Execution

## 7.1 Overview

Query execution in mallardb uses a per-client concurrency model where each client maintains a persistent DuckDB connection for its session. DuckDB handles write serialization internally via MVCC and optimistic concurrency control. This section specifies the routing logic, concurrency model, SQL rewriting, and execution semantics.

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

### 7.3.1 Per-Client Connections

Each client maintains a persistent DuckDB connection for its session:

```rust
// Conceptual structure
pub struct MallardbHandler {
    // Persistent connection for this client, wrapped in Mutex for thread-safety
    conn: Arc<Mutex<DuckDbConnection>>,
    session: SessionInfo,
}
```

**Write role clients**: Get a connection cloned from the base connection with full read-write access.

**Read role clients**: Get a new connection opened with `AccessMode::ReadOnly`.

### 7.3.2 DuckDB Concurrency Model

DuckDB manages write serialization internally:

- **Multiple connections**: Each client maintains its own persistent connection
- **Write serialization**: DuckDB uses MVCC and optimistic concurrency control
- **Read concurrency**: Multiple readers can execute simultaneously
- **Snapshot isolation**: Readers see consistent snapshots without blocking writers

This matches DuckDB's documented concurrency model where:
> Within a single process, DuckDB supports multiple writer connections using a combination of MVCC and optimistic concurrency control.

### 7.3.3 Thread Safety

DuckDB's `Connection` is not `Sync`, so each connection is wrapped in a `Mutex`:

```rust
pub struct DuckDbConnection {
    conn: Mutex<duckdb::Connection>,
}
```

### 7.3.4 Auto-Rollback on Error

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

mallardb uses a **minimal interception** approach. Most catalog queries pass through to DuckDB's native `pg_catalog` implementation.

### 7.5.1 Detection

Catalog queries requiring interception are detected by checking for:
- PostgreSQL-specific functions (`version()`, `current_database()`, `pg_backend_pid()`, etc.)
- Auth-related queries (`current_user`, `pg_roles`, `has_*_privilege()`)
- Session info (`pg_stat_activity`, `pg_settings`)
- Database-level info (`pg_database`)

### 7.5.2 Passthrough

Most `pg_catalog` and `information_schema` queries pass through directly to DuckDB:
- `pg_type`, `pg_class`, `pg_namespace`, `pg_attribute`
- `pg_proc`, `pg_index`, `pg_constraint`
- `information_schema.tables`, `information_schema.columns`

### 7.5.3 Synthesized Responses

When interception is needed, mallardb synthesizes PostgreSQL-compatible responses (see [Section 6](./06-catalog.md) for details).

## 7.6 SQL Rewriting

mallardb automatically rewrites certain SQL patterns for DuckDB compatibility.

### 7.6.1 Schema Rewriting

PostgreSQL's default `public` schema is rewritten to DuckDB's `main`:

```sql
-- Input (PostgreSQL style)
SELECT * FROM t WHERE table_schema = 'public'

-- Output (DuckDB compatible)
SELECT * FROM t WHERE table_schema = 'main'
```

This also applies to `nspname = 'public'` in pg_namespace queries.

### 7.6.2 AST-Based Rewriting

SQL rewriting uses an AST-based approach with string fallback:

1. Parse SQL using `sqlparser`
2. Walk AST to find schema references
3. Transform `'public'` → `'main'`
4. Fall back to string-based rewriting if parsing fails

### 7.6.3 Compatibility Macros

DuckDB is configured with PostgreSQL compatibility macros at startup:

```sql
CREATE MACRO IF NOT EXISTS array_lower(arr, dim) AS 1;
CREATE MACRO IF NOT EXISTS array_upper(arr, dim) AS len(arr);
CREATE MACRO IF NOT EXISTS current_setting(name) AS
    CASE WHEN name = 'search_path' THEN 'main' ELSE NULL END;
CREATE MACRO IF NOT EXISTS quote_ident(s) AS '"' || s || '"';
```

## 7.7 Query Timeout

mallardb SHOULD support query timeouts:

| Setting | Default | Description |
|---------|---------|-------------|
| `MALLARDB_QUERY_TIMEOUT_MS` | 0 | Query timeout (0 = disabled) |

When a query times out:
- Cancel execution if possible (DuckDB interrupt)
- Return SQLSTATE `57014` (query_canceled)
- Message: "canceling statement due to statement timeout"

## 7.8 DuckDB Feature Access

All DuckDB features are accessible through normal queries:

### 7.8.1 File Operations

```sql
-- Read Parquet
SELECT * FROM read_parquet('data/*.parquet');

-- Write Parquet
COPY table TO 'output.parquet' (FORMAT PARQUET);

-- Read CSV
SELECT * FROM read_csv_auto('data.csv');
```

### 7.8.2 Extensions

```sql
-- Load extension
INSTALL httpfs;
LOAD httpfs;

-- Use extension
SELECT * FROM read_parquet('s3://bucket/file.parquet');
```

### 7.8.3 DuckDB-Specific Syntax

DuckDB syntax passes through unchanged:

```sql
-- Exclude columns
SELECT * EXCLUDE (internal_id) FROM users;

-- Replace columns
SELECT * REPLACE (upper(name) AS name) FROM users;

-- Positional joins
SELECT * FROM t1 POSITIONAL JOIN t2;
```

## 7.9 Result Streaming

For large result sets, mallardb SHOULD stream results:

1. Execute query against DuckDB
2. Fetch rows in batches (configurable batch size)
3. Send `DataRow` messages as batches complete
4. Send `CommandComplete` when exhausted

| Setting | Default | Description |
|---------|---------|-------------|
| `MALLARDB_BATCH_SIZE` | 1000 | Rows per batch |

This prevents memory exhaustion for queries returning millions of rows.

## 7.10 Error Translation

DuckDB errors are translated to PostgreSQL format:

1. Catch DuckDB exception
2. Map to appropriate SQLSTATE (see [Section 4.6.2](./04-wire-protocol.md#462-sqlstate-mapping))
3. Format as `ErrorResponse` message
4. Include original DuckDB message in Message field
5. Include position information if available
