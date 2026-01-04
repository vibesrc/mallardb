# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mallardb** is a PostgreSQL-compatible gateway to DuckDB's federated query engine. It implements the PostgreSQL wire protocol (via pgwire) while executing all queries against DuckDB, allowing any PostgreSQL client to query files, object storage, and external databases.

## Build Commands

```bash
# Build
cargo build                         # Debug build
cargo build --release               # Release build (LTO enabled)

# Run
cargo run                           # Debug mode
cargo run --release                 # Release mode

# Test
cargo test --lib                    # Unit tests only
cargo test --test integration_test -- --ignored  # Integration tests (requires running server)
MALLARDB_PASSWORD=test make test-ci # Full CI test suite (starts server automatically)

# Lint
cargo fmt --check                   # Format check
cargo clippy -- -D warnings         # Lint check
```

## Architecture

```
PostgreSQL Clients → mallardb (Auth → SQL Rewriter → Catalog) → DuckDB → Data Sources
```

### Core Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | Server entry point, signal handling, startup script execution |
| `config.rs` | Environment variable configuration (`MALLARDB_*`/`POSTGRES_*` prefixes) |
| `backend.rs` | DuckDB connection management (largest module, per-connection concurrency) |
| `handler.rs` | pgwire protocol handlers |
| `auth.rs` | MD5 password authentication, role-based access (admin/reader) |
| `catalog.rs` | pg_catalog emulation and query interception |
| `sql_rewriter.rs` | SQL transformation (PostgreSQL → DuckDB compatibility) |
| `types.rs` | DuckDB type → PostgreSQL OID mapping |
| `error.rs` | Error types and SQLSTATE code mapping |

### Query Flow

1. Client connects → MD5 auth → determine role (admin/reader)
2. SQL received → strip null bytes → check if catalog query (intercept if so)
3. Rewrite SQL via AST (sqlparser-rs), fallback to string-based rewriting
4. Execute on per-connection DuckDB instance → auto-rollback on error
5. Map DuckDB types to PostgreSQL OIDs → return results

### Key Design Decisions

- **Per-client DuckDB connections**: Each PostgreSQL client gets a dedicated DuckDB connection (no shared state)
- **Two roles**: Admin (read-write) and Reader (SELECT-only, enforced at query level)
- **Auto-rollback**: Query errors trigger automatic rollback to prevent transaction deadlock
- **Catalog emulation**: Synthetic responses for pg_catalog queries (clients think it's PostgreSQL)

## Configuration

Required: `MALLARDB_PASSWORD` or `POSTGRES_PASSWORD`

Key variables (prefix `MALLARDB_` or `POSTGRES_`):
- `USER` (default: mallard) - Admin username
- `DATABASE` (default: ./data/mallard.db) - DuckDB file path
- `PORT` (default: 5432) - Listen port
- `READONLY_USER`/`READONLY_PASSWORD` - Optional read-only role

See `.env.example` for full reference.

## Testing

Integration tests require a running server instance. The `make test-ci` target handles this automatically:
1. Builds the project
2. Starts the server in background
3. Runs integration tests
4. Stops the server

## Documentation

Detailed RFC specification in `docs/rfc/` covers wire protocol, authentication, catalog emulation, type system, and security model.
