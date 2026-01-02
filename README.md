# mallardb

A PostgreSQL-compatible interface over DuckDB. Looks like Postgres, acts like DuckDB.

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Configure

Create a `.env` file or set environment variables:

```bash
# Required
POSTGRES_PASSWORD=secret

# Optional (defaults shown)
POSTGRES_USER=mallard
POSTGRES_DB=mallard
MALLARDB_PORT=5432
MALLARDB_DATA_DIR=./data

# Optional: Read-only user
POSTGRES_READONLY_USER=reader
POSTGRES_READONLY_PASSWORD=readerpass
```

### 3. Run

```bash
# With .env file in current directory
./target/release/mallardb

# Or with environment variables (only password is required)
POSTGRES_PASSWORD=secret ./target/release/mallardb

# With CLI options (override env vars)
./target/release/mallardb --data-dir /var/lib/mallardb --port 5433 --host 127.0.0.1
```

#### CLI Options

| Flag | Short | Description |
|------|-------|-------------|
| `--data-dir` | `-d` | Data directory path (overrides MALLARDB_DATA_DIR) |
| `--port` | `-p` | Listen port (overrides MALLARDB_PORT) |
| `--host` | `-H` | Listen host/address (overrides MALLARDB_HOST) |
| `--help` | `-h` | Print help |
| `--version` | `-V` | Print version |

### 4. Connect

```bash
# Using psql (default user/db is 'mallard')
psql -h 127.0.0.1 -p 5432 -U mallard -d mallard

# With password in env
PGPASSWORD=secret psql -h 127.0.0.1 -p 5432 -U mallard -d mallard

# Read-only user (if configured)
PGPASSWORD=readerpass psql -h 127.0.0.1 -p 5432 -U reader -d mallard
```

## Features

- **PostgreSQL Wire Protocol** - Connect with any PostgreSQL client (psql, DBeaver, Grafana, ORMs, etc.)
- **MD5 Password Authentication** - Standard PostgreSQL authentication
- **Role-Based Access Control** - Write users can modify data, read-only users can only query
- **DuckDB Backend** - Fast analytical queries with columnar storage
- **pg_catalog Emulation** - Compatible with ORMs and tools that query system catalogs
- **SQL Rewriting** - Automatic translation of PostgreSQL-specific syntax to DuckDB

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL Clients                    │
│          (psql, DBeaver, Grafana, ORMs, etc.)           │
└─────────────────────┬───────────────────────────────────┘
                      │ PostgreSQL Wire Protocol (port 5432)
                      ▼
┌─────────────────────────────────────────────────────────┐
│                      mallardb                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │    Auth     │  │   Catalog   │  │  Query Router   │  │
│  │  (MD5 pass) │  │  Emulation  │  │  (role-based)   │  │
│  └─────────────┘  └─────────────┘  └────────┬────────┘  │
│                                              │           │
│                    ┌─────────────────────────┼───────┐   │
│                    │                         │       │   │
│                    ▼                         ▼       │   │
│             ┌────────────┐           ┌────────────┐  │   │
│             │   Writer   │           │  Readers   │  │   │
│             │   Queue    │           │   Pool     │  │   │
│             └─────┬──────┘           └─────┬──────┘  │   │
│                   │                        │         │   │
└───────────────────┼────────────────────────┼─────────┘   │
                    │                        │             │
                    ▼                        ▼             │
              ┌──────────────────────────────────┐         │
              │            DuckDB                │         │
              │     (data.db in PGDATA)          │         │
              └──────────────────────────────────┘         │
```

## Example Usage

```sql
-- Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com');

-- Query data
SELECT * FROM users WHERE id = 1;

-- DuckDB-specific features work too
SELECT * FROM read_csv_auto('data.csv');
COPY users TO 'users.parquet' (FORMAT PARQUET);
```

## Init Scripts

mallardb supports automatic SQL script execution (following PostgreSQL Docker conventions):

```
/docker-entrypoint-initdb.d/   # Runs once on first database creation
├── 01-schema.sql
└── 02-seed.sql

/docker-entrypoint-startdb.d/  # Runs on every startup (idempotent)
└── 01-ensure-tables.sql
```

- `/docker-entrypoint-initdb.d/` - Scripts run once when the database is first created
- `/docker-entrypoint-startdb.d/` - Scripts run on every startup (use `CREATE TABLE IF NOT EXISTS`, etc.)

Scripts execute in alphabetical order. Only `.sql` files are processed.

Override paths with `MALLARDB_INITDB_DIR` and `MALLARDB_STARTDB_DIR` environment variables.

## Configuration Reference

All `MALLARDB_*` variables also accept `POSTGRES_*` prefix for compatibility.

| Variable | Description | Default |
|----------|-------------|---------|
| `MALLARDB_PASSWORD` | Admin password (required) | - |
| `MALLARDB_USER` | Admin username | mallard |
| `MALLARDB_DB` | Database name | $MALLARDB_USER |
| `MALLARDB_READONLY_USER` | Read-only username | (disabled) |
| `MALLARDB_READONLY_PASSWORD` | Read-only password | (disabled) |
| `MALLARDB_PORT` | Listen port | 5432 |
| `MALLARDB_HOST` | Listen address | 0.0.0.0 |
| `MALLARDB_DATA_DIR` | Data directory | ./data |

## Supported Types

| DuckDB Type | PostgreSQL OID |
|-------------|----------------|
| BOOLEAN | bool (16) |
| TINYINT, SMALLINT | int2 (21) |
| INTEGER | int4 (23) |
| BIGINT | int8 (20) |
| FLOAT | float4 (700) |
| DOUBLE | float8 (701) |
| VARCHAR, TEXT | text (25) |
| DATE | date (1082) |
| TIMESTAMP | timestamp (1114) |
| INTERVAL | interval (1186) |
| BLOB | bytea (17) |
| DECIMAL | numeric (1700) |

## License

MIT
