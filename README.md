# mallardb

Looks like an elephant. Quacks like a duck.

mallardb is a PostgreSQL-compatible gateway to DuckDB's federated query engine. It lets any PostgreSQL client (psql, DBeaver, Tableau, Grafana, Metabase, your favorite ORM) query files, object storage, and external databases through DuckDB, without custom drivers or plugins.

**DuckDB can already query almost anything. mallardb lets almost anything query DuckDB.**

## Why This Exists

DuckDB is a universal analytical query engine. It can query and join, in a single SQL statement:

- **Files**: Parquet, CSV, JSON, Excel
- **Object storage**: S3, GCS, Azure Blob (any S3-compatible endpoint)
- **Databases via native connectors**: PostgreSQL, MySQL, SQLite
- **Databases via ODBC**: SQL Server, Oracle, Access, and anything else with an ODBC driver
- **Additional sources via plugins**: the DuckDB extension ecosystem continues to grow

The problem: most BI tools, dashboards, and applications speak PostgreSQL. They don't speak DuckDB.

mallardb bridges this gap. It presents a PostgreSQL wire protocol interface, translates queries, and executes them against DuckDB. Your existing tools get access to DuckDB's federated query capabilities without modification.

## What This Enables

**Federated analytics**: Join a Parquet file in S3 with a table in your production PostgreSQL database, from Tableau, Grafana, or any BI tool:

```sql
-- This query executes inside DuckDB.
-- mallardb only provides the PostgreSQL-compatible endpoint.

-- Attach your production PostgreSQL database
ATTACH 'postgres://readonly:pass@prod.db/app' AS prod (TYPE POSTGRES);

-- Join S3 parquet files with live PostgreSQL data
SELECT
    o.order_id,
    o.total,
    c.name,
    c.segment
FROM read_parquet('s3://analytics/orders/*.parquet') o
JOIN prod.public.customers c ON o.customer_id = c.id
WHERE o.order_date > '2024-01-01';
```

**Zero-ETL exploration**: Query data where it lives. No pipelines, no copies, no waiting.

**BI tools on data lakes**: Point Tableau or Metabase at mallardb, query Parquet files as if they were tables.

**Existing DuckDB databases**: Point mallardb at any `.db` file and immediately expose it via PostgreSQL protocol.

## What This Is NOT

mallardb is intentionally limited. It is:

- **Not a PostgreSQL replacement**: It doesn't implement PostgreSQL semantics, just the wire protocol
- **Not an OLTP database**: DuckDB is analytical; use it for reads and batch operations
- **Not a scheduler or orchestrator**: It runs queries, it doesn't manage workflows
- **Not transactional**: No multi-statement ACID transactions across sources

If you need PostgreSQL, use PostgreSQL. mallardb is for when you need DuckDB's query capabilities but your tools only speak PostgreSQL.

## Safety Model

mallardb supports two roles:

| Role | Capabilities |
|------|--------------|
| **Admin** (read-write) | Full DuckDB access: create tables, write files, attach databases, install extensions |
| **Reader** (read-only) | Query only: SELECT statements, no modifications |

By default, connect your BI tools with the read-only role. Writes and administrative operations require the admin role. This is intentional.

```bash
# Admin role (full access)
PGPASSWORD=secret psql -U mallard -d mallard

# Read-only role (safe for dashboards)
PGPASSWORD=readerpass psql -U reader -d mallard
```

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Configure

```bash
# Required
export POSTGRES_PASSWORD=secret

# Optional
export POSTGRES_USER=mallard              # default: mallard
export MALLARDB_DATABASE=./data/mallard.db  # default: ./data/mallard.db
export MALLARDB_PORT=5432                 # default: 5432

# Read-only user (recommended for BI tools)
export POSTGRES_READONLY_USER=reader
export POSTGRES_READONLY_PASSWORD=readerpass
```

### 3. Run

```bash
./target/release/mallardb

# Or point at an existing DuckDB database
./target/release/mallardb --database /path/to/existing.db
```

### 4. Connect

```bash
PGPASSWORD=secret psql -h 127.0.0.1 -U mallard -d mallard
```

## Advanced Capabilities

These features are powerful but require understanding DuckDB. They're opt-in and user-owned.

### DuckDB Extensions

Install and use any DuckDB extension:

```sql
INSTALL httpfs;
LOAD httpfs;

-- Now query S3 directly
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');
```

### Native Database Connectors

Query PostgreSQL and MySQL directly:

```sql
ATTACH 'postgres://user:pass@host/db' AS prod (TYPE POSTGRES);
SELECT * FROM prod.users WHERE created_at > '2024-01-01';
```

### ODBC Sources

Connect to anything with an ODBC driver:

```sql
ATTACH 'DSN=sqlserver_prod' AS erp (TYPE ODBC);
SELECT * FROM erp.dbo.inventory;
```

### Scheduled Operations

DuckDB doesn't have a scheduler, but mallardb supports init scripts that run on startup. Combined with container orchestration or cron, you can build lightweight refresh workflows:

```
/docker-entrypoint-startdb.d/
└── 01-refresh-views.sql    # Runs on every startup
```

This is not a replacement for proper orchestration tools. Use it for simple cases.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Clients                       │
│        (psql, DBeaver, Tableau, Grafana, Metabase, ORMs)    │
└────────────────────────────┬────────────────────────────────┘
                             │ PostgreSQL Wire Protocol
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                         mallardb                            │
│  ┌──────────────┐   ┌──────────────────┐  ┌──────────────┐  │
│  │     Auth     │   │   SQL Rewriter   │  │   Catalog    │  │
│  │  (MD5 auth)  │   │  (PG → DuckDB)   │  │  Emulation   │  │
│  └──────────────┘   └──────────────────┘  └──────────────┘  │
│                              │                              │
│              ┌───────────────┴───────────────┐              │
│              ▼                               ▼              │
│        ┌──────────┐                    ┌──────────┐         │
│        │  Writer  │                    │  Reader  │         │
│        │  (admin) │                    │  (r/o)   │         │
│        └─────┬────┘                    └─────┬────┘         │
└──────────────┼───────────────────────────────┼──────────────┘
               │                               │
               ▼                               ▼
┌─────────────────────────────────────────────────────────────┐
│                          DuckDB                             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───────┐  │
│  │ Parquet │ │   S3    │ │Postgres │ │  ODBC   │ │ .csv  │  │
│  │  files  │ │ buckets │ │  scan   │ │ sources │ │ .json │  │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └───────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Configuration Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `MALLARDB_PASSWORD` | Admin password (required) | - |
| `MALLARDB_USER` | Admin username | mallard |
| `MALLARDB_DB` | Database name | $MALLARDB_USER |
| `MALLARDB_DATABASE` | Database file path | ./data/mallard.db |
| `MALLARDB_PORT` | Listen port | 5432 |
| `MALLARDB_HOST` | Listen address | 0.0.0.0 |
| `MALLARDB_READONLY_USER` | Read-only username | (disabled) |
| `MALLARDB_READONLY_PASSWORD` | Read-only password | (disabled) |

All variables accept `POSTGRES_*` prefix for Docker compatibility.

## CLI Options

| Flag | Short | Description |
|------|-------|-------------|
| `--database` | `-d` | Database file path |
| `--port` | `-p` | Listen port |
| `--host` | `-H` | Listen address |

## License

MIT
