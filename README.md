# mallardb

Looks like an elephant. Quacks like a duck.

You probably don’t need a traditional data warehouse. Most teams have medium data, not big data. DuckDB on a single node handles what most organizations actually have. mallardb puts a PostgreSQL endpoint in front of that engine.

One container. No cluster. No warehouse bills.

**DuckDB can already query almost anything. mallardb lets almost anything query DuckDB.**

![meme](docs/meme.png)

## Why This Exists

DuckDB can query anything (Parquet files, S3 buckets, PostgreSQL, MySQL, SQLite, CSV, JSON) and join them all in a single query. It stores data in a fast columnar format with ETL built right into the database. But DuckDB is an embedded engine. It doesn't speak PostgreSQL wire protocol.

Your BI tools do. Tableau, Metabase, Grafana, dbt: they all expect a PostgreSQL endpoint.

mallardb bridges that gap. One connection string gives your tools access to DuckDB's full power: federated queries across sources, columnar storage, vectorized execution, and extensions for everything from spatial data to machine learning. No warehouse cluster. No per-query billing. Just a PostgreSQL endpoint backed by DuckDB.

## The Simple Stack

```
Sources → mallardb → BI Tools
```

That's it. This can be your entire analytical layer. Tableau, Metabase, PowerBI, Grafana—they all speak PostgreSQL. Data flows in, dashboards query out. mallardb works as both an analytics engine and a lightweight ETL layer. Pull data in, reshape it, and query it fast.

## Three Modes

Use one, two, or all three. Mix freely.

| Mode            | How it works                                                             | Best for                                 |
| --------------- | ------------------------------------------------------------------------ | ---------------------------------------- |
| **Internal**    | Load data into DuckDB's native columnar tables                           | Fast queries, self-contained analytics   |
| **Parquet**     | ETL to S3 or local Parquet files, query in place                         | Portable data, open formats, sharing     |
| **Passthrough** | Query external databases, files, and object storage directly (read-only) | No copies, live analytics across systems |

```sql
-- Internal: create and query native tables
CREATE TABLE events AS SELECT * FROM read_parquet('s3://raw/events/*.parquet');
SELECT date_trunc('day', ts), count(*) FROM events GROUP BY 1;

-- Parquet: write to files, query later
COPY (SELECT * FROM events WHERE year = 2024) TO 's3://warehouse/events/2024.parquet';
SELECT * FROM read_parquet('s3://warehouse/events/*.parquet');

-- Read-only access is strongly recommended for production systems
ATTACH 'postgres://readonly:pass@prod/app' AS prod (TYPE POSTGRES);
SELECT * FROM prod.public.customers WHERE segment = 'enterprise';
```

## Quick Start (Docker)

Create a `compose.yml`:

```yaml
services:
  mallardb:
    image: ghcr.io/vibesrc/mallardb:latest
    ports:
      - 5432:5432
    volumes:
      - mallardb_data:/var/lib/mallardb
      # - ./initdb.d:/docker-entrypoint-initdb.d:ro   # schema, seeds (first boot only)
      # - ./startdb.d:/docker-entrypoint-startdb.d:ro # refresh views, ETL (every boot)
    environment:
      POSTGRES_USER: mallard
      POSTGRES_PASSWORD: mallard
      POSTGRES_DB: mallard

volumes:
  mallardb_data:
```

```bash
docker compose up
```

Connect with any PostgreSQL client:

```bash
PGPASSWORD=mallard psql -h 127.0.0.1 -U mallard -d mallard
```

To use an existing DuckDB file, mount it directly:

```bash
docker run -p 5432:5432 \
  -e POSTGRES_PASSWORD=secret \
  -v /path/to/your/data.db:/var/lib/mallardb/data/mallard.db \
  ghcr.io/vibesrc/mallardb:latest
```

## What This Enables

**Federated analytics**: Join a Parquet file in S3 with a table in your production PostgreSQL database, from any BI tool:

```sql
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

**BI tools on data lakes**: Point Tableau or Metabase at mallardb, query Parquet files as if they were tables.

**Existing DuckDB databases**: Point mallardb at any `.db` file and immediately expose it via PostgreSQL protocol.

## What This Is NOT

mallardb is intentionally limited. It is:

- **Not a PostgreSQL replacement**: It doesn't implement PostgreSQL semantics, just the wire protocol
- **Not an OLTP database**: DuckDB is analytical; use it for reads and batch operations
- **Not transactional**: No multi-statement ACID transactions across sources

If you need PostgreSQL, use PostgreSQL. mallardb is for when you need analytical power but your tools only speak PostgreSQL.

## Safety Model

mallardb supports two roles:

| Role                   | Capabilities                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------ |
| **Admin** (read-write) | Full DuckDB access: create tables, write files, attach databases, install extensions |
| **Reader** (read-only) | Query only: SELECT statements, no modifications                                      |

Connect your BI tools with the read-only role. Writes and administrative operations require the admin role.

## DuckDB Extensions

Install and use any DuckDB extension:

```sql
INSTALL httpfs;
LOAD httpfs;

-- Now query S3 directly
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');
```

Extensions are persisted in the data volume (`/var/lib/mallardb/extensions` in Docker), so they survive container restarts.

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
│  │ Native  │ │ Parquet │ │External │ │   S3    │ │ Flat  │  │
│  │ tables  │ │  files  │ │databases│ │ buckets │ │ files │  │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └───────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Configuration Reference

| Variable                     | Description               | Default           |
| ---------------------------- | ------------------------- | ----------------- |
| `MALLARDB_PASSWORD`          | Admin password (required) | -                 |
| `MALLARDB_USER`              | Admin username            | mallard           |
| `MALLARDB_DB`                | Database name             | $MALLARDB_USER    |
| `MALLARDB_DATABASE`          | Database file path        | ./data/mallard.db |
| `MALLARDB_PORT`              | Listen port               | 5432              |
| `MALLARDB_HOST`              | Listen address            | 0.0.0.0           |
| `MALLARDB_READONLY_USER`     | Read-only username        | (disabled)        |
| `MALLARDB_READONLY_PASSWORD` | Read-only password        | (disabled)        |

All variables accept `POSTGRES_*` prefix for Docker compatibility.

## CLI Options

| Flag         | Short | Description        |
| ------------ | ----- | ------------------ |
| `--database` | `-d`  | Database file path |
| `--port`     | `-p`  | Listen port        |
| `--host`     | `-H`  | Listen address     |
