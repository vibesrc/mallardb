# RFC: jobs.d — SQL-Native Job Scheduler for MallardDB

**Status:** Draft  
**Author:** Brennen Herbruck  
**Created:** 2026-01-04

## Summary

Add a filesystem-based SQL job scheduler to MallardDB called `jobs.d`. Jobs are directories containing sequential SQL files and a minimal `job.toml` config. Scheduled via cron, executed in-process, introspectable via SQL.

This is intentionally not Airflow, dbt, or Dagster. It's cron + SQL + DuckDB.

## Motivation

DuckDB's extension ecosystem enables SQL to directly query and join files (CSV, Parquet, Excel, JSON), object storage, HTTP endpoints, and external databases (Postgres, MySQL, ODBC). This makes it possible to express most analytical ETL pipelines entirely in SQL.

MallardDB already has `startdb.d` for boot-time SQL scripts. `jobs.d` extends this pattern with time-triggered execution, providing scheduling and operational clarity without orchestration complexity.

Together they form a SQL-native analytical runtime:

- `startdb.d` → deterministic boot-time SQL
- `jobs.d` → deterministic time-triggered SQL

## Design

### Directory Structure

```
jobs.d/
  accounting/
    job.toml
    01.sql
    02.sql
    03.sql
  daily_sync/
    job.toml
    01.sql
```

Each subdirectory represents exactly one job. SQL files execute in lexical order.

### job.toml

```toml
schedule = "0 */15 * * * *"  # cron expression (required)
enabled = true                # optional, default true
queue_max = 3                 # optional, max queued runs
timeout = "30m"               # optional, job timeout
```

That's the entire config surface. No retries, branching, dependencies, or backfills.

### Execution Semantics

- Each SQL file runs in its own transaction
- Files execute in lexical order
- On failure: stop the job, log the error, wait for next scheduled run
- Jobs queue in FIFO order per job—if a run is in progress when schedule fires, new run queues
- If `queue_max` is set, drop new runs when queue is full
- SQL files are loaded at job start, not file start (mid-run edits don't affect current execution)

### Environment Variable Interpolation

SQL files support `${ENV_VAR}` syntax, substituted at file load time:

```sql
ATTACH '${POSTGRES_CONNSTR}' AS source (TYPE postgres);
COPY source.public.orders TO '${S3_BUCKET}/orders.parquet';
```

Missing env var → fail the job with clear error. No silent empty strings.

Implementation: regex `\$\{([A-Z_][A-Z0-9_]*)\}`, substitute from environment.

### Dynamic Reloading

File watcher on `jobs.d/` picks up changes live. No server restart required.

| Change | Behavior |
|--------|----------|
| New directory | Register job, start scheduling |
| `job.toml` modified | Update schedule/config, queue stays intact |
| SQL file modified | Next run picks up new version |
| Directory deleted | Stop scheduling, clear queue |

Debounce changes by ~1 second to handle multi-file updates (e.g., `git pull`).

## Introspection Schema

All tables live in the `_mallardb` schema.

### _mallardb.jobs

Virtual table reflecting current registered jobs from filesystem:

```sql
CREATE TABLE _mallardb.jobs (
  name TEXT,           -- directory name
  schedule TEXT,       -- cron expression
  enabled BOOLEAN,     -- default true
  queue_max INTEGER,   -- null if not set
  timeout TEXT,        -- null if not set
  file_count INTEGER,  -- number of .sql files
  queued INTEGER,      -- current queue depth
  running BOOLEAN      -- true if executing
);
```

Read-only. Backed by in-memory scheduler state, not persisted.

### _mallardb.job_queue

Current queue state:

```sql
CREATE TABLE _mallardb.job_queue (
  job_name TEXT,
  queued_at TIMESTAMP,
  started_at TIMESTAMP  -- null if waiting
);
```

### _mallardb.job_runs

Historical execution log:

```sql
CREATE TABLE _mallardb.job_runs (
  id INTEGER PRIMARY KEY,
  job_name TEXT,
  file_name TEXT,       -- null for job-level records
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status TEXT,          -- 'running', 'success', 'failed'
  error_message TEXT
);
```

This is a real DuckDB table, persisted and queryable.

### Example Queries

```sql
-- All registered jobs
SELECT * FROM _mallardb.jobs;

-- Currently running or queued
SELECT name, queued, running FROM _mallardb.jobs WHERE running OR queued > 0;

-- Failed runs in last 24 hours
SELECT job_name, file_name, error_message, started_at
FROM _mallardb.job_runs
WHERE status = 'failed' AND started_at > NOW() - INTERVAL '24 hours';

-- Average runtime per job
SELECT job_name, AVG(finished_at - started_at) AS avg_duration
FROM _mallardb.job_runs
WHERE status = 'success' AND file_name IS NULL
GROUP BY job_name;
```

## Implementation Notes

### Crates

- `notify` — filesystem watching
- `tokio-cron-scheduler` or `cron` — schedule parsing and triggering
- `toml` — config parsing

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                   File Watcher                       │
│            (notify, watches jobs.d/)                 │
└─────────────────────┬───────────────────────────────┘
                      │ register/update/remove
                      ▼
┌─────────────────────────────────────────────────────┐
│                   Job Registry                       │
│         (in-memory, backs _mallardb.jobs)           │
└─────────────────────┬───────────────────────────────┘
                      │ schedule fires
                      ▼
┌─────────────────────────────────────────────────────┐
│                   Job Queue                          │
│          (per-job FIFO, backs job_queue)            │
└─────────────────────┬───────────────────────────────┘
                      │ executor pulls
                      ▼
┌─────────────────────────────────────────────────────┐
│                  Job Executor                        │
│   (load files, interpolate env, execute, log)       │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│                    DuckDB                            │
│            (via existing writer queue)              │
└─────────────────────────────────────────────────────┘
```

### Execution Flow

1. Schedule fires → push job onto its queue
2. If queue exceeds `queue_max`, drop the new entry
3. Executor pops from queue, sets `started_at`
4. Load all `.sql` files from directory, sort lexically
5. For each file: interpolate env vars, execute, log to `job_runs`
6. On success: log job completion, remove from queue
7. On failure: log error, stop job, remove from queue

### Writer Queue Integration

Jobs execute through MallardDB's existing writer queue, respecting the single-writer constraint. Jobs don't get special treatment—they're just another source of write operations.

## Out of Scope

- Retries
- Dependencies between jobs
- Backfills
- Branching/conditional execution
- External notifications (webhook, email, etc.)
- Job parameters/arguments
- Manual triggering via SQL

Users who need these features should use a real orchestrator. Jobs can query `_mallardb.job_runs` and implement their own alerting via scheduled SQL if needed.

## Future Considerations

Not planned, but possible extensions if demand exists:

- `MALLARDB_JOB_NAME` / `MALLARDB_RUN_ID` env vars injected at runtime
- `_mallardb.trigger_job('name')` function for manual execution
- Job run log retention policy (auto-delete old rows)
- Prometheus metrics endpoint for job stats

## References

- [MallardDB](https://github.com/vibesrc/mallardb)
- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [PostgreSQL Docker entrypoint conventions](https://github.com/docker-library/postgres)