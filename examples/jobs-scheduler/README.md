# Jobs Scheduler Example

This example demonstrates MallardDB's built-in SQL job scheduler. Jobs are defined as directories containing `job.toml` configuration and numbered SQL files that execute in order.

## Quick Start

```bash
# Start MallardDB with the example jobs
docker compose up -d

# Connect with psql
psql -h localhost -U mallard -d mallard
```

## Example Jobs

### metrics_collector

Runs every minute to collect sample metrics:

- `job.toml` - Schedule: every minute, queue max 2, timeout 30s
- `01_ensure_table.sql` - Creates the metrics table if needed
- `02_collect.sql` - Inserts a random metric value

### data_cleanup

Runs daily at midnight to clean up old data:

- `job.toml` - Schedule: midnight daily, timeout 5m
- `01_cleanup_metrics.sql` - Deletes metrics older than `RETENTION_DAYS`
- `02_cleanup_job_runs.sql` - Deletes job run history older than 30 days

## Environment Variables

Jobs support `${VAR}` interpolation in SQL files:

| Variable | Description | Default |
|----------|-------------|---------|
| `RETENTION_DAYS` | Days to keep metrics | 7 |
| `METRICS_TABLE` | Target table name | system_metrics |

## Introspection

Query job status via SQL:

```sql
-- List all registered jobs
SELECT * FROM _mallardb.jobs;

-- View current queue
SELECT * FROM _mallardb.job_queue;

-- Recent job runs
SELECT job_name, status, started_at, finished_at
FROM _mallardb.job_runs
ORDER BY started_at DESC
LIMIT 20;

-- Failed runs in last 24 hours
SELECT job_name, file_name, error_message, started_at
FROM _mallardb.job_runs
WHERE status = 'failed'
  AND started_at > NOW() - INTERVAL '24 hours';

-- Average runtime per job
SELECT job_name,
       AVG(finished_at - started_at) AS avg_duration,
       COUNT(*) AS run_count
FROM _mallardb.job_runs
WHERE status = 'success' AND file_name IS NULL
GROUP BY job_name;
```

## Dynamic Reloading

Jobs reload automatically when files change:

```bash
# Edit a job config (takes effect within ~1 second)
vim jobs/metrics_collector/job.toml

# Add a new job
mkdir jobs/new_job
echo 'schedule = "0 */5 * * * *"' > jobs/new_job/job.toml
echo 'SELECT 1' > jobs/new_job/01.sql

# Remove a job
rm -rf jobs/old_job
```

## Creating Your Own Jobs

1. Create a directory under `jobs/`:
   ```bash
   mkdir jobs/my_etl_job
   ```

2. Add `job.toml`:
   ```toml
   schedule = "0 0 * * * *"  # Every hour
   enabled = true
   queue_max = 1             # Optional: max queued runs
   timeout = "10m"           # Optional: job timeout
   ```

3. Add numbered SQL files:
   ```bash
   # Files execute in lexical order
   echo 'CREATE TABLE IF NOT EXISTS ...' > jobs/my_etl_job/01_setup.sql
   echo 'INSERT INTO ... SELECT ...' > jobs/my_etl_job/02_extract.sql
   echo 'UPDATE ... SET ...' > jobs/my_etl_job/03_transform.sql
   ```

## Cron Expression Format

Standard 6-field cron expressions:

```
┌──────────── second (0-59)
│ ┌────────── minute (0-59)
│ │ ┌──────── hour (0-23)
│ │ │ ┌────── day of month (1-31)
│ │ │ │ ┌──── month (1-12)
│ │ │ │ │ ┌── day of week (0-6, Sun=0)
│ │ │ │ │ │
* * * * * *
```

Examples:
- `0 * * * * *` - Every minute
- `0 */15 * * * *` - Every 15 minutes
- `0 0 * * * *` - Every hour
- `0 0 0 * * *` - Daily at midnight
- `0 0 0 * * 1` - Weekly on Monday
