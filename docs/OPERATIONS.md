# Operations Runbook

How to tell whether this system is healthy, what its failure modes look like, and how to back it up and put it back together. Commands assume the stack is running and use the local dev credentials; substitute values from `.env` where changed.

A shorthand used throughout:

```bash
alias chc="docker exec payments-clickhouse clickhouse-client --user analytics --password analytics_local"
```

## Service health at a glance

```bash
docker compose ps
```

Every long-running service has a healthcheck, so this one command is the first triage step:

| Service | Healthcheck | Unhealthy means |
|---|---|---|
| `payments-clickhouse` | `GET /ping` on 8123 | server down or not accepting HTTP |
| `payments-superset` | `GET /health` on 8088 | gunicorn down or metadata DB unreachable |
| `superset-metadata-db` | `pg_isready` | Postgres not accepting connections |
| `payments-producer` | heartbeat file younger than 30 s | no successful insert for 30 s, even if the process is alive and retrying |
| `superset-init` | none, by design | one-shot job; its exit status gates the superset service via `service_completed_successfully` |

The producer healthcheck deserves the explanation: the process touches `/tmp/heartbeat` only after ClickHouse confirms an insert (`wait_for_async_insert=1`). A producer that is running but failing to insert (network split, auth failure, full disk) goes unhealthy within 30 seconds while its log shows `flush failed, will retry` lines.

## Is data flowing? The System Monitoring dashboard

Superset has a "System Monitoring" dashboard (auto refresh 30 s) showing the three numbers that matter, each also queryable directly:

**Ingestion lag**, the whole path in one number (generate, buffer, insert, commit):

```sql
SELECT dateDiff('second', max(event_time), now()) AS lag_seconds
FROM payments.events;
```

Healthy is under ~15 s (a 5 s flush interval plus insert time; observed steady state on this machine is 1 to 6 s). A climbing lag with a healthy producer container means inserts succeed but generation stalled; a climbing lag with an unhealthy producer means inserts are failing.

**Ingestion rate**, rows per minute from the minute rollup (observed ~1,400 to 2,400 depending on the daily cycle):

```sql
SELECT minute, sum(txn_count) AS rows_per_minute
FROM payments.platform_minute_stats
WHERE minute > now() - INTERVAL 10 MINUTE
GROUP BY minute ORDER BY minute;
```

**Materialized view consistency**, which should always hold exactly (the MVs are insert triggers, they cannot drift unless someone bypasses them):

```sql
SELECT
    (SELECT count() FROM payments.events) AS raw,
    (SELECT sum(txn_count) FROM payments.platform_minute_stats) AS minute_rollup,
    (SELECT sum(txn_count) FROM payments.daily_merchant_stats WHERE day >= '2026-07-01') AS daily_rollup;
```

The three numbers match while the producer streams. If they diverge, someone inserted into a target table directly or dropped and recreated an MV without backfilling; see the backfill pattern in SCHEMA.md.

## ClickHouse health via system tables

**system.parts** answers "is storage healthy". Active part count per table is the single best insert-discipline signal:

```sql
SELECT table, count() AS active_parts, sum(rows) AS rows,
       formatReadableSize(sum(data_compressed_bytes)) AS on_disk
FROM system.parts
WHERE database = 'payments' AND active
GROUP BY table ORDER BY rows DESC;
```

Observed healthy state: `events` holds ~100K rows in single-digit active parts. That is batching plus `async_insert` plus background merges working. Hundreds of active parts on a table means some writer is inserting tiny batches; find it before merges fall behind (the server eventually refuses inserts with TOO_MANY_PARTS).

**system.metrics** is current server state; the ones worth watching here:

```sql
SELECT metric, value FROM system.metrics
WHERE metric IN ('Query', 'Merge', 'PartsActive', 'TCPConnection', 'HTTPConnection');
```

**system.merges** shows merge activity in flight (usually empty at this volume; long-running entries mean merges are struggling):

```sql
SELECT database, table, elapsed, progress FROM system.merges;
```

**system.query_log** is the flight recorder. Slowest queries of the last hour:

```sql
SELECT event_time, query_duration_ms, read_rows, substring(query, 1, 80) AS q
FROM system.query_log
WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 1 HOUR
ORDER BY query_duration_ms DESC LIMIT 10;
```

## How TTL retention behaves

`events` and `platform_minute_stats` carry `TTL ... + INTERVAL 90 DAY` with `ttl_only_drop_parts = 1`. What to actually expect:

- **Expiry is eventual, not on the dot.** TTL runs as part of merges. A row can outlive its 90th day until a merge (or the periodic TTL task, `merge_with_ttl_timeout`, default 4 hours) touches its part.
- **Whole parts drop at once.** With `ttl_only_drop_parts`, ClickHouse waits until every row in a part is expired, then drops the part, which is nearly free. Monthly partitions mean parts age out cleanly month by month. Without the setting, parts get rewritten to delete rows out of the middle, which is IO the data layout makes unnecessary.
- **Verify what will expire when** by looking at part date ranges:

```sql
SELECT partition, count() AS parts, min(min_date) AS oldest, max(max_date) AS newest
FROM system.parts
WHERE database = 'payments' AND table = 'events' AND active
GROUP BY partition ORDER BY partition;
```

- **Force TTL evaluation now** (after changing a TTL, or to test):

```sql
ALTER TABLE payments.events MATERIALIZE TTL;
-- or force a merge, which applies TTL as a side effect:
OPTIMIZE TABLE payments.events FINAL;
```

- **The history table is exempt on purpose.** `daily_merchant_stats` has no TTL; it is the permanent record. If someone proposes adding one, the segments view and the entire history dashboard are the blast radius.

## Backup and restore

The compose file mounts a dedicated `clickhouse_backups` volume at `/backups` and [clickhouse/config.d/backups.xml](../clickhouse/config.d/backups.xml) registers it as an allowed backup disk. Backups therefore survive container recreation and live outside the data volume they protect.

**Back up the analytics database** (tested: ~4 MB for the full dataset):

```sql
BACKUP DATABASE payments TO Disk('backups', 'payments-2026-07-06.zip');
```

**Restore a single table without touching the live one** (tested; this is also the way to verify a backup is usable, which an untested backup is not):

```sql
RESTORE TABLE payments.merchants AS payments.merchants_restored
FROM Disk('backups', 'payments-2026-07-06.zip');
SELECT count() FROM payments.merchants_restored;  -- expect 3000
DROP TABLE payments.merchants_restored;
```

**Full disaster restore**, in order:

1. Stop the producer: `docker compose stop producer` (nothing writes during restore).
2. `RESTORE DATABASE payments FROM Disk('backups', '<file>')` (drop the damaged database first if it still exists).
3. Restart the producer. It re-derives its traffic weights from the restored rollup at startup, so no producer state needs restoring.

**Superset metadata** (dashboards, charts, users) lives in Postgres, standard tooling applies:

```bash
docker exec superset-metadata-db pg_dump -U superset superset > superset_metadata.sql
```

Though for this repo the committed dashboard export plus `build_dashboard.py` already reconstruct the BI layer from code, which is the better recovery path: `superset db upgrade` on a fresh metadata volume, then re-run the build script.

**Copy backups off the machine**: `docker cp payments-clickhouse:/backups/<file> .`

## Reseeding and resets

- **Re-run the history backfill** any time: `py -3.11 scripts/backfill_history.py`. It drops and reloads exactly the 24 monthly history partitions and never touches partitions the live stream writes.
- **Full reset**: `docker compose down -v` deletes all volumes (data, metadata, backups). Next start recreates the schema from `clickhouse/init/`, then run the backfill, then `build_dashboard.py`.
- **Producer restarts are stateless.** Weights come from the rollup at startup, ids are UUID based, and the MVs pick up wherever inserts resume. `docker compose restart producer` any time.

## Failure modes seen while building this

- **Producer unhealthy, logs show flushes failing:** ClickHouse is down or unreachable. The producer buffers and retries; once inserts succeed the heartbeat resumes. Data generated during the outage window is retained in the buffer, not lost, unless the container itself restarts.
- **A dashboard chart shows lower numbers than expected on a rollup table:** almost always a raw read of a Summing/Aggregating target. Reads must aggregate (`sum(...) GROUP BY`, `uniqMerge(...)`); see SCHEMA.md.
- **superset-init exits non-zero on a version bump:** run `docker compose run --rm superset-init` to see the migration output directly; the web service will not start against an unmigrated schema, which is the guardrail working as intended.
