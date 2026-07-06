# Schema Design

DDL lives in [clickhouse/init/](../clickhouse/init/), applied automatically on the first container start. This document explains the decisions in the order they were made: storage strategy first, then query patterns, then engines and keys. In ClickHouse the physical layout is the schema design; get the sort key or the retention story wrong and no amount of tuning later saves you.

## The storage strategy: raw expires, rollups are forever

The live system stores payment data at three grains with three lifetimes:

| Table | Grain | Engine | Retention | Grows with |
|---|---|---|---|---|
| `events` | one row per payment event | MergeTree | 90 days (TTL) | traffic |
| `platform_minute_stats` | per minute, platform wide | AggregatingMergeTree | 90 days (TTL) | time |
| `daily_merchant_stats` | per merchant per day | SummingMergeTree | forever | merchants x days |

This split is the answer to the only storage question that matters for a stream: raw events grow with traffic and would grow without bound, but almost every question asked of data older than a few weeks is an aggregate question. So raw events live 90 days for drill down and incident investigation, and the daily rollup (which grows at a bounded, predictable rate: 3,000 merchants times 365 days is about 1M small rows a year at worst) is the permanent record. Losing raw events past 90 days is a deliberate, documented trade, not an accident.

## Query patterns

Two workloads now, not one:

**Operational (seconds to hours old):** volume and failure rate right now, per minute trend over the last few hours, which merchants are failing abnormally. Hits `events` and `platform_minute_stats`, refreshed every 30 seconds by the live dashboard.

**Historical (days to years):** monthly volume trends, merchant segmentation, top merchants over a quarter. Hits `daily_merchant_stats` exclusively; it never needs raw events.

Both workloads scan many rows, touch few columns, and aggregate hard, which is the shape columnar storage is built for.

## Why MergeTree

`MergeTree` is ClickHouse's workhorse table engine. Data arrives in sorted immutable parts, each column stored as its own compressed file, and background threads merge small parts into bigger ones. Three properties matter here:

- **Columnar reads.** A query touching 4 of 7 columns reads roughly 4/7 of the bytes, in practice far less because sorted, low variety columns compress extremely well.
- **A sparse primary index.** One index entry per granule (8,192 rows by default), so the whole index stays in memory and range scans are cheap.
- **Ordered storage.** Rows sort physically by the `ORDER BY` key, so rows queried together sit together on disk.

The merge process is also what makes TTL and the aggregating engines work, which is why every specialized engine below is a MergeTree variant: they are all hooks into the same merge machinery.

## The events table

```sql
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (merchant_id, event_time)
TTL event_time + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1
```

**ORDER BY (merchant_id, event_time).** The dominant per-row access pattern is merchant scoped: "show me merchant X's events in this window" during an incident, and the anomaly detector's per-merchant recent windows. With this key each merchant is one contiguous sorted run per part. Platform wide time queries do not need the sort key: partition pruning cuts them to the touched months, and the per-minute rollup answers most of them anyway. The key is spent on the access pattern that has no other index to fall back on. `transaction_id` stays out of the key: it is an identifier nobody filters by, and a unique leading column would destroy the compression that sorted repetition buys.

**PARTITION BY month, decided at creation.** Partitions give coarse pruning for time ranges and instant data management (`DROP PARTITION`). Monthly is the right grain: day partitions at this volume would create thousands of tiny parts. The reason partitioning happened in the producer PR rather than the hardening PR is that ClickHouse cannot change a table's partitioning afterwards; it requires a full rebuild (new table, `INSERT SELECT`, rename swap). Partitioning is a birth decision.

**TTL, added later on purpose.** `TTL event_time + INTERVAL 90 DAY` was added to the live table with `ALTER TABLE ... MODIFY TTL`, a metadata-only operation, which is exactly why retention did not need to be decided at creation. TTL deletes during merges, so expiry is eventual, not on the dot of day 90; a part whose newest row expired yesterday may sit on disk until the next merge touches it. `ttl_only_drop_parts = 1` tells ClickHouse to wait until an entire part has expired and then drop the part whole (nearly free) instead of rewriting parts to delete rows out of their middle (a full part rewrite). With monthly partitions and a 90 day TTL, parts age out cleanly a month at a time.

## Column types

| Column | Type | Why |
|---|---|---|
| `merchant_id` | `LowCardinality(String)` | 3,000 distinct values. Dictionary encoding stores each string once and 2 byte codes in the data. Pays off below roughly 10K distinct values. |
| `payment_method`, `failure_reason` | `LowCardinality(String)` | 3 and 5 distinct values. Same reasoning, stronger effect. |
| `status` | `Enum8` | Two known states, one byte, and the schema rejects typos at insert time. |
| `amount` | `Decimal(18,2)` | Money. Floats drift under aggregation; Decimal arithmetic is exact. |
| `event_time` | `DateTime` | Second precision, 4 bytes. The historical dataset only carried dates; a live system needs minutes and seconds. |
| `failure_reason` default `''` | not `Nullable` | Nullable adds a null mask file per part and branching to reads. Empty string is an unambiguous "no failure". |

## Materialized views: the real-time aggregates

A ClickHouse materialized view is an **insert trigger, not a scheduled refresh**. Every block inserted into `events` passes through the view's SELECT and the partial aggregate lands in the target table. Two consequences drive everything below:

1. An MV never re-reads existing data. Backfilling history means inserting directly into the target table (`INSERT INTO target SELECT ... ` shaped like the MV query), which is what [scripts/backfill_history.py](../scripts/backfill_history.py) does.
2. The target holds partial aggregates from different insert blocks. Merges combine them eventually, so correct reads always re-aggregate (`sum(...) GROUP BY ...`). A raw `SELECT *` silently undercounts. Every dashboard query in this repo aggregates.

### platform_minute_stats: AggregatingMergeTree

Per minute: transaction count, failure count, amount, and **distinct active merchants**. That last metric forces the engine choice. Counts and sums merge by addition, but distinct counts do not: 900 distinct merchants in one insert block plus 900 in another is not 1,800. Merging distinct counts requires keeping the intermediate HyperLogLog style state, which is what `AggregateFunction(uniq, String)` stores and what `AggregatingMergeTree` knows how to combine during merges. Reads finalize the state with `uniqMerge(active_merchants)`.

The plain sums ride along as `SimpleAggregateFunction(sum, ...)` columns: stored as ordinary numbers, summed on merge, no state overhead. So the table uses full aggregate state only where the math demands it.

### daily_merchant_stats: SummingMergeTree

Per merchant per day: transaction count, failure count, successful amount. Every column is a count or a sum, all of which merge by plain addition, so `SummingMergeTree` (which sums numeric columns of rows sharing a sort key during merges) is sufficient and simpler: plain numeric columns, readable without merge functions, no aggregate states to understand. `AggregatingMergeTree` earns its complexity only when something cannot be summed; using it here would be engine cosplay.

This table is fed twice: by the MV for the live stream, and by the backfill for the 24 months of generated history. The backfill window (2024-07 through 2026-06) aligns exactly with the table's monthly partitions, so re-running the backfill drops those partitions and reloads them, never touching partitions the live stream writes into. Aligning partition keys with reload units is what makes backfills safely repeatable.

## The segments view

`merchant_segments` translates the RFM segmentation from my [payment-merchant-clv-segmentation](https://github.com/RidhanPar/payment-merchant-clv-segmentation) project into SQL over the daily rollup: quintile scores via `ntile(5)`, then the original rule table mapping score combinations to segments. It reads `daily_merchant_stats` rather than raw events because RFM needs each merchant's full lifetime and raw events expire at 90 days. Daily grain loses nothing: R has day precision by definition, F and M are sums.

Two details earned their place through actual failures during development:

- **Quintile tie breaking.** On a live platform most of the active book transacted yesterday, so recency ties across thousands of merchants. `ntile` splits ties by arbitrary row order, which turned R scores into noise (Champions collapsed from 553 to 22 the first time the view ran against live data). The window now orders by recency, then frequency, then monetary: deterministic, and defensible as "among equally recent merchants, the busier one ranks higher".
- **SummingMergeTree safe recency.** "Last day with a success" is computed as `maxIf(day, txn_count > failed_count)` over possibly unmerged partial rows. This is merge safe because each partial row aggregates one insert block and is internally consistent: a block containing at least one success necessarily has `txn_count > failed_count`.

Still a plain view: 3,000 output rows, milliseconds to compute, always current.

## The legacy transactions table

The original `payments.transactions` table (day precision, bulk loaded) is superseded by this pipeline: its history now lives in `daily_merchant_stats` via the backfill, and its role as dashboard source ends when the dashboards repoint to the live tables. It is no longer created for fresh clones and will be dropped from the running instance in the dashboard migration stage.
