# Payments Analytics on ClickHouse and Apache Superset

A local real-time analytics stack: ClickHouse as the analytical database, Apache Superset as the BI layer, wired together with Docker Compose. The dataset is a simulated payments portfolio of about 575K transactions across 3,000 merchants over 24 months, reused from my [payment-merchant-clv-segmentation](https://github.com/RidhanPar/payment-merchant-clv-segmentation) project.

Built in stages, one pull request per stage:

1. Docker Compose stack and setup documentation
2. ClickHouse schema (MergeTree table, materialized view)
3. Batched data load
4. Superset dashboard, exported and committed
5. Query benchmarks

## Why ClickHouse for this workload

The dashboard queries share one shape: scan hundreds of thousands of transaction rows, touch 3 or 4 of the 7 columns, aggregate by merchant or by time. A row store reads every column of every row to answer that; a columnar engine reads only the columns the query names, and because each column is stored sorted and together, it compresses to a fraction of its raw size (this table: 6.41 MiB on disk for 575K rows, 2.4x under the raw bytes). Add MergeTree's sparse primary index and partition pruning and the result is single digit millisecond aggregations with no pre-computed cubes, no query cache warming, and no indexes to hand tune per dashboard filter.

The trade is that ClickHouse is bad at what OLTP databases are good at: point lookups, updates, deletes, high concurrency small transactions. Nothing in a BI workload needs those, which is why the pairing with Superset works: Superset generates exactly the aggregate heavy, low concurrency SQL that ClickHouse eats.

## Quick start

```bash
git clone https://github.com/RidhanPar/clickhouse-payments-analytics.git
cd clickhouse-payments-analytics
docker compose up -d --build      # ClickHouse + Superset + metadata DB, schema auto-applied
pip install -r requirements.txt
python scripts/backfill_history.py # seed merchants + 24 months of daily history
python scripts/build_dashboard.py  # create charts + dashboard, or import the committed export
```

Superset then runs at http://localhost:8088 (admin / admin_local). [docs/SETUP.md](docs/SETUP.md) documents every configuration choice in the stack; [docs/SCHEMA.md](docs/SCHEMA.md) explains the table design.

## Dataset and load

The generator is copied unchanged from the segmentation project (seeded with `numpy` seed 42, so every run produces the identical dataset): 3,000 merchants with lognormal ticket sizes and activity rates, industry dependent decline rates, and a churned subset, producing 575,226 transactions across 24 months.

```bash
pip install -r requirements.txt
py -3.11 scripts/backfill_history.py
```

The backfill generates the data in memory, aggregates it to daily grain, and inserts it over the HTTP interface in 100K row batches (raw history would be deleted by the 90 day TTL on events, so history is seeded into the rollup table; docs/SCHEMA.md explains the storage split). Batching matters in ClickHouse: every insert becomes an immutable part on disk, so small frequent inserts create part counts that stall background merges. The load verifies transaction counts against the generator afterwards.

Measured on this machine (Docker Desktop on Windows, ClickHouse 24.8):

| Metric | Value |
|---|---|
| Transactions loaded | 575,226 |
| Merchants loaded | 3,000 |
| Insert wall time | 2.92 s |
| Insert throughput | ~197,000 rows/s |
| Overall failure rate | 6.74% |
| On-disk size (transactions) | 6.41 MiB compressed (15.14 MiB uncompressed) |

The materialized view target ends up with 372,370 rows against 575,226 in the base table, a modest 1.5x reduction at this data density (many merchants transact less than daily). The reduction grows with volume; the point of the pattern is that the aggregate table grows with merchants times days while the base table grows with transactions.

## Dashboard

The "Payments Portfolio Overview" dashboard recreates the core views from the segmentation project as Superset charts running directly on ClickHouse:

| Chart | Source | Query shape |
|---|---|---|
| Transaction volume over time | `transactions` | monthly `count(*)` |
| Failure rate by merchant segment | virtual dataset joining `transactions` to the `merchant_segments` view | `countIf(status = 'Failed') / count(*)` per segment |
| Top merchants by processed volume | `transactions` | `sumIf(amount, status = 'Success')` per merchant, top 15 |
| Daily failed transactions | `daily_merchant_stats` (materialized view target) | `sum(failed_count)` per day, aggregating again at read time as SummingMergeTree requires |

The dashboard is built by [scripts/build_dashboard.py](scripts/build_dashboard.py) through the Superset REST API, so the entire definition is reviewable code, and the result is exported to [superset/exports/payments_portfolio_dashboard.zip](superset/exports/payments_portfolio_dashboard.zip). To reproduce on a fresh stack, either re-run the script or import the zip in the Superset UI (Dashboards, Import). Superset masks database passwords in exports, so an import prompts for the ClickHouse password (`analytics_local` unless changed in `.env`).

Screenshots: to be added under `docs/img/` after review.

## Benchmarks (measured on the running system)

Run by [scripts/benchmark.py](scripts/benchmark.py) while the producer streamed: median of 5 runs after a warmup, Docker Desktop for Windows, ClickHouse 24.8. At benchmark time the stream had accumulated 657,686 raw events over roughly two days of running, alongside 380,877 daily rollup rows carrying 24 months of history. Server execution comes from `system.query_log`; end to end includes the HTTP round trip through Docker's network stack, which adds a near constant ~45 ms on this machine and would be the first thing to investigate if these were user facing latencies.

| Query | Server execution | End to end (HTTP) | Rows read | Data read |
|---|---|---|---|---|
| Per-minute volume, last 3h (minute rollup) | 6 ms | 51 ms | 361 | 7.05 KiB |
| Per-minute volume, last 3h (raw events) | 8 ms | 55 ms | 90,609 | 442.43 KiB |
| Active merchants per minute, last 3h (uniqMerge) | 24 ms | 69 ms | 362 | 32.52 KiB |
| One merchant's raw event history (primary key hit) | 8 ms | 52 ms | 28,960 | 370.80 KiB |
| Anomaly watchlist, full view | 34 ms | 76 ms | 188,247 | 1.36 MiB |
| Monthly volume, 25 months (daily rollup) | 12 ms | 57 ms | 381,047 | 9.45 MiB |
| Daily failures, full history (daily rollup) | 10 ms | 57 ms | 381,047 | 3.63 MiB |
| Full RFM segmentation, computed live | 41 ms | 70 ms | 384,076 | 13.09 MiB |

What the numbers show, honestly:

- **The minute rollup earns its keep in rows read.** The same per-minute answer costs 361 rows via the rollup against 90,609 via raw events, a 250x IO saving that grows linearly with traffic, even though both still finish in single digit milliseconds at this volume. The wall clock gap comes with scale; the IO gap is already real.
- **Part pruning limits even the raw scan.** The 3 hour raw query read 90,609 of 657,686 rows without any help from the sort key (event_time is the second key column): ClickHouse skipped older parts entirely using per-part min/max metadata, because parts written by a stream are naturally time ordered.
- **The sparse primary index works.** One merchant's full two-day history read 28,960 rows out of 657K: MergeTree reads whole 8,192 row granules around the merchant's contiguous runs rather than scanning the table.
- **`uniqMerge` finalizes stored HyperLogLog states** from the AggregatingMergeTree in 24 ms; computing distinct merchants per minute from raw events on every dashboard refresh would re-scan the stream each time.
- **The anomaly watchlist, the most complex query in the system** (a 15 minute raw window, a 7 day baseline union, a join to the dimension table, and a pooled two-proportion z test), runs in 34 ms on the server. This is why detection is a view instead of a pipeline: there is nothing to schedule, restart, or fall behind.
- **Insert side:** the bulk backfill sustained ~197K rows/s in 100K batches. The live producer streams ~25 to 40 events/s (by design) with an observed ingestion lag of 1 to 6 seconds, and 657K streamed events sit in 8 active parts, which is the batching plus `async_insert` discipline doing its job.

## End to end verification

[scripts/verify_live.py](scripts/verify_live.py) checks the running system: TTL present in the DDL, events flowing (rate over a 45 s window), ingestion lag under 30 s, exact materialized view consistency (raw count = minute rollup sum = daily rollup sum), insert batching health (active part count), and that an injected anomaly burst is caught by the watchlist while it runs. The script identifies the burst merchant from the raw data itself (no natural merchant exceeds a 40% failure rate on 100+ events in 10 minutes), so detection is verified against ground truth rather than against the detector's own opinion.

The first full run of this script is what exposed the watchlist precision problem described in the anomaly view comments (7 thin-baseline false positives beside the real burst), which is the point of running verification against a live stream instead of eyeballing a demo. After switching the detector to a pooled two-proportion z test at 3.5 sigma, the rerun passed 8 of 8:

```
PASS  TTL on events: 90 day TTL with ttl_only_drop_parts
PASS  TTL on platform_minute_stats: 90 day TTL with ttl_only_drop_parts
PASS  events flowing: 1431 events in 45s (31.8/s)
PASS  ingestion lag: 2s behind wall clock (threshold 30s)
PASS  materialized view consistency: events=633,618 minute_rollup=633,618 daily_rollup=633,618
PASS  insert batching (active parts): 633,618 rows in 8 active parts
PASS  anomaly detection: ground truth burst MCH100539 (73% over 106 events) is on the watchlist (watchlist size 1)
PASS  watchlist precision: 1 merchants listed during burst (expect the burst plus at most stray 3 sigma noise)
```
