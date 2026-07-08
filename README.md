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

## Benchmarks

Run by [scripts/benchmark.py](scripts/benchmark.py): median of 5 runs after a warmup, on Docker Desktop for Windows, ClickHouse 24.8, 575,226 row transactions table. Server execution comes from `system.query_log`; end to end includes the HTTP round trip through Docker's network stack, which adds a near constant ~45 ms on this machine and would be the first thing to investigate if these were user facing latencies.

| Query | Server execution | End to end (HTTP) | Rows read | Data read |
|---|---|---|---|---|
| Monthly volume and value, full 24 months | 8 ms | 54 ms | 575,226 | 5.49 MiB |
| One merchant's daily history (primary key hit) | 10 ms | 61 ms | 171,872 | 1.36 MiB |
| Top 15 merchants by successful volume | 16 ms | 59 ms | 575,226 | 6.03 MiB |
| Failure reasons in Q1 2026 (partition pruning) | 5 ms | 49 ms | 114,831 | 448.56 KiB |
| Daily failures from the base table | 6 ms | 52 ms | 575,226 | 1.65 MiB |
| Daily failures from the materialized view | 5 ms | 52 ms | 372,370 | 3.55 MiB |
| Full RFM segmentation, computed live | 24 ms | 71 ms | 578,250 | 7.14 MiB |

What the numbers show, honestly:

- **Full scans are cheap in a column store.** The monthly rollup aggregates all 575K rows in 8 ms because it reads three columns (5.49 MiB), not the table.
- **Partition pruning works.** The Q1 2026 query read 114,831 rows, roughly 3 months out of 24, before any filtering. The monthly partition key did that, not the WHERE clause.
- **The sparse index reads granules, not rows.** The single merchant query matched about 1,200 rows but read 171,872: MergeTree indexes every 8,192nd row, so it reads one whole granule per monthly partition where the merchant appears. At this scale that looks like overhead; at billions of rows it is exactly the mechanism that keeps merchant queries from scanning everything.
- **The materialized view does not pay off at 575K rows**, and the numbers say so: 5 ms via the MV against 6 ms against the base table. Its value is structural: the aggregate table grows with merchants times days while the base table grows with traffic, so the gap widens with volume. Building the pattern correctly at small scale is the point.
- **The live RFM segmentation is the most expensive query (24 ms)** because it joins, aggregates, and runs three window functions over the full table. Still comfortably interactive, which is why it stayed a plain view instead of another materialization.
