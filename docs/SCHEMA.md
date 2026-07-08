# Schema Design

DDL lives in [clickhouse/init/](../clickhouse/init/). This document explains the decisions, in the order they were made: query patterns first, then engine, then sort key, then everything else. That ordering matters because in ClickHouse the physical layout is the schema design; get the sort key wrong and no index added later will save you.

## Start from the query patterns

The dashboards ask four kinds of questions:

1. Transaction volume and value over time (portfolio-wide, by day or month)
2. Failure rate by merchant segment
3. Top merchants by processed volume
4. Daily failures per merchant (the operations view)

Three of the four aggregate by merchant, and everything filters or groups by time. So the workload is: scan many rows, few columns (`merchant_id`, `transaction_date`, `amount`, `status`), aggregate hard. That is the exact shape columnar storage is built for, and it drives every choice below.

## Why MergeTree

`MergeTree` is ClickHouse's workhorse table engine. Data arrives in sorted immutable parts, each column stored as its own compressed file, and background threads merge small parts into bigger ones. Three properties matter for this workload:

- **Columnar reads.** A query touching 4 of the 7 columns reads roughly 4/7 of the bytes, and in practice far less because sorted, low variety columns compress extremely well.
- **A sparse primary index.** MergeTree does not index every row. It stores one index entry per granule (8,192 rows by default), which keeps the whole index in memory and makes range scans cheap.
- **Ordered storage.** Rows are physically sorted by the `ORDER BY` key, so rows that are queried together sit together on disk.

The specialized engines (`ReplacingMergeTree`, `SummingMergeTree`, and others) are all MergeTree variants with extra merge time behavior. The base table needs none of that: transactions are immutable facts, inserted once, never updated. Plain `MergeTree` is the correct default and anything fancier would need justifying, not the other way round.

## The ORDER BY key

```sql
ORDER BY (merchant_id, transaction_date)
```

In MergeTree the `ORDER BY` clause is the primary index (there is no separate `PRIMARY KEY` here, it defaults to the sort key). The sparse index stores the key values at each granule boundary, so a filter on a key prefix lets ClickHouse skip every granule whose key range cannot match.

Why `merchant_id` first:

- The dominant pattern is merchant level aggregation over time. With this key, all rows for one merchant are physically contiguous and sorted by date inside that run. A query like "daily volume for merchant X in Q1" reads a handful of granules instead of scanning the table.
- Cardinality ordering: the useful rule is lower cardinality columns first when queries filter on them. 3,000 merchants split the table into 3,000 sorted runs; within each, dates sort ascending. Both key columns stay usable.

Why not `transaction_date` first: it would optimize "everything on date D" at the cost of scattering each merchant's rows across the entire table, which turns every merchant scoped query into a full scan. Portfolio wide time queries do not need the sort key anyway, because partition pruning (next section) already cuts them down by month, and a full scan of 575K rows takes ClickHouse milliseconds regardless. The sort key is spent on the access pattern that benefits most.

`transaction_id` is deliberately not in the key. It is a row identifier nobody filters by, and a unique first key column would destroy the compression that comes from sorted repetition.

## Partitioning

```sql
PARTITION BY toYYYYMM(transaction_date)
```

Partitions are for data management and coarse pruning, not query speed within a month. 24 months of data gives 24 partitions, so any query with a date range filter skips whole months before the primary index is even consulted, and dropping old data (`ALTER TABLE ... DROP PARTITION`) is instant. A common beginner mistake is partitioning by day, which creates thousands of tiny parts and slows everything down; months are the right grain at this volume.

## Column types

| Column | Type | Why |
|---|---|---|
| `merchant_id` | `LowCardinality(String)` | 3,000 distinct values. Dictionary encoding stores each string once and 2 byte codes in the data, which shrinks the column and speeds GROUP BY. The guidance is that LowCardinality pays off below roughly 10K distinct values. |
| `payment_method`, `failure_reason` | `LowCardinality(String)` | 3 and 5 distinct values. Same reasoning, stronger effect. |
| `status` | `Enum8` | Two known states, stored as one byte, and the schema rejects typos like 'Sucess' at insert time. |
| `amount` | `Decimal(18,2)` | Money. Floats accumulate rounding error under aggregation; a `sum()` over half a million floats can drift cents. Decimal arithmetic is exact. |
| `transaction_date` | `Date` | 2 bytes. The source data has day precision, so `DateTime` would waste 2 bytes per row pretending to precision that does not exist. |
| `failure_reason` default `''` | not `Nullable` | Nullable columns carry a separate null mask file per part and add branching to every read. An empty string is an unambiguous "no failure" here, so the null machinery buys nothing. |

## The materialized view

```sql
CREATE MATERIALIZED VIEW payments.daily_merchant_stats_mv
TO payments.daily_merchant_stats AS
SELECT transaction_date AS day, merchant_id,
       count() AS txn_count,
       countIf(status = 'Failed') AS failed_count,
       sumIf(amount, status = 'Success') AS success_amount
FROM payments.transactions
GROUP BY day, merchant_id;
```

The single most misunderstood ClickHouse feature, so precision matters:

- **It is an insert trigger, not a scheduled refresh.** Every block inserted into `transactions` passes through the SELECT, and the partial aggregate for that block is appended to the target table. It never re-reads existing data, which also means it must be created before the data load (or backfilled explicitly with `INSERT INTO ... SELECT`).
- **The target is `SummingMergeTree`.** Partial results for the same `(merchant_id, day)` arrive from different insert blocks as separate rows; this engine sums the numeric columns of rows with an identical sort key during background merges.
- **Queries must still aggregate.** Merges are eventual, so at any moment the target may hold several unmerged rows per key. Correct reads are always `sum(txn_count) ... GROUP BY`, never a raw `SELECT *`. Getting this wrong produces silently understated numbers, which is exactly the kind of bug a BI layer bakes into a dashboard, so the dashboard queries in this repo always aggregate.

Why this view exists: the daily failures dashboard panel asks "failures per day, filterable by merchant" on every refresh. Against the base table that is a 575K row scan each time; against the pre-aggregate it reads at most one row per merchant per day. At this data size both are fast, honestly. The benchmark section in the README quantifies the actual gap. The design earns its keep with scale: transaction tables grow by orders of magnitude, the daily aggregate grows only with merchants times days, and the pattern (raw immutable facts plus incrementally maintained aggregates) is the standard ClickHouse answer to dashboard latency.

Why `SummingMergeTree` and not `AggregatingMergeTree`: everything here is a count or a sum, which SummingMergeTree handles with plain numeric columns. AggregatingMergeTree with `AggregateFunction` state columns is the right tool once you need averages, uniques, or quantiles maintained incrementally; using it for plain sums adds complexity for nothing.

## The segments view

`payments.merchant_segments` is a plain view (computed at query time) that translates the RFM segmentation from my [payment-merchant-clv-segmentation](https://github.com/RidhanPar/payment-merchant-clv-segmentation) project into SQL: quintile scores via `ntile(5)` window functions over recency, frequency, and monetary value, then the same rule table mapping scores to segments (Champions, Loyal, New, Needs Attention, At Risk, Dormant). Only successful transactions count toward frequency and monetary value, matching the original.

Not materialized, on purpose: after aggregation it is one row per merchant, 3,000 rows. ClickHouse computes it in milliseconds, and a live view always reflects the latest loaded data. Materializing it would add merge time machinery to save nothing.

One ClickHouse specific detail: with default settings a `LEFT JOIN` fills non matches with type defaults rather than NULLs, so a merchant with no successful transactions gets `last_success_date = 1970-01-01`. The view checks for that epoch value and substitutes the merchant's signup date, so "never converted" merchants get the worst recency instead of a nonsense 56 year gap.
