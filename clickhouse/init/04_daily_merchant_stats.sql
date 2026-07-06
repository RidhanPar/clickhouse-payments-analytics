-- Daily per-merchant rollup: the permanent history of the platform.
--
-- Fed two ways: the materialized view aggregates the live event stream as
-- it arrives, and scripts/backfill_history.py seeds 24 months of history
-- directly into the target table (a materialized view never re-reads
-- existing data, so backfill must write to the target itself).
--
-- Engine choice: SummingMergeTree, because every column here is a count or
-- a sum, and those merge by plain addition. The per-minute table needed
-- AggregatingMergeTree for its distinct count; this one does not, and the
-- simpler engine is easier to reason about and to read (plain numeric
-- columns, no merge functions).
--
-- No TTL: this table IS the long-term record after raw events expire at 90
-- days. It grows with merchants times days, which stays small forever
-- (3,000 merchants times 365 days is about 1M rows a year at worst).
--
-- Reads must still aggregate (sum ... GROUP BY): merges are eventual, so
-- several partial rows can exist per (merchant_id, day) at any moment.

CREATE TABLE IF NOT EXISTS payments.daily_merchant_stats
(
    day            Date,
    merchant_id    LowCardinality(String),
    txn_count      UInt64,
    failed_count   UInt64,
    success_amount Decimal(38, 2)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (merchant_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS payments.daily_merchant_stats_mv
TO payments.daily_merchant_stats AS
SELECT
    toDate(event_time)          AS day,
    merchant_id,
    count()                     AS txn_count,
    countIf(status = 'Failed')  AS failed_count,
    sumIf(amount, status = 'Success') AS success_amount
FROM payments.events
GROUP BY day, merchant_id;
