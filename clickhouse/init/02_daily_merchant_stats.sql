-- Pre-aggregated daily volume and failure counts per merchant.
--
-- A ClickHouse materialized view is an insert trigger, not a scheduled
-- refresh: every block inserted into payments.transactions is aggregated by
-- the SELECT below and the partial result is written into the target table.
-- SummingMergeTree then collapses rows sharing the same ORDER BY key during
-- background merges. Because merges are eventual, correct queries against
-- daily_merchant_stats must still aggregate (sum(...) GROUP BY), never read
-- raw rows. docs/SCHEMA.md explains why this design was chosen.

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
    transaction_date            AS day,
    merchant_id,
    count()                     AS txn_count,
    countIf(status = 'Failed')  AS failed_count,
    sumIf(amount, status = 'Success') AS success_amount
FROM payments.transactions
GROUP BY day, merchant_id;
