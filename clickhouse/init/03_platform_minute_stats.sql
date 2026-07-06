-- Platform-wide per-minute aggregates for the live operations dashboard.
--
-- Engine choice: AggregatingMergeTree, not SummingMergeTree, because one of
-- the metrics (distinct active merchants per minute) is not a sum. Distinct
-- counts cannot be added across insert blocks; they need a mergeable
-- intermediate state, which is exactly what AggregateFunction(uniq, String)
-- stores. The plain sums ride along as SimpleAggregateFunction columns,
-- which behave like SummingMergeTree columns (raw numbers, summed on merge)
-- without the overhead of full aggregate states.
--
-- Reads must merge states: sum(txn_count), uniqMerge(active_merchants),
-- always with GROUP BY minute. Raw SELECTs see partial rows.
--
-- Same 90 day TTL as raw events: minute grain data has no value past the
-- operational window, and the daily rollup carries the long history.

CREATE TABLE IF NOT EXISTS payments.platform_minute_stats
(
    minute           DateTime,
    txn_count        SimpleAggregateFunction(sum, UInt64),
    failed_count     SimpleAggregateFunction(sum, UInt64),
    amount_sum       SimpleAggregateFunction(sum, Decimal(38, 2)),
    active_merchants AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY minute
TTL minute + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS payments.platform_minute_stats_mv
TO payments.platform_minute_stats AS
SELECT
    toStartOfMinute(event_time)          AS minute,
    count()                              AS txn_count,
    countIf(status = 'Failed')           AS failed_count,
    sum(amount)                          AS amount_sum,
    uniqState(CAST(merchant_id, 'String')) AS active_merchants
FROM payments.events
GROUP BY minute;
