-- Raw payment event stream, written continuously by the producer service.
-- Full reasoning in docs/SCHEMA.md.
--
-- Retention: raw events live 90 days, then TTL deletes them during merges.
-- Long horizon history is preserved in daily_merchant_stats, which has no
-- TTL. This split is the whole storage strategy: raw data grows with
-- traffic, the rollup grows only with merchants times days.
--
-- ttl_only_drop_parts makes TTL wait until every row in a part is expired
-- and then drop the part whole, which is nearly free, instead of rewriting
-- parts to delete rows out of the middle. With monthly partitions and a 90
-- day TTL, parts age out cleanly month by month.

CREATE TABLE IF NOT EXISTS payments.events
(
    transaction_id String,
    merchant_id    LowCardinality(String),
    event_time     DateTime,
    amount         Decimal(18, 2),
    payment_method LowCardinality(String),
    status         Enum8('Success' = 1, 'Failed' = 2),
    failure_reason LowCardinality(String) DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (merchant_id, event_time)
TTL event_time + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1;
