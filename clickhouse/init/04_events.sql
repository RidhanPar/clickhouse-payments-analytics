-- Raw payment event stream, written continuously by the producer service.
--
-- Same design reasoning as the transactions table (docs/SCHEMA.md), with one
-- difference: event_time is a DateTime because a live system needs minute and
-- second granularity for operational queries, where the historical bulk
-- dataset only carried day precision.
--
-- PARTITION BY is decided at creation time on purpose: ClickHouse cannot
-- change a table's partitioning later without a full rebuild (create new
-- table, INSERT SELECT, swap). Retention TTL is added in the schema
-- hardening stage, because TTL can be added to a live table with a cheap
-- metadata-only ALTER.

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
ORDER BY (merchant_id, event_time);
