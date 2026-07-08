-- Base tables. Full reasoning in docs/SCHEMA.md.
-- These scripts run automatically on the first container start (empty data
-- volume). On an existing volume, apply them manually:
--   docker exec payments-clickhouse clickhouse-client --user analytics \
--     --password <pw> --queries-file /docker-entrypoint-initdb.d/01_tables.sql

CREATE TABLE IF NOT EXISTS payments.transactions
(
    transaction_id   String,
    merchant_id      LowCardinality(String),
    transaction_date Date,
    amount           Decimal(18, 2),
    payment_method   LowCardinality(String),
    status           Enum8('Success' = 1, 'Failed' = 2),
    -- Empty string means the transaction succeeded. Deliberately not
    -- Nullable(String): Nullable adds a separate null mask file per part and
    -- slows filters, and '' is unambiguous here.
    failure_reason   LowCardinality(String) DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (merchant_id, transaction_date);

CREATE TABLE IF NOT EXISTS payments.merchants
(
    merchant_id     LowCardinality(String),
    industry        LowCardinality(String),
    signup_date     Date,
    avg_ticket_size Decimal(10, 2),
    weekly_txn_rate Float32,
    decline_rate    Float32
)
ENGINE = MergeTree
ORDER BY merchant_id;
