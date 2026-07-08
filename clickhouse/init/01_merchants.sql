-- Merchant dimension table, seeded by scripts/backfill_history.py.
-- 3,000 rows; the producer reads it at startup to give every live event a
-- merchant personality (traffic share, ticket size, baseline decline rate).
--
-- Init scripts in this directory run automatically on the first container
-- start (empty data volume). On an existing volume, apply manually:
--   docker exec payments-clickhouse clickhouse-client --user analytics \
--     --password <pw> --queries-file /docker-entrypoint-initdb.d/<file>.sql

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
