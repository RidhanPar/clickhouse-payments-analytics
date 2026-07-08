-- Anomaly watchlist: merchants whose current failure rate is significantly
-- above their own expected rate.
--
-- Method: for each merchant with at least 20 events and 5 failures in the
-- last 15 minutes, a one-sided two-proportion z test against a per-merchant
-- baseline. When the baseline is itself estimated from data, the test uses
-- the pooled standard error sqrt(p*(1-p) * (1/n + 1/m)) with m baseline
-- transactions; when the baseline is the configured prior it is treated as
-- exact (the 1/m term drops). A merchant is listed at z >= 3.5 AND at least
-- 5 points above baseline in absolute terms.
--
-- Both refinements were forced by observed false positives, not invented up
-- front. The first end to end verification run listed 7 merchants alongside
-- the injected burst, every one with a thin observed baseline (an hour or
-- two of live data): treating a noisy baseline as exact inflates z exactly
-- when the baseline is weakest, which the pooled error corrects. And with
-- ~300 eligible merchants re-tested every dashboard refresh, a 3 sigma
-- threshold fires on someone most of the time; 3.5 sigma cuts the chance
-- rate ~10x while leaving real bursts (z of 20 and up) untouched. The
-- absolute floor keeps statistically-significant-but-tiny bumps on high
-- volume merchants off the list, because 6.9% against a 6.7% baseline is
-- not something an operator should be paged for.
--
-- The baseline is a two-level hierarchy, and the reason is a false positive
-- this view produced within minutes of first being turned on:
--
-- 1. Observed: the merchant's own failure rate over the trailing 7 completed
--    days (daily rollup) plus today's events up to the start of the
--    detection window. Used when it covers at least 50 transactions.
-- 2. Configured prior: the merchant's decline_rate from the dimension table
--    (the underwriting expectation for that merchant). Used otherwise.
--
-- The first version fell back to the platform-wide average instead, and
-- promptly flagged a Digital Goods merchant running its perfectly normal
-- 15% decline rate against the 7% platform baseline. Judging a merchant
-- against other merchants is wrong whenever failure rates are structurally
-- different per merchant; the fallback must be merchant specific, and the
-- configured rate always exists. Once a merchant has ~an hour of live
-- traffic, its observed baseline takes over automatically.
--
-- Cost: one scan of a 15 minute event window plus one scan of 7 days of
-- rollup, milliseconds. Computed live on every dashboard refresh, so there
-- is no detection pipeline to operate or fall behind.

CREATE OR REPLACE VIEW payments.merchant_anomalies AS
WITH
    recent AS
    (
        SELECT
            merchant_id,
            count()                    AS txns_15m,
            countIf(status = 'Failed') AS failed_15m,
            failed_15m / txns_15m      AS failure_rate_15m
        FROM payments.events
        WHERE event_time > now() - INTERVAL 15 MINUTE
        GROUP BY merchant_id
        HAVING txns_15m >= 20 AND failed_15m >= 5
    ),
    trailing AS
    (
        SELECT
            merchant_id,
            sum(txns)               AS baseline_txns,
            sum(failed)             AS baseline_failed,
            sum(failed) / sum(txns) AS observed_rate
        FROM
        (
            SELECT merchant_id, sum(txn_count) AS txns, sum(failed_count) AS failed
            FROM payments.daily_merchant_stats
            WHERE day >= today() - 7 AND day < today()
            GROUP BY merchant_id
            UNION ALL
            SELECT merchant_id, count() AS txns, countIf(status = 'Failed') AS failed
            FROM payments.events
            WHERE event_time >= today() AND event_time <= now() - INTERVAL 15 MINUTE
            GROUP BY merchant_id
        )
        GROUP BY merchant_id
    )
SELECT
    merchant_id,
    industry,
    txns_15m,
    failed_15m,
    round(failure_rate_15m, 4) AS failure_rate_15m,
    round(baseline_rate, 4)    AS baseline_rate,
    baseline_source,
    round(z_score, 2)          AS z_score
FROM
(
    SELECT
        r.merchant_id      AS merchant_id,
        m.industry         AS industry,
        r.txns_15m         AS txns_15m,
        r.failed_15m       AS failed_15m,
        r.failure_rate_15m AS failure_rate_15m,
        if(t.baseline_txns >= 50, t.observed_rate, m.decline_rate) AS baseline_rate,
        if(t.baseline_txns >= 50, 'observed 7d', 'configured prior') AS baseline_source,
        -- Pooled rate for the two-proportion test; equals the prior when the
        -- baseline is configured rather than observed.
        if(t.baseline_txns >= 50,
           (r.failed_15m + t.baseline_failed) / (r.txns_15m + t.baseline_txns),
           m.decline_rate) AS pooled_rate,
        (r.failure_rate_15m - baseline_rate)
            / sqrt(greatest(pooled_rate * (1 - pooled_rate), 0.0001)
                   * (1 / r.txns_15m
                      + if(t.baseline_txns >= 50, 1 / t.baseline_txns, 0)))
            AS z_score
    FROM recent AS r
    LEFT JOIN trailing AS t USING (merchant_id)
    LEFT JOIN payments.merchants AS m USING (merchant_id)
)
WHERE z_score >= 3.5 AND failure_rate_15m >= baseline_rate + 0.05
ORDER BY z_score DESC;
