-- RFM merchant segments, computed at query time.
--
-- This mirrors the segmentation rules from my payment-merchant-clv-segmentation
-- project (analysis/rfm.py) translated to SQL: score Recency, Frequency and
-- Monetary value into quintiles, then map score combinations to named
-- segments. Only successful transactions count toward F and M; a failed
-- charge is not revenue.
--
-- A plain VIEW, not a materialized one, on purpose: it runs over one row per
-- merchant (3,000 rows after aggregation), so computing it live costs
-- milliseconds and it always reflects the latest data.

CREATE OR REPLACE VIEW payments.merchant_segments AS
WITH (SELECT max(transaction_date) + 1 FROM payments.transactions) AS snapshot_date
SELECT
    merchant_id,
    industry,
    recency_days,
    frequency,
    monetary,
    R_score,
    F_score,
    M_score,
    (F_score + M_score) / 2 AS FM_score,
    multiIf(
        R_score >= 4 AND FM_score >= 4,   'Champions',
        R_score >= 3 AND FM_score >= 3.5, 'Loyal',
        R_score >= 4 AND FM_score <= 2.5, 'New',
        R_score <= 2 AND FM_score >= 3.5, 'At Risk',
        R_score <= 2 AND FM_score <= 2.5, 'Dormant',
        'Needs Attention')  AS segment
FROM
(
    SELECT
        merchant_id,
        industry,
        recency_days,
        frequency,
        monetary,
        -- ntile numbers buckets 1..5 in sort order, so sorting worst first
        -- gives the standard convention: 5 is always the best score.
        ntile(5) OVER (ORDER BY recency_days DESC) AS R_score,
        ntile(5) OVER (ORDER BY frequency ASC)     AS F_score,
        ntile(5) OVER (ORDER BY monetary ASC)      AS M_score
    FROM
    (
        SELECT
            m.merchant_id AS merchant_id,
            m.industry    AS industry,
            -- Merchants with no successful transaction fall back to signup
            -- date, so recency measures how long they have gone without ever
            -- converting. ClickHouse LEFT JOIN fills misses with defaults
            -- (Date 1970-01-01), hence the epoch check instead of a NULL check.
            dateDiff('day',
                     if(s.last_success_date = toDate(0), m.signup_date, s.last_success_date),
                     snapshot_date) AS recency_days,
            s.frequency   AS frequency,
            s.monetary    AS monetary
        FROM payments.merchants AS m
        LEFT JOIN
        (
            SELECT
                merchant_id,
                max(transaction_date) AS last_success_date,
                count()               AS frequency,
                sum(amount)           AS monetary
            FROM payments.transactions
            WHERE status = 'Success'
            GROUP BY merchant_id
        ) AS s USING (merchant_id)
    )
);
