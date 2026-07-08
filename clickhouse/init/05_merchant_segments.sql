-- RFM merchant segments, computed at query time.
--
-- Mirrors the segmentation rules from my payment-merchant-clv-segmentation
-- project (analysis/rfm.py) translated to SQL: quintile scores for Recency,
-- Frequency and Monetary value via ntile(5), then the same rule table
-- mapping scores to named segments. Only successful transactions count
-- toward F and M; a failed charge is not revenue.
--
-- Reads daily_merchant_stats, not raw events, on purpose: raw events expire
-- after 90 days, and RFM needs the full lifetime of each merchant. Daily
-- grain changes nothing for R (day precision by definition), F (sum of
-- counts) or M (sum of amounts).
--
-- SummingMergeTree note: the source may hold several partial rows per
-- (merchant_id, day). The sums are trivially merge safe. The recency test
-- (maxIf(day, txn_count > failed_count)) is also safe: a day had at least
-- one success if and only if at least one of its partial rows has
-- txn_count > failed_count, because each partial row is internally
-- consistent (it aggregates one insert block).
--
-- A plain VIEW, not materialized: one row per merchant after aggregation,
-- 3,000 rows, milliseconds to compute, always current.

CREATE OR REPLACE VIEW payments.merchant_segments AS
WITH (SELECT max(day) + 1 FROM payments.daily_merchant_stats) AS snapshot_date
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
        --
        -- Tie breakers matter on a live platform: most of the active book
        -- transacted yesterday, so recency alone ties across thousands of
        -- merchants and ntile would split those ties by arbitrary row order,
        -- turning R scores into noise. Ties break by engagement (frequency,
        -- then monetary), which is deterministic and defensible: among
        -- equally recent merchants, the busier one is the better one.
        ntile(5) OVER (ORDER BY recency_days DESC, frequency ASC, monetary ASC) AS R_score,
        ntile(5) OVER (ORDER BY frequency ASC, monetary ASC)                    AS F_score,
        ntile(5) OVER (ORDER BY monetary ASC, frequency ASC)                    AS M_score
    FROM
    (
        SELECT
            m.merchant_id AS merchant_id,
            m.industry    AS industry,
            -- Merchants with no successful day fall back to signup date, so
            -- recency measures how long they have gone without converting.
            -- LEFT JOIN misses arrive as Date defaults (1970-01-01), hence
            -- the epoch check instead of a NULL check.
            dateDiff('day',
                     if(s.last_success_day = toDate(0), m.signup_date, s.last_success_day),
                     snapshot_date) AS recency_days,
            s.frequency   AS frequency,
            s.monetary    AS monetary
        FROM payments.merchants AS m
        LEFT JOIN
        (
            SELECT
                merchant_id,
                maxIf(day, txn_count > failed_count)   AS last_success_day,
                sum(txn_count) - sum(failed_count)     AS frequency,
                sum(success_amount)                    AS monetary
            FROM payments.daily_merchant_stats
            GROUP BY merchant_id
        ) AS s USING (merchant_id)
    )
);
