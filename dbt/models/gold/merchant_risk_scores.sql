-- ============================================================
-- Gold Layer: merchant_risk_scores
-- ============================================================
-- PURPOSE: Risk score per merchant category for the dashboard bar chart.
-- Shows which merchant types generate the most fraud alerts.
-- ============================================================

{{
    config(materialized='table')
}}

SELECT
    merchant_category,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)        AS total_flags,
    ROUND(
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                   AS fraud_rate_pct,
    ROUND(AVG(CASE WHEN is_flagged THEN fraud_score END), 1) AS avg_fraud_score,
    ROUND(SUM(amount), 2)                               AS total_volume_usd,
    ROUND(SUM(CASE WHEN is_flagged THEN amount ELSE 0 END), 2) AS flagged_volume_usd,
    COUNT(DISTINCT customer_id)                         AS unique_customers,
    COUNT(DISTINCT merchant_country)                    AS countries_seen,

    -- Risk classification for color-coding in the dashboard
    CASE
        WHEN ROUND(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) > 50
            THEN 'CRITICAL'
        WHEN ROUND(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) > 20
            THEN 'HIGH'
        WHEN ROUND(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) > 5
            THEN 'MEDIUM'
        ELSE 'LOW'
    END AS merchant_risk_tier,

    CURRENT_TIMESTAMP() AS refreshed_at

FROM {{ ref('fraud_flags') }}
GROUP BY merchant_category
ORDER BY fraud_rate_pct DESC
