-- ============================================================
-- Gold Layer: high_risk_customers
-- ============================================================
-- PURPOSE: Ranked list of customers with the most fraud activity.
-- Used by the Streamlit "Customer Risk Lookup" panel.
-- Full refresh daily.
-- ============================================================

{{
    config(materialized='table')
}}

WITH customer_stats AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS total_flags,
        SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_flags,
        SUM(amount) AS total_spend_usd,
        SUM(CASE WHEN is_flagged THEN amount ELSE 0 END) AS flagged_spend_usd,
        AVG(CASE WHEN is_flagged THEN fraud_score ELSE NULL END) AS avg_fraud_score,
        MAX(fraud_score) AS max_fraud_score,
        MAX(event_time) AS last_seen,
        MAX(CASE WHEN is_flagged THEN event_time END) AS last_flagged,
        -- Count distinct countries (geo diversity = higher risk)
        COUNT(DISTINCT merchant_country) AS countries_used,
        MODE(merchant_country) AS most_common_country
    FROM {{ ref('fraud_flags') }}
    GROUP BY customer_id, customer_name
)

SELECT
    customer_id,
    customer_name,
    total_transactions,
    total_flags,
    critical_flags,
    ROUND(total_flags::FLOAT / NULLIF(total_transactions, 0) * 100, 1) AS fraud_rate_pct,
    ROUND(total_spend_usd, 2) AS total_spend_usd,
    ROUND(flagged_spend_usd, 2) AS flagged_spend_usd,
    ROUND(avg_fraud_score, 1) AS avg_fraud_score,
    max_fraud_score,
    countries_used,
    most_common_country,
    last_seen,
    last_flagged,

    -- Risk tier classification
    CASE
        WHEN critical_flags > 0 OR fraud_rate_pct > 50 THEN 'CRITICAL'
        WHEN total_flags >= 3 OR fraud_rate_pct > 20   THEN 'HIGH'
        WHEN total_flags >= 1 OR fraud_rate_pct > 5    THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,

    -- Rank by total fraud flags (1 = most flagged customer)
    RANK() OVER (ORDER BY total_flags DESC, avg_fraud_score DESC) AS fraud_rank,

    CURRENT_TIMESTAMP() AS refreshed_at

FROM customer_stats
WHERE total_flags > 0   -- Only include customers with at least one flag
ORDER BY fraud_rank
