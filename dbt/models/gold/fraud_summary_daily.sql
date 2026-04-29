-- ============================================================
-- Gold Layer: fraud_summary_daily
-- ============================================================
-- PURPOSE: Daily fraud KPIs for the Streamlit dashboard and reporting.
-- One row per day with total transactions, fraud counts, rates, and amounts.
-- Full refresh daily (Airflow triggers at 2am after dbt Silver completes).
-- ============================================================

{{
    config(materialized='table')
}}

SELECT
    transaction_date,

    -- Volume metrics
    COUNT(*)                                AS total_transactions,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged_transactions,
    SUM(CASE WHEN is_fraud_label THEN 1 ELSE 0 END) AS confirmed_fraud,

    -- Rate metrics
    ROUND(
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS fraud_flag_rate_pct,
    ROUND(
        SUM(CASE WHEN is_fraud_label THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS confirmed_fraud_rate_pct,

    -- Amount metrics
    SUM(amount)                             AS total_volume_usd,
    SUM(CASE WHEN is_flagged THEN amount ELSE 0 END) AS flagged_volume_usd,
    AVG(amount)                             AS avg_transaction_amount,
    MAX(amount)                             AS max_transaction_amount,

    -- Risk tier breakdown
    SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN risk_level = 'HIGH'     THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN risk_level = 'MEDIUM'   THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN risk_level = 'LOW'      THEN 1 ELSE 0 END) AS low_count,

    -- Rule breakdown (what caused the flags)
    SUM(CASE WHEN amount_flag   THEN 1 ELSE 0 END) AS amount_rule_hits,
    SUM(CASE WHEN merchant_flag THEN 1 ELSE 0 END) AS merchant_rule_hits,
    SUM(CASE WHEN geo_flag      THEN 1 ELSE 0 END) AS geo_rule_hits,

    CURRENT_TIMESTAMP() AS refreshed_at

FROM {{ ref('fraud_flags') }}
GROUP BY transaction_date
ORDER BY transaction_date DESC
