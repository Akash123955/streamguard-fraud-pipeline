-- ============================================================
-- Silver Layer: fraud_flags
-- ============================================================
-- PURPOSE: Join clean transactions with Spark-generated fraud alerts.
-- Every transaction gets a fraud score and risk level (defaulting to 0/LOW
-- if Spark didn't flag it).
-- This is the "enriched" view used by all Gold models.
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('clean_transactions') }}
    {% if is_incremental() %}
        WHERE silver_loaded_at > (SELECT MAX(silver_loaded_at) FROM {{ this }})
    {% endif %}
),

alerts AS (
    -- Fraud alerts written by Spark → Snowflake connector
    SELECT
        transaction_id,
        fraud_score,
        risk_level,
        fraud_reason,
        amount_flag,
        merchant_flag,
        geo_flag,
        alerted_at
    FROM {{ source('streamguard', 'FRAUD_ALERTS') }}
)

SELECT
    t.*,

    -- Fraud signal columns (NULL → 0/FALSE/LOW if not flagged)
    COALESCE(a.fraud_score, 0)          AS fraud_score,
    COALESCE(a.risk_level, 'LOW')       AS risk_level,
    a.fraud_reason,
    COALESCE(a.amount_flag, FALSE)      AS amount_flag,
    COALESCE(a.merchant_flag, FALSE)    AS merchant_flag,
    COALESCE(a.geo_flag, FALSE)         AS geo_flag,
    a.alerted_at,

    -- Was this transaction flagged by Spark?
    CASE WHEN a.transaction_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_flagged,

    CURRENT_TIMESTAMP() AS fraud_flags_loaded_at

FROM transactions t
LEFT JOIN alerts a ON t.transaction_id = a.transaction_id
