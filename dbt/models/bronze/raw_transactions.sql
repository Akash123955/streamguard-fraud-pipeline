-- ============================================================
-- Bronze Layer: raw_transactions
-- ============================================================
-- PURPOSE: Exact copy of TRANSACTIONS_RAW with minimal transformations.
-- Raw data is NEVER modified — this is the source of truth and audit trail.
-- Incremental: only picks up rows ingested since the last dbt run.
--
-- Interview talking point:
--   "Bronze is append-only. If we discover a bug in Silver or Gold logic,
--    we can always re-derive correct data from Bronze without losing anything."
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

SELECT
    id                  AS raw_id,
    transaction_id,
    customer_id,
    customer_name,
    event_time,
    ingested_at,
    amount,
    currency,
    merchant_name,
    merchant_category,
    merchant_country,
    customer_lat,
    customer_lon,
    card_last4,
    card_type,
    is_fraud_label,     -- Ground truth label from producer
    fraud_type,
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ source('streamguard', 'TRANSACTIONS_RAW') }}

{% if is_incremental() %}
    -- Only process rows ingested since our last dbt run
    -- This makes each run fast — we don't re-scan the full history
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
