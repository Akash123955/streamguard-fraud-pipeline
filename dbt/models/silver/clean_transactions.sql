-- ============================================================
-- Silver Layer: clean_transactions
-- ============================================================
-- PURPOSE: Deduplicated, type-cast, and enriched transactions.
-- Adds derived fields useful for analysis (hour of day, weekend flag, etc.)
-- Removes duplicates that can occur if Kafka delivers a message twice.
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

WITH deduped AS (
    -- Use ROW_NUMBER to keep only the first occurrence if duplicates exist
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY dbt_loaded_at ASC
        ) AS row_num
    FROM {{ ref('raw_transactions') }}

    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    transaction_id,
    customer_id,
    customer_name,
    event_time,

    -- Amount normalization: round to 2 decimal places
    ROUND(amount, 2) AS amount,
    UPPER(TRIM(currency)) AS currency,

    -- Merchant fields: clean whitespace, standardize case
    TRIM(merchant_name)                 AS merchant_name,
    LOWER(TRIM(merchant_category))      AS merchant_category,
    UPPER(TRIM(merchant_country))       AS merchant_country,

    -- Geo coordinates: validate range (lat: -90 to 90, lon: -180 to 180)
    CASE
        WHEN customer_lat BETWEEN -90 AND 90 THEN customer_lat
        ELSE NULL
    END AS customer_lat,
    CASE
        WHEN customer_lon BETWEEN -180 AND 180 THEN customer_lon
        ELSE NULL
    END AS customer_lon,

    card_last4,
    UPPER(card_type) AS card_type,

    -- Derived time features (useful for ML features and dashboard filters)
    DATE_TRUNC('day', event_time)           AS transaction_date,
    HOUR(event_time)                        AS transaction_hour,
    DAYOFWEEK(event_time)                   AS day_of_week,
    CASE WHEN DAYOFWEEK(event_time) IN (1, 7)
         THEN TRUE ELSE FALSE END           AS is_weekend,

    -- Amount buckets for easy analysis
    CASE
        WHEN amount < 10    THEN 'micro'
        WHEN amount < 100   THEN 'small'
        WHEN amount < 500   THEN 'medium'
        WHEN amount < 1000  THEN 'large'
        ELSE 'very_large'
    END AS amount_bucket,

    is_fraud_label,
    fraud_type,
    dbt_loaded_at,
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM deduped
WHERE row_num = 1   -- Keep only first occurrence of each transaction_id
