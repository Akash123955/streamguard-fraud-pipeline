-- ============================================================
-- StreamGuard — Snowflake Setup
-- ============================================================
-- Run this once in a Snowflake worksheet to create everything.
-- Sign up for a free trial at: https://signup.snowflake.com
--
-- KEY INTERVIEW TALKING POINT:
-- FRAUD_ALERTS and CUSTOMER_RISK_PROFILES use HYBRID TABLES.
-- Unlike regular Snowflake tables (columnar, optimized for analytics),
-- Hybrid Tables have a row-store index that enables millisecond
-- single-row lookups — exactly what fraud detection needs when
-- checking if a customer is already flagged mid-transaction.
-- ============================================================

-- ── Database and Schema ─────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS STREAMGUARD;
USE DATABASE STREAMGUARD;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- ── Warehouse ────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS STREAMGUARD_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60           -- Suspend after 60 seconds of inactivity (saves credits)
    AUTO_RESUME = TRUE
    COMMENT = 'StreamGuard fraud detection warehouse';

USE WAREHOUSE STREAMGUARD_WH;

-- ── Sequence for auto-incrementing IDs ──────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS txn_seq START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS alert_seq START = 1 INCREMENT = 1;

-- ============================================================
-- TABLE 1: TRANSACTIONS_RAW
-- Regular Snowflake table (columnar) — all incoming transactions.
-- Optimized for analytics queries (aggregations, GROUP BY, etc.)
-- Used by dbt Bronze layer.
-- ============================================================
CREATE TABLE IF NOT EXISTS TRANSACTIONS_RAW (
    id              NUMBER DEFAULT txn_seq.NEXTVAL,
    transaction_id  VARCHAR(100) NOT NULL,
    customer_id     VARCHAR(20)  NOT NULL,
    customer_name   VARCHAR(200),
    event_time      TIMESTAMP_NTZ NOT NULL,
    ingested_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    amount          FLOAT        NOT NULL,
    currency        VARCHAR(10)  DEFAULT 'USD',
    merchant_name   VARCHAR(200),
    merchant_category VARCHAR(50),
    merchant_country  VARCHAR(10),
    customer_lat    FLOAT,
    customer_lon    FLOAT,
    card_last4      VARCHAR(4),
    card_type       VARCHAR(20),
    is_fraud_label  BOOLEAN,        -- Ground truth from producer (for model validation)
    fraud_type      VARCHAR(50),
    PRIMARY KEY (id)
);

-- ============================================================
-- TABLE 2: FRAUD_ALERTS — HYBRID TABLE
-- ============================================================
-- WHY HYBRID TABLE?
--   When a new transaction arrives, we need to check in <1ms:
--   "Has this customer been flagged in the last 10 minutes?"
--   Regular Snowflake tables do a full column scan — too slow.
--   Hybrid Tables maintain a row-store index so lookups by
--   customer_id or transaction_id are O(1), like a database index.
--
-- This is a key differentiator you can talk about in interviews.
-- ============================================================
CREATE HYBRID TABLE IF NOT EXISTS FRAUD_ALERTS (
    alert_id        NUMBER DEFAULT alert_seq.NEXTVAL,
    transaction_id  VARCHAR(100) NOT NULL,
    customer_id     VARCHAR(20)  NOT NULL,
    event_time      TIMESTAMP_NTZ NOT NULL,
    alerted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    amount          FLOAT,
    merchant_name   VARCHAR(200),
    merchant_category VARCHAR(50),
    merchant_country  VARCHAR(10),
    customer_lat    FLOAT,
    customer_lon    FLOAT,
    card_type       VARCHAR(20),
    fraud_score     NUMBER(5,2),
    risk_level      VARCHAR(20),    -- LOW / MEDIUM / HIGH / CRITICAL
    fraud_reason    VARCHAR(500),
    amount_flag     BOOLEAN,
    merchant_flag   BOOLEAN,
    geo_flag        BOOLEAN,
    PRIMARY KEY (alert_id),
    INDEX idx_customer (customer_id),           -- Fast lookup by customer
    INDEX idx_transaction (transaction_id),     -- Fast lookup by transaction
    INDEX idx_risk_level (risk_level)           -- Filter by risk level
);

-- ============================================================
-- TABLE 3: CUSTOMER_RISK_PROFILES — HYBRID TABLE
-- ============================================================
-- Maintains a running risk score per customer.
-- Updated each time a fraud alert fires for that customer.
-- Used for real-time checks: "Is this a repeat offender?"
-- ============================================================
CREATE HYBRID TABLE IF NOT EXISTS CUSTOMER_RISK_PROFILES (
    customer_id         VARCHAR(20)  NOT NULL,
    total_transactions  NUMBER DEFAULT 0,
    total_fraud_alerts  NUMBER DEFAULT 0,
    fraud_rate          FLOAT  DEFAULT 0.0,
    cumulative_fraud_score NUMBER DEFAULT 0,
    last_alert_time     TIMESTAMP_NTZ,
    last_seen_country   VARCHAR(10),
    risk_tier           VARCHAR(20) DEFAULT 'UNKNOWN',  -- LOW / MEDIUM / HIGH / CRITICAL
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_id),
    INDEX idx_risk_tier (risk_tier)
);

-- ============================================================
-- STREAM: Change Data Capture on TRANSACTIONS_RAW
-- ============================================================
-- A Snowflake Stream tracks INSERT/UPDATE/DELETE operations on
-- TRANSACTIONS_RAW. dbt can query this stream to process only
-- new rows since the last run — much more efficient than a full scan.
-- ============================================================
CREATE STREAM IF NOT EXISTS transactions_raw_stream
    ON TABLE TRANSACTIONS_RAW
    APPEND_ONLY = TRUE      -- We only ever INSERT, never UPDATE/DELETE raw data
    COMMENT = 'CDC stream for dbt Bronze → Silver incremental processing';

-- ============================================================
-- STORED PROCEDURE: Refresh daily fraud summary
-- ============================================================
-- Called by Airflow at 2am each night after dbt runs.
-- Updates CUSTOMER_RISK_PROFILES based on the prior day's alerts.
-- ============================================================
CREATE OR REPLACE PROCEDURE refresh_customer_risk_profiles()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CUSTOMER_RISK_PROFILES tgt
    USING (
        SELECT
            customer_id,
            COUNT(*) AS total_alerts,
            SUM(fraud_score) AS total_score,
            MAX(event_time) AS last_alert,
            MAX(merchant_country) AS last_country,
            -- Risk tier based on number of alerts in last 30 days
            CASE
                WHEN COUNT(*) = 0 THEN 'LOW'
                WHEN COUNT(*) <= 2 THEN 'MEDIUM'
                WHEN COUNT(*) <= 5 THEN 'HIGH'
                ELSE 'CRITICAL'
            END AS new_risk_tier
        FROM FRAUD_ALERTS
        WHERE event_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY customer_id
    ) src ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET
        total_fraud_alerts = src.total_alerts,
        cumulative_fraud_score = src.total_score,
        last_alert_time = src.last_alert,
        last_seen_country = src.last_country,
        risk_tier = src.new_risk_tier,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, total_fraud_alerts, cumulative_fraud_score,
        last_alert_time, last_seen_country, risk_tier
    ) VALUES (
        src.customer_id, src.total_alerts, src.total_score,
        src.last_alert, src.last_country, src.new_risk_tier
    );

    RETURN 'Customer risk profiles refreshed at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- ── Verify setup ─────────────────────────────────────────────────────────────
SHOW TABLES;
SHOW HYBRID TABLES;
SHOW STREAMS;
