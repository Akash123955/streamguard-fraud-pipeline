# StreamGuard — How to Run Locally

This guide covers everything you need to run StreamGuard from scratch —
whether you just cloned it from GitHub or are picking it back up after a break.

---

## Prerequisites

Make sure you have these installed before starting:

| Tool | Version | Check |
|------|---------|-------|
| Docker Desktop | Any recent | `docker --version` |
| Python | 3.9+ | `python3 --version` |
| Java (JDK 17) | 17 | `java -version` |

**Snowflake:** You need a free trial account at https://signup.snowflake.com
(Already set up? Skip to Step 3.)

---

## First-Time Setup (do this once)

### Step 1 — Clone the repo

```bash
git clone https://github.com/Akash123955/streamguard-fraud-pipeline.git
cd streamguard-fraud-pipeline
```

### Step 2 — Create Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install snowflake-connector-python
```

### Step 3 — Set up Snowflake (first time only)

**3a. Create a service user in Snowflake**

Log in to your Snowflake account, open a worksheet, and run:

```sql
CREATE USER IF NOT EXISTS STREAMGUARD_SVC
    PASSWORD = 'StreamGuard@2024!'
    DEFAULT_ROLE = ACCOUNTADMIN
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE ACCOUNTADMIN TO USER STREAMGUARD_SVC;
```

**3b. Generate a key pair for authentication**

```bash
openssl genrsa -out snowflake_key.p8 2048
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_public_key.pub
cat snowflake_public_key.pub
```

**3c. Set the public key on the service user**

Copy everything between `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----`
(including those lines), then run in your Snowflake worksheet:

```sql
ALTER USER STREAMGUARD_SVC SET RSA_PUBLIC_KEY='<paste public key content here>';
```

**3d. Find your Snowflake account details**

Run this in a Snowflake worksheet:
```sql
SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME();
```
Note the two values — you need them for `.env`.

**3e. Create the database and tables**

Still in the Snowflake worksheet, run all the SQL from `snowflake/setup.sql`.
This creates the `STREAMGUARD` database, `TRANSACTIONS_RAW`, `FRAUD_ALERTS`
(Hybrid Table), `CUSTOMER_RISK_PROFILES` (Hybrid Table), and the CDC stream.

**3f. Create the Gold views**

Run this in the Snowflake worksheet:

```sql
CREATE SCHEMA IF NOT EXISTS STREAMGUARD.GOLD;

CREATE OR REPLACE VIEW STREAMGUARD.GOLD.FRAUD_SUMMARY_DAILY AS
SELECT
    DATE(event_time)                                          AS transaction_date,
    COUNT(*)                                                  AS total_transactions,
    COUNT(*)                                                  AS flagged_transactions,
    ROUND(100.0, 2)                                           AS fraud_flag_rate_pct,
    ROUND(SUM(amount), 2)                                     AS flagged_volume_usd,
    ROUND(SUM(amount), 2)                                     AS total_volume_usd,
    AVG(fraud_score)                                          AS avg_fraud_score,
    SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END)  AS critical_count,
    SUM(CASE WHEN risk_level = 'HIGH'     THEN 1 ELSE 0 END)  AS high_count,
    SUM(CASE WHEN risk_level = 'MEDIUM'   THEN 1 ELSE 0 END)  AS medium_count,
    SUM(CASE WHEN amount_flag   THEN 1 ELSE 0 END)            AS amount_rule_hits,
    SUM(CASE WHEN merchant_flag THEN 1 ELSE 0 END)            AS merchant_rule_hits,
    SUM(CASE WHEN geo_flag      THEN 1 ELSE 0 END)            AS geo_rule_hits
FROM STREAMGUARD.PUBLIC.FRAUD_ALERTS
GROUP BY DATE(event_time);

CREATE OR REPLACE VIEW STREAMGUARD.GOLD.MERCHANT_RISK_SCORES AS
SELECT
    merchant_category,
    COUNT(*)                    AS total_flags,
    ROUND(AVG(fraud_score), 1)  AS avg_fraud_score,
    ROUND(SUM(amount), 2)       AS flagged_volume_usd,
    CASE
        WHEN AVG(fraud_score) >= 65 THEN 'CRITICAL'
        WHEN AVG(fraud_score) >= 35 THEN 'HIGH'
        WHEN AVG(fraud_score) >= 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END                         AS merchant_risk_tier,
    ROUND(100.0, 1)             AS fraud_rate_pct
FROM STREAMGUARD.PUBLIC.FRAUD_ALERTS
GROUP BY merchant_category
ORDER BY avg_fraud_score DESC;

CREATE OR REPLACE VIEW STREAMGUARD.GOLD.HIGH_RISK_CUSTOMERS AS
SELECT
    customer_id,
    customer_id                                               AS customer_name,
    COUNT(*)                                                  AS total_flags,
    ROUND(AVG(fraud_score), 1)                                AS fraud_rate_pct,
    MAX(fraud_score)                                          AS max_fraud_score,
    CASE
        WHEN MAX(fraud_score) >= 65 THEN 'CRITICAL'
        WHEN MAX(fraud_score) >= 35 THEN 'HIGH'
        WHEN MAX(fraud_score) >= 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                       AS risk_tier,
    MAX(event_time)                                           AS last_flagged,
    LISTAGG(DISTINCT merchant_country, ', ')
        WITHIN GROUP (ORDER BY merchant_country)              AS countries_used,
    ROW_NUMBER() OVER (ORDER BY SUM(fraud_score) DESC)        AS fraud_rank
FROM STREAMGUARD.PUBLIC.FRAUD_ALERTS
GROUP BY customer_id;
```

### Step 4 — Configure credentials

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```env
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
PYSPARK_PYTHON=venv/bin/python
PYSPARK_DRIVER_PYTHON=venv/bin/python

KAFKA_BOOTSTRAP_SERVERS=localhost:9093

SNOWFLAKE_ACCOUNT=fec02769
SNOWFLAKE_REGION=us-east-1
SNOWFLAKE_USER=STREAMGUARD_SVC
SNOWFLAKE_PRIVATE_KEY_PATH=snowflake_key.p8
SNOWFLAKE_DATABASE=STREAMGUARD
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=STREAMGUARD_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Replace `SNOWFLAKE_ACCOUNT` with your account locator and `SNOWFLAKE_REGION`
with your region (both from Step 3d).

---

## Running the Pipeline (every time)

Open **4 terminal tabs** in the project folder. Run one command per tab in order.

```bash
# All tabs: activate the virtual environment first
source venv/bin/activate
```

### Tab 1 — Start Docker infrastructure

```bash
docker compose up -d
```

Wait ~20 seconds for Kafka to fully initialize, then verify:
- Kafka UI: http://localhost:8080 — topics `raw-transactions` and `fraud-alerts` visible
- Spark UI: http://localhost:8081 — 1 worker registered

### Tab 2 — Transaction Producer

```bash
python kafka/transaction_producer.py
```

You should see output like:
```
Connected to Kafka!
Starting transaction stream: 10,000 txns/min
[    ] Sent: 100 | Rate: 7,909 txn/min | Fraud: 2 (2.0%)
[FRAUD] Sent: 200 | Rate: 7,904 txn/min | Fraud: 4 (2.0%)
```

### Tab 3 — Spark Fraud Detector

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
PYSPARK_PYTHON=venv/bin/python \
PYSPARK_DRIVER_PYTHON=venv/bin/python \
python spark/fraud_detector.py
```

Wait ~30 seconds for Spark to start. You will see batch output every 5 seconds:
```
Batch: 1
|customer_id|amount |risk_level|fraud_score|
|CUST_00042 |4750.00|CRITICAL  |100        |
```

### Tab 4 — Snowflake Connector

```bash
python snowflake/snowflake_connector.py
```

Output:
```
Connecting to Snowflake...
Connected to Snowflake as STREAMGUARD_SVC
Listening for fraud alerts on Kafka...
→ Inserted 100 fraud alerts into Snowflake FRAUD_ALERTS
```

### Tab 5 — Dashboard

```bash
streamlit run dashboard/app.py
```

Opens automatically at **http://localhost:8501**

Wait ~10 seconds for the Kafka feed to load. You should see:
- Live fraud alerts populating the feed
- Merchant risk bar chart
- KPI metrics from Snowflake
- Top 10 high-risk customers table

---

## Stopping the Pipeline

```bash
# Stop Python processes (Ctrl+C in each tab, or run this)
pkill -f "transaction_producer.py"
pkill -f "fraud_detector.py"
pkill -f "snowflake_connector.py"
pkill -f "streamlit run"

# Stop Docker
docker compose down
```

---

## Snowflake — Preventing Credit Charges

The warehouse `STREAMGUARD_WH` is configured with `AUTO_SUSPEND = 60` seconds.
It suspends itself automatically when not in use — no manual action needed.

To manually suspend (just in case):
```sql
ALTER WAREHOUSE STREAMGUARD_WH SUSPEND;
```

The free trial gives $400 of credits. With an XSMALL warehouse and auto-suspend,
a full day of running StreamGuard uses less than $1 of credits.

---

## Running the Tests

No Docker or Snowflake needed for unit tests:

```bash
source venv/bin/activate
PYSPARK_PYTHON=venv/bin/python \
PYSPARK_DRIVER_PYTHON=venv/bin/python \
pytest tests/ -v
```

All 17 tests should pass in ~15 seconds.

---

## Quick Reference

| URL | What |
|-----|------|
| http://localhost:8501 | Streamlit fraud dashboard |
| http://localhost:8080 | Kafka UI — browse live messages |
| http://localhost:8081 | Spark UI — worker status |

| File | What |
|------|------|
| `kafka/transaction_producer.py` | Generates fake transactions |
| `spark/fraud_detector.py` | Scores transactions in real time |
| `spark/fraud_rules.py` | All 4 fraud detection rules |
| `snowflake/snowflake_connector.py` | Writes alerts to Snowflake |
| `dashboard/app.py` | Streamlit live dashboard |
| `tests/test_fraud_rules.py` | Unit tests |
| `.env` | Your credentials (never commit this) |
| `snowflake_key.p8` | Your private key (never commit this) |
