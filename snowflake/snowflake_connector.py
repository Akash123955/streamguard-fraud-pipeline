"""
StreamGuard — Snowflake Connector
====================================
Consumes fraud alerts from Kafka topic `fraud-alerts` and writes
them to Snowflake FRAUD_ALERTS (Hybrid Table) in micro-batches.

Also writes all raw transactions to TRANSACTIONS_RAW.

Run this alongside the Spark detector:
  python snowflake/snowflake_connector.py

Prerequisites:
  1. Sign up at https://signup.snowflake.com (free trial)
  2. Run snowflake/setup.sql in a Snowflake worksheet
  3. Copy .env.example → .env and fill in your Snowflake credentials
  4. pip install snowflake-connector-python python-dotenv kafka-python
"""

import json
import os
import time
from datetime import datetime, timezone

import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# ── Snowflake config from .env ─────────────────────────────────────────────────
SF_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "STREAMGUARD"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "STREAMGUARD_WH"),
    "role":      os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
BATCH_SIZE = 100        # Write to Snowflake every 100 messages (reduces API calls)
BATCH_TIMEOUT_SEC = 10  # Or every 10 seconds, whichever comes first


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SF_CONFIG)
    print(f"Connected to Snowflake account: {SF_CONFIG['account']}")
    return conn


def write_fraud_alerts_batch(cursor, alerts: list):
    """
    Bulk insert a batch of fraud alerts into FRAUD_ALERTS (Hybrid Table).

    Using executemany() is much faster than individual INSERT statements
    — it batches the rows into a single round-trip to Snowflake.
    """
    if not alerts:
        return

    sql = """
        INSERT INTO FRAUD_ALERTS (
            transaction_id, customer_id, event_time, amount,
            merchant_name, merchant_category, merchant_country,
            customer_lat, customer_lon, card_type,
            fraud_score, risk_level, fraud_reason,
            amount_flag, merchant_flag, geo_flag
        ) VALUES (
            %(transaction_id)s, %(customer_id)s, %(event_time)s, %(amount)s,
            %(merchant_name)s, %(merchant_category)s, %(merchant_country)s,
            %(customer_lat)s, %(customer_lon)s, %(card_type)s,
            %(fraud_score)s, %(risk_level)s, %(fraud_reason)s,
            %(amount_flag)s, %(merchant_flag)s, %(geo_flag)s
        )
    """
    cursor.executemany(sql, alerts)
    print(f"  → Inserted {len(alerts)} fraud alerts into Snowflake FRAUD_ALERTS")


def write_raw_transactions_batch(cursor, transactions: list):
    """Bulk insert raw transactions into TRANSACTIONS_RAW."""
    if not transactions:
        return

    sql = """
        INSERT INTO TRANSACTIONS_RAW (
            transaction_id, customer_id, customer_name, event_time,
            amount, currency, merchant_name, merchant_category,
            merchant_country, customer_lat, customer_lon,
            card_last4, card_type, is_fraud_label, fraud_type
        ) VALUES (
            %(transaction_id)s, %(customer_id)s, %(customer_name)s, %(event_time)s,
            %(amount)s, %(currency)s, %(merchant_name)s, %(merchant_category)s,
            %(merchant_country)s, %(customer_lat)s, %(customer_lon)s,
            %(card_last4)s, %(card_type)s, %(is_fraud)s, %(fraud_type)s
        )
    """
    cursor.executemany(sql, transactions)


def consume_and_write():
    """Main loop: consume Kafka → buffer → batch write to Snowflake."""

    # Connect to Snowflake
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Subscribe to fraud-alerts topic
    consumer = KafkaConsumer(
        "fraud-alerts",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="snowflake-writer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=BATCH_TIMEOUT_SEC * 1000,
    )

    print(f"\nListening for fraud alerts on Kafka...")
    print(f"Writing to Snowflake in batches of {BATCH_SIZE} (or every {BATCH_TIMEOUT_SEC}s)\n")

    batch = []
    last_write = time.time()
    total_written = 0

    while True:
        try:
            for message in consumer:
                alert = message.value
                # Normalize the event_time to a format Snowflake accepts
                alert["event_time"] = alert.get("timestamp", datetime.now(timezone.utc).isoformat())
                batch.append(alert)

                # Flush when batch is full OR timeout elapsed
                if len(batch) >= BATCH_SIZE or (time.time() - last_write) >= BATCH_TIMEOUT_SEC:
                    write_fraud_alerts_batch(cursor, batch)
                    conn.commit()
                    total_written += len(batch)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total written: {total_written}")
                    batch = []
                    last_write = time.time()

            # consumer_timeout_ms expired — flush any remaining
            if batch:
                write_fraud_alerts_batch(cursor, batch)
                conn.commit()
                total_written += len(batch)
                batch = []
                last_write = time.time()

        except KeyboardInterrupt:
            print("\nStopping Snowflake connector...")
            if batch:
                write_fraud_alerts_batch(cursor, batch)
                conn.commit()
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
            # Reconnect
            conn = get_snowflake_connection()
            cursor = conn.cursor()

    cursor.close()
    conn.close()
    consumer.close()
    print(f"Done. Total alerts written: {total_written}")


if __name__ == "__main__":
    consume_and_write()
