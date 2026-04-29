"""
StreamGuard — Transaction Producer
====================================
Simulates a live payment stream at ~10,000 transactions/minute.

What this does:
  - Generates realistic credit card transactions using Faker
  - Injects 2% fraudulent transactions with real fraud signatures
  - Publishes each transaction as a JSON message to Kafka topic: raw-transactions
  - Partitions by customer_id so all txns for the same customer land on the
    same Spark partition (critical for accurate velocity checks)

Run locally:
  python kafka/transaction_producer.py
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"   # 9093 = external port on your Mac
KAFKA_TOPIC = "raw-transactions"
TRANSACTIONS_PER_MINUTE = 10_000
SLEEP_BETWEEN_MESSAGES = 60 / TRANSACTIONS_PER_MINUTE   # ~0.006 seconds

NUM_CUSTOMERS = 500     # Pool of 500 simulated customers
FRAUD_RATE = 0.02       # 2% of transactions are fraudulent

fake = Faker()
Faker.seed(42)          # Reproducible fake data
random.seed(42)

# ── Customer pool ──────────────────────────────────────────────────────────────
# Pre-generate 500 customers so the same customer_id reappears repeatedly.
# This makes velocity and geo-anomaly checks meaningful.
CUSTOMERS = [
    {
        "customer_id": f"CUST_{str(i).zfill(5)}",
        "name": fake.name(),
        "home_country": random.choice(["US", "US", "US", "CA", "GB", "AU"]),  # mostly US
        "home_lat": round(random.uniform(25.0, 48.0), 6),   # continental US latitude range
        "home_lon": round(random.uniform(-124.0, -67.0), 6),
    }
    for i in range(1, NUM_CUSTOMERS + 1)
]

# ── Merchant categories ────────────────────────────────────────────────────────
LEGIT_CATEGORIES = [
    "grocery", "restaurant", "gas_station", "retail", "pharmacy",
    "entertainment", "travel", "healthcare", "subscription", "utilities",
]
HIGH_RISK_CATEGORIES = ["unknown", "crypto_exchange", "wire_transfer"]

# ── Fraud signatures ───────────────────────────────────────────────────────────
# These match the fraud rules in spark/fraud_detector.py
FRAUD_MERCHANTS = ["Dark Web Store", "CryptoFast Exchange", "QuickWire Transfer"]
FRAUD_COUNTRIES = ["RU", "NG", "KP"]   # High-fraud countries in simulated data


def build_legit_transaction(customer: dict) -> dict:
    """Generate a realistic, non-fraudulent transaction."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "customer_name": customer["name"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(1.0, 499.0), 2),         # Normal spend range
        "currency": "USD",
        "merchant_name": fake.company(),
        "merchant_category": random.choice(LEGIT_CATEGORIES),
        "merchant_country": customer["home_country"],           # Same country as customer
        "customer_lat": customer["home_lat"] + random.uniform(-0.5, 0.5),
        "customer_lon": customer["home_lon"] + random.uniform(-0.5, 0.5),
        "card_last4": str(random.randint(1000, 9999)),
        "card_type": random.choice(["VISA", "MASTERCARD", "AMEX"]),
        "is_fraud": False,
        "fraud_type": None,
    }


def build_fraud_transaction(customer: dict) -> dict:
    """
    Generate a fraudulent transaction with one of three fraud signatures:
      1. High amount ($1,000–$9,999) at a suspicious merchant
      2. Transaction in a high-fraud country (geo anomaly)
      3. Both — worst case
    """
    fraud_type = random.choice(["high_amount", "geo_anomaly", "both"])

    txn = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "customer_name": customer["name"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "currency": "USD",
        "card_last4": str(random.randint(1000, 9999)),
        "card_type": random.choice(["VISA", "MASTERCARD", "AMEX"]),
        "is_fraud": True,
        "fraud_type": fraud_type,
    }

    if fraud_type == "high_amount":
        txn.update({
            "amount": round(random.uniform(1000.0, 9999.0), 2),
            "merchant_name": random.choice(FRAUD_MERCHANTS),
            "merchant_category": random.choice(HIGH_RISK_CATEGORIES),
            "merchant_country": customer["home_country"],
            "customer_lat": customer["home_lat"],
            "customer_lon": customer["home_lon"],
        })
    elif fraud_type == "geo_anomaly":
        txn.update({
            "amount": round(random.uniform(50.0, 500.0), 2),
            "merchant_name": fake.company(),
            "merchant_category": random.choice(LEGIT_CATEGORIES),
            "merchant_country": random.choice(FRAUD_COUNTRIES),
            "customer_lat": round(random.uniform(50.0, 65.0), 6),   # Far from US
            "customer_lon": round(random.uniform(30.0, 60.0), 6),
        })
    else:  # both
        txn.update({
            "amount": round(random.uniform(1000.0, 9999.0), 2),
            "merchant_name": random.choice(FRAUD_MERCHANTS),
            "merchant_category": random.choice(HIGH_RISK_CATEGORIES),
            "merchant_country": random.choice(FRAUD_COUNTRIES),
            "customer_lat": round(random.uniform(50.0, 65.0), 6),
            "customer_lon": round(random.uniform(30.0, 60.0), 6),
        })

    return txn


def create_producer() -> KafkaProducer:
    """Connect to Kafka with JSON serialization and customer_id partitioning."""
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # Serialize each message as UTF-8 JSON
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Partition key = customer_id bytes
                # This ensures all txns for the same customer go to the same partition
                # — critical for accurate velocity checks in Spark
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",             # Wait for all replicas to confirm
                retries=3,
                linger_ms=5,            # Batch messages for 5ms to improve throughput
                batch_size=16384,
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt + 1}/5). Retrying in 3s...")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 5 attempts. Is Docker running?")


def main():
    producer = create_producer()

    print(f"\nStarting transaction stream: {TRANSACTIONS_PER_MINUTE:,} txns/min")
    print(f"Fraud rate: {FRAUD_RATE*100:.0f}%  |  Topic: {KAFKA_TOPIC}")
    print("=" * 60)

    total_sent = 0
    total_fraud = 0
    start_time = time.time()

    try:
        while True:
            # Pick a random customer from our pool
            customer = random.choice(CUSTOMERS)

            # Decide: fraudulent or legitimate?
            if random.random() < FRAUD_RATE:
                transaction = build_fraud_transaction(customer)
                total_fraud += 1
                flag = "FRAUD"
            else:
                transaction = build_legit_transaction(customer)
                flag = "    "

            # Send to Kafka — key=customer_id ensures consistent partitioning
            producer.send(
                topic=KAFKA_TOPIC,
                key=transaction["customer_id"],
                value=transaction,
            )

            total_sent += 1

            # Print status every 100 transactions
            if total_sent % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = total_sent / elapsed * 60
                fraud_pct = total_fraud / total_sent * 100
                print(
                    f"[{flag}] Sent: {total_sent:,} | "
                    f"Rate: {actual_rate:,.0f} txn/min | "
                    f"Fraud: {total_fraud} ({fraud_pct:.1f}%) | "
                    f"Latest: {transaction['customer_id']} ${transaction['amount']:.2f} "
                    f"{transaction['merchant_country']}"
                )

            # Throttle to hit our target rate
            time.sleep(SLEEP_BETWEEN_MESSAGES)

    except KeyboardInterrupt:
        print(f"\nStopped. Total sent: {total_sent:,} | Fraud: {total_fraud}")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
