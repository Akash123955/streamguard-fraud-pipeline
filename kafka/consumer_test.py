"""
StreamGuard — Kafka Consumer Test
===================================
Quick sanity check: verifies that messages are flowing into Kafka topics.

Run this after starting the transaction producer to confirm everything works:
  python kafka/consumer_test.py

It will print the last 10 messages from raw-transactions and any messages
in fraud-alerts (populated once the Spark detector is running).
"""

import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"


def peek_topic(topic: str, num_messages: int = 10):
    """Read and print the latest N messages from a Kafka topic."""
    print(f"\n{'='*60}")
    print(f"Topic: {topic} — last {num_messages} messages")
    print('='*60)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",     # Only read new messages
        enable_auto_commit=False,
        consumer_timeout_ms=5000,       # Stop after 5 seconds of no messages
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"test-consumer-{topic}",
    )

    count = 0
    for message in consumer:
        txn = message.value
        risk = txn.get("risk_level", "N/A")
        fraud_flag = "FRAUD" if txn.get("is_fraud") or txn.get("fraud_score", 0) > 0 else "    "
        print(
            f"[{fraud_flag}] {txn.get('customer_id', 'N/A')} | "
            f"${txn.get('amount', 0):.2f} | "
            f"{txn.get('merchant_category', 'N/A')} | "
            f"{txn.get('merchant_country', 'N/A')} | "
            f"Risk: {risk}"
        )
        count += 1
        if count >= num_messages:
            break

    if count == 0:
        print("No messages received (topic may be empty or producer not running).")

    consumer.close()


if __name__ == "__main__":
    print("StreamGuard — Kafka Topic Inspector")
    peek_topic("raw-transactions", num_messages=10)
    peek_topic("fraud-alerts", num_messages=10)
