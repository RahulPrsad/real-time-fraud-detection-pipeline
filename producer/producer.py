"""
Transaction Producer
====================
Simulates real-time financial transactions and publishes them to Kafka.
Intentionally seeds ~10% fraud-like patterns so the detection layer
has real signals to catch.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
TPS = float(os.getenv("TRANSACTIONS_PER_SECOND", "5"))

# ── City coordinate lookup (lat, lon) ─────────────────────────────────────────
CITIES = {
    "New York":     (40.7128, -74.0060),
    "Los Angeles":  (34.0522, -118.2437),
    "Chicago":      (41.8781, -87.6298),
    "Houston":      (29.7604, -95.3698),
    "Phoenix":      (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio":  (29.4241, -98.4936),
    "Dallas":       (32.7767, -96.7970),
    "Miami":        (25.7617, -80.1918),
    "Seattle":      (47.6062, -122.3321),
    "Tokyo":        (35.6762, 139.6503),
    "London":       (51.5074, -0.1278),
    "Paris":        (48.8566, 2.3522),
    "Sydney":       (-33.8688, 151.2093),
}

CITY_NAMES = list(CITIES.keys())
TRANSACTION_TYPES = ["purchase", "transfer", "withdrawal", "online_payment", "refund"]

# Track recent activity per user for rapid-fire injection
user_recent: dict[str, list] = {}


def make_normal_transaction(user_id: str) -> dict:
    city = random.choice(CITY_NAMES)
    lat, lon = CITIES[city]
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.uniform(5, 3000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "latitude": lat + random.uniform(-0.1, 0.1),
        "longitude": lon + random.uniform(-0.1, 0.1),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant": fake.company(),
        "is_seed_fraud": False,
    }


def make_high_value_transaction(user_id: str) -> dict:
    """Rule 1 seed: amount above $10 000."""
    txn = make_normal_transaction(user_id)
    txn["amount"] = round(random.uniform(10_001, 95_000), 2)
    txn["is_seed_fraud"] = True
    return txn


def make_rapid_transactions(user_id: str) -> list[dict]:
    """Rule 2 seed: burst of 6 transactions within seconds."""
    city = random.choice(CITY_NAMES)
    lat, lon = CITIES[city]
    return [
        {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": round(random.uniform(50, 500), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "transaction_type": "online_payment",
            "merchant": fake.company(),
            "is_seed_fraud": True,
        }
        for _ in range(6)
    ]


def make_location_anomaly(user_id: str) -> list[dict]:
    """Rule 3 seed: ONE transaction placed in a city far from the user's last known city.
    Spark detects the anomaly via lag() comparison — we only need one message here,
    not two, to avoid flooding the topic with location-anomaly events."""
    # Pick a city that is geographically extreme (far from any US base city)
    anomaly_city = random.choice(["Tokyo", "London", "Paris", "Sydney"])
    lat, lon = CITIES[anomaly_city]
    return [{
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.uniform(100, 2000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "city": anomaly_city,
        "latitude": lat + random.uniform(-0.05, 0.05),
        "longitude": lon + random.uniform(-0.05, 0.05),
        "transaction_type": "purchase",
        "merchant": fake.company(),
        "is_seed_fraud": True,
    }]


def connect_producer() -> KafkaProducer:
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[producer] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Broker not ready (attempt {attempt}/10) — retrying in 5 s…")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def main() -> None:
    producer = connect_producer()
    users = [f"user_{i:04d}" for i in range(1, 51)]
    # Small pool of "at-risk" users reused for rapid-fire so bursts hit the same user_id
    rapid_fire_users = [f"user_{i:04d}" for i in range(1, 6)]
    delay = 1.0 / TPS
    msg_count = 0

    print(f"[producer] Streaming to topic '{KAFKA_TOPIC}' at ~{TPS} TPS")

    while True:
        roll = random.random()

        # Balanced: ~5% high-value | ~5% rapid-fire | ~5% location | ~85% normal
        if roll < 0.05:
            user_id = random.choice(users)
            batch = [make_high_value_transaction(user_id)]
        elif roll < 0.10:
            # Use the small rapid-fire pool so same user appears multiple times quickly
            user_id = random.choice(rapid_fire_users)
            batch = make_rapid_transactions(user_id)
        elif roll < 0.15:
            user_id = random.choice(users)
            batch = make_location_anomaly(user_id)
        else:
            user_id = random.choice(users)
            batch = [make_normal_transaction(user_id)]

        for txn in batch:
            producer.send(KAFKA_TOPIC, value=txn)
            msg_count += 1
            if msg_count % 50 == 0:
                print(f"[producer] {msg_count} messages sent | last: {txn['transaction_id'][:8]}… "
                      f"${txn['amount']:,.2f} | {txn['city']}")

        producer.flush()
        time.sleep(delay)


if __name__ == "__main__":
    main()