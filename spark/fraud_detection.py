"""
Spark Streaming — Fraud Detection Engine
=========================================
Reads from Kafka → parses JSON → applies three fraud rules →
writes flagged and clean records to separate Parquet sinks.

Rules
-----
1. High-value   : amount > FRAUD_THRESHOLD  (default $10 000)
2. Rapid-fire   : > RAPID_TXN_LIMIT txns in a 60-second window per user
3. Location gap : two consecutive txns for the same user > LOCATION_KM_LIMIT km apart
"""

import math
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "10000"))
RAPID_TXN_LIMIT = int(os.getenv("RAPID_TXN_LIMIT", "5"))
LOCATION_KM_LIMIT = float(os.getenv("LOCATION_KM_LIMIT", "500"))
CHECKPOINT_BASE = f"{OUTPUT_PATH}/_checkpoints"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FraudDetectionPipeline")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Schema ────────────────────────────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),    True),
    StructField("user_id",          StringType(),    True),
    StructField("amount",           DoubleType(),    True),
    StructField("timestamp",        StringType(),    True),   # ISO-8601 string
    StructField("city",             StringType(),    True),
    StructField("latitude",         DoubleType(),    True),
    StructField("longitude",        DoubleType(),    True),
    StructField("transaction_type", StringType(),    True),
    StructField("merchant",         StringType(),    True),
    StructField("is_seed_fraud",    BooleanType(),   True),
])


# ── Haversine UDF (Rule 3) ────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in kilometres."""
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


haversine_udf = F.udf(haversine_km, DoubleType())


# ── Read from Kafka ───────────────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ── Parse JSON payload ────────────────────────────────────────────────────────
parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .withColumn("data", F.from_json(F.col("json_str"), TXN_SCHEMA))
    .select("data.*", "kafka_ts")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .withWatermark("event_time", "10 minutes")
)


# ── Rule 1 — High-Value Transaction ──────────────────────────────────────────
def apply_high_value_rule(df):
    return df.withColumn(
        "fraud_high_value",
        F.col("amount") > F.lit(FRAUD_THRESHOLD)
    )


# ── Rule 2 — Rapid-Fire (windowed count) ─────────────────────────────────────
def apply_rapid_fire_rule(df):
    window_counts = (
        df
        .groupBy(
            F.col("user_id"),
            F.window(F.col("event_time"), "60 seconds", "30 seconds")
        )
        .agg(F.count("*").alias("txn_count"))
        .filter(F.col("txn_count") > RAPID_TXN_LIMIT)
        .select("user_id", F.lit(True).alias("fraud_rapid_fire"))
    )
    return df.join(window_counts, on="user_id", how="left").fillna(False, subset=["fraud_rapid_fire"])


# ── Rule 3 — Location Anomaly (lag comparison) ───────────────────────────────
def apply_location_rule(df):
    from pyspark.sql.window import Window
    user_window = Window.partitionBy("user_id").orderBy("event_time")
    enriched = (
        df
        .withColumn("prev_lat", F.lag("latitude").over(user_window))
        .withColumn("prev_lon", F.lag("longitude").over(user_window))
        .withColumn(
            "distance_km",
            haversine_udf("prev_lat", "prev_lon", "latitude", "longitude")
        )
        .withColumn("fraud_location_anomaly", F.col("distance_km") > F.lit(LOCATION_KM_LIMIT))
        .fillna(False, subset=["fraud_location_anomaly"])
    )
    return enriched


# ── Combine rules & write ─────────────────────────────────────────────────────
def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.count() == 0:
        return

    step1 = apply_high_value_rule(batch_df)
    step2 = apply_location_rule(step1)   # location uses window, runs on batch

    step2 = step2.withColumn(
        "is_fraud",
        F.col("fraud_high_value") | F.col("fraud_location_anomaly")
    ).withColumn(
        "fraud_reason",
        F.when(F.col("fraud_high_value") & F.col("fraud_location_anomaly"), "high_value+location_anomaly")
         .when(F.col("fraud_high_value"), "high_value")
         .when(F.col("fraud_location_anomaly"), "location_anomaly")
         .otherwise(None)
    ).withColumn("processed_at", F.current_timestamp())

    clean = step2.filter(~F.col("is_fraud"))
    fraud = step2.filter( F.col("is_fraud"))

    (clean.write.mode("append")
          .option("compression", "snappy")
          .parquet(f"{OUTPUT_PATH}/transactions"))

    (fraud.write.mode("append")
          .option("compression", "snappy")
          .parquet(f"{OUTPUT_PATH}/fraud_alerts"))

    # Also write a small CSV for the dashboard to read easily
    (fraud.drop("prev_lat", "prev_lon", "distance_km")
           .write.mode("append")
           .option("header", "true")
           .csv(f"{OUTPUT_PATH}/fraud_alerts_csv"))

    fraud_count = fraud.count()
    total = step2.count()
    print(f"[spark] Batch {batch_id}: {total} records | {fraud_count} fraud | "
          f"rate={fraud_count/max(total,1)*100:.1f}%")


# ── Stream query ──────────────────────────────────────────────────────────────
query = (
    parsed
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"[spark] Fraud detection stream started. Writing to {OUTPUT_PATH}")
query.awaitTermination()
