# Real-Time Fraud Detection Pipeline

A production-style streaming pipeline using **Apache Kafka**, **Apache Spark Streaming**, **Docker Compose**, and **Streamlit**.

```
[Python Producer] → [Kafka] → [Spark Streaming] → [Fraud Detection] → [Parquet/CSV] → [Streamlit Dashboard]
```

---

## Prerequisites

| Tool | Minimum version |
|------|----------------|
| Docker Desktop | 24.x |
| Docker Compose | 2.x (included with Docker Desktop) |
| Free RAM | 6 GB |
| Free disk | 5 GB |

---

## Project layout

```
fraud-detection/
<<<<<<< HEAD
├── docker-compose.yml          # Orchestrates all services
=======
├── docker-compose.yml          
>>>>>>> 68f75264affd4489de478e1117f6e8cd80db304e
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Kafka transaction simulator
├── spark/
│   ├── Dockerfile
│   └── fraud_detection.py      # Spark Streaming + fraud rules
├── dashboard/
│   ├── Dockerfile
│   └── dashboard.py            # Streamlit visualisation
└── output/                     # Written by Spark (auto-created)
    ├── transactions/            # Clean Parquet files
    ├── fraud_alerts/            # Flagged Parquet files
    └── fraud_alerts_csv/        # CSV copies for easy inspection
```

---

## Step-by-step setup

### Step 1 — Clone / create the project

```bash
# If you received this as a zip, unzip it. Otherwise:
mkdir fraud-detection && cd fraud-detection
# Then copy all files into the structure above.
```

### Step 2 — Build all Docker images

```bash
docker compose build
```

This downloads base images and installs dependencies. Takes ~5 minutes on first run.

### Step 3 — Start the full pipeline

```bash
docker compose up -d
```

Services start in dependency order: Zookeeper → Kafka → Producer + Spark → Dashboard.

### Step 4 — Verify services are running

```bash
docker compose ps
```

Expected output:
```
NAME         STATUS          PORTS
zookeeper    running         0.0.0.0:2181->2181/tcp
kafka        running         0.0.0.0:9092->9092/tcp
kafka-ui     running         0.0.0.0:8080->8080/tcp
producer     running
spark        running
dashboard    running         0.0.0.0:8501->8501/tcp
```

### Step 5 — Watch logs

```bash
# See all services together
docker compose logs -f

# Individual service logs
docker compose logs -f producer   # transaction events
docker compose logs -f spark      # batch processing + fraud counts
docker compose logs -f dashboard  # Streamlit startup
```

### Step 6 — Open the interfaces

| Interface | URL | What you see |
|-----------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | Live fraud alerts, charts, geo map |
| Kafka UI | http://localhost:8080 | Topic browser, consumer lag |

> **Note:** The Streamlit dashboard will show "Waiting for data…" for the first 30–60 seconds while Spark processes its first micro-batch.

### Step 7 — Inspect output files

Spark writes Parquet files to the `./output` directory on your host machine:

```bash
# See what has been written
ls -lh output/transactions/
ls -lh output/fraud_alerts/

# Read CSV alerts (easier for quick inspection)
cat output/fraud_alerts_csv/*.csv | head -20
```

---

## Fraud detection rules

| Rule | Condition | Fraud reason label |
|------|-----------|--------------------|
| High-value | `amount > $10,000` | `high_value` |
| Rapid-fire | `> 5 transactions in 60 seconds for same user` | `rapid_fire` |
| Location anomaly | `> 500 km between consecutive transactions` | `location_anomaly` |
| Combined | Both high-value AND location | `high_value+location_anomaly` |

### Tuning thresholds

Edit environment variables in `docker-compose.yml`:

```yaml
environment:
  FRAUD_THRESHOLD: "10000"      # USD amount ceiling
  RAPID_TXN_LIMIT: "5"          # max txns per 60-second window
  LOCATION_KM_LIMIT: "500"      # max km between consecutive txns
```

Then restart just the Spark service:

```bash
docker compose restart spark
```

---

## Stopping the pipeline

```bash
# Stop all containers (keeps output data)
docker compose down

# Stop and remove output data too
docker compose down && rm -rf output/
```

---

## Scaling tips

| Goal | Change |
|------|--------|
| Higher throughput | Increase `TRANSACTIONS_PER_SECOND` in `producer` env |
| Faster fraud detection | Reduce Spark trigger from `30 seconds` to `10 seconds` in `fraud_detection.py` |
| More Kafka partitions | Set `KAFKA_NUM_PARTITIONS: 6` in `docker-compose.yml` |
| Persistent Kafka data | Add a named volume to the `kafka` service |

---

## Troubleshooting

**Spark keeps restarting**
Spark needs ~4 GB RAM. Increase Docker Desktop memory limit to 6 GB in Settings → Resources.

**Dashboard shows no data after 2 minutes**
Check Spark logs: `docker compose logs spark`. Look for connection errors or schema mismatches.

**Kafka UI shows no messages**
Confirm the producer is running: `docker compose logs producer`. It retries for 50 seconds before failing.

**Port conflict on 8080**
Change `"8080:8080"` to `"8081:8080"` in `docker-compose.yml` for the `kafka-ui` service.
