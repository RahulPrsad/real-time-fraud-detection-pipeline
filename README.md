# Real-Time Fraud Detection Pipeline

An end-to-end **real-time data engineering pipeline** built using Apache Kafka and Apache Spark Streaming to process live financial transactions and detect fraudulent activities with low latency.

---

## Project Overview

Financial systems generate massive volumes of transaction data every second. Detecting fraud in such environments requires **real-time processing** rather than traditional batch systems.

This project simulates a **real-world fraud detection system** where streaming transaction data is ingested, processed, and analyzed instantly to identify suspicious patterns.

---

##  Architecture

```
Python Producer → Kafka → Spark Streaming → Fraud Detection → Storage → Dashboard
```

### Workflow

1. **Data Producer**
   - Generates real-time transaction data
   - Sends data to Kafka topic (`transactions`)

2. **Kafka (Message Broker)**
   - Buffers incoming data streams
   - Ensures scalability and fault tolerance

3. **Spark Streaming (Processing Engine)**
   - Consumes data from Kafka
   - Parses JSON and applies schema
   - Performs transformations

4. **Fraud Detection Layer**
   - Identifies suspicious transactions using rules:
     - High-value transactions (> ₹50,000)
     - Rapid repeated transactions
     - Location anomalies

5. **Storage Layer**
   - Stores processed data in Parquet/CSV format

6. **Dashboard (Optional)**
   - Visualizes fraud alerts and system metrics using Streamlit

---

## Tech Stack

- Apache Kafka (Real-time data streaming)
- Apache Spark Streaming (Stream processing)
- Python / PySpark
- Docker (Kafka setup)
- Streamlit (Optional dashboard)
- Parquet (Storage format)

---

## Project Structure

```
real-time-fraud-detection-pipeline/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── spark_streaming.py
│
├── data/
│
├── output/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
├── LICENSE
```

---

## Features

- Real-time transaction processing
- Fraud detection using rule-based logic
- Scalable streaming architecture
- Fault-tolerant data pipeline
- Optimized storage using Parquet

---

## Fraud Detection Rules

- 🚨 Transactions above ₹50,000 flagged
- ⚡ Multiple transactions within short time window
- 🌍 Sudden change in transaction location
- 🔁 Repeated transactions

---

## Sample Output

- Flagged fraudulent transactions
- Total transactions processed
- Fraud detection rate

(Add screenshots here)

---

##  Why This Project?

- Demonstrates real-time data engineering skills
- Mimics real-world fintech systems
- Shows understanding of distributed systems
- Combines streaming + analytics

---

##  Use Cases

- Banking fraud detection
- UPI transaction monitoring
- Credit card anomaly detection
- Fintech risk analysis systems

---
