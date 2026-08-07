# 🚀 Real-Time Fraud Detection Pipeline (Version 2 - ML Based)

A production-style **real-time fraud detection pipeline** built using **Apache Kafka**, **Apache Spark Structured Streaming**, **Machine Learning**, **Docker Compose**, and **Streamlit**.

The pipeline continuously ingests transaction events, performs feature engineering, predicts fraudulent transactions using a pre-trained machine learning model, stores predictions, and visualizes results in a live dashboard.

---

## 📌 Architecture

```text
                +------------------+
                | Python Producer  |
                +--------+---------+
                         |
                         v
                  +-------------+
                  | Apache Kafka|
                  +------+------+
                         |
                         v
        +---------------------------------+
        | Spark Structured Streaming      |
        |                                 |
        |  • Feature Engineering          |
        |  • ML Model Inference           |
        +---------------+-----------------+
                        |
         +--------------+--------------+
         |                             |
         v                             v
  Fraud Predictions             All Transactions
         |                             |
         +--------------+--------------+
                        |
                        v
              Parquet / CSV Storage
                        |
                        v
              Streamlit Dashboard
```

---

# ✨ Features

- Real-time transaction streaming
- Apache Kafka event ingestion
- Spark Structured Streaming pipeline
- Feature engineering on streaming data
- ML-based fraud prediction
- Fraud probability scoring
- Parquet and CSV output
- Live Streamlit dashboard
- Docker Compose deployment
- Kafka UI monitoring
- Easily replaceable ML models

---

# 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Streaming | Apache Kafka |
| Processing | Apache Spark Structured Streaming |
| Machine Learning | Scikit-learn |
| Dashboard | Streamlit |
| Storage | Parquet / CSV |
| Containerization | Docker Compose |
| Language | Python |

---

# 📁 Project Structure

```text
fraud-detection/
│
├── docker-compose.yml
│
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
├── spark/
│   ├── Dockerfile
│   ├── fraud_detection.py
│   ├── feature_engineering.py
│   ├── train_model.py
│   ├── model.pkl
│   └── scaler.pkl
│
├── dashboard/
│   ├── Dockerfile
│   └── dashboard.py
│
└── output/
    ├── transactions/
    ├── fraud_predictions/
    └── fraud_predictions_csv/
```

---

# ⚙️ Prerequisites

- Docker
- Docker Compose
- Python 3.10+
- Git

---

# 🚀 Getting Started

## 1. Clone the Repository

```bash
git clone https://github.com/yourusername/fraud-detection.git

cd fraud-detection
```

---

## 2. Train the Machine Learning Model

If a pretrained model is not available, train one using:

```bash
python spark/train_model.py
```

This generates:

```
model.pkl
scaler.pkl
```

---

## 3. Build Docker Images

```bash
docker compose build
```

---

## 4. Start the Pipeline

```bash
docker compose up -d
```

This starts:

- Zookeeper
- Kafka
- Kafka UI
- Producer
- Spark Streaming
- Streamlit Dashboard

---

## 5. Verify Running Containers

```bash
docker compose ps
```

Example:

```text
NAME         STATUS

zookeeper    running
kafka        running
producer     running
spark        running
dashboard    running
```

---

## 6. View Logs

All services

```bash
docker compose logs -f
```

Individual services

```bash
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f dashboard
```

---

# 📊 Dashboard

Open:

```
http://localhost:8501
```

The dashboard displays:

- Total transactions
- Fraud count
- Fraud probability
- Fraud trends
- Transaction history
- Live updates

---

# 📈 Kafka UI

Open:

```
http://localhost:8080
```

Monitor:

- Topics
- Producers
- Consumers
- Consumer Lag
- Messages

---

# 🤖 Machine Learning Pipeline

Each incoming transaction follows the workflow below.

```text
Transaction

↓

Feature Engineering

↓

Scaling

↓

ML Model

↓

Fraud Probability

↓

Prediction

↓

Storage
```

---

# 🧠 Feature Engineering

Example features include:

- Transaction amount
- Merchant category
- Payment method
- Transaction frequency
- Average spending
- User risk score
- Device type
- Geographic distance
- Previous fraud history
- Time of transaction

---

# 📦 Supported Models

Any Scikit-learn compatible model can be deployed.

Examples:

- Logistic Regression
- Random Forest
- XGBoost
- LightGBM
- CatBoost
- Isolation Forest

Simply replace:

```
model.pkl
```

No changes to the streaming pipeline are required.

---

# 📂 Output

Spark writes results into:

```
output/
```

Example structure:

```text
output/

transactions/

fraud_predictions/

fraud_predictions_csv/
```

Prediction example

| Transaction | Prediction | Probability |
|-------------|-----------|-------------|
| TX1001 | Fraud | 0.96 |
| TX1002 | Legitimate | 0.08 |

---

# ⚙️ Configuration

Modify the Spark environment variables in:

```
docker-compose.yml
```

Example:

```yaml
environment:
  MODEL_PATH: "/app/model.pkl"
  SCALER_PATH: "/app/scaler.pkl"
  FRAUD_THRESHOLD: "0.80"
  BATCH_INTERVAL: "10"
```

Restart Spark:

```bash
docker compose restart spark
```

---

# 🔄 Retraining the Model

When new labeled data becomes available:

```bash
python spark/train_model.py
```

Restart Spark:

```bash
docker compose restart spark
```

---

# 📊 End-to-End Workflow

```text
Python Producer
        │
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Feature Engineering
        │
        ▼
Machine Learning Model
        │
        ▼
Fraud Probability
        │
        ▼
Fraud Prediction
        │
        ▼
Parquet / CSV Storage
        │
        ▼
Streamlit Dashboard
```

---

# 🛑 Stop the Pipeline

Keep output data:

```bash
docker compose down
```

Remove containers and generated output:

```bash
docker compose down

rm -rf output/
```

---

# 🔮 Future Enhancements

- Deep Learning (LSTM / Transformer)
- Online Model Retraining
- MLflow Model Registry
- Feature Store Integration
- Prometheus & Grafana Monitoring
- Kubernetes Deployment
- CI/CD with GitHub Actions
- REST API for Predictions
- Cloud Deployment (AWS, Azure, GCP)

---

# 📜 License

This project is licensed under the MIT License.

---

# 👨‍💻 Author

**Rahul Prasad**

Computer Science Engineering Student

Apache Spark • Kafka • Machine Learning • Data Engineering • Stream Processing
