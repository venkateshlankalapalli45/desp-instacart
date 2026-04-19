# Instacart Reorder Prediction — DSP Defense 1

End-to-end MLOps project: a production-grade prediction service for the Instacart reorder dataset, built with FastAPI, Streamlit, Apache Airflow 3, PostgreSQL, and Docker.

---

## Architecture

```
┌─────────────┐    ┌───────────────┐    ┌──────────────────┐
│  Streamlit  │───▶│  FastAPI API  │───▶│   PostgreSQL     │
│  Webapp     │    │  (port 8000)  │    │  (dsp + airflow) │
│  (port 8501)│    └───────────────┘    └──────────────────┘
└─────────────┘            ▲                      ▲
                           │                      │
                   ┌───────────────┐    ┌──────────────────┐
                   │  Airflow      │    │  /data/          │
                   │  DAGs         │────│  raw/ good/ bad/ │
                   │  (port 8080)  │    └──────────────────┘
                   └───────────────┘
```

## Services

| Service           | URL                    | Credentials     |
|-------------------|------------------------|-----------------|
| Streamlit webapp  | http://localhost:8501  | —               |
| FastAPI docs      | http://localhost:8000/docs | —           |
| Airflow webserver | http://localhost:8080  | admin / admin   |
| PostgreSQL        | localhost:5432         | postgres / postgres |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- At least 4 GB of free RAM

### 1 — Clone and configure

```bash
git clone https://github.com/<your-org>/dsp-instacart.git
cd dsp-instacart
cp .env.example .env
```

### 2 — Start the stack

```bash
docker compose up --build -d
```

Wait ~60 seconds for all services to initialize, then verify:

```bash
docker compose ps          # all services should be "healthy" or "running"
curl http://localhost:8000/health   # → {"status": "healthy"}
```

### 3 — Generate sample data for the DAGs

```bash
# Split the training set into 10-row chunks for the ingestion DAG
python scripts/split_dataset.py --input data/train.csv --output data/raw/

# (Optional) inject some errors to see GE validation in action
python scripts/generate_errors.py --input data/raw/ --output data/raw/ --fraction 0.3
```

### 4 — Open the UI

- **Streamlit** → http://localhost:8501 — make single or batch predictions
- **Airflow** → http://localhost:8080 — monitor the ingestion and prediction DAGs
- **FastAPI docs** → http://localhost:8000/docs — explore the REST API

---

## Training the Model

```bash
python ml/train.py --data data/train.csv --output models/
```

This writes `models/model.joblib` and `models/scaler.joblib`.  
Restart `model_service` after retraining:

```bash
docker compose restart model_service
```

---

## Running Tests Locally

```bash
pip install pytest pytest-cov pandas numpy scikit-learn pydantic fastapi sqlalchemy psycopg2-binary
pytest tests/ -v --tb=short
```

---

## Airflow DAGs

| DAG                     | Schedule   | Description                                      |
|-------------------------|------------|--------------------------------------------------|
| `instacart_ingestion`   | every 1 min | Scans `/data/raw/`, validates with GX, routes rows to `good/` or `bad/`, saves stats |
| `instacart_predictions` | every 2 min | Reads `/data/good/`, calls `/predict`, archives files |

---

## API Endpoints

| Method | Path               | Description                          |
|--------|--------------------|--------------------------------------|
| GET    | `/health`          | Liveness probe                       |
| POST   | `/predict`         | Single or batch reorder prediction   |
| GET    | `/past-predictions`| Historical predictions with filters  |

### Example prediction request

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [{"order_dow": 2, "order_hour_of_day": 14, "days_since_prior_order": 7.0, "add_to_cart_order": 3, "department_id": 4, "aisle_id": 24}]}'
```

---

## Project Structure

```
dsp-instacart/
├── data/               # Raw, good, bad, processed, predicted CSV chunks
├── dags/
│   ├── ingestion_dag.py
│   └── prediction_dag.py
├── docker/
│   └── pg-init-scripts/   # Multi-database PostgreSQL init
├── ml/
│   └── train.py           # Model training script
├── model_service/         # FastAPI prediction service
│   ├── main.py
│   ├── models.py
│   ├── database.py
│   ├── Dockerfile
│   └── requirements.txt
├── models/                # Trained model artifacts
├── scripts/
│   ├── generate_errors.py
│   └── split_dataset.py
├── tests/                 # pytest test suite
├── webapp/                # Streamlit multipage app
│   ├── Home.py
│   ├── pages/
│   │   ├── 1_Predict.py
│   │   └── 2_Past_Predictions.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── init.sql
├── .env.example
└── .gitignore
```

---

## Stopping the Stack

```bash
docker compose down          # stop containers (data persists)
docker compose down -v       # stop and delete all volumes (full reset)
```
