# Instacart Reorder Prediction — DSP Defense 1

Production MLOps pipeline: FastAPI + Streamlit + Airflow 3.x + PostgreSQL + Docker.

---

## Services

| Service           | URL                    | Credentials         |
|-------------------|------------------------|---------------------|
| Streamlit webapp  | http://localhost:8501  | —                   |
| FastAPI docs      | http://localhost:8000/docs | —               |
| Airflow webserver | http://localhost:8080  | admin / admin       |
| PostgreSQL        | localhost:5432         | admin / secure_password123 |

---

## Quick Start

### 1 — Clone and configure

```bash
git clone https://github.com/venkateshlankalapalli45/desp-instacart.git
cd desp-instacart
cp .env.example .env
```

### 2 — Start the stack

```bash
docker compose up --build -d
```

Wait ~2 minutes for Airflow to initialise. Verify:

```bash
curl http://localhost:8000/health   # → {"status":"healthy"}
```

### 3 — Generate data files for Airflow ingestion

```bash
# Generate 30 chunk files (10 rows each) → data/raw_data/
python scripts/split_data.py data/instacart_sample.csv data/raw_data 30

# (Optional) inject errors into some files for demo
python scripts/data_error_injection.py data/instacart_sample.csv data/raw_data/error_file.csv 0.4
```

### 4 — Open the apps

- **Streamlit** → http://localhost:8501
- **Airflow**   → http://localhost:8080 (login: admin / admin)
- **API docs**  → http://localhost:8000/docs

---

## Training the Model

```bash
python scripts/train.py --data data/instacart_sample.csv --output model/saved_model/
docker compose restart api
```

---

## Running Tests Locally

```bash
pip install pytest pytest-cov pandas numpy scikit-learn pydantic fastapi sqlalchemy psycopg2-binary
pytest tests/ -v --tb=short
```

---

## Airflow DAGs

| DAG                   | Schedule   | Description                                                        |
|-----------------------|------------|--------------------------------------------------------------------|
| `data_ingestion_dag`  | every 1 min | Picks random CSV from `raw_data`, validates with GX Core, saves stats, routes good/bad rows |
| `prediction_job_dag`  | every 2 min | Reads new files from `good_data`, calls `/predict` in one batch call. Skips if no new data |

---

## Project Structure

```
desp-instacart/
├── dags/               # Airflow 3.x DAGs
├── data/               # raw_data/, good_data/, bad_data/, gx/
├── db_init/            # PostgreSQL init SQL
├── model/              # Trained model artifact (model.pkl)
├── model_service/      # FastAPI prediction service
├── scripts/            # train.py, split_data.py, data_error_injection.py
├── tests/              # pytest test suite
├── webapp/             # Streamlit multipage app
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Stop

```bash
docker compose down          # keep volumes
docker compose down -v       # full reset
```
