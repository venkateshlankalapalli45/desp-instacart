"""
prediction_dag.py — Airflow 3.x batch prediction DAG.

Schedule: every 2 minutes
Tasks:
  check_for_new_data  → lists CSV files in good_data not yet predicted
                        raises AirflowSkipException if none → entire DAG run = skipped
  make_predictions    → reads all new files, sends ONE batch API call
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

API_URL = os.getenv("MODEL_SERVICE_URL", "http://api:8000")
DATA_DIR = "/opt/airflow/data"
GOOD_DATA_PATH = os.path.join(DATA_DIR, "good_data")
TRACKER_FILE = os.path.join(DATA_DIR, ".last_predicted_files")

FEATURES = ["order_dow", "order_hour_of_day", "days_since_prior"]

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _load_already_predicted() -> set[str]:
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            return {line.strip() for line in f if line.strip()}
    return set()


def _mark_as_predicted(file_names: list[str]) -> None:
    with open(TRACKER_FILE, "a") as f:
        for name in file_names:
            f.write(name + "\n")


@dag(
    dag_id="prediction_job_dag",
    description="Batch predictions on validated Instacart CSV chunks",
    schedule="*/2 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["instacart", "prediction"],
)
def prediction_dag():

    @task
    def check_for_new_data() -> list[str]:
        """
        Returns list of new file names in good_data.
        Raises AirflowSkipException if there are none — this marks the
        entire DAG run as skipped (not just this task).
        """
        os.makedirs(GOOD_DATA_PATH, exist_ok=True)
        all_files = {f for f in os.listdir(GOOD_DATA_PATH) if f.endswith(".csv")}
        already_predicted = _load_already_predicted()
        new_files = sorted(all_files - already_predicted)

        print(f"Good data files: {len(all_files)} total, {len(new_files)} new.")

        if not new_files:
            raise AirflowSkipException("No new data in good_data — DAG run skipped.")

        return new_files

    @task
    def make_predictions(new_files: list[str]) -> dict:
        """
        Reads all new CSV files, builds ONE batch payload, and calls /predict once.
        """
        all_records = []
        file_row_counts: dict[str, int] = {}

        for file_name in new_files:
            file_path = os.path.join(GOOD_DATA_PATH, file_name)
            try:
                df = pd.read_csv(file_path)
            except Exception as exc:
                print(f"  Cannot read {file_name}: {exc} — skipping.")
                continue

            available = [c for c in FEATURES if c in df.columns]
            if not available:
                print(f"  No features found in {file_name} — skipping.")
                continue

            for _, row in df.iterrows():
                user_id = int(row["user_id"]) if "user_id" in row else 0
                record = {
                    "user_id": user_id,
                    "features": {
                        "order_dow": int(row["order_dow"]),
                        "order_hour_of_day": int(row["order_hour_of_day"]),
                        "days_since_prior": float(row["days_since_prior"]),
                    },
                    "source": "scheduled",
                }
                all_records.append(record)

            file_row_counts[file_name] = len(df)

        if not all_records:
            print("No valid records to predict.")
            return {"total": 0, "reordered": 0}

        # Single API call for the entire batch
        payload = {"predictions": all_records}
        try:
            resp = requests.post(f"{API_URL}/predict", json=payload, timeout=60)
            resp.raise_for_status()
            predictions = resp.json()["predictions"]
            reordered_count = sum(1 for p in predictions if p["reordered"])
            print(
                f"Batch prediction complete: {len(predictions)} rows, "
                f"{reordered_count} will reorder."
            )
            _mark_as_predicted(list(file_row_counts.keys()))
            return {"total": len(predictions), "reordered": reordered_count}
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"API call failed: {exc}") from exc

    # ── DAG wiring ─────────────────────────────────────────────
    new_files = check_for_new_data()
    make_predictions(new_files)


prediction_dag()
