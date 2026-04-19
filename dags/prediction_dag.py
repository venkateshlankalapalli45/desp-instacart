"""
prediction_dag.py — Airflow 3.x DAG for scheduled batch predictions.

Schedule: every 2 minutes
Pipeline:
  1. find_good_files      — locate validated CSV files in /data/good/
  2. run_predictions      — POST rows to the model service /predict endpoint
  3. archive_predicted    — move processed good files to /data/predicted/

If no new files are found, the DAG skips gracefully (AirflowSkipException).
"""

from __future__ import annotations

import glob
import os
import shutil
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

import pandas as pd

# ── Constants ─────────────────────────────────────────────────────
GOOD_DIR = os.getenv("GOOD_DATA_DIR", "/data/good")
PREDICTED_DIR = os.getenv("PREDICTED_DATA_DIR", "/data/predicted")
MODEL_SERVICE_URL = os.getenv("MODEL_SERVICE_URL", "http://model_service:8000")

FEATURES = [
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "add_to_cart_order",
    "department_id",
    "aisle_id",
]

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="instacart_predictions",
    description="Batch predictions on validated Instacart CSV chunks",
    schedule="*/2 * * * *",  # every 2 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["instacart", "prediction"],
)
def prediction_dag():

    @task
    def find_good_files() -> list[str]:
        """Return CSV paths from GOOD_DIR that have not yet been predicted."""
        os.makedirs(GOOD_DIR, exist_ok=True)
        os.makedirs(PREDICTED_DIR, exist_ok=True)

        already_predicted = {
            os.path.basename(p)
            for p in glob.glob(os.path.join(PREDICTED_DIR, "*.csv"))
        }
        all_good = sorted(glob.glob(os.path.join(GOOD_DIR, "*.csv")))
        new_files = [f for f in all_good if os.path.basename(f) not in already_predicted]

        print(f"Found {len(new_files)} new good file(s) to predict.")

        if not new_files:
            raise AirflowSkipException("No new files to predict. Skipping DAG run.")

        return new_files

    @task
    def run_predictions(file_paths: list[str]) -> dict:
        """
        For each good file, build the features payload and call POST /predict.
        Returns summary stats pushed via XCom.
        """
        if not file_paths:
            raise AirflowSkipException("Empty file list — nothing to predict.")

        total_rows = 0
        total_reordered = 0
        failed_files = []

        for path in file_paths:
            fname = os.path.basename(path)
            print(f"Predicting {fname} ...")

            try:
                df = pd.read_csv(path)
            except Exception as exc:
                print(f"  Cannot read {fname}: {exc}")
                failed_files.append(fname)
                continue

            # Keep only known feature columns that are present
            available = [c for c in FEATURES if c in df.columns]
            if not available:
                print(f"  No feature columns found in {fname}, skipping.")
                failed_files.append(fname)
                continue

            features_df = df[available].copy()

            # Cast types to Python natives for JSON serialisation
            records = []
            for _, row in features_df.iterrows():
                record = {}
                for col in FEATURES:
                    if col not in row:
                        continue
                    val = row[col]
                    if col == "days_since_prior_order":
                        record[col] = float(val)
                    else:
                        record[col] = int(val)
                records.append(record)

            if not records:
                continue

            payload = {"features": records}

            try:
                resp = requests.post(
                    f"{MODEL_SERVICE_URL}/predict?source=scheduled",
                    json=payload,
                    timeout=30,
                )
                resp.raise_for_status()
                predictions = resp.json()["predictions"]
                reordered_count = sum(1 for p in predictions if p["reordered"])

                print(
                    f"  {fname}: {len(predictions)} predictions, "
                    f"{reordered_count} will reorder."
                )
                total_rows += len(predictions)
                total_reordered += reordered_count

            except requests.exceptions.RequestException as exc:
                print(f"  API call failed for {fname}: {exc}")
                failed_files.append(fname)

        summary = {
            "total_rows": total_rows,
            "total_reordered": total_reordered,
            "failed_files": failed_files,
        }
        print(f"\nBatch summary: {summary}")
        return summary

    @task
    def archive_predicted(file_paths: list[str]) -> None:
        """Move good files to the predicted/ directory after successful inference."""
        if not file_paths:
            return
        os.makedirs(PREDICTED_DIR, exist_ok=True)
        for path in file_paths:
            dest = os.path.join(PREDICTED_DIR, os.path.basename(path))
            shutil.move(path, dest)
            print(f"Archived {os.path.basename(path)} → predicted/")

    # ── DAG wiring ────────────────────────────────────────────────
    files = find_good_files()
    summary = run_predictions(files)
    archive_predicted(files)

    # Ensure archive runs after predictions are done
    summary >> archive_predicted(files)


prediction_dag()
