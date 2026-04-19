"""
ingestion_dag.py — Airflow 3.x DAG for Instacart data ingestion & validation.

Schedule: every 1 minute
Pipeline:
  1. scan_new_files     — find unprocessed CSV chunks in /data/raw/
  2. validate_files     — run Great Expectations (GX Core v1) checks on each file
  3. store_stats        — persist ingestion statistics to the database
  4. route_files        — copy valid rows → /data/good/, invalid rows → /data/bad/
"""

from __future__ import annotations

import glob
import os
import shutil
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ── Constants ─────────────────────────────────────────────────────
RAW_DIR = os.getenv("RAW_DATA_DIR", "/data/raw")
GOOD_DIR = os.getenv("GOOD_DATA_DIR", "/data/good")
BAD_DIR = os.getenv("BAD_DATA_DIR", "/data/bad")
PROCESSED_DIR = os.getenv("PROCESSED_DATA_DIR", "/data/processed")
POSTGRES_CONN_ID = "postgres_dsp"

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


def _build_gx_suite(context: gx.DataContext) -> str:
    """Create (or retrieve) the expectation suite for Instacart data."""
    suite_name = "instacart_suite"
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = context.suites.add(
            gx.ExpectationSuite(name=suite_name)
        )

    # Clear existing expectations and rebuild to ensure idempotency
    suite.expectations = []

    from great_expectations.expectations import (
        ExpectColumnToExist,
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToBeOfType,
        ExpectColumnValuesToNotBeNull,
    )

    for col in FEATURES:
        suite.add_expectation(ExpectColumnToExist(column=col))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="order_dow", min_value=0, max_value=6)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="order_hour_of_day", min_value=0, max_value=23)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="days_since_prior_order", min_value=0.0, max_value=30.0
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="add_to_cart_order", min_value=1, max_value=100)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="department_id", min_value=1, max_value=21)
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="aisle_id", min_value=1, max_value=134)
    )

    context.suites.update(suite)
    return suite_name


@dag(
    dag_id="instacart_ingestion",
    description="Scan, validate, and route new Instacart CSV chunks",
    schedule="* * * * *",  # every 1 minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["instacart", "ingestion", "great-expectations"],
)
def ingestion_dag():

    @task
    def scan_new_files() -> list[str]:
        """Return paths of CSV files in RAW_DIR not yet moved to processed/."""
        os.makedirs(RAW_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        processed = {
            os.path.basename(p)
            for p in glob.glob(os.path.join(PROCESSED_DIR, "**/*.csv"), recursive=True)
        }
        all_files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
        new_files = [f for f in all_files if os.path.basename(f) not in processed]

        print(f"Found {len(new_files)} new file(s) to process.")
        return new_files

    @task
    def validate_files(file_paths: list[str]) -> list[dict]:
        """
        Run Great Expectations validation on each file.
        Returns a list of result dicts with keys:
          path, valid_rows, invalid_rows, total_rows, passed, failed_expectations
        """
        if not file_paths:
            return []

        os.makedirs(GOOD_DIR, exist_ok=True)
        os.makedirs(BAD_DIR, exist_ok=True)

        # Build in-memory GX context (no local filesystem store needed)
        context = gx.get_context(mode="ephemeral")
        suite_name = _build_gx_suite(context)

        results = []

        for path in file_paths:
            fname = os.path.basename(path)
            print(f"Validating {fname} ...")

            try:
                df = pd.read_csv(path)
            except Exception as exc:
                print(f"  Cannot read {fname}: {exc}")
                shutil.copy(path, os.path.join(BAD_DIR, fname))
                results.append({
                    "path": path,
                    "valid_rows": 0,
                    "invalid_rows": 0,
                    "total_rows": 0,
                    "passed": False,
                    "failed_expectations": [f"unreadable: {exc}"],
                })
                continue

            # Validate row by row using pandas datasource
            data_source = context.data_sources.add_pandas("pd_source")
            data_asset = data_source.add_dataframe_asset(name=fname)
            batch_def = data_asset.add_batch_definition_whole_dataframe(name="batch")
            batch = batch_def.get_batch(batch_parameters={"dataframe": df})

            validation_def = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"val_{fname}",
                    data=batch_def,
                    suite=context.suites.get(suite_name),
                )
            )
            result = context.run_validation_definition(validation=validation_def)

            # ── Identify bad rows (column-level check) ────────────
            bad_mask = pd.Series([False] * len(df), index=df.index)

            for col in FEATURES:
                if col not in df.columns:
                    bad_mask[:] = True
                    continue
                # Null check
                bad_mask |= df[col].isna()

            # Range checks
            range_checks = {
                "order_dow": (0, 6),
                "order_hour_of_day": (0, 23),
                "days_since_prior_order": (0.0, 30.0),
                "add_to_cart_order": (1, 100),
                "department_id": (1, 21),
                "aisle_id": (1, 134),
            }
            for col, (lo, hi) in range_checks.items():
                if col in df.columns:
                    numeric = pd.to_numeric(df[col], errors="coerce")
                    bad_mask |= numeric.isna() | (numeric < lo) | (numeric > hi)

            # Duplicate check (mark all but first occurrence)
            bad_mask |= df.duplicated(keep="first")

            good_df = df[~bad_mask].copy()
            bad_df = df[bad_mask].copy()

            # Write split files
            good_path = os.path.join(GOOD_DIR, fname)
            bad_path = os.path.join(BAD_DIR, fname)
            good_df.to_csv(good_path, index=False)
            if not bad_df.empty:
                bad_df.to_csv(bad_path, index=False)

            passed = result.success
            failed = [
                str(r["expectation_config"]["type"])
                for r in result.results
                if not r["success"]
            ]

            print(
                f"  {fname}: {len(good_df)} valid / {len(bad_df)} invalid rows"
                f" | suite passed={passed}"
            )

            results.append({
                "path": path,
                "valid_rows": int(len(good_df)),
                "invalid_rows": int(len(bad_df)),
                "total_rows": int(len(df)),
                "passed": bool(passed),
                "failed_expectations": failed,
            })

        return results

    @task
    def store_stats(validation_results: list[dict]) -> None:
        """Persist ingestion statistics to the ingestion_stats table."""
        if not validation_results:
            print("No results to store.")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO ingestion_stats
                (file_name, total_rows, valid_rows, invalid_rows, passed, failed_expectations)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        for r in validation_results:
            cursor.execute(insert_sql, (
                os.path.basename(r["path"]),
                r["total_rows"],
                r["valid_rows"],
                r["invalid_rows"],
                r["passed"],
                ", ".join(r["failed_expectations"]) if r["failed_expectations"] else "",
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Stored stats for {len(validation_results)} file(s).")

    @task
    def archive_files(file_paths: list[str]) -> None:
        """Move processed raw files to the processed/ directory."""
        if not file_paths:
            return
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        for path in file_paths:
            dest = os.path.join(PROCESSED_DIR, os.path.basename(path))
            shutil.move(path, dest)
            print(f"Archived {os.path.basename(path)} → processed/")

    # ── DAG wiring ────────────────────────────────────────────────
    files = scan_new_files()
    results = validate_files(files)
    store_stats(results)
    archive_files(files)


ingestion_dag()
