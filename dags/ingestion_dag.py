"""
ingestion_dag.py — Airflow 3.x ingestion + GX Core v1 validation DAG.

Schedule: every 1 minute
Tasks:
  read_data        → picks one random CSV from raw_data, pushes path via XCom
  validate_data    → runs GX Core v1 Checkpoint, computes criticality
  save_statistics  → stores stats to PostgreSQL via PostgresHook
  send_alerts      → logs Teams-style alert for Medium/High criticality
  split_and_save   → routes rows to good_data / bad_data
Tasks save_statistics, send_alerts, split_and_save run in parallel after validate_data.
"""
from __future__ import annotations

import json
import os
import random
import shutil
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = "/opt/airflow/data"
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw_data")
GOOD_DATA_PATH = os.path.join(DATA_DIR, "good_data")
BAD_DATA_PATH = os.path.join(DATA_DIR, "bad_data")
GX_ROOT = os.path.join(DATA_DIR, "gx")

REQUIRED_COLS = ["user_id", "order_dow", "order_hour_of_day", "days_since_prior"]
POSTGRES_CONN_ID = "postgres_default"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="data_ingestion_dag",
    description="Ingest, validate, and route Instacart CSV chunks",
    schedule="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["instacart", "ingestion"],
)
def ingestion_dag():

    @task
    def read_data() -> dict:
        for p in [RAW_DATA_PATH, GOOD_DATA_PATH, BAD_DATA_PATH]:
            os.makedirs(p, exist_ok=True)

        files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]
        if not files:
            raise AirflowSkipException("No files in raw_data — skipping DAG run.")

        chosen = random.choice(files)
        file_path = os.path.join(RAW_DATA_PATH, chosen)
        print(f"Selected file: {chosen}")
        return {"file_name": chosen, "file_path": file_path}

    @task
    def validate_data(file_info: dict) -> dict:
        file_path = file_info["file_path"]
        file_name = file_info["file_name"]

        # ── Read CSV ───────────────────────────────────────────
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            return {
                "criticality": "High",
                "bad_indices": [],
                "total_records": 0,
                "bad_count": 0,
                "missing_column": True,
                "error": str(exc),
            }

        total_records = len(df)

        # ── GX Core v1 setup (ephemeral context) ──────────────
        context = gx.get_context(mode="ephemeral")

        suite = context.suites.add(gx.ExpectationSuite(name="instacart_suite"))
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column="user_id"))
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column="order_dow"))
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column="order_hour_of_day"))
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column="days_since_prior"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_dow"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_hour_of_day"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="days_since_prior"))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column="order_dow", min_value=0, max_value=6)
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="order_hour_of_day", min_value=0, max_value=23
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="days_since_prior", min_value=0, max_value=30
            )
        )

        # ── Batch definition ────────────────────────────────
        ds = context.data_sources.add_pandas("pd_source")
        asset = ds.add_dataframe_asset(name=file_name)
        batch_def = asset.add_batch_definition_whole_dataframe("batch")

        val_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"val_{file_name.replace('.', '_')}",
                data=batch_def,
                suite=suite,
            )
        )

        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"cp_{file_name.replace('.', '_')}",
                validation_definitions=[val_def],
                result_format="COMPLETE",
            )
        )

        result = checkpoint.run(batch_parameters={"dataframe": df})

        # ── Build Data Docs ────────────────────────────────
        try:
            context.build_data_docs()
        except Exception as exc:
            print(f"Data Docs build warning: {exc}")

        # ── Extract bad indices ────────────────────────────
        missing_col = False
        bad_indices: set[int] = set()

        run_results = result.run_results
        for run_key in run_results:
            vr = run_results[run_key]
            for res in vr.results:
                if not res.success:
                    etype = res.expectation_config.type
                    if "column_to_exist" in etype:
                        missing_col = True
                    ul = getattr(res.result, "unexpected_index_list", None)
                    if ul:
                        bad_indices.update(ul)

        # Row-level safety net
        bad_mask = pd.Series([False] * total_records, index=df.index)
        for col in REQUIRED_COLS:
            if col not in df.columns:
                bad_mask[:] = True
                missing_col = True
            else:
                bad_mask |= df[col].isna()

        range_checks = {
            "order_dow": (0, 6),
            "order_hour_of_day": (0, 23),
            "days_since_prior": (0.0, 30.0),
        }
        for col, (lo, hi) in range_checks.items():
            if col in df.columns:
                numeric = pd.to_numeric(df[col], errors="coerce")
                bad_mask |= numeric.isna() | (numeric < lo) | (numeric > hi)

        bad_mask |= df.duplicated(keep="first")
        bad_indices.update(df[bad_mask].index.tolist())

        bad_count = len(bad_indices)
        invalid_pct = (bad_count / total_records * 100) if total_records > 0 else 0

        if missing_col or invalid_pct > 50:
            criticality = "High"
        elif invalid_pct >= 10:
            criticality = "Medium"
        elif invalid_pct > 0:
            criticality = "Low"
        else:
            criticality = "None"

        print(
            f"Validation complete: {total_records} rows, {bad_count} invalid "
            f"({invalid_pct:.1f}%) — criticality={criticality}"
        )

        return {
            "criticality": criticality,
            "bad_indices": list(bad_indices),
            "total_records": total_records,
            "bad_count": bad_count,
            "missing_column": missing_col,
        }

    @task
    def save_statistics(file_info: dict, validation: dict) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            INSERT INTO ingestion_stats
                (run_id, file_name, rows_total, rows_valid, rows_invalid, error_summary)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                str(datetime.utcnow().timestamp()),
                file_info["file_name"],
                validation["total_records"],
                validation["total_records"] - validation["bad_count"],
                validation["bad_count"],
                json.dumps({
                    "criticality": validation["criticality"],
                    "missing_column": validation["missing_column"],
                }),
            ),
        )
        print(f"Stats saved for {file_info['file_name']}")

    @task
    def send_alerts(file_info: dict, validation: dict) -> None:
        criticality = validation["criticality"]
        if criticality not in ("Medium", "High"):
            print("No alert needed (criticality=None or Low).")
            return

        bad = validation["bad_count"]
        total = validation["total_records"]
        pct = (bad / total * 100) if total else 0
        print("─" * 60)
        print("  TEAMS ALERT — Data Quality Alerts channel")
        print(f"  Criticality : {criticality}")
        print(f"  File        : {file_info['file_name']}")
        print(f"  Issues      : {bad}/{total} rows ({pct:.1f}%) failed validation")
        print(f"  Missing col : {validation['missing_column']}")
        print("─" * 60)

    @task
    def split_and_save_data(file_info: dict, validation: dict) -> None:
        file_path = file_info["file_path"]
        file_name = file_info["file_name"]
        bad_indices = set(validation["bad_indices"])
        total = validation["total_records"]

        if total == 0:
            shutil.move(file_path, os.path.join(BAD_DATA_PATH, file_name))
            return

        df = pd.read_csv(file_path)

        if not bad_indices:
            shutil.move(file_path, os.path.join(GOOD_DATA_PATH, file_name))
            print(f"All rows valid → moved to good_data/{file_name}")
        elif len(bad_indices) >= total:
            shutil.move(file_path, os.path.join(BAD_DATA_PATH, file_name))
            print(f"All rows invalid → moved to bad_data/{file_name}")
        else:
            good_df = df.drop(index=list(bad_indices))
            bad_df = df.iloc[list(bad_indices)]
            good_df.to_csv(os.path.join(GOOD_DATA_PATH, f"good_{file_name}"), index=False)
            bad_df.to_csv(os.path.join(BAD_DATA_PATH, f"bad_{file_name}"), index=False)
            os.remove(file_path)
            print(f"Split: {len(good_df)} good, {len(bad_df)} bad rows")

    # ── DAG wiring ─────────────────────────────────────────────
    file_info = read_data()
    validation = validate_data(file_info)

    # save_statistics, send_alerts, split_and_save run in parallel
    save_statistics(file_info, validation)
    send_alerts(file_info, validation)
    split_and_save_data(file_info, validation)


ingestion_dag()
