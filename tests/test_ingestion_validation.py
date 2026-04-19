"""
Tests for row-level validation logic extracted from ingestion_dag.py.

We test the bad-row detection logic (null, range, type, duplicate checks)
without requiring a running Airflow or database.
"""
import numpy as np
import pandas as pd
import pytest

FEATURES = [
    "order_dow", "order_hour_of_day", "days_since_prior_order",
    "add_to_cart_order", "department_id", "aisle_id",
]

RANGE_CHECKS = {
    "order_dow": (0, 6),
    "order_hour_of_day": (0, 23),
    "days_since_prior_order": (0.0, 30.0),
    "add_to_cart_order": (1, 100),
    "department_id": (1, 21),
    "aisle_id": (1, 134),
}


def detect_bad_rows(df: pd.DataFrame) -> pd.Series:
    """Replication of the bad-mask logic from ingestion_dag.validate_files."""
    bad_mask = pd.Series([False] * len(df), index=df.index)

    for col in FEATURES:
        if col not in df.columns:
            bad_mask[:] = True
            continue
        bad_mask |= df[col].isna()

    for col, (lo, hi) in RANGE_CHECKS.items():
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            bad_mask |= numeric.isna() | (numeric < lo) | (numeric > hi)

    bad_mask |= df.duplicated(keep="first")
    return bad_mask


@pytest.fixture
def clean_row():
    return {
        "order_dow": 2, "order_hour_of_day": 14,
        "days_since_prior_order": 7.0, "add_to_cart_order": 3,
        "department_id": 4, "aisle_id": 24,
    }


def test_clean_row_passes(clean_row):
    df = pd.DataFrame([clean_row])
    assert not detect_bad_rows(df).any()


def test_null_value_flagged(clean_row):
    row = clean_row.copy()
    row["order_dow"] = None
    df = pd.DataFrame([row])
    assert detect_bad_rows(df).all()


def test_out_of_range_flagged(clean_row):
    row = clean_row.copy()
    row["order_dow"] = 9  # > 6
    df = pd.DataFrame([row])
    assert detect_bad_rows(df).all()


def test_negative_range_flagged(clean_row):
    row = clean_row.copy()
    row["days_since_prior_order"] = -1.0
    df = pd.DataFrame([row])
    assert detect_bad_rows(df).all()


def test_non_numeric_string_flagged(clean_row):
    row = clean_row.copy()
    row["department_id"] = "not_a_number"
    df = pd.DataFrame([row])
    assert detect_bad_rows(df).all()


def test_duplicate_flagged(clean_row):
    df = pd.DataFrame([clean_row, clean_row])
    mask = detect_bad_rows(df)
    # First occurrence is kept, second is flagged
    assert not mask.iloc[0]
    assert mask.iloc[1]


def test_mixed_batch_separates_correctly(clean_row):
    clean_row2 = clean_row.copy()
    clean_row2["order_dow"] = 5  # distinct from clean_row so not a duplicate
    bad_row = clean_row.copy()
    bad_row["order_dow"] = 3
    bad_row["aisle_id"] = 999  # > 134 — invalid
    df = pd.DataFrame([clean_row, clean_row2, bad_row])
    mask = detect_bad_rows(df)
    good = df[~mask]
    bad = df[mask]
    assert len(good) == 2
    assert len(bad) == 1


def test_missing_column_flags_all_rows(clean_row):
    df = pd.DataFrame([clean_row])
    df = df.drop(columns=["aisle_id"])
    mask = detect_bad_rows(df)
    assert mask.all()
