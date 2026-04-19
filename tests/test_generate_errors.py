"""Tests for scripts/generate_errors.py"""
import numpy as np
import pandas as pd
import pytest

from scripts.generate_errors import inject_errors

FEATURES = [
    "order_dow", "order_hour_of_day", "days_since_prior_order",
    "add_to_cart_order", "department_id", "aisle_id",
]


@pytest.fixture
def clean_df():
    return pd.DataFrame([{
        "order_dow": 2,
        "order_hour_of_day": 14,
        "days_since_prior_order": 7.0,
        "add_to_cart_order": 3,
        "department_id": 4,
        "aisle_id": 24,
        "reordered": 1,
    }] * 5)


def test_null_injects_nan(clean_df):
    result = inject_errors(clean_df, "null")
    # At least one NaN should now exist in feature columns
    assert result[FEATURES].isna().any().any()


def test_range_value_out_of_bounds(clean_df):
    result = inject_errors(clean_df, "range")
    # At least one feature value should exceed its valid upper bound
    out_of_bounds = (
        (result["order_dow"] > 6) |
        (result["order_hour_of_day"] > 23) |
        (result["days_since_prior_order"] > 30) |
        (result["add_to_cart_order"] > 100) |
        (result["department_id"] > 21) |
        (result["aisle_id"] > 134)
    )
    assert out_of_bounds.any()


def test_category_injects_string_in_numeric_col(clean_df):
    result = inject_errors(clean_df, "category")
    has_bad = (
        result["department_id"].astype(str).str.match(r"[A-Za-z/]") |
        result["aisle_id"].astype(str).str.match(r"[A-Za-z/]")
    )
    assert has_bad.any()


def test_type_injects_non_numeric_string(clean_df):
    result = inject_errors(clean_df, "type")
    # At least one numeric column should now contain a non-numeric string
    cols_to_check = ["order_dow", "order_hour_of_day", "add_to_cart_order"]
    has_string = any(
        result[c].astype(str).str.contains(r"[A-Za-z_]", regex=True).any()
        for c in cols_to_check
    )
    assert has_string


def test_duplicate_adds_row(clean_df):
    original_len = len(clean_df)
    result = inject_errors(clean_df, "duplicate")
    assert len(result) == original_len + 1


def test_outlier_extreme_value(clean_df):
    result = inject_errors(clean_df, "outlier")
    extreme = pd.to_numeric(result["days_since_prior_order"], errors="coerce")
    assert (extreme.abs() > 30).any()


def test_schema_adds_or_removes_column(clean_df):
    result = inject_errors(clean_df, "schema")
    original_cols = set(clean_df.columns)
    result_cols = set(result.columns)
    # Either a new column was added OR a column was removed
    assert result_cols != original_cols


def test_inject_preserves_original_df(clean_df):
    original_copy = clean_df.copy()
    inject_errors(clean_df, "null")
    pd.testing.assert_frame_equal(clean_df, original_copy)


def test_unknown_error_type_raises(clean_df):
    with pytest.raises(ValueError, match="Unknown error type"):
        inject_errors(clean_df, "nonexistent_error")
