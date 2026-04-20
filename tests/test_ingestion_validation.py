import pandas as pd
import pytest

REQUIRED_COLS = ["user_id", "order_dow", "order_hour_of_day", "days_since_prior"]

RANGE_CHECKS = {
    "order_dow": (0, 6),
    "order_hour_of_day": (0, 23),
    "days_since_prior": (0.0, 30.0),
}


def detect_bad_rows(df: pd.DataFrame) -> pd.Series:
    bad = pd.Series([False] * len(df), index=df.index)
    for col in REQUIRED_COLS:
        if col not in df.columns:
            bad[:] = True
            return bad
        bad |= df[col].isna()
    for col, (lo, hi) in RANGE_CHECKS.items():
        if col in df.columns:
            num = pd.to_numeric(df[col], errors="coerce")
            bad |= num.isna() | (num < lo) | (num > hi)
    bad |= df.duplicated(keep="first")
    return bad


@pytest.fixture
def clean_row():
    return {"user_id": 1, "order_dow": 2, "order_hour_of_day": 14,
            "days_since_prior": 7.0, "reordered": 1}


def test_clean_row_passes(clean_row):
    assert not detect_bad_rows(pd.DataFrame([clean_row])).any()


def test_null_flagged(clean_row):
    row = clean_row.copy(); row["order_dow"] = None
    assert detect_bad_rows(pd.DataFrame([row])).all()


def test_out_of_range_flagged(clean_row):
    row = clean_row.copy(); row["order_dow"] = 9
    assert detect_bad_rows(pd.DataFrame([row])).all()


def test_negative_days_flagged(clean_row):
    row = clean_row.copy(); row["days_since_prior"] = -1.0
    assert detect_bad_rows(pd.DataFrame([row])).all()


def test_string_in_numeric_flagged(clean_row):
    row = clean_row.copy(); row["order_hour_of_day"] = "abc"
    assert detect_bad_rows(pd.DataFrame([row])).all()


def test_duplicate_flagged(clean_row):
    df = pd.DataFrame([clean_row, clean_row])
    mask = detect_bad_rows(df)
    assert not mask.iloc[0] and mask.iloc[1]


def test_missing_column_flags_all(clean_row):
    df = pd.DataFrame([clean_row]).drop(columns=["order_dow"])
    assert detect_bad_rows(df).all()


def test_criticality_logic():
    def criticality(missing_col, bad_count, total):
        pct = (bad_count / total * 100) if total else 0
        if missing_col or pct > 50:
            return "High"
        elif pct >= 10:
            return "Medium"
        elif pct > 0:
            return "Low"
        return "None"

    assert criticality(True, 0, 10) == "High"
    assert criticality(False, 6, 10) == "High"
    assert criticality(False, 3, 10) == "Medium"
    assert criticality(False, 1, 10) == "Medium"  # 10% exactly → Medium
    assert criticality(False, 0, 10) == "None"
