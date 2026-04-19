"""
generate_errors.py — inject realistic data quality errors into Instacart CSV chunks.

Usage:
    python scripts/generate_errors.py [--input data/raw/] [--output data/raw/]

Error types injected (7 categories):
  1. null          — missing required field value
  2. range         — numeric value outside valid bounds
  3. category      — invalid categorical/enum value
  4. type          — wrong data type (e.g. string in numeric field)
  5. duplicate     — exact duplicate row
  6. outlier       — statistically extreme numeric value
  7. schema        — unexpected extra column or missing column
"""

import argparse
import glob
import os
import random

import numpy as np
import pandas as pd

FEATURES = [
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "add_to_cart_order",
    "department_id",
    "aisle_id",
]

# Valid bounds per column
VALID_RANGES = {
    "order_dow": (0, 6),
    "order_hour_of_day": (0, 23),
    "days_since_prior_order": (0.0, 30.0),
    "add_to_cart_order": (1, 100),
    "department_id": (1, 21),
    "aisle_id": (1, 134),
}


def _inject_null(df: pd.DataFrame) -> pd.DataFrame:
    """Set a random cell in a random row to NaN."""
    col = random.choice(FEATURES)
    idx = random.randint(0, len(df) - 1)
    df.loc[idx, col] = np.nan
    return df


def _inject_range(df: pd.DataFrame) -> pd.DataFrame:
    """Set a value outside its valid range (e.g. order_dow = 9)."""
    col = random.choice(list(VALID_RANGES.keys()))
    lo, hi = VALID_RANGES[col]
    idx = random.randint(0, len(df) - 1)
    # Pick a value clearly outside the range
    df.loc[idx, col] = hi + random.randint(10, 50)
    return df


def _inject_category(df: pd.DataFrame) -> pd.DataFrame:
    """Insert an invalid string value where a number is expected."""
    col = random.choice(["department_id", "aisle_id"])
    idx = random.randint(0, len(df) - 1)
    df.loc[idx, col] = random.choice(["UNKNOWN", "N/A", "other", "X99"])
    return df


def _inject_type(df: pd.DataFrame) -> pd.DataFrame:
    """Replace a numeric field with a non-castable string."""
    col = random.choice(["order_dow", "order_hour_of_day", "add_to_cart_order"])
    idx = random.randint(0, len(df) - 1)
    df.loc[idx, col] = "not_a_number"
    return df


def _inject_duplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Append a duplicate of a random row."""
    dup_row = df.iloc[[random.randint(0, len(df) - 1)]].copy()
    return pd.concat([df, dup_row], ignore_index=True)


def _inject_outlier(df: pd.DataFrame) -> pd.DataFrame:
    """Set days_since_prior_order to an extreme value (e.g. 999)."""
    idx = random.randint(0, len(df) - 1)
    df.loc[idx, "days_since_prior_order"] = random.choice([999, -99, 10000])
    return df


def _inject_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Add an unexpected column OR drop a required column."""
    action = random.choice(["add_column", "drop_column"])
    if action == "add_column":
        df["unexpected_field"] = "garbage"
    else:
        col_to_drop = random.choice(FEATURES)
        df = df.drop(columns=[col_to_drop])
    return df


ERROR_INJECTORS = {
    "null": _inject_null,
    "range": _inject_range,
    "category": _inject_category,
    "type": _inject_type,
    "duplicate": _inject_duplicate,
    "outlier": _inject_outlier,
    "schema": _inject_schema,
}


def inject_errors(df: pd.DataFrame, error_type: str) -> pd.DataFrame:
    """Return a copy of df with the specified error injected."""
    df = df.copy()
    injector = ERROR_INJECTORS.get(error_type)
    if injector is None:
        raise ValueError(f"Unknown error type: {error_type}. Choose from {list(ERROR_INJECTORS)}")
    return injector(df)


def process_directory(input_dir: str, output_dir: str, error_fraction: float = 0.3) -> None:
    """
    For each CSV in input_dir, randomly decide (with probability error_fraction)
    whether to inject an error, then write to output_dir.
    """
    os.makedirs(output_dir, exist_ok=True)
    csv_files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))

    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    error_types = list(ERROR_INJECTORS.keys())
    injected = 0

    for path in csv_files:
        fname = os.path.basename(path)
        df = pd.read_csv(path)

        if random.random() < error_fraction:
            err = random.choice(error_types)
            df = inject_errors(df, err)
            out_name = fname.replace(".csv", f"_err_{err}.csv")
            print(f"  [ERROR:{err}] {fname} → {out_name}")
            injected += 1
        else:
            out_name = fname

        df.to_csv(os.path.join(output_dir, out_name), index=False)

    print(f"\nProcessed {len(csv_files)} files, injected errors into {injected}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject data errors into CSV files")
    parser.add_argument("--input", default="data/raw/", help="Directory with clean CSV chunks")
    parser.add_argument("--output", default="data/raw/", help="Directory to write output CSVs")
    parser.add_argument(
        "--fraction", type=float, default=0.3,
        help="Fraction of files to inject errors into (default: 0.3)"
    )
    args = parser.parse_args()

    random.seed(42)
    np.random.seed(42)
    process_directory(args.input, args.output, args.fraction)
