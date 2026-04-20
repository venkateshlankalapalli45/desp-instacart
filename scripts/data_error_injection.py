"""
Inject realistic data quality errors into Instacart CSV files.

Usage:
    python scripts/data_error_injection.py <input.csv> <output.csv> <probability>

Error types (7 required for Defense 1):
  1. Schema     — missing required column (file-level, 5% chance)
  2. Null       — NaN in a required field
  3. Range      — numeric value outside valid bounds
  4. Categorical — invalid enum value
  5. Type       — wrong data type (string in numeric field)
  6. Duplicate  — duplicate rows
  7. Format     — regex mismatch (invalid user_id format)
"""
import os
import random
import sys

import numpy as np
import pandas as pd

REQUIRED_COLS = ["user_id", "order_dow", "order_hour_of_day", "days_since_prior", "reordered"]


def inject_errors(df: pd.DataFrame, probability: float) -> pd.DataFrame:
    df = df.copy()

    # ── 1. Schema: drop a required column (file-level, 5% chance) ──
    if random.random() < 0.05:
        col = random.choice(REQUIRED_COLS)
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f"  [Schema] Dropped column '{col}'")

    for i in range(len(df)):
        for col in list(df.columns):
            if random.random() >= probability:
                continue
            val = df.at[i, col]
            if pd.isna(val):
                continue

            choice = random.choice(["null", "range", "cat", "type"])

            # ── 2. Null ──────────────────────────────────────────
            if choice == "null":
                df.at[i, col] = np.nan

            # ── 3. Range (numeric only) ──────────────────────────
            elif choice == "range" and pd.api.types.is_numeric_dtype(df[col]):
                df.at[i, col] = val * 9999

            # ── 4. Categorical (string only) ─────────────────────
            elif choice == "cat" and not pd.api.types.is_numeric_dtype(df[col]):
                df.at[i, col] = "INVALID_CATEGORY"

            # ── 5. Type (put string in numeric) ──────────────────
            elif choice == "type" and pd.api.types.is_numeric_dtype(df[col]):
                df.at[i, col] = "WRONG_TYPE"

    # ── 6. Duplicates ─────────────────────────────────────────────
    if random.random() < probability:
        dupes = df.sample(frac=max(probability / 2, 0.1), replace=True)
        df = pd.concat([df, dupes], ignore_index=True)
        print(f"  [Duplicate] Added {len(dupes)} duplicate rows")

    # ── 7. Format mismatch (user_id → invalid format) ─────────────
    if "user_id" in df.columns and random.random() < probability:
        idx = df.sample(1).index[0]
        df.at[idx, "user_id"] = "invalid_user@id"
        print(f"  [Format] Injected invalid user_id at row {idx}")

    return df


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python data_error_injection.py <input.csv> <output.csv> <probability>")
        sys.exit(1)

    in_file, out_file, prob = sys.argv[1], sys.argv[2], float(sys.argv[3])
    df_in = pd.read_csv(in_file)
    df_out = inject_errors(df_in, prob)
    os.makedirs(os.path.dirname(os.path.abspath(out_file)), exist_ok=True)
    df_out.to_csv(out_file, index=False)
    print(f"Injection complete. Saved to {out_file}")
