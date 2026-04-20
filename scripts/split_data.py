"""
Split a CSV dataset into fixed-size chunk files of exactly 10 rows each.

Usage:
    python scripts/split_data.py <input.csv> <raw_data_folder> <num_files>

Each output file is named:  instacart_sample_part1.csv, part2.csv, …
The script generates exactly <num_files> files, each with exactly 10 rows.
Rows beyond num_files * 10 are ignored.
"""
import math
import os
import sys

import pandas as pd

ROWS_PER_FILE = 10


def split_data(input_path: str, output_dir: str, num_files: int) -> None:
    df = pd.read_csv(input_path)
    total_available = len(df)
    total_needed = num_files * ROWS_PER_FILE

    if total_available < total_needed:
        # Repeat the dataset until we have enough rows
        repeats = math.ceil(total_needed / total_available)
        df = pd.concat([df] * repeats, ignore_index=True)

    os.makedirs(output_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(input_path))[0]

    print(f"Splitting {input_path} → {num_files} files of {ROWS_PER_FILE} rows each ...")

    for i in range(num_files):
        start = i * ROWS_PER_FILE
        chunk = df.iloc[start: start + ROWS_PER_FILE]
        out_name = os.path.join(output_dir, f"{base}_part{i + 1}.csv")
        chunk.to_csv(out_name, index=False)

    print(f"Done. {num_files} files written to {output_dir}/")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python split_data.py <input.csv> <raw_data_folder> <num_files>")
        sys.exit(1)

    split_data(
        input_path=sys.argv[1],
        output_dir=sys.argv[2],
        num_files=int(sys.argv[3]),
    )
