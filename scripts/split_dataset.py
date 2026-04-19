"""
split_dataset.py — split a CSV dataset into fixed-size chunk files.

Usage:
    python scripts/split_dataset.py [--input data/train.csv] \
                                    [--output data/raw/] \
                                    [--chunk-size 10]

Each output file is named:  chunk_0001.csv, chunk_0002.csv, …
Default chunk size is 10 rows (as specified by the project brief).
"""

import argparse
import math
import os

import pandas as pd


def split_dataset(input_path: str, output_dir: str, chunk_size: int = 10) -> None:
    df = pd.read_csv(input_path)
    total_rows = len(df)
    n_chunks = math.ceil(total_rows / chunk_size)

    os.makedirs(output_dir, exist_ok=True)

    print(f"Splitting {input_path} ({total_rows} rows) into {n_chunks} chunks of {chunk_size} rows …")

    for i in range(n_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        filename = f"chunk_{i + 1:04d}.csv"
        filepath = os.path.join(output_dir, filename)
        chunk.to_csv(filepath, index=False)

    print(f"Done. {n_chunks} files written to {output_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a CSV into fixed-size chunks")
    parser.add_argument("--input", default="data/train.csv", help="Path to source CSV")
    parser.add_argument("--output", default="data/raw/", help="Output directory for chunks")
    parser.add_argument(
        "--chunk-size", type=int, default=10,
        help="Number of rows per chunk (default: 10)"
    )
    args = parser.parse_args()

    split_dataset(args.input, args.output, args.chunk_size)
