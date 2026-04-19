"""Tests for scripts/split_dataset.py"""
import os
import tempfile

import pandas as pd
import pytest

from scripts.split_dataset import split_dataset


@pytest.fixture
def sample_csv(tmp_path):
    """Write a 25-row CSV to a temp file and return the path."""
    df = pd.DataFrame({
        "order_dow": range(25),
        "order_hour_of_day": [14] * 25,
        "days_since_prior_order": [7.0] * 25,
        "add_to_cart_order": [3] * 25,
        "department_id": [4] * 25,
        "aisle_id": [24] * 25,
        "reordered": [1] * 25,
    })
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_correct_number_of_chunks(sample_csv, tmp_path):
    out_dir = str(tmp_path / "chunks")
    split_dataset(sample_csv, out_dir, chunk_size=10)
    files = sorted(os.listdir(out_dir))
    # 25 rows / 10 = 3 chunks (10, 10, 5)
    assert len(files) == 3


def test_chunk_filenames_are_padded(sample_csv, tmp_path):
    out_dir = str(tmp_path / "chunks")
    split_dataset(sample_csv, out_dir, chunk_size=10)
    files = sorted(os.listdir(out_dir))
    assert files[0] == "chunk_0001.csv"
    assert files[1] == "chunk_0002.csv"
    assert files[2] == "chunk_0003.csv"


def test_total_rows_preserved(sample_csv, tmp_path):
    out_dir = str(tmp_path / "chunks")
    split_dataset(sample_csv, out_dir, chunk_size=10)
    files = sorted(os.listdir(out_dir))
    total = sum(
        len(pd.read_csv(os.path.join(out_dir, f))) for f in files
    )
    assert total == 25


def test_last_chunk_has_remainder_rows(sample_csv, tmp_path):
    out_dir = str(tmp_path / "chunks")
    split_dataset(sample_csv, out_dir, chunk_size=10)
    last = pd.read_csv(os.path.join(out_dir, "chunk_0003.csv"))
    assert len(last) == 5


def test_chunk_size_10_on_exact_multiple(tmp_path):
    """20 rows / 10 = exactly 2 chunks."""
    df = pd.DataFrame({"order_dow": range(20), "reordered": [0] * 20})
    src = str(tmp_path / "exact.csv")
    df.to_csv(src, index=False)
    out_dir = str(tmp_path / "out")
    split_dataset(src, out_dir, chunk_size=10)
    assert len(os.listdir(out_dir)) == 2


def test_single_row_dataset(tmp_path):
    df = pd.DataFrame({"order_dow": [3], "reordered": [1]})
    src = str(tmp_path / "one.csv")
    df.to_csv(src, index=False)
    out_dir = str(tmp_path / "out")
    split_dataset(src, out_dir, chunk_size=10)
    files = os.listdir(out_dir)
    assert len(files) == 1
    assert len(pd.read_csv(os.path.join(out_dir, files[0]))) == 1
