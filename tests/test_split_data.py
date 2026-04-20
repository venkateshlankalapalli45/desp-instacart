import os
import pandas as pd
import pytest
from scripts.split_data import split_data


@pytest.fixture
def sample_csv(tmp_path):
    df = pd.DataFrame({
        "user_id": range(100),
        "order_dow": [2] * 100,
        "order_hour_of_day": [14] * 100,
        "days_since_prior": [7.0] * 100,
        "reordered": [1] * 100,
    })
    p = tmp_path / "sample.csv"
    df.to_csv(p, index=False)
    return str(p)


def test_generates_correct_number_of_files(sample_csv, tmp_path):
    out = str(tmp_path / "out")
    split_data(sample_csv, out, num_files=10)
    assert len(os.listdir(out)) == 10


def test_each_file_has_exactly_10_rows(sample_csv, tmp_path):
    out = str(tmp_path / "out")
    split_data(sample_csv, out, num_files=10)
    for f in os.listdir(out):
        df = pd.read_csv(os.path.join(out, f))
        assert len(df) == 10, f"{f} has {len(df)} rows, expected 10"


def test_filenames_are_numbered(sample_csv, tmp_path):
    out = str(tmp_path / "out")
    split_data(sample_csv, out, num_files=3)
    files = sorted(os.listdir(out))
    assert "part1" in files[0]
    assert "part2" in files[1]
    assert "part3" in files[2]


def test_small_dataset_gets_repeated(tmp_path):
    df = pd.DataFrame({
        "user_id": range(5),
        "order_dow": [2] * 5,
        "order_hour_of_day": [14] * 5,
        "days_since_prior": [7.0] * 5,
        "reordered": [1] * 5,
    })
    src = str(tmp_path / "small.csv")
    df.to_csv(src, index=False)
    out = str(tmp_path / "out")
    split_data(src, out, num_files=3)
    assert len(os.listdir(out)) == 3
    for f in os.listdir(out):
        assert len(pd.read_csv(os.path.join(out, f))) == 10
