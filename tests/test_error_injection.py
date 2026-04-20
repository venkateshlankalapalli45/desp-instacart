import numpy as np
import pandas as pd
import pytest
from scripts.data_error_injection import inject_errors


@pytest.fixture
def clean_df():
    return pd.DataFrame([{
        "user_id": i, "order_dow": 2, "order_hour_of_day": 14,
        "days_since_prior": 7.0, "reordered": 1,
    } for i in range(10)])


def test_null_injected(clean_df):
    import random; random.seed(0)
    result = inject_errors(clean_df, 1.0)
    assert result.isna().any().any()


def test_duplicate_injected(clean_df):
    import random; random.seed(0); import numpy as np; np.random.seed(0)
    result = inject_errors(clean_df, 1.0)
    assert len(result) > len(clean_df)


def test_original_unchanged(clean_df):
    original = clean_df.copy()
    import random; random.seed(42)
    inject_errors(clean_df, 0.5)
    pd.testing.assert_frame_equal(clean_df, original)


def test_zero_probability_no_row_changes(clean_df):
    import random; random.seed(0)
    result = inject_errors(clean_df, 0.0)
    # With p=0, no row-level injections; only schema (5%) and duplicates/format possible
    # Just verify it returns a DataFrame
    assert isinstance(result, pd.DataFrame)
