"""This module contains the test suite for `drop_created_and_updated()`."""

import json

import pandas as pd
import pytest

from src.transform.drop_created_and_updated import drop_created_and_updated

@pytest.fixture
def test_data_frame():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_drop_created_and_updated_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["department"])
        return df


@pytest.mark.describe("drop_created_and_updated()")
@pytest.mark.it("should return a new data frame with create_at and last_updated removed")
def test_drop_created_and_updated(test_data_frame):
    """drop_created_and_updated() should remove created_at and last_updated columns from passed data frame.
    """
    result = drop_created_and_updated(test_data_frame)
    assert "created_at" not in result.columns
    assert "last_updated" not in result.columns

