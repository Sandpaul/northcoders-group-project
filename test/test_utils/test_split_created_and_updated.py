"""This module contains the definition for `split_created_and_updated()`."""
import datetime
import json

import pandas as pd
import pytest

from src.utils.split_created_and_updated import split_created_and_updated


@pytest.fixture
def test_data_frame():
    """Sets up a test data frame."""
    with open(
        "test/test_transform/test_data/test_sales_order_data2.json"
    ) as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["sales_order"])
        return df


@pytest.fixture
def control_data_frame():
    """Sets up a control data frame."""
    with open(
        "test/test_transform/test_data/test_sales_order_data2.json"
    ) as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["sales_order"])
        return df


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("splits created at into created date and time")
def test_splits_created_at(test_data_frame):
    """split_created_and_updated() should split created_at column into created_date and created_time columns."""
    result = split_created_and_updated(test_data_frame)
    assert "created_date" in result.columns


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("returns correct date and time data in created_date and created_time columns")
def test_splits_created_at_correctly(test_data_frame):
    """split_created_and_updated() populate created_date and created_time columns with correct data."""
    result = split_created_and_updated(test_data_frame)
    expected_dates = [datetime.date(2024, 2, 20), datetime.date(2024, 2, 19)]
    expected_times = [datetime.time(15, 7, 9, 880000),datetime.time(15, 7, 9, 880000),]
    dates = result["created_date"].tolist()
    times = result["created_time"].tolist()
    assert dates == expected_dates
    assert times == expected_times