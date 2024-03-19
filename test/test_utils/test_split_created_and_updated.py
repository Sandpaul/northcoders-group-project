"""This module contains the definition for `split_created_and_updated()`."""

import datetime
import json

import pandas as pd
import pytest

from src.utils.split_created_and_updated import split_created_and_updated


@pytest.fixture
def test_data_frame():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_sales_order_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["sales_order"])
        return df


@pytest.fixture
def control_data_frame():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_sales_order_data.json") as f:
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
    assert "created_time" in result.columns


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it(
    "returns correct date and time data in created_date and created_time columns"
)
def test_splits_created_at_correctly(test_data_frame):
    """split_created_and_updated() populate created_date and created_time columns with correct data."""
    result = split_created_and_updated(test_data_frame)
    expected_dates = [
        datetime.date(2024, 2, 20),
        datetime.date(2024, 2, 19),
    ]
    expected_times = [
        datetime.time(15, 7, 9, 880000),
        datetime.time(15, 7, 9, 880000),
    ]
    dates = result["created_date"].tolist()
    times = result["created_time"].tolist()
    assert dates == expected_dates
    assert times == expected_times


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("deletes created_at column")
def test_deletes_created_at(test_data_frame):
    """split_created_and_updated() should delete the created_at column"""
    result = split_created_and_updated(test_data_frame)
    assert "created_at" not in result.columns


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("splits last_updated into last_updated date and time")
def test_splits_last_updated(test_data_frame):
    """split_created_and_updated() should split last_updated column into last_updated_date and last_updated_times columns."""
    result = split_created_and_updated(test_data_frame)
    assert "last_updated_date" in result.columns
    assert "last_updated_time" in result.columns


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it(
    "returns correct date and time data in last_updated_date and last_updated_times columns"
)
def test_splits_last_updated_correctly(test_data_frame):
    """split_created_and_updated() populate last_updated_date and last_updated_times columns with correct data."""
    result = split_created_and_updated(test_data_frame)
    expected_dates = [
        datetime.date(2024, 2, 20),
        datetime.date(2024, 2, 19),
    ]
    expected_times = [
        datetime.time(15, 7, 9, 880000),
        datetime.time(15, 7, 9, 880005),
    ]
    dates = result["last_updated_date"].tolist()
    times = result["last_updated_time"].tolist()
    assert dates == expected_dates
    assert times == expected_times


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("deletes last_updated column")
def test_deletes_last_updated(test_data_frame):
    """split_created_and_updated() should delete the last_updated column"""
    result = split_created_and_updated(test_data_frame)
    assert "last_updated" not in result.columns


@pytest.mark.describe("split_created_and_updated()")
@pytest.mark.it("should not mutate passed data frame")
def test_does_not_mutate_arg(test_data_frame, control_data_frame):
    """split_created_and_updated() should not mutate the passed dataframe."""
    result = split_created_and_updated(test_data_frame)
    assert test_data_frame.equals(control_data_frame)
    assert result.equals(control_data_frame) is False
