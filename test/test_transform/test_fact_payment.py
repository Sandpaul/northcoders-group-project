"""This module contains the test suite for `fact_payment()`."""

import datetime
import json

import pandas as pd
import pytest

from src.transform.fact_payment import fact_payment


@pytest.fixture
def payment_df():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_payment_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["payment"])
        return df


@pytest.fixture
def control_df():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_payment_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["payment"])
        return df


@pytest.mark.describe("fact_payment()")
@pytest.mark.it("should return a dataframe")
def test_fact_payment_returns_df(payment_df):
    """fact_payment() should return a dataframe"""
    result = fact_payment(payment_df)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("fact_payment()")
@pytest.mark.it("dataframe should return correct columns")
def test_dataframe_returns_correct_columns(payment_df):
    """fact_payment() should return data frame with correcy columns."""
    result = fact_payment(payment_df)
    print(list(result.columns))
    expected = [
        "counterparty_id",
        "currency_id",
        "paid",
        "payment_amount",
        "payment_date",
        "payment_id",
        "payment_type_id",
        "transaction_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
    ]
    assert list(result.columns) == expected


@pytest.mark.describe("fact_payment()")
@pytest.mark.it(
    "dataframe should contain correct created_date and created_time"
)  # noqa
def test_fact_payment_returns_correct_created(payment_df):
    """should return the correct created_date and created_time"""
    result = fact_payment(payment_df)
    expected_dates = [
        datetime.date(2024, 2, 22),
        datetime.date(2024, 2, 22),
    ]
    expected_times = [
        datetime.time(14, 48, 10, 434000),
        datetime.time(15, 59, 10, 43000),
    ]
    dates = result["created_date"].tolist()
    times = result["created_time"].tolist()
    assert dates == expected_dates
    assert times == expected_times


@pytest.mark.describe("fact_payment()")
@pytest.mark.it(
    "dataframe should contain correct last_updated_date and last_updated"
)  # noqa
def test_fact_payment_returns_correct_last_updated(payment_df):
    """should return the correct last_updated_date and last_updated"""
    result = fact_payment(payment_df)
    expected_dates = [
        datetime.date(2024, 2, 22),
        datetime.date(2024, 2, 22),
    ]
    expected_times = [
        datetime.time(14, 48, 10, 434000),
        datetime.time(15, 59, 10, 43000),
    ]
    dates = result["last_updated_date"].tolist()
    times = result["last_updated_time"].tolist()
    assert dates == expected_dates
    assert times == expected_times


@pytest.mark.describe("fact_payment()")
@pytest.mark.it("should not mutate passed data frame")
def test_does_not_mutate_arg(payment_df, control_df):
    """fact_payment() should not mutate the passed data frame."""
    result = fact_payment(payment_df)
    assert payment_df.equals(control_df) is True
    assert result.equals(payment_df) is False
