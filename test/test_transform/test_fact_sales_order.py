"""This module contains the test suite for `fact_sales_order()`."""

import datetime
import json

import pandas as pd
import pytest

from src.transform.fact_sales_order import fact_sales_order


@pytest.fixture
def sales_order_df():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_sales_order_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["sales_order"])
        return df


@pytest.fixture
def control_df():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_sales_order_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["sales_order"])
        return df


@pytest.mark.describe("fact_sales_order()")
@pytest.mark.it("should return a dataframe")
def test_fact_sales_order_returns_df(sales_order_df):
    """should return a dataframe"""
    result = fact_sales_order(sales_order_df)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("fact_sales_order()")
@pytest.mark.it(
    "dataframe should contain correct created_date and created_time"
)  # noqa
def test_fact_sales_order_returns_correct_created(sales_order_df):
    """should return the correct created_date and created_time"""
    result = fact_sales_order(sales_order_df)
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


@pytest.mark.describe("fact_sales_order()")
@pytest.mark.it(
    "dataframe should contain correct last_updated_date and last_updated"
)  # noqa
def test_fact_sales_order_returns_correct_last_updated(sales_order_df):
    """should return the correct last_updated_date and last_updated"""
    result = fact_sales_order(sales_order_df)
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


@pytest.mark.describe("fact_sales_order()")
@pytest.mark.it("dataframe should contain correct column names")  # noqa
def test_dataframe_returns_correct_column_names(sales_order_df):
    result = fact_sales_order(sales_order_df)
    expected = [
        "agreed_delivery_date",
        "agreed_delivery_location_id",
        "agreed_payment_date",
        "counterparty_id",
        "currency_id",
        "design_id",
        "sales_order_id",
        "sales_staff_id",
        "unit_price",
        "units_sold",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
    ]
    assert list(result.columns) == expected


@pytest.mark.describe("fact_sales_order()")
@pytest.mark.it("should not mutate passed data frame")
def test_does_not_mutate_arg(sales_order_df, control_df):
    """fact_sales_order() should not mutate the passed data frame."""
    result = fact_sales_order(sales_order_df)
    assert sales_order_df.equals(control_df) is True
    assert result.equals(sales_order_df) is False
