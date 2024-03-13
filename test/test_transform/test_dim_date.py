"""This module contains the test suite for create_dim_date()."""

import pytest

from src.transform.create_dim_date import create_dim_date


@pytest.mark.describe("create_dim_date()")
@pytest.mark.it("should return a dataframe")
def test_should_return_data_frame():
    """create_dim_date() should return a dataframe"""
    result = create_dim_date("2020-01-01", "2024-12-31")
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("create_dim_date()")
@pytest.mark.it("should return dataframe with correct columns")
def test_should_return_correct_columns():
    """should return a dataframe with the correct number of columns"""
    result = create_dim_date("2020-01-01", "2024-12-31")
    result_columns = result.columns.to_list()
    expected_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
        "last_updated_date",
        "last_updated_time",
    ]  # noqa
    assert result_columns == expected_columns
