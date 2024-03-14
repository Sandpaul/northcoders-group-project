"""This module contains the test suite for dim_location()"""

import os
import json

import pandas as pd
import pytest

from src.transform.dim_location import dim_location


@pytest.fixture
def address_df():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_address_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["address"])
        return df


@pytest.fixture
def control_df():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_address_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["address"])
        return df


@pytest.mark.describe("dim_location()")
@pytest.mark.it("should return a dataframe")
def test_dim_location_returns_a_dataframe(address_df):
    """Should return a dataframe"""
    result = dim_location(address_df)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("dim_location()")
@pytest.mark.it("check if the dataframe has the required column names")
def test_function_returns_the_correct_columns(address_df):
    """Returned dataframe has the required column names returned"""
    result = dim_location(address_df)
    expected = [
        "location_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
    ]
    assert list(result.columns) == expected


@pytest.mark.describe("dim_location()")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(address_df):
    """Returned dataframe has the required column names returned"""
    result = dim_location(address_df)
    assert "created_at" not in result.columns
    assert "last_updated" not in result.columns


@pytest.mark.describe("dim_location()")
@pytest.mark.it("should not mutate passed data frame")
def test_does_not_mutate_arg(address_df, control_df):
    """dim_location() should not mutate passed data frame."""
    result = dim_location(address_df)
    assert address_df.equals(control_df) is True
    assert result.equals(control_df) is False
