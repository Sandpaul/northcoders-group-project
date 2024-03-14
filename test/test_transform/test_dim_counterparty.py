"""This module contains the test suite for dim_counterparty()."""

import os
import json

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from src.transform.dim_counterparty import dim_counterparty
from src.utils.parquet_file_reader import parquet_file_reader


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Create mock s3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def address_df_1():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_address_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["address"])
        return df


@pytest.fixture
def address_df_2():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_address_data2.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["address"])
        return df


@pytest.fixture
def counterparty_df():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_counterparty_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["counterparty"])
        return df


@pytest.fixture
def bucket(s3, address_df_1, address_df_2):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data_to_write_1 = pd.DataFrame.to_parquet(address_df_1)
    s3.put_object(
        Body=data_to_write_1,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="address/2022-11-03/14:20:51.563.json",
    )
    data_to_write_2 = pd.DataFrame.to_parquet(address_df_2)
    s3.put_object(
        Body=data_to_write_2,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="address/2022-11-04/14:20:51.563.json",
    )


@pytest.mark.describe("dim_counterparty")
@pytest.mark.it("should return a dataframe")
def test_dim_counterparty_returns_data_frame(
    bucket,
    counterparty_df,
):
    """dim_counterparty() should return a dataframe"""
    result = dim_counterparty(counterparty_df)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("dim_counterparty")
@pytest.mark.it("should return dataframe with correct column names")
def test_function_returns_the_correct_columns(s3, bucket, counterparty_df):
    """Returned dataframe has the required column names returned"""
    result = dim_counterparty(counterparty_df)
    expected = [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]
    assert list(result.columns) == expected


@pytest.mark.describe("dim_counterparty")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(
    s3,
    bucket,
    counterparty_df,
):
    """Returned dataframe has the required column names returned"""
    result = dim_counterparty(counterparty_df)
    assert "created_at" not in result.columns
    assert "last_updated" not in result.columns


@pytest.mark.describe("dim_counterparty")
@pytest.mark.it("should join the counterparty and address data correctly")
def test_function_joins_correctly(s3, bucket, counterparty_df):
    """dim_counterparty should complete the correct joins succesfully"""
    result = dim_counterparty(counterparty_df)
    assert result.get("counterparty_legal_address_line_1").get(0) == "6826 Herzog Via"
    assert (
        result.get("counterparty_legal_address_line_1").get(1) == "6102 Rogahn Skyway"
    )
    assert result.get("counterparty_legal_address_line_1").get(2) == "27 Paul Place"
