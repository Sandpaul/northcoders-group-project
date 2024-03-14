"""This module contains the test suite for `get_archived_table_data()`."""

import json
import os

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from src.utils.get_archived_table_data import get_archived_table_data


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
def test_df_1():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_department_data1.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["department"])
        return df


@pytest.fixture
def test_df_2():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_department_data2.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["department"])
        return df


@pytest.fixture
def bucket(s3, test_df_1, test_df_2):
    """Create mock s3 bucket with parquet files."""
    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data_to_write_1 = pd.DataFrame.to_parquet(test_df_1)
    s3.put_object(
        Body=data_to_write_1,
        Bucket="test_bucket",
        Key="department/2022-11-03/14:20:51.563/.parquet",
    )
    data_to_write_2 = pd.DataFrame.to_parquet(test_df_2)
    s3.put_object(
        Body=data_to_write_2,
        Bucket="test_bucket",
        Key="department/2022-11-04/14:20:51.563/.parquet",
    )


@pytest.mark.describe("get_archived_table_data()")
@pytest.mark.it("should return a dataframe")
def test_returns_df(bucket):
    """get_archived_table_data() should return a dataframe."""
    result = get_archived_table_data("department", "test_bucket")
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("get_archived_table_data()")
@pytest.mark.it("should return a dataframe with correct number of rows")
def test_returns_correct_row_num(bucket, test_df_1, test_df_2):
    """get_archived_table_data() should return correct number of rows."""
    result = get_archived_table_data("department", "test_bucket")
    assert len(result) == len(test_df_1) + len(test_df_2)


@pytest.mark.describe("get_archived_table_data()")
@pytest.mark.it("should return a dataframe with correct data")
def test_returns_correct_data(bucket, test_df_1, test_df_2):
    """get_archived_table_data() should return correct data."""
    merged_df = pd.concat([test_df_1, test_df_2], ignore_index=True)
    result = get_archived_table_data("department", "test_bucket")
    assert result.equals(merged_df)
