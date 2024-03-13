"""This module contains the test suite for `parquet_file_reader()`."""

import io
import os

import boto3
from moto import mock_aws
import pandas as pd
import pytest

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
def example_data():
    """Create mock data."""
    mock_data = [
        {"id": 1, "make": "Ford", "model": "Mustang"},
        {"id": 2, "make": "Toyota", "model": "Yaris"},
        {"id": 3, "make": "Honda", "model": "Civic"},
        {"id": 4, "make": "BMW", "model": "X5"},
    ]
    return pd.DataFrame.from_records(mock_data)


@pytest.fixture
def bucket(s3, example_data):
    """Create mock s3 bucket with parquet file."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data_to_write = pd.DataFrame.to_parquet(example_data)
    s3.put_object(
        Body=data_to_write,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="cars/2022-11-03/14:20:51.563/.parquet",
    )


@pytest.mark.describe("parquet_file_reader()")
@pytest.mark.it("should retrieve data frame from file")
def test_retrieve_df_from_file(s3, example_data, bucket):
    """parquet_file_reader() should use passed filepath to retrieve file from s3 and return dataframe."""
    file_path = "cars/2022-11-03/14:20:51.563/.parquet"
    bucket_name = "totesys-etl-ingestion-bucket-teamness-120224"
    result = parquet_file_reader(file_path, bucket_name)
    assert type(result).__name__ == "DataFrame"
    assert result.equals(example_data)
