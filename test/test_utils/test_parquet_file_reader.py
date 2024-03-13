"""This module contains the test suite for `parquet_file_reader()`."""

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
    return {
        "cars": [
            {"id": 1, "make": "Ford", "model": "Mustang"},
            {"id": 2, "make": "Toyota", "model": "Yaris"},
            {"id": 3, "make": "Honda", "model": "Civic"},
            {"id": 4, "make": "BMW", "model": "X5"},
        ],
    }


@pytest.fixture
def example_df(example_data):
    """Create example dataframe."""
    data_to_write = example_data["cars"]
    return pd.DataFrame.from_records(data_to_write)


@pytest.fixture
def example_dict(example_df):
    """Create example dict."""
    return {
        "timestamp": '2022-11-03 14:20:51.563',
        "cars": example_df
    }


@pytest.fixture
def bucket(s3, example_dict):
    """Create mock s3 bucket with parquet file."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data_to_write = example_dict
    s3.put_object(
        Body=data_to_write,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="cars/2022-11-03/14:20:51.563/.parquet",
    )