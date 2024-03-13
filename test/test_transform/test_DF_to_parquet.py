"""This file contains the test suite for DF_to_parquet() only."""

import pandas as pd
import os
import boto3
import pytest
from moto import mock_aws


from src.transform.DF_to_parquet import DF_to_parquet


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
def bucket(s3):
    """Create mock s3 bucket."""
    return s3.create_bucket(
        Bucket="totesys-etl-processed-data-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def example_dataframe():
    return pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})


@pytest.fixture
def example_dict(example_dataframe):
    return {
        "timestamp": "2022-11-03 14:20:51.563",
        "table_name": example_dataframe,
    }


@mock_aws
def test_DF_to_parquet_saves_to_bucket(bucket, s3, example_dict):

    response1 = s3.list_objects_v2(
        Bucket="totesys-etl-processed-data-bucket-teamness-120224"
    )
    assert response1["KeyCount"] == 0
    DF_to_parquet(example_dict)
    response2 = s3.list_objects_v2(
        Bucket="totesys-etl-processed-data-bucket-teamness-120224"
    )
    assert response2["KeyCount"] == 1


@mock_aws
def test_DF_to_parquet_correct_file_name(bucket, s3, example_dict):
    DF_to_parquet(example_dict)
    response = s3.list_objects_v2(
        Bucket="totesys-etl-processed-data-bucket-teamness-120224"
    )
    assert (
        response["Contents"][0]["Key"] == "table_name/2022-11-03/14:20:51.563.parquet"
    )  # noqa


def test_DF_to_parquet_invalid_dataframe(bucket, s3, example_dict):
    """DF_to_parquet() should raise a
    ValueError if an invalid DataFrame is provided."""
    example_dict["table_name"] = "Not a DataFrame"
    with pytest.raises(ValueError, match="Invalid DataFrame provided."):
        DF_to_parquet(example_dict)


@mock_aws
def test_DF_to_parquet_missing_timestamp(bucket, s3, example_dict):
    """DF_to_parquet() should raise a KeyError
    if the timestamp key is missing."""
    del example_dict["timestamp"]
    with pytest.raises(KeyError, match="timestamp"):
        DF_to_parquet(example_dict)
