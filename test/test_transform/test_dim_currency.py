import os
import io
import json

import boto3
from moto import mock_aws
import pytest
import pandas as pd

from src.utils.parquet_file_reader import parquet_file_reader
from src.transform.dim_currency import dim_currency


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
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("test/test_transform/test_data/test_currency_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["currency"])
        parquet = pd.DataFrame.to_parquet(df)
        s3.put_object(
            Body=parquet,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="currency/2024-02-22/11:35:20.078376.parquet",
        )
        

@pytest.fixture
def control_df():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_currency_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["currency"])
        return df


@pytest.fixture
def file_path():
    """Mock filepath."""
    return "currency/2024-02-22/11:35:20.078376.parquet"


@pytest.fixture
def bucket_name():
    """Mock bucket name."""
    return "totesys-etl-ingestion-bucket-teamness-120224"


@pytest.mark.describe("dim_currency()")
@pytest.mark.it("should return a dataframe")
def test_dim_currency_returns_df(s3, bucket, file_path, bucket_name):
    """should return a new dataframe"""
    test_data = parquet_file_reader(file_path, bucket_name)
    result = dim_currency(test_data)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("dim_currency()")
@pytest.mark.it("returned dataframe should contain correct keys")  # noqa
def test_dim_currency_returns_correct_num_keys(s3, bucket, file_path, bucket_name):
    """dim_currency() should return the correct keys."""
    test_data = parquet_file_reader(file_path, bucket_name)
    result = dim_currency(test_data)
    assert set(result.keys()) == set(
        [
            "currency_id",
            "currency_code",
            "currency_name",
        ]
    )


@pytest.mark.describe("dim_currency()")
@pytest.mark.it("dataframe should contain correct currency_name")  # noqa
def test_dim_currency_returns_correct_currency_name(s3, bucket, file_path, bucket_name):
    """should return the correct currency_name"""
    test_data = parquet_file_reader(file_path, bucket_name)
    result = dim_currency(test_data)
    assert result.get("currency_name").get(0) == "British Pound"
    

@pytest.mark.describe("dim_currency()")
@pytest.mark.it("should not mutate original dataframe")  # noqa
def test_dim_currency_works_on_multiple_rows(s3, bucket, file_path, bucket_name, control_df):
    """dim_currency() should not mutate the passed dataframe."""
    test_data = parquet_file_reader(file_path, bucket_name)
    result = dim_currency(test_data)
    assert test_data.equals(control_df) is True
    assert test_data.equals(result) is False
