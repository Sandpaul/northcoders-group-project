"""This file contains the test suite for DF_to_parquet() only."""

import os
from pprint import pprint

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from src.transform.df_to_parquet import df_to_parquet


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
def bucket_name():
    """Mock bucket name."""
    return "totesys-etl-processed-data-bucket-teamness-120224"


@pytest.fixture
def test_df():
    return pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})


@pytest.mark.describe("df_to_parquet()")
@pytest.mark.it("should save parquet file to bucket")
def test_saves_file_in_bucket(s3, bucket, bucket_name, test_df):
    """df_to_parquet() should succesfully save files in the processed data bucket."""
    response1 = s3.list_objects_v2(Bucket=bucket_name)
    assert response1["KeyCount"] == 0
    file_name = "sales_order/2024-01-01/00.00.000000.parquet"
    df_to_parquet(test_df, file_name)
    response2 = s3.list_objects_v2(Bucket=bucket_name)
    assert response2["KeyCount"] == 1


@pytest.mark.describe("df_to_parquet()")
@pytest.mark.it("should save files with the correct name")
def test_saves_correct_file_name(s3, bucket, bucket_name, test_df):
    """df_to_parquet() should save files with correct name."""
    file_names = [
        "address/2024-01-01/00.00.000000.parquet",
        "counterparty/2024-01-01/00.00.000000.parquet",
        "currency/2024-01-01/00.00.000000.parquet",
        "design/2024-01-01/00.00.000000.parquet",
        "payment_type/2024-01-01/00.00.000000.parquet",
        "staff/2024-01-01/00.00.000000.parquet",
        "transaction/2024-01-01/00.00.000000.parquet",
        "payment/2024-01-01/00.00.000000.parquet",
        "purchase_order/2024-01-01/00.00.000000.parquet",
        "sales_order/2024-01-01/00.00.000000.parquet",
    ]
    for f in file_names:
        df_to_parquet(df=test_df, file_name=f)
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = [file["Key"] for file in response["Contents"]]
    expected_files = [
        "dim_counterparty/2024-01-01/00.00.000000.parquet",
        "dim_currency/2024-01-01/00.00.000000.parquet",
        "dim_design/2024-01-01/00.00.000000.parquet",
        "dim_location/2024-01-01/00.00.000000.parquet",
        "dim_payment_type/2024-01-01/00.00.000000.parquet",
        "dim_staff/2024-01-01/00.00.000000.parquet",
        "dim_transaction/2024-01-01/00.00.000000.parquet",
        "fact_payment/2024-01-01/00.00.000000.parquet",
        "fact_purchase_order/2024-01-01/00.00.000000.parquet",
        "fact_sales_order/2024-01-01/00.00.000000.parquet",
    ]
    assert files == expected_files


@pytest.mark.describe("df_to_parquet()")
@pytest.mark.it("should raise ValueError when passed invalid file name")
def test_raises_value_error_1(test_df):
    """df_to_parquet() should raise a ValueError if passed an invalid file_name."""
    with pytest.raises(ValueError):
        filename = "department/2020-01-01/00:00:00.000000"
        df_to_parquet(test_df, filename)


@pytest.mark.describe("df_to_parquet()")
@pytest.mark.it("should raise ValueError when passed non data frame")
def test_raises_value_error_2():
    """df_to_parquet() should raise a ValueError if passed an invalid file_name."""
    with pytest.raises(ValueError):
        filename = "address/2020-01-01/00:00:00.000000"
        df_to_parquet({}, filename)