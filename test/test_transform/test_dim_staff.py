"""This module contains the test suite for transform_staff()."""

import os
import json

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from src.transform.dim_staff import dim_staff


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
def department_df_1():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_department_data1.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["department"])
        return df


@pytest.fixture
def department_df_2():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_department_data2.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["department"])
        return df


@pytest.fixture
def staff_df():
    """Sets up a test data frame."""
    with open("test/test_transform/test_data/test_staff_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["staff"])
        return df


@pytest.fixture
def control_df():
    """Sets up a control data frame."""
    with open("test/test_transform/test_data/test_staff_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["staff"])
        return df


@pytest.fixture
def bucket(s3, department_df_1, department_df_2):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data_to_write_1 = pd.DataFrame.to_parquet(department_df_1)
    s3.put_object(
        Body=data_to_write_1,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="department/2022-11-03/14:20:51.563.json",
    )
    data_to_write_2 = pd.DataFrame.to_parquet(department_df_2)
    s3.put_object(
        Body=data_to_write_2,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="department/2022-11-04/14:20:51.563.json",
    )


@pytest.mark.describe("dim_staff()")
@pytest.mark.it("should return a dataframe")
def test_dim_staff_returns_a_dataframe(bucket, staff_df):
    """Should return a dictionary with dataframe"""
    result = dim_staff(staff_df)
    assert type(result).__name__ == "DataFrame"


@pytest.mark.describe("dim_staff()")
@pytest.mark.it("check if the dataframe has the required column names")
def test_function_returns_the_correct_columns(bucket, staff_df):
    """Returned dataframe has the required column names returned"""
    result = dim_staff(staff_df)
    expected = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]
    assert list(result.columns) == expected


@pytest.mark.describe("dim_staff()")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(bucket, staff_df):
    """Returned dataframe has the required column names returned"""
    result = dim_staff(staff_df)
    assert "created_at" not in result.columns
    assert "last_updated" not in result.columns
    assert "department_id" not in result.columns
    assert "manager" not in result.columns


@pytest.mark.describe("dim_staff()")
@pytest.mark.it("should join the staff and address data correctly")
def test_function_joins_correctly(bucket, staff_df):
    """dim_staff should complete the correct joins succesfully"""
    result = dim_staff(staff_df)
    assert result.get("department_name").get(0) == "Purchasing"
    assert result.get("department_name").get(1) == "Facilities"
    assert result.get("department_name").get(2) == "Production"


@pytest.mark.describe("dim_staff()")
@pytest.mark.it("should not mutate passed data frames")
def test_does_not_mutate_arg(s3, bucket, staff_df, control_df):
    """dim_staff() should not mutate passed data frame."""
    result = dim_staff(staff_df)
    assert staff_df.equals(control_df) is True
    assert result.equals(control_df) is False
