"""This module contains the test suite for transform_staff()."""

import os
from moto import mock_aws
import boto3
import pytest
from src.transform.dim_staff import transform_staff
from src.transform.read_ingestion_file_data import read_ingestion_file_data


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
    with open("test/test_transform/test_data/test_staff_data.json") as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="staff/2022-11-03/14:20:51.563.json",
        )
    with open("test/test_transform/test_data/test_department_data1.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="department/2022-11-03/14:20:51.563.json",
        )
    with open("test/test_transform/test_data/test_department_data2.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="department/2022-11-04/14:20:51.563.json",
        )


@pytest.mark.describe("transform_staff")
@pytest.mark.it("should return a dictionary with dataframe")
def test_transform_staff_returns_a_dictionary(s3, bucket):
    """Should return a dictionary with dataframe"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)
    assert isinstance(result, dict)


@pytest.mark.describe("transform_staff")
@pytest.mark.it("should return a dataframe in the staff key of the dictionary")  # noqa
def test_function_returns_a_dataframe_in_the_dictionary(s3, bucket):
    """Returned staff key should be a dataframe"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)
    assert type(result["staff"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_staff")
@pytest.mark.it("check if the dataframe has the required column names")
def test_function_returns_the_correct_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)
    expected = [
        "staff_record_id",
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
        "last_updated_date",
        "last_updated_time",
    ]
    assert list(result["staff"].columns) == expected


@pytest.mark.describe("transform_staff")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)
    column_headers = list(result["staff"].columns)
    for column in column_headers:
        assert "department_id" != column
        assert "address_id" != column
        assert "created_at" != column
        assert "last_updated" != column
        assert "manager" != column


@pytest.mark.describe("transform_staff")
@pytest.mark.it("returns the correct timestamp")
def test_function_returns_right_timestamp(s3, bucket):
    """Returned dataframe has the required column names returned"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)["timestamp"]  # noqa
    expected = "2024-02-22 17:25:21.054448"
    assert result == expected


@pytest.mark.describe("transform_staff")
@pytest.mark.it("should join the staff and address data correctly")
def test_function_joins_correctly(s3, bucket):
    """transform_staff should complete the correct joins succesfully"""
    staff_file_path = "staff/2022-11-03/14:20:51.563.json"
    staff_test_data = read_ingestion_file_data(staff_file_path)
    result = transform_staff(staff_test_data)
    assert result["staff"].get("department_name").get(0) == "Purchasing"
    assert result["staff"].get("department_name").get(1) == "Facilities"
    assert result["staff"].get("department_name").get(2) == "Production"
