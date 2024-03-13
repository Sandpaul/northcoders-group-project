"""This module contains the test suite for transform_counterparty()."""

import os
from moto import mock_aws
import boto3
import pytest
from src.transform.dim_counterparty import transform_counterparty
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
    with open("test/test_transform/test_data/test_counterparty_data.json") as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="counterparty/2022-11-03/14:20:51.563.json",
        )
    with open("test/test_transform/test_data/test_address_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="address/2022-11-03/14:20:51.563.json",
        )
    with open("test/test_transform/test_data/test_address_data2.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="address/2022-11-04/14:20:51.563.json",
        )


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it("should return a dictionary with dataframe")
def test_transform_counterparty_returns_a_dictionary(s3, bucket):
    """Should return a dictionary with dataframe"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)
    assert isinstance(result, dict)


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it(
    "should return a dataframe in the counterparty key of the dictionary"
)  # noqa
def test_function_returns_a_dataframe_in_the_dictionary(s3, bucket):
    """Returned counterparty key should be a dataframe"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)
    assert type(result["counterparty"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it("check if the dataframe has the required column names")
def test_function_returns_the_correct_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)
    expected = [
        "counterparty_record_id",
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
        "last_updated_date",
        "last_updated_time",
    ]
    assert list(result["counterparty"].columns) == expected


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)
    column_headers = list(result["counterparty"].columns)
    for column in column_headers:
        assert "legal_address_id" != column
        assert "address_id" != column


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it("returns the correct timestamp")
def test_function_returns_right_timestamp(s3, bucket):
    """Returned dataframe has the required column names returned"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)["timestamp"]  # noqa
    expected = "2022-11-03 14:20:51.563"
    assert result == expected


@pytest.mark.describe("transform_counterparty")
@pytest.mark.it("should join the counterparty and address data correctly")
def test_function_joins_correctly(s3, bucket):
    """transform_counterparty should complete the correct joins succesfully"""
    counterparty_file_path = "counterparty/2022-11-03/14:20:51.563.json"
    counterparty_test_data = read_ingestion_file_data(counterparty_file_path)
    result = transform_counterparty(counterparty_test_data)
    assert (
        result["counterparty"].get("counterparty_legal_address_line_1").get(0)
        == "6826 Herzog Via"
    )
    assert (
        result["counterparty"].get("counterparty_legal_address_line_1").get(1)
        == "6102 Rogahn Skyway"
    )
    assert (
        result["counterparty"].get("counterparty_legal_address_line_1").get(2)
        == "27 Paul Place"
    )
