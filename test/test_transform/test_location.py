from src.transform.dim_location import transform_location
from src.transform.read_ingestion_file_data import read_ingestion_file_data
from moto import mock_aws
import os
import boto3
import pytest


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
        "timestamp": "2022-11-03 14:20:51.563",
        "cars": [
            {"id": 1, "make": "Ford", "model": "Mustang"},
            {"id": 2, "make": "Toyota", "model": "Yaris"},
            {"id": 3, "make": "Honda", "model": "Civic"},
            {"id": 4, "make": "BMW", "model": "X5"},
        ],
    }


@pytest.fixture
def bucket(s3, example_data):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("test/test_transform/test_data/test_address_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="address/2022-11-03/14:20:51.563.json",
        )


@pytest.mark.describe("transform_location")
@pytest.mark.it("should return a dictionary with dataframe")
def test_transform_location_returns_a_dictionary(s3, bucket):
    """Should return a dictionary with dataframe"""

    location_file_path = "address/2022-11-03/14:20:51.563.json"
    location_data = read_ingestion_file_data(location_file_path)

    result = transform_location(location_data)
    expected = dict

    assert type(result) is expected


@pytest.mark.describe("transform_location")
@pytest.mark.it(
    "should return a dataframe in the counterparty key of the dictionary"
)  # noqa
def test_function_returns_a_dataframe_in_the_dictionary(s3, bucket):
    """Returned counterparty key should be a dataframee"""

    location_file_path = "address/2022-11-03/14:20:51.563.json"
    location_data = read_ingestion_file_data(location_file_path)

    result = transform_location(location_data)

    assert type(result["location"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_location")
@pytest.mark.it("check if the dataframe has the required column names")
def test_function_returns_the_correct_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""

    location_file_path = "address/2022-11-03/14:20:51.563.json"
    location_data = read_ingestion_file_data(location_file_path)

    result = transform_location(location_data)

    expected = [
        "location_record_id",
        "address_id",
        "address_line_1",
        "address_line_2",  # noqa
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
        "last_updated_date",
        "last_updated_time",
    ]
    assert list(result["location"].columns) == expected


@pytest.mark.describe("transform_location")
@pytest.mark.it("does not have the unwanted columns after merging")
def test_function_deletes_unwanted_columns(s3, bucket):
    """Returned dataframe has the required column names returned"""

    location_file_path = "address/2022-11-03/14:20:51.563.json"
    location_data = read_ingestion_file_data(location_file_path)

    result = transform_location(location_data)
    column_headers = list(result["location"].columns)
    for column in column_headers:
        assert "created_at" != column
        assert "last_updated" != column


@pytest.mark.describe("transform_location")
@pytest.mark.it("returns the correct timestamp")
def test_function_returns_right_timestamp(s3, bucket):
    """Returned dataframe has the required column names returned"""

    location_file_path = "address/2022-11-03/14:20:51.563.json"
    location_data = read_ingestion_file_data(location_file_path)

    result = transform_location(location_data)["timestamp"]
    expected = "2022-11-03 14:20:51.563"

    assert result == expected
