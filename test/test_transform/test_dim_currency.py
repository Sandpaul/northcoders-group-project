from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.dim_currency import transform_currency
import os
import boto3
from moto import mock_aws
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
        "timestamp": "2024-02-22 11:35:20.078376",
        "currency": [
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "currency_code": "GBP",
                "currency_id": 1,
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "currency_code": "USD",
                "currency_id": 2,
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "currency_code": "EUR",
                "currency_id": 3,
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ],
    }


@pytest.fixture
def bucket(s3, example_data):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("test/test_transform/test_data/test_currency_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="currency/2024-02-22/11:35:20.078375.json",
        )
    with open("test/test_transform/test_data/test_currency_data2.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="currency/2024-02-22/11:35:20.078376.json",
        )


@pytest.mark.describe("transform_currency()")
@pytest.mark.it("should return dictionary with a dataframe")
def test_transform_currency_returns_df(s3, bucket, example_data):
    """should return a dictionary with a dataframe"""
    file_path = "currency/2024-02-22/11:35:20.078375.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_currency(test_data)
    assert type(result["currency"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_currency()")
@pytest.mark.it("dataframe should contain correct number of keys")  # noqa
def test_transform_design_returns_correct_created(s3, bucket, example_data):
    file_path = "currency/2024-02-22/11:35:20.078375.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_currency(test_data)
    assert set(result["currency"].keys()) == set(
        [
            "currency_record_id",
            "currency_code",
            "currency_id",
            "currency_name",
            "last_updated_date",
            "last_updated_time",
        ]
    )  # noqa


@pytest.mark.describe("transform_currency()")
@pytest.mark.it("dataframe should contain correct currency_name")  # noqa
def test_transform_currency_returns_correct_currency_name(s3, bucket, example_data):
    """should return the correct currency_name"""
    file_path = "currency/2024-02-22/11:35:20.078376.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_currency(test_data)
    assert result["currency"].get("currency_name").get(0) == "British Pound"
    assert result["currency"].get("currency_name").get(1) == "US dollar"


@pytest.mark.describe("transform_currency()")
@pytest.mark.it("should work when passed currency list with more than one dict")  # noqa
def test_transform_currency_works_on_multiple_dicts(s3, bucket, example_data):
    file_path = "currency/2024-02-22/11:35:20.078376.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_currency(test_data)

    assert result["currency"].get("currency_name").get(0) == "British Pound"
    assert result["currency"].get("currency_name").get(1) == "US dollar"
