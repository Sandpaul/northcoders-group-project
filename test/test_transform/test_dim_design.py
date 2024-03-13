from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.dim_design import transform_design
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
        "timestamp": "2022-11-03 14:20:51.563",
        "design": [
            {
                "design_id": 1,
                "design_name": "tulip",
                "file_location": "Manchester",
                "file_name": "Mustang",
                "created_at": "2024-02-19 15:07:09.880000",
                "last_updated": "2024-02-20 15:07:09.880000",
            },
            {
                "design_id": 2,
                "design_name": "rose",
                "file_location": "Manchester",
                "file_name": "Mustang",
                "created_at": "2024-02-19 15:07:09.880000",
                "last_updated": "2024-02-20 15:07:09.880000",
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
    with open("test/test_transform/test_data/test_design_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="design/2022-11-03/14:20:51.563.json",
        )
    with open("test/test_transform/test_data/test_design_data2.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="design/2022-11-02/14:20:51.564.json",
        )


@pytest.mark.describe("transform_design()")
@pytest.mark.it("should return dictionary with a dataframe")
def test_transform_design_returns_df(s3, bucket, example_data):
    """should return a dictionary with a dataframe"""
    file_path = "design/2022-11-03/14:20:51.563.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_design(test_data)
    assert type(result["design"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_design()")
@pytest.mark.it("dataframe should contain correct number of keys")  # noqa
def test_transform_design_returns_correct_created(s3, bucket, example_data):
    file_path = "design/2022-11-03/14:20:51.563.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_design(test_data)
    assert set(result["design"].keys()) == set(
        [
            "design_record_id",
            "design_id",
            "design_name",  # noqa
            "file_location",
            "file_name",
            "last_updated_date",
            "last_updated_time",
        ]
    )  # noqa


@pytest.mark.describe("transform_design()")
@pytest.mark.it("should work when passed design list with more than one dict")  # noqa
def test_transform_design_works_on_multiple_dicts(s3, bucket, example_data):
    file_path = "design/2022-11-02/14:20:51.564.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_design(test_data)

    assert result["design"].get("design_name").get(0) == "tulip"
    assert result["design"].get("design_name").get(1) == "rose"
