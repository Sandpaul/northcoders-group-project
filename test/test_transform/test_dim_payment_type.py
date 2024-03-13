from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.dim_payment_type import transform_payment_type
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
        "timestamp": "2024-02-22 17:25:21.054449",
        "payment_type": [
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
                "payment_type_id": 1,
                "payment_type_name": "SALES_RECEIPT",
            },
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
                "payment_type_id": 2,
                "payment_type_name": "SALES_REFUND",
            },
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
                "payment_type_id": 3,
                "payment_type_name": "PURCHASE_PAYMENT",
            },
            {
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
                "payment_type_id": 4,
                "payment_type_name": "PURCHASE_REFUND",
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
    with open("test/test_transform/test_data/test_payment_type_data.json") as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="payment_type/2024-02-22/17:25:21.054448.json",
        )
    with open(
        "test/test_transform/test_data/test_payment_type_data2.json"
    ) as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="payment_type/2024-02-22/17:25:21.054449.json",
        )


@pytest.mark.describe("transform_payment_type()")
@pytest.mark.it("should return dictionary with a dataframe")
def test_transform_payment_type_returns_df(s3, bucket, example_data):
    """should return a dictionary with a dataframe"""
    file_path = "payment_type/2024-02-22/17:25:21.054448.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment_type(test_data)
    assert type(result["payment_type"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_payment_type()")
@pytest.mark.it("dataframe should contain correct number of keys")  # noqa
def test_transform_payment_type_returns_correct_created(s3, bucket, example_data):
    file_path = "payment_type/2024-02-22/17:25:21.054448.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment_type(test_data)
    assert set(result["payment_type"].keys()) == set(
        [
            "payment_type_record_id",
            "payment_type_id",
            "payment_type_name",
            "last_updated_date",
            "last_updated_time",
        ]
    )  # noqa


@pytest.mark.describe("transform_payment_type()")
@pytest.mark.it(
    "should work when passed payment_type list with more than one dict"
)  # noqa
def test_transform_payment_type_works_on_multiple_dicts(s3, bucket, example_data):
    file_path = "payment_type/2024-02-22/17:25:21.054449.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment_type(test_data)

    assert result["payment_type"].get("payment_type_name").get(0) == "SALES_RECEIPT"
    assert result["payment_type"].get("payment_type_name").get(1) == "SALES_REFUND"
