from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.fact_payment import transform_payment
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
        "timestamp": "2024-02-22 14:50:20.010165",
        "payment": [
            {
                "company_ac_number": 72098432,
                "counterparty_ac_number": 27214177,
                "counterparty_id": 4,
                "created_at": "2024-02-22 14:48:10.434000",
                "currency_id": 2,
                "last_updated_time": "2024-02-22 14:48:10.434000",
                "paid": False,
                "payment_amount": "82207.80",
                "payment_date": "2024-02-23",
                "payment_id": 9881,
                "payment_type_id": 1,
                "transaction_id": 9881,
            },
            {
                "company_ac_number": 65525440,
                "counterparty_ac_number": 19472783,
                "counterparty_id": 13,
                "created_at": "2024-02-22 15:59:10.043000",
                "currency_id": 3,
                "last_updated_time": "2024-02-22 15:59:10.043000",
                "paid": False,
                "payment_amount": "319428.33",
                "payment_date": "2024-02-26",
                "payment_id": 9882,
                "payment_type_id": 1,
                "transaction_id": 9882,
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
    with open("test/test_transform/test_data/test_payment_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="payment/2024-02-22/14:50:20.010164.json",
        )
    with open("test/test_transform/test_data/test_payment_data2.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="payment/2024-02-22/14:50:20.010165.json",
        )


@pytest.mark.describe("transform_payment()")
@pytest.mark.it("should return dictionary with a dataframe")
def test_transform_payment_returns_df(s3, bucket, example_data):
    """should return a dictionary with a dataframe"""
    file_path = "payment/2024-02-22/14:50:20.010164.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment(test_data)
    assert type(result["payment"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_payment()")
@pytest.mark.it("dataframe should contain correct number of keys")  # noqa
def test_dataframe_returns_correct_number_of_keys(s3, bucket, example_data):
    file_path = "payment/2024-02-22/14:50:20.010164.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment(test_data)
    assert set(result["payment"].keys()) == set(
        [
            "payment_record_id",
            "payment_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "transaction_record_id",
            "counterparty_record_id",
            "payment_amount",
            "currency_record_id",
            "payment_type_record_id",
            "paid",
            "payment_date",
        ]
    )


@pytest.mark.describe("transform_payment()")
@pytest.mark.it(
    "dataframe should contain correct created_date and created_time"
)  # noqa
def test_transform_payment_returns_correct_created(s3, bucket, example_data):
    """should return the correct created_date and created_time"""
    file_path = "payment/2024-02-22/14:50:20.010165.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment(test_data)
    assert result["payment"].get("created_date").get(1) == "2024-02-22"
    assert result["payment"].get("created_time").get(1) == "15:59:10.043000"


@pytest.mark.describe("transform_payment()")
@pytest.mark.it(
    "dataframe should contain correct last_updated_date and last_updated"
)  # noqa
def test_transform_payment_returns_correct_last_updated(s3, bucket, example_data):
    """should return the correct last_updated_date and last_updated"""
    file_path = "payment/2024-02-22/14:50:20.010165.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment(test_data)
    assert result["payment"].get("last_updated_date").get(0) == "2024-02-22"
    assert result["payment"].get("last_updated_time").get(0) == "14:48:10.434000"


@pytest.mark.describe("transform_payment()")
@pytest.mark.it("should work when passed payment list with more than one dict")  # noqa
def test_transform_payment_works_on_multiple_dicts(s3, bucket, example_data):
    file_path = "payment/2024-02-22/14:50:20.010165.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_payment(test_data)

    assert result["payment"].get("transaction_record_id").get(0) == 9881
    assert result["payment"].get("transaction_record_id").get(1) == 9882
