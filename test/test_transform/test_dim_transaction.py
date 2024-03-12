from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.dim_transaction import transform_transaction
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


@pytest.fixture(scope='function')
def s3(aws_credentials):
    """Create mock s3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name='eu-west-2')


@pytest.fixture
def example_data():
    """Create mock data."""
    return {
        "timestamp": "2024-02-22 14:50:20.010164",
        "transaction": [
            {
                "created_at": "2024-02-22 15:59:10.043000",
                "last_updated": "2024-02-22 15:59:10.043000",
                "purchase_order_id": None,
                "sales_order_id": 6929,
                "transaction_id": 9882,
                "transaction_type": "SALE"
            },
            {
                "created_at": "2024-02-22 14:48:10.434000",
                "last_updated": "2024-02-22 14:48:10.434000",
                "purchase_order_id": None,
                "sales_order_id": 6928,
                "transaction_id": 9881,
                "transaction_type": "SALE"
            }
        ]
    }


@pytest.fixture
def bucket(s3, example_data):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with open('test/test_transform/test_data/test_transaction_data.json') as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="transaction/2024-02-22/14:50:20.010163.json"
        )
    with open("test/test_transform/test_data/test_transaction_data2.json") as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="transaction/2024-02-22/14:50:20.010164.json"
        )


@pytest.mark.describe('transform_transaction()')
@pytest.mark.it('should return dictionary with a dataframe')
def test_transform_transaction_returns_df(s3, bucket, example_data):
    """should return a dictionary with a dataframe"""
    file_path = "transaction/2024-02-22/14:50:20.010163.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_transaction(test_data)
    assert type(result["transaction"]).__name__ == 'DataFrame'


@pytest.mark.describe('transform_transaction()')
@pytest.mark.it('dataframe should contain correct number of keys')  # noqa
def test_transform_transaction_returns_correct_created(
        s3, bucket, example_data):
    file_path = "transaction/2024-02-22/14:50:20.010163.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_transaction(test_data)
    assert set(result["transaction"].keys()) == set(
        ['transaction_record_id',
         'transaction_id',
         'transaction_type',
         'sales_order_id',
         'purchase_order_id',
         "last_updated_date",
         "last_updated_time"])


@pytest.mark.describe('transform_transaction()')
@pytest.mark.it('should work when passed transaction list with more than one dict')  # noqa
def test_transform_transaction_works_on_multiple_dicts(
        s3, bucket, example_data):
    file_path = "transaction/2024-02-22/14:50:20.010164.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_transaction(test_data)

    assert result["transaction"].get(
        "transaction_id").get(0) == 9882
    assert result["transaction"].get(
        "transaction_id").get(1) == 9881
    assert result["transaction"].get(
        "purchase_order_id").get(0) is None
