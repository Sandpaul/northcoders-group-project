"""This module contains the test suites for read_ingestion_file_data()."""

import os
import boto3
from moto import mock_aws
import pytest

from src.transform.read_ingestion_file_data import read_ingestion_file_data


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
        "timestamp": '2022-11-03 14:20:51.563',
        "cars": [
            {'id': 1, 'make': 'Ford', 'model': 'Mustang'},
            {'id': 2, 'make': 'Toyota', 'model': 'Yaris'},
            {'id': 3, 'make': 'Honda', 'model': 'Civic'},
            {'id': 4, 'make': 'BMW', 'model': 'X5'}
        ]
    }


@pytest.fixture
def bucket(s3):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    with open('test/test_transform/test_data/test_data.json') as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="2022-11-03/14:20:51.563/cars.json"
        )


@pytest.mark.describe('read_ingestion_file_data()')
@pytest.mark.it('should retrieve data from a file in the ingestion bucket and return it as a dictionary')  # noqa
def test_reads_data_from_file_in_s3(s3, bucket, example_data):
    """should successfully read data from a file in an s3 bucket and return it as a dictionary
    """  # noqa
    file_path = "2022-11-03/14:20:51.563/cars.json"
    result = read_ingestion_file_data(file_path)
    assert isinstance(result, dict)
    assert result == example_data
