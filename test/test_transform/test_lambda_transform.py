"""This module contains the test suite for lambda_handler() function
for the transformation lambda."""

import json
import pytest
import os
import boto3
from moto import mock_aws

from src.transform.lambda_handler import grab_file_name


@pytest.fixture
def valid_event():
    with open("test/test_transform/test_data/valid_test_event.json") as v:
        event = json.loads(v.read())
    return event


@pytest.fixture
def invalid_event():
    with open("test/test_transform/test_data/invalid_test_event.json") as i:
        event = json.loads(i.read())
    return event


@pytest.fixture
def file_type_event():
    with open("test/test_transform/test_data/file_type_event.json") as i:
        event = json.loads(i.read())
    return event


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
    with open("test/test_transform/test_data/test_transaction_data.json") as f:
        json_to_write = f.read()
        s3.put_object(
            Body=json_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="transaction/2024-02-22/18:00:20.106733.json",
        )


@pytest.fixture
def proc_bucket(s3):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-processed-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def test_grab_file_name(valid_event):
    key_result = grab_file_name(valid_event["Records"])
    assert key_result == "transaction/2024-02-22/18:00:20.106733.json"


def test_grab_file_name_if_file_name_not_present(invalid_event):
    with pytest.raises(KeyError):
        grab_file_name(invalid_event["Records"])


# @mock_aws
# def test_lambda_handler_gets_file_name(valid_event, caplog, s3, bucket, proc_bucket):  # noqa
#     with caplog.at_level(logging.INFO):
#         lambda_handler(valid_event, {})
#         assert 'File name is transaction/2024-02-22/18:00:20.106733.json!' in caplog.text  # noqa


# @mock_aws
# def test_lambda_handler_gets_JSON_data(valid_event, caplog, s3, bucket, proc_bucket):  # noqa
#     with caplog.at_level(logging.INFO):
#         lambda_handler(valid_event, {}, 'totesys-etl-processed-data-bucket-teamness-120224')  # noqa
#         assert 'JSON file taken from S3 ingestion bucket!' in caplog.text
