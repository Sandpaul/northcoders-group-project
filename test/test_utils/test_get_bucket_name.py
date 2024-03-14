"""This module contains the test suite for `get_bucket_name()`."""

import os

import boto3
from moto import mock_aws
import pytest

from src.utils.get_bucket_name import get_bucket_name


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


@pytest.fixture(scope="function")
def buckets(s3):
    """Create mock s3 buckets."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
    s3.create_bucket(
        Bucket="totesys-etl-processed_data-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )


@pytest.mark.describe("get_bucket_name()")
@pytest.mark.it("should return correct bucket name")
def test_returns_correct_bucket_name(buckets):
    """get_bucket_name() should return the correct bucket name based on input."""
    result1 = get_bucket_name("ingestion")
    result2 = get_bucket_name("processed")
    assert result1 == "totesys-etl-ingestion-bucket-teamness-120224"
    assert result2 == "totesys-etl-processed_data-bucket-teamness-120224"


@pytest.mark.describe("get_bucket_name()")
@pytest.mark.it("should raise error if passed invalid argument")
def test_raises_error_for_invalid_arguments(buckets):
    """get_bucket_name() should return the correct bucket name based on input."""
    result = get_bucket_name("banana")
