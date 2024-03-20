"""This module contains the test suite for `get_secret_dict()`."""

import os

import boto3
from moto import mock_aws
import pytest

from src.utils.get_secret_dict import get_secret_dict


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def sm(aws_credentials):
    """Mock secretsmanager client"""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mock_secret(sm):
    """Create mock secret."""
    return sm.create_secret(
        Name="mock_secret",
        SecretString='{"host" : "test_host","port" : "0000","database" : "test_database","user" : "test_user","password" : "test_password"}',
    )


@pytest.mark.describe("get_secret_dict()")
@pytest.mark.it("should return a dictionary")
def test_returns_dict(sm, mock_secret):
    result = get_secret_dict("mock_secret")
    assert isinstance(result, dict)


@pytest.mark.describe("get_secret_dict()")
@pytest.mark.it("should return correct secret data")
def test_returns_correct_data(sm, mock_secret):
    result = get_secret_dict("mock_secret")
    assert result["host"] == "test_host"
    assert result["port"] == "0000"
    assert result["database"] == "test_database"
    assert result["user"] == "test_user"
    assert result["password"] == "test_password"
