"""This file contains thw test suite for the load `lambda_handler()`.
"""

import json
import logging
import os
from unittest.mock import patch

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from src.transform.df_to_parquet import df_to_parquet
from src.load.load import lambda_handler


@pytest.fixture
def valid_event():
    with open("test/test_load/test_data/valid_load_test_event.json") as v:
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
        Bucket="totesys-etl-processed-data-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("test/test_transform/test_data/test_transaction_data.json") as f:
        data = f.read()
        json_data = json.loads(data)
        df = pd.DataFrame.from_records(json_data["transaction"])
        data_to_write = pd.DataFrame.to_parquet(df)
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-processed-data-bucket-teamness-120224",
            Key="dim_transaction/2024-02-22/18:00:20.106733.parquet",
        )


@pytest.fixture(scope="function")
def sm(aws_credentials):
    """Mock secretsmanager client"""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mock_dw_credentials(sm):
    """Create mock dw credentials."""
    return sm.create_secret(
        Name="dw_credentials",
        SecretString='{"host" : "test_host","port" : "0000","database" : "test_database","user" : "test_user","password" : "test_password"}',
    )


@pytest.mark.describe("lambda_handler()")
@pytest.mark.it("should log correct file name")
@patch("src.load.load.create_engine")
def test_logs_correct_file_name(
    create_engine_mock,
    valid_event,
    bucket,
    caplog,
    mock_dw_credentials,
):
    with caplog.at_level(logging.INFO):
        lambda_handler(valid_event, {})
        assert (
            "File name is dim_transaction/2024-02-22/18:00:20.106733.parquet!"
            in caplog.text
        )


@pytest.mark.describe("lambda_handler()")
@pytest.mark.it("should log correct bucket name")
@patch("src.load.load.create_engine")
def test_logs_correct_bucket_name(
    create_engine_mock,
    valid_event,
    bucket,
    caplog,
    mock_dw_credentials,
):
    with caplog.at_level(logging.INFO):
        lambda_handler(valid_event, {})
        assert (
            "dim_transaction/2024-02-22/18:00:20.106733.parquet retrieved from totesys-etl-processed-data-bucket-teamness-120224"
            in caplog.text
        )


@pytest.mark.describe("lambda_handler()")
@pytest.mark.it("should log correct table name")
@patch("src.load.load.create_engine")
def test_logs_correct_table_name(
    create_engine_mock,
    valid_event,
    bucket,
    caplog,
    mock_dw_credentials,
):
    with caplog.at_level(logging.INFO):
        lambda_handler(valid_event, {})
        assert "dim_transaction data:" in caplog.text


@pytest.mark.describe("lambda_handler()")
@pytest.mark.it("should connect to data warehouse")
@patch("src.load.load.create_engine")
def test_connection_to_warehouse(
    create_engine_mock,
    valid_event,
    bucket,
    sm,
    mock_dw_credentials,
):
    lambda_handler(valid_event, {})
    create_engine_mock.assert_called_once_with(
        "postgresql+pg8000://test_user:test_password@test_host:0000/test_database"
    )
