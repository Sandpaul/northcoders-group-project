"""This file contains th test suite for the functions in the
load.py file.
"""

import os
from moto import mock_aws
from unittest.mock import patch
import boto3
import json
import pytest
import pandas as pd
from src.transform.DF_to_parquet import DF_to_parquet
from src.load.load import (
    transform_parquet_to_dataframe,
    load_dataframe_to_database,
    lambda_handler,
)


@pytest.fixture
def valid_event():
    with open("test/test_load/test_data/valid_load_test_event.json") as v:
        event = json.loads(v.read())
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
    return s3.create_bucket(
        Bucket="totesys-etl-processed-data-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
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
        SecretString='{"host" : "test_host","port" : "test_port","database" : "test_database","user" : "test_user","password" : "test_password"}',
    )


@pytest.fixture
def example_dataframe():
    return pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})


@pytest.fixture
def example_dict(example_dataframe):
    return {
        "timestamp": "2022-11-03 14:20:51.563",
        "table_name": example_dataframe,
    }


@mock_aws
@pytest.mark.describe("transform_parquet_to_dataframe")
@pytest.mark.it("returns list of tablename and dataframe")
def test_function_returns_list_of_dataframe_and_tablename(
    bucket, s3, example_dict
):  # noqa
    DF_to_parquet(example_dict)
    file_path = "table_name/2022-11-03/14:20:51.563.parquet"
    result = transform_parquet_to_dataframe(file_path)
    assert isinstance(result, list)
    assert len(result) == 2
    assert isinstance(result[0], str)
    assert type(result[1]).__name__ == "DataFrame"


@pytest.mark.describe("load_dataframe_to_database()")
@pytest.mark.it("should invoke transform_parquet_to_dataframe")
@patch("src.load.load.transform_parquet_to_dataframe")
@patch("src.load.load.create_engine")
def test_load_dataframe_to_database_calls_transform_parquet_to_dataframe(
    transform_parquet_to_dataframe_mock,
    create_engine_mock,
    bucket,
    example_dict,
    sm,
    mock_dw_credentials,
):  # noqa
    DF_to_parquet(example_dict)
    file_path = "table_name/2022-11-03/14:20:51.563.parquet"
    load_dataframe_to_database(file_path)
    transform_parquet_to_dataframe_mock.assert_called_once()


@pytest.mark.describe("load_dataframe_to_database()")
@pytest.mark.it("should connect to data warehouse")
@patch("src.load.load.transform_parquet_to_dataframe")
@patch("src.load.load.create_engine")
def test_connection_to_warehouse(
    create_engine_mock,
    transform_parquet_to_dataframe_mock,
    bucket,
    example_dict,
    sm,
    mock_dw_credentials,
):  # noqa
    DF_to_parquet(example_dict)
    file_path = "table_name/2022-11-03/14:20:51.563.parquet"
    load_dataframe_to_database(file_path)
    create_engine_mock.assert_called_once_with(
        "postgresql+pg8000://test_user:test_password@test_host:test_port/test_database"
    )


@pytest.mark.describe("lambda_handler()")
@pytest.mark.it("lambda handler should cause all other functions to run")
@patch("src.load.load.grab_file_name")
@patch("src.load.load.load_dataframe_to_database")
def test_lambda_handler_runs_all_other_functions(
    load_dataframe_to_database_mock, grab_file_name_mock, valid_event
):  # noqa
    lambda_handler(valid_event, None)
    load_dataframe_to_database_mock.assert_called_once()
    grab_file_name_mock.assert_called_once()
