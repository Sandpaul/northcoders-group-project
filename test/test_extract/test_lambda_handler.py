"""This module contains the test suite for lambda_handler() function
for the extraction lambda"""

import os
from unittest.mock import patch
import boto3
import pytest
from moto import mock_aws
from src.extract.lambda_handler import lambda_handler


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def ssm(aws_credentials):
    """Create mock ssm client."""
    with mock_aws():
        yield boto3.client("ssm", region_name="eu-west-2")


@pytest.fixture
def parameter(ssm):
    """Create mock ssm parameter."""
    return ssm.put_parameter(
        Name="last_ingested_timestamp",
        Type="String",
        Value="1970-01-01 00:00:00.000000",
    )


def test_lambda_handler_success(ssm, parameter):
    """lambda_handler should invoke retrieve_data_from_totesys,
    sql_to_list_of_dicts and parquet_file_maker."""
    with patch(
        "src.extract.lambda_handler.retrieve_data_from_totesys"
    ) as mock_main:  # noqa
        mock_main.return_value = [
            {"table1": [{"key": "value"}]},
            {"table2": [{"key2": "value2"}]},
        ]
        with patch(
            "src.extract.lambda_handler.sql_to_list_of_dicts"
        ) as mock_sql_list:  # noqa
            with patch(
                "src.extract.lambda_handler.parquet_file_maker"
            ) as mock_parquet_maker:  # noqa
                lambda_handler({}, {})
                mock_main.assert_called_once()
                mock_sql_list.assert_called_with({"table2": [{"key2": "value2"}]})
                assert mock_parquet_maker.called is True


def test_lambda_handler_runtime_error():
    """lamda_handler should sucessfully raise runtime errors."""
    with patch(
        "src.extract.lambda_handler.retrieve_data_from_totesys"
    ) as mock_main:  # noqa
        mock_main.side_effect = Exception("test runtime error")

        with pytest.raises(RuntimeError):
            lambda_handler({}, {})
