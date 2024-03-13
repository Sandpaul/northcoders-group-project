"""This module contains the test suite for create_current_timestamp(),
get_timestamp(), update_timestamp(), connect_to_totesys(),
retrieve_data_from_table() and retrieve_data_from_totesys()"""

import os
from unittest import mock
from unittest.mock import MagicMock, patch, call
import datetime
import unittest
import pytest
import pg8000
import boto3
from moto import mock_aws
from src.extract.extract import (
    retrieve_data_from_totesys,
    connect_to_totesys,
    create_current_timestamp,
    get_timestamp,
    retrieve_data_from_table,
    update_timestamp,
)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def ssm(aws_credentials):
    """Create mock ssm client."""
    with mock_aws():
        yield boto3.client("ssm", region_name='eu-west-2')


@pytest.fixture
def parameter(ssm):
    """Create mock ssm parameter."""
    return ssm.put_parameter(
        Name="last_ingested_timestamp",
        Type="String",
        Value="1970-01-01 00:00:00.000000"
    )

@pytest.fixture(scope="function")
def sm(aws_credentials):
    """Mock secretsmanager client"""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mock_db_credentials(sm):
    """Create mock db credentials."""
    return sm.create_secret(
        Name="db_credentials",
        SecretString='{"host" : "test_host","port" : "test_port","database" : "test_database","user" : "test_user","password" : "test_password"}'
    )
    

@pytest.mark.describe("create_current_timestamp()")
@pytest.mark.it("create_current_timestamp() should return a string")
def test_create_current_timestamp():
    """create_current_timestamp() should return a string
    """    
    result = create_current_timestamp()
    assert isinstance(result, str)


@pytest.mark.describe("get_timestamp()")
@pytest.mark.it("should return correct timestamp")
def test_get_timestamp_retrieves_paramater_from_aws_systems_manager(parameter):
    """get_timestamp() should successfully retrieve
    a parameter from AWS systems manager"""
    result = get_timestamp("last_ingested_timestamp")
    assert result["Parameter"]["Value"] == "1970-01-01 00:00:00.000000"


@pytest.mark.describe("update_timestamp()")
@pytest.mark.it("should update successfully update param in AWS ssm")
def tests_retrieve_from_totesys_updates_last_ingested_timestamp_param(ssm):
    """update_timestamp() should successfully update param in AWS ssm
    """
    update_timestamp("demo_put_timestamp", "hello")
    expected = "hello"
    assert get_timestamp("demo_put_timestamp")["Parameter"]["Value"]\
        == expected
    update_timestamp("demo_put_timestamp", "goodbye")
    expected = "goodbye"
    assert get_timestamp("demo_put_timestamp")["Parameter"]["Value"]\
        == expected


@pytest.mark.describe("connect_to_totesys()")
@pytest.mark.it("should sucessfully connect to totesys db")
def test_connection_to_totesys(sm, mock_db_credentials):
    """connect_to_totesys() should successfully connect to totesys db.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        connect_to_totesys()
        mock_conn.assert_called_once_with(
            host="test_host",
            port="test_port",
            database="test_database",
            user="test_user",
            password="test_password"
        )


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct timestamp")
def test_correct_timestamp_returned_in_result(sm, mock_db_credentials):
    """retrieve_data_from_table() should return the correct current timestamp.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        mock_cursor = MagicMock(name="mock_cursor")
        mock_cursor.fetchall.return_value = [1, 2, 3]
        mock_conn.cursor.return_value = mock_cursor
        current_timestamp = "2024-02-15 15:19:53.816597"
        table_name = "sales_order"
        last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
        result = retrieve_data_from_table(
            table_name=table_name,
            current_timestamp=current_timestamp,
            conn=mock_conn,
            last_ingested_timestamp=last_ingested_timestamp
        )
        assert result["timestamp"] == current_timestamp


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct tablename")
def test_correct_tablename_returned_in_result(sm, mock_db_credentials):
    """retrieve_data_from_table() should return correct table name.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        mock_cursor = MagicMock(name="mock_cursor")
        mock_cursor.fetchall.return_value = [1, 2, 3]
        mock_conn.cursor.return_value = mock_cursor
        current_timestamp = "2024-02-15 15:19:53.816597"
        table_name = "sales_order"
        last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
        result = retrieve_data_from_table(
            table_name=table_name,
            current_timestamp=current_timestamp,
            conn=mock_conn,
            last_ingested_timestamp=last_ingested_timestamp
        )
        assert result["table_name"] == table_name


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct columns")
def test_correct_columns_returned_in_result(sm, mock_db_credentials):
    """retrieve_data_from_table() should return correct columns.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        mock_cursor = MagicMock(name="mock_cursor")
        mock_cursor.fetchall.return_value = [1, 2, 3]
        mock_cursor.description = [["currency_id"], ["currency_code"], ["created_at"], ["last_updated"]]
        mock_conn.cursor.return_value = mock_cursor
        current_timestamp = "2024-02-15 15:19:53.816597"
        table_name = "currency"
        last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
        expected = ["currency_id", "currency_code", "created_at", "last_updated"]
        result = retrieve_data_from_table(
            table_name=table_name,
            current_timestamp=current_timestamp,
            conn=mock_conn,
            last_ingested_timestamp=last_ingested_timestamp
        )
        assert result["table_columns"] == expected


@pytest.mark.describe('retrieve_data_from_table()')
@pytest.mark.it('should return correct rows')
def test_correct_rows_returned_by_retrieve_data_from_table(sm, mock_db_credentials):
    """retrieve_data_from_table() should return correct rows
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
        expected_rows = (
            [
                1,
                "GBP",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
            [
                2,
                "USD",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
            [
                3,
                "EUR",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
        )
        mock_cursor = MagicMock(name="mock_cursor")
        mock_cursor.fetchall.return_value = expected_rows
        mock_cursor.description = [["currency_id"], ["currency_code"], ["created_at"], ["last_updated"]]
        mock_conn.cursor.return_value = mock_cursor
        result = retrieve_data_from_table(
            table_name="currency",
            current_timestamp="2024-02-16 10:30:53.816597",
            last_ingested_timestamp=last_ingested_timestamp,
            conn=mock_conn
        )
        assert result["table_rows"] == expected_rows


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return None when row length is 0")
def test_nothing_is_returned_when_rows_length_is_0(sm, mock_db_credentials):
    """retrieve_data_from_tables() should return nothing when rows length is 0.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
        result = retrieve_data_from_table(
                table_name="currency",
                current_timestamp="2024-02-16 10:30:53.816597",
                last_ingested_timestamp=last_ingested_timestamp,
                conn=mock_conn
            )
        assert result is None


@pytest.mark.describe("retrieve_data_from_totesys()")
@pytest.mark.it("should call retrieve_data_from_table() correctly")
def test_retrieve_from_totesys_calls_retrieve_from_table(ssm, parameter, sm, mock_db_credentials):
    """retrieve_data_from_totesys should call retrieve_data_from_table() correctly for each table name."""
    test_li_timestamp = "2200-01-01"
    test_current_timestamp = "test_current_timestamp"
    with mock.patch("src.extract.extract.retrieve_data_from_table") as mock_retrieve_data_from_table:
        with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
            result = retrieve_data_from_totesys(
                last_ingested_timestamp=test_li_timestamp,
                current_timestamp = test_current_timestamp,
                conn=mock_conn,
            )
            calls = [
                call("counterparty", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("currency", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("address", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("department", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("design", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("staff", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("sales_order", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("payment", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("payment_type", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("purchase_order", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01"),
                call("transaction", "test_current_timestamp", mock_conn, last_ingested_timestamp="2200-01-01")
            ]
            mock_retrieve_data_from_table.assert_has_calls(calls)


@pytest.mark.describe("retrieve_data_from_totesys()")
@pytest.mark.it("should return same timestamp for each dict")
def test_retrieve_from_totesys_has_correct_timestamp_on_each_dict(sm, mock_db_credentials, ssm, parameter):
    """retrieve_data_from_totesys should return
    a list of dicts, each one should have the same timestamp"""
    test_current_timestamp = "test_timestamp"
    with mock.patch("src.extract.extract.retrieve_data_from_table", return_value = {"timestamp": test_current_timestamp}):
        with mock.patch("src.extract.extract.pg8000.connect"):
            result = retrieve_data_from_totesys(
            current_timestamp=test_current_timestamp)
            for i in result:
                assert i["timestamp"] == "test_timestamp"


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Programming Error")
def test_programming_error_table():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = pg8000.ProgrammingError
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
    with pytest.raises(pg8000.ProgrammingError):
        retrieve_data_from_table(
            table_name="table_name",
            current_timestamp="current_timestamp",
            last_ingested_timestamp=last_ingested_timestamp,
            conn=mock_conn
        )


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Unexpected Error")
def test_unexpected_error_table():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    last_ingested_timestamp = {"Parameter":
                                {"Value": "2020-02-19 10:47:13.137440"}}
    with pytest.raises(RuntimeError):
        retrieve_data_from_table(
            table_name="table_name",
            current_timestamp="current_timestamp",
            last_ingested_timestamp=last_ingested_timestamp,
            conn=mock_conn,
        )


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Key Error")
def test_value_error_table():
    with pytest.raises(KeyError) as exception_info:
        retrieve_data_from_table("your_table_name",
                                 "current_timestamp",
                                 last_ingested_timestamp={},
                                 conn=1)
    assert "KeyError" in str(exception_info.value)


# class TestRetrieveDataFromTotesys(unittest.TestCase):
#     @patch('src.extract.extract.retrieve_data_from_table')
#     def test_value_error(self, mock_retrieve_data_from_table):
#         mock_retrieve_data_from_table.side_effect = \
#             ValueError("Mocked ValueError")
#         with self.assertRaisesRegex(RuntimeError,
#                                     "ValueError occurred: Mocked ValueError"):
#             retrieve_data_from_totesys()


# @pytest.mark.describe("retrieve_data_from_totesys()")
# @pytest.mark.it("should return a unexpected Error")
# def test_unexpected_error_totesys(sm, mock_db_credentials):
#     with mock.patch('src.extract.extract.retrieve_data_from_table',
#                     side_effect=Exception("Mocked exception")):
#         with unittest.TestCase.assertRaises(None, RuntimeError) \
#          as context:
#             retrieve_data_from_totesys(current_timestamp=None,
#                                        last_ingested_timestamp=None)
#         assert str(context.exception) == \
#             "An unexpected error occurred: Mocked exception"
