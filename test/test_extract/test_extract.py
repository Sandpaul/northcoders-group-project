"""This module contains the test suite for create_current_timestamp(),
get_timestamp(), update_timestamp(), connect_to_totesys(),
retrieve_data_from_table() and retrieve_data_from_totesys()"""

from unittest import mock
from unittest.mock import MagicMock, patch
import datetime
import unittest
import pytest
import pg8000
from src.extract.extract import (
    retrieve_data_from_totesys,
    connect_to_totesys,
    create_current_timestamp,
    get_timestamp,
    retrieve_data_from_table,
    update_timestamp,
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
def test_get_timestamp_retrieves_paramater_from_aws_systems_manager():
    """get_timestamp() should successfully retrieve
    a parameter from AWS systems manager"""
    result = get_timestamp("demo-timestamp")
    assert result["Parameter"]["Value"] == "2022-04-05 10:00:00.111111"


@pytest.mark.describe("update_timestamp()")
@pytest.mark.it("should update successfully update param in AWS ssm")
def tests_retrieve_from_totesys_updates_last_ingested_timestamp_param():
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
def test_connection_to_totesys():
    """connect_to_totesys() should successfully connect to totesys db.
    """
    with mock.patch("src.extract.extract.pg8000.connect") as mock_conn:
        connect_to_totesys()
        mock_conn.assert_called_once_with(
            host=
            port=
            database=
            user=
            password=
        )


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct timestamp")
def test_correct_timestamp_returned_in_result():
    """retrieve_data_from_table() should return the correct current timestamp.
    """
    current_timestamp = "2024-02-15 15:19:53.816597"
    table_name = "sales_order"
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    result = retrieve_data_from_table(
        table_name, current_timestamp,
        last_ingested_timestamp=last_ingested_timestamp
    )
    assert result["timestamp"] == current_timestamp


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct tablename")
def test_correct_tablename_returned_in_result():
    """retrieve_data_from_table() should return correct table name.
    """
    current_timestamp = "2024-02-15 15:19:53.816597"
    table_name = "sales_order"
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    result = retrieve_data_from_table(
        table_name, current_timestamp,
        last_ingested_timestamp=last_ingested_timestamp
    )
    assert result["table_name"] == table_name


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return correct columns")
def test_correct_columns_returned_in_result():
    """retrieve_data_from_table() should return correct columns.
    """
    current_timestamp = "2024-02-15 15:19:53.816597"
    table_name = "currency"
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    expected = ["currency_id", "currency_code", "created_at", "last_updated"]
    result = retrieve_data_from_table(
        table_name, current_timestamp,
        last_ingested_timestamp=last_ingested_timestamp
    )
    assert result["table_columns"] == expected


@pytest.mark.describe('retrieve_data_from_table()')
@pytest.mark.it('should return correct rows')
def test_correct_rows_returned_by_retrieve_data_from_table():
    """retrieve_data_from_table() should return correct rows
    """
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    result = retrieve_data_from_table(
        "currency",
        "2024-02-16 10:30:53.816597",
        last_ingested_timestamp=last_ingested_timestamp,
    )
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
    assert result["table_rows"] == expected_rows


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return None when row length is 0")
def test_nothing_is_returned_when_rows_length_is_0():
    """retrieve_data_from_tables() should return nothing when rows length is 0.
    """
    test_mock = MagicMock()
    result = retrieve_data_from_table(
        "currency", "2024-02-16 10:30:53.816597", test_mock
    )
    assert result is None


@pytest.mark.describe("retrieve_data_from_totesys()")
@pytest.mark.it("should return dict for each table")
def test_retrieve_from_totesys_returns_dict_for_each_table():
    """retrieve_data_from_totesys should return a list with one dictionary per
    totesys table that data has been retrieved from"""
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    result = retrieve_data_from_totesys(
        last_ingested_timestamp=last_ingested_timestamp)
    assert len(result) == 11


@pytest.mark.describe("retrieve_data_from_totesys()")
@pytest.mark.it("should return same timestamp for each dict")
def test_retrieve_from_totesys_has_correct_timestamp_on_each_dict():
    """retrieve_data_from_totesys should return
    a list of dicts, each one should have the same timestamp"""
    last_ingested_timestamp = {"Parameter":
                               {"Value": "2020-02-19 10:47:13.137440"}}
    result = retrieve_data_from_totesys(
        last_ingested_timestamp=last_ingested_timestamp)
    timestamp = result[0]["timestamp"]
    for i in result:
        assert i["timestamp"] == timestamp


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Programming Error")
def test_programming_error_table():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = pg8000.ProgrammingError
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    with pytest.raises(pg8000.ProgrammingError):
        retrieve_data_from_table("your_table_name",
                                 "current_timestamp",
                                 conn=mock_conn)


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Unexpected Error")
def test_unexpected_error_table():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    with pytest.raises(RuntimeError):
        retrieve_data_from_table("your_table_name",
                                 "current_timestamp",
                                 conn=mock_conn)


@pytest.mark.describe("retrieve_data_from_table()")
@pytest.mark.it("should return a Key Error")
def test_value_error_table():
    with pytest.raises(KeyError) as exception_info:
        retrieve_data_from_table("your_table_name",
                                 "current_timestamp",
                                 last_ingested_timestamp={})
    assert "KeyError" in str(exception_info.value)


class TestRetrieveDataFromTotesys(unittest.TestCase):
    @patch('src.extract.extract.retrieve_data_from_table')
    def test_value_error(self, mock_retrieve_data_from_table):
        mock_retrieve_data_from_table.side_effect = \
            ValueError("Mocked ValueError")
        with self.assertRaisesRegex(RuntimeError,
                                    "ValueError occurred: Mocked ValueError"):
            retrieve_data_from_totesys()


@pytest.mark.describe("retrieve_data_from_totesys()")
@pytest.mark.it("should return a unexpected Error")
def test_unexpected_error_totesys():
    with mock.patch('src.extract.extract.retrieve_data_from_table',
                    side_effect=Exception("Mocked exception")):
        with unittest.TestCase.assertRaises(None, RuntimeError) \
         as context:
            retrieve_data_from_totesys(current_timestamp=None,
                                       last_ingested_timestamp=None)
        assert str(context.exception) == \
            "An unexpected error occurred: Mocked exception"
