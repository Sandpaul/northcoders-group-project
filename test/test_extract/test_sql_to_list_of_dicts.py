"""This file contains the test suite for sql_to_list_of_dicts() only. """

import pytest

from src.extract.sql_to_list_of_dicts import sql_to_list_of_dicts


def test_sql_to_list_of_dicts_valid_data():
    data_dict = {
        "timestamp": "2024-02-15 07:44:47.010000",
        "table_name": "counterparty",
        "table_columns": [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ],
        "table_rows": (
            (1, "Company A", 123, "John Doe", "Jane Doe", "2022-01-01", "2022-01-02"),
            (
                2,
                "Company B",
                456,
                "Alice Smith",
                "Bob Smith",
                "2022-02-01",
                "2022-02-02",
            ),
        ),
    }
    result = sql_to_list_of_dicts(data_dict)
    expected = {
        "timestamp": "2024-02-15 07:44:47.010000",
        "counterparty": [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "Company A",
                "legal_address_id": 123,
                "commercial_contact": "John Doe",
                "delivery_contact": "Jane Doe",
                "created_at": "2022-01-01",
                "last_updated": "2022-01-02",
            },
            {
                "counterparty_id": 2,
                "counterparty_legal_name": "Company B",
                "legal_address_id": 456,
                "commercial_contact": "Alice Smith",
                "delivery_contact": "Bob Smith",
                "created_at": "2022-02-01",
                "last_updated": "2022-02-02",
            },
        ],
    }
    assert result == expected


def test_sql_to_list_of_dicts_missing_timestamp():
    data_dict = {
        "table_name": "counterparty",
        "table_columns": [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ],
        "table_rows": (
            (1, "Company A", 123, "John Doe", "Jane Doe", "2022-01-01", "2022-01-02"),
            (
                2,
                "Company B",
                456,
                "Alice Smith",
                "Bob Smith",
                "2022-02-01",
                "2022-02-02",
            ),
        ),
    }
    with pytest.raises(ValueError, match="Missing timestamp!"):
        sql_to_list_of_dicts(data_dict)


def test_sql_to_list_of_dicts_missing_table_name():
    data_dict = {
        "timestamp": "2024-02-15 07:44:47.010000",
        "table_columns": [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ],
        "table_rows": (
            (1, "Company A", 123, "John Doe", "Jane Doe", "2022-01-01", "2022-01-02"),
            (
                2,
                "Company B",
                456,
                "Alice Smith",
                "Bob Smith",
                "2022-02-01",
                "2022-02-02",
            ),
        ),
    }
    with pytest.raises(ValueError, match="Missing tablename!"):
        sql_to_list_of_dicts(data_dict)


def test_sql_to_list_of_dicts_missing_table_rows():
    data_dict = {
        "timestamp": "2024-02-15 07:44:47.010000",
        "table_name": "counterparty",
        "table_columns": [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ],
    }
    with pytest.raises(ValueError, match="Missing rows!"):
        sql_to_list_of_dicts(data_dict)


def test_sql_to_list_of_dicts_missing_column_names():
    data_dict = {
        "timestamp": "2024-02-15 07:44:47.010000",
        "table_name": "counterparty",
        "table_rows": (
            (1, "Company A", 123, "John Doe", "Jane Doe", "2022-01-01", "2022-01-02"),
            (
                2,
                "Company B",
                456,
                "Alice Smith",
                "Bob Smith",
                "2022-02-01",
                "2022-02-02",
            ),
        ),
    }
    with pytest.raises(ValueError, match="Missing column names!"):
        sql_to_list_of_dicts(data_dict)
