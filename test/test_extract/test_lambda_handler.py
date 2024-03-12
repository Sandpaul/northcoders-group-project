"""This module contains the test suite for lambda_handler() function
for the extraction lambda"""

from unittest.mock import patch
import pytest
from src.extract.lambda_handler import lambda_handler


def test_lambda_handler_success():
    """lambda_handler should invoke retrieve_data_from_totesys,
    sql_to_list_of_dicts and json_file_maker."""
    with patch('src.extract.lambda_handler.retrieve_data_from_totesys') as mock_main:  # noqa
        mock_main.return_value = [{'table1': [{'key': 'value'}]},
                                  {'table2': [{'key2': 'value2'}]}]
        with patch('src.extract.lambda_handler.sql_to_list_of_dicts') as mock_sql_list:  # noqa
            with patch('src.extract.lambda_handler.json_file_maker') as mock_json_maker:  # noqa
                lambda_handler({}, {})
                mock_main.assert_called_once()
                mock_sql_list.assert_called_with(
                    {'table2': [{'key2': 'value2'}]})
                assert mock_json_maker.called is True


def test_lambda_handler_runtime_error():
    """lamda_handler should sucessfully raise runtime errors."""
    with patch('src.extract.lambda_handler.retrieve_data_from_totesys') as mock_main:  # noqa
        mock_main.side_effect = Exception('test runtime error')

        with pytest.raises(RuntimeError):
            lambda_handler({}, {})
