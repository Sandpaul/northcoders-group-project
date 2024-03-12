"""This module contains the definition for transform_transaction()."""

import pandas as pd


def transform_transaction(transaction_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from transaction table in totesys.

    Args:
        transaction_data (dict): dict of data from transaction file from
        ingestion bucket

    Returns:
        transaction_data_copy (dict): copy of transaction_data with data
        dict replaced by dataframe
    """
    transaction_data_copy = transaction_data.copy()
    row_data = transaction_data_copy["transaction"]

    for row in row_data:
        row.pop("created_at")
        row.pop("last_updated")
        if row["sales_order_id"] == None:
            row["sales_order_id"] == 0
        if row["purchase_order_id"] == None:
            row["purchase_order_id"] == 0

    transaction_data_copy["transaction"] = pd.DataFrame.from_records(row_data)

    transaction_data_copy["transaction"]["last_updated_date"] = "1970-01-01"
    transaction_data_copy["transaction"]["last_updated_time"] = "00:00"

    transaction_data_copy["transaction"]['last_updated_date'] = pd.to_datetime(
        transaction_data_copy["transaction"]['last_updated_date'], format="%Y-%m-%d").dt.date  # noqa
    transaction_data_copy["transaction"]['last_updated_time'] = pd.to_datetime(
        transaction_data_copy["transaction"]['last_updated_time'], format="%H:%M").dt.time  # noqa

    transaction_data_copy["transaction"].insert(
        0, 'transaction_record_id', range(
            1, len(
                transaction_data_copy["transaction"]) + 1))

    return transaction_data_copy
