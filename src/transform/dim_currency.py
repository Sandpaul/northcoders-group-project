"""This module contains the definition for transform_currency()."""

import pandas as pd


def transform_currency(currency_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from currency table in totesys.

    Args:
        currency_data (dict): dict of data from currency file from
        ingestion bucket

    Returns:
        currency_data_copy (dict): copy of currency with data dict
        replaced by dataframe
    """
    currency_data_copy = currency_data.copy()
    row_data = currency_data_copy["currency"]

    for row in row_data:
        row.pop("created_at")
        row.pop("last_updated")
        if row["currency_code"] == 'GBP':
            row["currency_name"] = 'British Pound'
        elif row["currency_code"] == 'USD':
            row["currency_name"] = 'US dollar'
        elif row["currency_code"] == 'EUR':
            row["currency_name"] = 'euro'

    currency_data_copy["currency"] = pd.DataFrame.from_records(row_data)

    currency_data_copy["currency"]["last_updated_date"] = "1970-01-01"
    currency_data_copy["currency"]["last_updated_time"] = "00:00"

    currency_data_copy["currency"]['last_updated_date'] = pd.to_datetime(
        currency_data_copy["currency"]['last_updated_date'], format="%Y-%m-%d").dt.date  # noqa
    currency_data_copy["currency"]['last_updated_time'] = pd.to_datetime(
        currency_data_copy["currency"]['last_updated_time'], format="%H:%M").dt.time  # noqa

    currency_data_copy["currency"].insert(
        0, 'currency_record_id', range(
            1, len(
                currency_data_copy["currency"]) + 1))

    return currency_data_copy
