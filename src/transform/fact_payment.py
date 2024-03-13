"""This module contains the definition for transform_payment()"""

import pandas as pd


def transform_payment(payment_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from payment table in totesys.

    Args:
        payment_data (dict): dict of data from payment file from
        ingestion bucket

    Returns:
        payment_data_copy (dict): copy of payment_data with data dict
        replaced by dataframe
    """
    payment_data_copy = payment_data.copy()
    row_data = payment_data_copy["payment"]

    for row in row_data:
        created_at = row["created_at"].split(" ")
        row["created_date"] = created_at[0]
        row["created_time"] = created_at[1]
        row.pop("created_at")
        last_updated = row["last_updated"].split(" ")
        row["last_updated_date"] = last_updated[0]
        row["last_updated_time"] = last_updated[1]
        row.pop("last_updated")
        row.pop("company_ac_number")
        row.pop("counterparty_ac_number")
        row["payment_amount"] = float(row["payment_amount"])

    payment_data_copy["payment"] = pd.DataFrame.from_records(row_data)

    payment_data_copy["payment"].rename(
        columns={
            "transaction_id": "transaction_record_id",
            "counterparty_id": "counterparty_record_id",
            "currency_id": "currency_record_id",
            "payment_type_id": "payment_type_record_id",
        },
        inplace=True,
    )

    payment_data_copy["payment"]["last_updated_date"] = pd.to_datetime(
        payment_data_copy["payment"]["last_updated_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    payment_data_copy["payment"]["last_updated_time"] = pd.to_datetime(
        payment_data_copy["payment"]["last_updated_time"], format="mixed"
    ).dt.time  # noqa
    payment_data_copy["payment"]["created_date"] = pd.to_datetime(
        payment_data_copy["payment"]["created_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    payment_data_copy["payment"]["created_time"] = pd.to_datetime(
        payment_data_copy["payment"]["created_time"], format="mixed"
    ).dt.time  # noqa
    payment_data_copy["payment"]["payment_date"] = pd.to_datetime(
        payment_data_copy["payment"]["payment_date"], format="%Y-%m-%d"
    ).dt.date  # noqa

    return payment_data_copy
