"""This module contains definition for transform_payment_type()."""

import pandas as pd


def transform_payment_type(payment_type_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from payment_type table in totesys.

    Args:
        payment_type_data (dict): dict of data from payment_type file from
        ingestion bucket

    Returns:
        payment_type_data_copy (dict): copy of payment_type_data with data dict
        replaced by dataframe
    """
    payment_type_data_copy = payment_type_data.copy()
    row_data = payment_type_data_copy["payment_type"]

    for row in row_data:
        row.pop("created_at")
        row.pop("last_updated")

    payment_type_data_copy["payment_type"] = pd.DataFrame.from_records(row_data)

    payment_type_data_copy["payment_type"]["last_updated_date"] = "1970-01-01"
    payment_type_data_copy["payment_type"]["last_updated_time"] = "00:00"

    payment_type_data_copy["payment_type"]["last_updated_date"] = (
        pd.to_datetime(  # noqa
            payment_type_data_copy["payment_type"]["last_updated_date"],
            format="%Y-%m-%d",
        ).dt.date
    )  # noqa
    payment_type_data_copy["payment_type"]["last_updated_time"] = (
        pd.to_datetime(  # noqa
            payment_type_data_copy["payment_type"]["last_updated_time"], format="%H:%M"
        ).dt.time
    )  # noqa

    payment_type_data_copy["payment_type"].insert(
        0,
        "payment_record_id",
        range(1, len(payment_type_data_copy["payment_type"]) + 1),
    )

    payment_type_data_copy["payment_type"].rename(
        columns={
            "payment_record_id": "payment_type_record_id",
            "payment_type_id": "payment_record_id",
        },
        inplace=True,
    )

    return payment_type_data_copy
