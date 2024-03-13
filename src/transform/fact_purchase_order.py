"""This module contains the definition for transform_purchase_order()."""

import pandas as pd


def transform_purchase_order(purchase_order_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from purchase_order table in totesys.

    Args:
        purchase_order_data (dict): dict of data from purchase_order file from
        ingestion bucket

    Returns:
        purchase_order_data_copy (dict): copy of purchase_order_data with
        data dict replaced by dataframe
    """

    purchase_order_data_copy = purchase_order_data.copy()

    row_data = purchase_order_data["purchase_order"]

    for row in row_data:
        created_at = row["created_at"].split(" ")
        row["created_date"] = created_at[0]
        row["created_time"] = created_at[1]
        row.pop("created_at")

        last_updated = row["last_updated"].split(" ")
        row["last_updated_date"] = last_updated[0]
        row["last_updated_time"] = last_updated[1]
        row.pop("last_updated")

        row["item_unit_price"] = float(row["item_unit_price"])

    purchase_order_data_copy["purchase_order"] = pd.DataFrame.from_records(row_data)

    purchase_order_data_copy["purchase_order"].rename(
        columns={
            "staff_id": "staff_record_id",
            "counterparty_id": "counterparty_record_id",
            "currency_id": "currency_record_id",
        },
        inplace=True,
    )

    purchase_order_data_copy["purchase_order"]["last_updated_date"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["last_updated_date"],
        format="%Y-%m-%d",
    ).dt.date  # noqa
    purchase_order_data_copy["purchase_order"]["last_updated_time"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["last_updated_time"], format="mixed"
    ).dt.time  # noqa
    purchase_order_data_copy["purchase_order"]["created_date"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["created_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    purchase_order_data_copy["purchase_order"]["created_time"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["created_time"], format="mixed"
    ).dt.time  # noqa
    purchase_order_data_copy["purchase_order"]["agreed_payment_date"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["agreed_payment_date"],
        format="%Y-%m-%d",
    ).dt.date  # noqa
    purchase_order_data_copy["purchase_order"]["agreed_delivery_date"] = pd.to_datetime(
        purchase_order_data_copy["purchase_order"]["agreed_delivery_date"],
        format="%Y-%m-%d",
    ).dt.date  # noqa

    return purchase_order_data_copy
