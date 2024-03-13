"""This module contains the definition for transform_sales_order()"""

import pandas as pd


def transform_sales_order(sales_order_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from sales_order table in totesys.

    Args:
        sales_order_data (dict): dict of data from sales_order file from
        ingestion bucket

    Returns:
        sales_order_data_copy (dict): copy of sales_order_data with data dict
        replaced by dataframe
    """

    sales_order_data_copy = sales_order_data.copy()

    row_data = sales_order_data_copy["sales_order"]

    for row in row_data:
        created_at = row["created_at"].split(" ")
        row["created_date"] = created_at[0]
        row["created_time"] = created_at[1]
        row.pop("created_at")

        last_updated = row["last_updated"].split(" ")
        row["last_updated_date"] = last_updated[0]
        row["last_updated_time"] = last_updated[1]
        row.pop("last_updated")

        row["unit_price"] = float(row["unit_price"])

    sales_order_data_copy["sales_order"] = pd.DataFrame.from_records(row_data)

    sales_order_data_copy["sales_order"].rename(
        columns={
            "staff_id": "sales_staff_id",
            "counterparty_id": "counterparty_record_id",
            "currency_id": "currency_record_id",
            "design_id": "design_record_id",
        },
        inplace=True,
    )

    sales_order_data_copy["sales_order"]["last_updated_date"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["last_updated_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    sales_order_data_copy["sales_order"]["last_updated_time"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["last_updated_time"], format="mixed"
    ).dt.time  # noqa
    sales_order_data_copy["sales_order"]["created_date"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["created_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    sales_order_data_copy["sales_order"]["created_time"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["created_time"], format="mixed"
    ).dt.time  # noqa
    sales_order_data_copy["sales_order"]["agreed_payment_date"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["agreed_payment_date"], format="%Y-%m-%d"
    ).dt.date  # noqa
    sales_order_data_copy["sales_order"]["agreed_delivery_date"] = pd.to_datetime(
        sales_order_data_copy["sales_order"]["agreed_delivery_date"], format="%Y-%m-%d"
    ).dt.date  # noqa

    return sales_order_data_copy
