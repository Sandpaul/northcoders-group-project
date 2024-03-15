"""This module contains the definition for `fact_sales_order()`."""

from src.utils.split_created_and_updated import split_created_and_updated


def fact_sales_order(sales_order_data):
    """
    A function to transform data from sales_order table in totesys database, ready for insertion into fact_sales_order table in data warehouse.

    Args:
        sales_order_data (data frame): a data frame containing data extracted from sales_order table in totesys.

    Returns:
        df (data frame): the transformed data frame ready for insertion into fact_sales_order table in data warehouse.
    """

    df = split_created_and_updated(sales_order_data)

    df.rename(
        columns={
            "staff_id": "sales_staff_id",
        },
        inplace=True,
    )

    return df
