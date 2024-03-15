"""This module contains the definition for `fact_payment()`."""

from src.utils.split_created_and_updated import split_created_and_updated


def fact_payment(payment_data):
    """
    A function to transform data from payment table in totesys database, ready for insertion into fact_payment table in data warehouse.

    Args:
        payment_data (data frame): a data frame containing data extracted from payments table in totesys.

    Returns:
        df (data frame): the transformed data frame ready for insertion into fact_payment table in data warehouse.
    """

    df = split_created_and_updated(payment_data)
    df.drop("company_ac_number", axis=1, inplace=True)
    df.drop("counterparty_ac_number", axis=1, inplace=True)

    return df
