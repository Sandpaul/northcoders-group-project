"""This module contains the definition for `dim_counterparty()`."""

import pandas as pd

from src.utils.get_archived_table_data import get_archived_table_data
from src.utils.get_bucket_name import get_bucket_name


def dim_counterparty(counterparty_data):
    """A function to transform data from counterparty table in totesys ready to be loaded into dim_counterparty table in data warehouse.

    Args:
        counterparty_data (data frame): data frame of counterparty table data.

    Returns:
        dim_counterparty_df (data frame): the transformed data ready to be loaded into dim_counterparty table.
    """

    bucket_name = get_bucket_name("ingestion")

    address_df = get_archived_table_data(table_name="address", bucket_name=bucket_name)

    address_df.drop(
        columns=[
            "created_at",
            "last_updated",
        ],
        inplace=True,
    )

    counterparty_df = counterparty_data.copy(deep=True)

    counterparty_df = counterparty_df[
        [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
        ]
    ]

    dim_counterparty_df = pd.merge(
        counterparty_df,
        address_df,
        left_on="legal_address_id",
        right_on="address_id",
    )

    dim_counterparty_df.drop(
        columns=[
            "legal_address_id",
            "address_id",
        ],
        inplace=True,
    )

    dim_counterparty_df.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        },
        inplace=True,
    )

    return dim_counterparty_df
