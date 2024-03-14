"""This module contains the definition for transform_location()"""

import pandas as pd

from src.utils.drop_created_and_updated import drop_created_and_updated


def dim_location(address_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from address table in totesys.

    Args:
        address_data (data frame): data frame containing data extracted from totesys address table.

    Returns:
        df (data frame): transformed data frame ready for insertion into dim_location table of data warehouse.
    """

    df = drop_created_and_updated(address_data)

    df.rename(
        columns={
            "address_id": "location_id",
        },
        inplace=True,
    )

    return df
