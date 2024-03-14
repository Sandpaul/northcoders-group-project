"""This module contains the definition for transform_currency()."""

import ccy


def dim_currency(currency_data):
    """A function to transform data from totesys currency table ready for insertion into data warehouse dim_currency table.

    Args:
        currency_data (dataframe): A pandas dataframe representing data extracted from currency table of totesys database.
        
        It will:
            - Drop the created_at and last_updated columns.
            - Use currency codes to populate a currency name column.

    Returns:
        df (dataframe): A transformed dataframe ready for insertion into dim_currency table of data warehouse.
    """
    df = currency_data.copy(deep=True)

    df.drop("created_at", axis=1, inplace=True)
    df.drop("last_updated", axis=1, inplace=True)

    currency_codes = df["currency_code"].tolist()
    currency_names = [ccy.currency(c).name for c in currency_codes]

    df["currency_name"] = currency_names

    return df
