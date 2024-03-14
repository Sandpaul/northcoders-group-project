"""This module contains the definition for transform_currency()."""

import ccy


def dim_currency(currency_data):
    
    df = currency_data.copy(deep=True)
    df.drop('created_at', axis=1, inplace=True)
    df.drop('last_updated', axis=1, inplace=True)
    currency_codes = df["currency_code"].tolist()
    currency_names = [
        ccy.currency(c).name for c in currency_codes
    ]
    df["currency_name"] = currency_names
    return df