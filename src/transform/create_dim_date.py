"""This module contains the definition of create_dim_date()."""

import pandas as pd


def create_dim_date(start_date, end_date):
    """Function to create data for a dim_date table.

    Args:
        start_date (str): required start date for dim_date table
        end_date (str): required end date for dim_date table

    Returns:
        dim_date_df: Data frame.
    """

    df_date = pd.DataFrame(
        {"date_id": pd.date_range(start=f"{start_date}", end=f"{end_date}", freq="D")}
    )  # noqa

    df_date["year"] = df_date.date_id.dt.year
    df_date["month"] = df_date.date_id.dt.month
    df_date["day"] = df_date.date_id.dt.day
    df_date["day_of_week"] = df_date.date_id.dt.day_of_week + 1
    df_date["day_name"] = df_date.date_id.dt.day_name()
    df_date["month_name"] = df_date.date_id.dt.month_name()
    df_date["quarter"] = df_date.date_id.dt.quarter
    df_date["last_updated_date"] = "1970-01-01"
    df_date["last_updated_time"] = "00:00"

    return df_date
