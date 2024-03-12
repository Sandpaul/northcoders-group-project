"""This module contains the definition for transform_location()"""

import pandas as pd


def transform_location(location_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from location table in totesys.

    Args:
        location_data (dict): dict of data from location file from
        ingestion bucket

    Returns:
        location_data_copy (dict): copy of location_data with data dict
        replaced by dataframe
    """

    location_data = location_data.copy()
    location_rows = location_data['address']

    location_df = pd.DataFrame.from_records(location_rows)
    location_df.drop(columns=['created_at', 'last_updated'], inplace=True)

    location_df["last_updated_date"] = "1970-01-01"
    location_df["last_updated_time"] = "00:00"

    location_df['last_updated_date'] = pd.to_datetime(
        location_df['last_updated_date'], format="%Y-%m-%d").dt.date
    location_df['last_updated_time'] = pd.to_datetime(
        location_df['last_updated_time'], format="%H:%M").dt.time

    location_data['location'] = location_df
    del location_data['address']

    return location_data
