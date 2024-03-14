"""This module contains the definition for `split_created_and_updated()`."""

import pandas as pd

def split_created_and_updated(data_frame):
    """A function to split the `created_at` column into `created_date` and `created_time` and `last_updated` into `last_updated_date` and `last_updated_time`.

    Args:
        data_frame (data frame): the data frame from which created_at and last_updated columns are to be dropped.

    Returns:
        df (data frame): the new data frame with created_at and last_updated columns split into date and time.
    """

    df = data_frame.copy(deep=True)
    
    df["created_at"] = pd.to_datetime(df["created_at"], format="%Y-%m-%d %H:%M:%S.%f")
    df["created_date"] = df["created_at"].dt.date
    df["created_time"] = df["created_at"].dt.time
    
    # df["last_updated"] = pd.to_datetime(format="%Y-%m-%d %H:%M:%S.%f")
    
    
    

    # df.drop("created_at", axis=1, inplace=True)
    # df.drop("last_updated", axis=1, inplace=True)

    return df
