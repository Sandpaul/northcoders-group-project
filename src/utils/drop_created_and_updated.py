"""This module contains the definition for `drop_created_and_updated()`."""


def drop_created_and_updated(data_frame):
    """A function to drop the `created_at` and `last_updated` columns from a data frame.

    Args:
        data_frame (data frame): the data frame from which created_at and last_updated columns are to be dropped.

    Returns:
        df (data frame): the new data frame with created_at and last_updated columns removed.
    """

    df = data_frame.copy(deep=True)

    df.drop("created_at", axis=1, inplace=True)
    df.drop("last_updated", axis=1, inplace=True)

    return df
