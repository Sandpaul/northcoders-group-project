"""This module contains the definition for `get_archived_table_data()`."""

import pandas as pd
import boto3

from src.utils.parquet_file_reader import parquet_file_reader


def get_archived_table_data(table_name, bucket_name):
    """A function to retrieve all file data from specified table in an s3 bucket and return it as a joined data frame.

    Args:
        table_name (str): string of the table name that you wish to retrieve data for.
        bucket_name (str): name of the s3 bucket where data is stored

    Returns:
        df (data frame): a data frame with all of the data from the passed table name combined.
    """

    s3 = boto3.client("s3")

    response = s3.list_objects(
        Bucket=bucket_name,
        Prefix=f"{table_name}/",
        Delimiter="/",
    )

    subfolders = [
        common_prefix["Prefix"] for common_prefix in response.get("CommonPrefixes", [])
    ]

    merged_df = pd.DataFrame()

    files_list = []

    for folder in subfolders:
        folder_response = s3.list_objects(
            Bucket=bucket_name,
            Prefix=folder,
        )
        folder_objects = folder_response.get("Contents")

        file = [item["Key"] for item in folder_objects]

        files_list.append(file[0])

    for file in files_list:
        file_df = parquet_file_reader(file, bucket_name)

        merged_df = pd.concat([merged_df, file_df], ignore_index=True)

    return merged_df
