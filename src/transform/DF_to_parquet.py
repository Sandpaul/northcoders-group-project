"""This file contains the function to convert DataFrames to Parquet
format files"""

import logging
import boto3
import pandas


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def DF_to_parquet(dict, bucket='totesys-etl-processed-data-bucket-teamness-120224'):  # noqa 
    """
    This function should take a pandas DataFrame, write it to a Parquet file,
    and then upload it to the s3 processed bucket.

    ---
    ## Args:
    ---
    - `dataframe`: dictionary- Has 2 keys- timestamp and table_name.
    The value for timestamp is the time from the json file name.
    The value for table_name is the DataFrame of that table.
    Example:
    {
    'timestamp': timestamp,
    'table_name': DataFrame
    }
    ---
    ## Returns:
    ---
    - Should return a message to confirm that the DataFrame has been
    successfully converted to a parquet file and then sent to the s3
    processed bucket
    """
    s3 = boto3.client('s3')
    timestamp = dict['timestamp']
    key_names = list(dict.keys())
    df = None
    table_name = None
    for key_name in key_names:
        if key_name != 'timestamp':
            table_name = key_name
            df = dict[f'{table_name}']
            break

    if not isinstance(df, pandas.DataFrame):
        raise ValueError("Invalid DataFrame provided.")

    split_timestamp = timestamp.split(' ')

    date = split_timestamp[0]

    time = split_timestamp[1]

    s3_key = f"{table_name}/{date}/{time}.parquet"

    parquet_file = pandas.DataFrame.to_parquet(df)
    s3.put_object(
        Bucket=bucket,
        Body=parquet_file,
        Key=s3_key
    )
    logger.info(f"{table_name}/{date}/{time} successfully created.")
