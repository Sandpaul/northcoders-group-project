"""This module contains the definition for `parquet_file_maker()`"""

import logging
import pandas as pd
import boto3


def parquet_file_maker(data):
    """
    A function to take a list of dictionaries, write them
    to a dataframe and send them to an s3 bucket.

    Args:
        data (list of dictionaries)
            e.g. {
                timestamp: time,
                  table_name: [
                        {'id': 1, 'make': 'Ford', 'model': 'Mustang'},
                        {'id': 2, 'make': 'Toyota', 'model': 'Yaris'},
                        {'id': 3, 'make': 'Honda', 'model': 'Civic'},
                        {'id': 4, 'make': 'BMW', 'model': 'X5'}
                        ]
                    }

    Return:
        Message that cofirms parquet file has
        been created and stored successfully.
            e.g. "`tablename/date/time.parquet` successfully created."
                 "`staff/2024-02-14/10:00:00.parquet` successfully created."

    Raises:
        ValueError if data list is empty.
        TypeError if data list contains any non-dict items.
        KeyError if timestamp key is not present.

    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    dict_keys = list(data.keys())

    if data[dict_keys[1]] == []:
        logger.error("ValueError - no data.")
        raise ValueError("Data is empty")

    for i in data[dict_keys[1]]:
        if isinstance(i, dict) is False:
            logger.error(f"TypeError - {i} is not a dictionary")
            raise TypeError("There is an element in the list that is not a dictionary")  # noqa

    if "timestamp" not in dict_keys:
        logger.error("KeyError - no timestamp.")
        raise KeyError("No timestamp.")

    split_timestamp = data['timestamp'].split(" ")
    date = split_timestamp[0]
    time = split_timestamp[1]
    table_name = dict_keys[1]
    data_to_write = data[table_name]

    s3_client = boto3.client("s3")

    df = pd.DataFrame.from_records(data_to_write)

    parquet_file = pd.DataFrame.to_parquet(df)

    s3_client.put_object(
        Body=parquet_file,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key=f"{table_name}/{date}/{time}.parquet")

    logger.info(f"{table_name}/{date}/{time}.parquet successfully created.")
