"""This module contains the definition for `json_file_maker()`"""

import json
import logging
import boto3


def json_file_maker(data):
    """
    A function to take a list of dictionaries, write them
    to a json file and send them to an s3 bucket.

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
        Message that cofirms json file has
        been created and stored successfully.
            e.g. "`tablename/date/time.json` succesfully created."
                 "`staff/2024-02-14/10:00:00.json` succesffully created."

    Raises:
        ValueError if data list is empty.
        TypeError if data list contains any non-dict items.
        KeyError if timestamp key is not present.

    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    dict_keys = list(data.keys())
    print(dict_keys)

    split_timestamp = data["timestamp"].split(" ")
    date = split_timestamp[0]
    time = split_timestamp[1]
    table_name = dict_keys[1]
    data_to_write = data

    s3_client = boto3.client("s3")
    json_data = json.dumps(data_to_write, indent=4, sort_keys=True, default=str)

    s3_client.put_object(
        Body=json_data,
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key=f"{table_name}/{date}/{time}.json",
    )

    if data[dict_keys[1]] == []:
        raise ValueError("Data is empty")

    for i in data[dict_keys[1]]:
        if type(i) is not dict:
            raise TypeError(
                "There is an element in the list that is not a dictionary"
            )  # noqa

    if "timestamp" not in dict_keys:
        raise KeyError("Timestamp does not exist")

    else:
        logger.info(f"{date}/{time}/{table_name} successfully created.")
