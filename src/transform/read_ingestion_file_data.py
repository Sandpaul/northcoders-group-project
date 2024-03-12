"""This module contains the definitions for read_ingestion_file_data()."""

import json
import boto3


def read_ingestion_file_data(file_path):
    """Function to read a file from s3 ingestion bucket and return the data.

    Args:
        filepath (str): filepath of the file to be read
        e.g. `tablename/YYYY-MM-DD/HH.MM.SS.SSSSSS`
        bucket (str):

    Returns:
        parsed_data (dict): parsed data from the accessed json file
        e.g. {
                'timestamp': '2022-11-03 14:20:51.563',
                'cars': [
                        {'id': 1, 'make': 'Ford', 'model': 'Mustang'},
                        {'id': 2, 'make': 'Toyota', 'model': 'Yaris'},
                        {'id': 3, 'make': 'Honda', 'model': 'Civic'},
                        {'id': 4, 'make': 'BMW', 'model': 'X5'}
                    ]
                }

    """
    s3 = boto3.client('s3')

    response = s3.get_object(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key=file_path
    )

    file_data = response['Body'].read().decode('utf-8')

    parsed_data = json.loads(file_data)

    return parsed_data
