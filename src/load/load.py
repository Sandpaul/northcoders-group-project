"""
This file contains the functions transform_parquet_to_dataframe(),
load_dataframe_to_database(), grab_file_name() and lambda_handler().
"""

import io
import logging
import pandas as pd
import boto3
from sqlalchemy import create_engine

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transform_parquet_to_dataframe(formatted_file_name):
    """
    Function to fetch parquet file from the s3 processed bucket
    and convert to a pandas DataFrame.

    Args:
            `formatted_file_name`- string of a formatted file path

    Returns:
            List containing the table name and the pandas DataFrame.
    """

    s3 = boto3.client('s3')
    response = s3.get_object(
        Bucket='totesys-etl-processed-data-bucket-teamness-120224',
        Key=formatted_file_name
    )

    table_name = formatted_file_name.split('/')[0]
    content = response['Body'].read()
    content_in_bytes = io.BytesIO(content)
    df = pd.read_parquet(content_in_bytes)
    return [table_name, df]


def load_dataframe_to_database(file_path):
    """
    Function to insert DataFrame into PSQL database in the appropriate
    table.

    Args:
            `file_path`- String of the file path of the DataFrame.

    Returns:
            Message to say that data has been inserted into the database.

    """
    
    formatted_file_name = file_path.replace("%3A", ":")
    df_and_table_name = transform_parquet_to_dataframe(formatted_file_name)
    df = df_and_table_name[1]
    table_name = df_and_table_name[0]

    if table_name == 'payment' or table_name == 'purchase_order' or table_name == 'sales_order':  # noqa
        table_name = f'fact_{table_name}'
    elif table_name == 'address':
        table_name = 'dim_location'
    else:
        table_name = f'dim_{table_name}'

    engine = create_engine('postgresql+pg8000://')  # noqa

    with engine.connect() as connection:

        connection.begin()

        try:

            df.to_sql(table_name, connection, index=False, if_exists="append")

            connection.commit()

        except Exception as e:

            connection.rollback()

            print(f"An error occurred: {e}")

            raise

        finally:

            connection.close()

    logger.info(f"Data loaded to {table_name}")


def lambda_handler(event, context):
    """
    Lambda handler function to run all other functions that are to be uploaded
    to the load lambda on AWS.

    Args:
            `event`- A valid S3 PutObject event

    Returns:
            A message to inform about which DataFrame is being inserted into
    the data warehouse.
            A message to inform that the DataFrame has been
            inserted into the data warehouse.

    Raises:
            RuntimeError- Raises if an unexpected error occurs
            KeyError- Raises if problem with 1 of the functions

    """
    try:
        file_path = grab_file_name(event['Records'])
        logger.info(
            f'Parquet file to be inserted into data warehouse: {file_path}')

        load_dataframe_to_database(file_path)
        logger.info('Parquet file has been inserted into Data Warehouse!')

    except KeyError as k:
        logger.error(k)
    except Exception as e:
        logger.error(e)
        raise RuntimeError


def grab_file_name(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]['s3']['object']['key']
