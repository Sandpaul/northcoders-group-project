"""This file contains the definition for the load `lambda_handler()`.
"""

import io
import json
import logging
import pandas as pd
import boto3
from sqlalchemy import create_engine

from src.utils.get_bucket_name import get_bucket_name
from src.utils.get_secret_dict import get_secret_dict
from src.utils.parquet_file_reader import parquet_file_reader

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    file_name = event["Records"][0]["s3"]["object"]["key"]
    formatted_file_name = file_name.replace("%3A", ":")
    logger.info(f"File name is {formatted_file_name}!")

    bucket_name = get_bucket_name("processed")
    df = parquet_file_reader(formatted_file_name, bucket_name)
    logger.info(f"{formatted_file_name} retrieved from {bucket_name}")

    table_name = formatted_file_name.split("/")[0]
    logger.info(f"{table_name} data: {df}")

    
    dw_dict = get_secret_dict("dw_credentials")

    engine = create_engine(
        f'postgresql+pg8000://{dw_dict["user"]}:{dw_dict["password"]}@{dw_dict["host"]}:{dw_dict["port"]}/{dw_dict["database"]}'
    )

    with engine.connect() as connection:

        connection.begin()

        try:
            df.to_sql(
                table_name,
                connection,
                index=False,
                if_exists="append",
            )

            connection.commit()

        except Exception as e:
            connection.rollback()
            print(f"An error occurred: {e}")
            raise

        finally:
            connection.close()

    logger.info(f"Data loaded to {table_name}")
