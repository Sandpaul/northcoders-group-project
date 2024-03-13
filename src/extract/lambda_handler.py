"""This module contains the definition for lambda_handler() function
for the extraction lambda.
"""

import logging
from src.extract.extract import (
    retrieve_data_from_totesys,
    create_current_timestamp,
    update_timestamp,
    get_timestamp,
)
from src.extract.sql_to_list_of_dicts import sql_to_list_of_dicts
from src.extract.json_file_maker import json_file_maker

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Brings the 3 extraction functions together
    to extract data from totesys and get it into json files
    Runs each function in order to extract data from the sql database,
    convert that to a list of dictionaries, write to a json file and send
    that to the ingestion s3 bucket

    Args:

    Raises:
        RunTimeError: Raises if an unexpected error occurs
        KeyError: Raises if an error in the extraction functions

    """
    try:
        current_timestamp = create_current_timestamp()
        last_ingested_timestamp = get_timestamp("last_ingested_timestamp")

        data = retrieve_data_from_totesys(
            current_timestamp=current_timestamp,
            last_ingested_timestamp=last_ingested_timestamp,
        )  # noqa
        logger.info(f"SQL Data: {data}")
        for x in data:
            formatted_data = sql_to_list_of_dicts(x)
            logger.info(f"Table Data: {x}")
            json_file_maker(formatted_data)
            logger.info("Table data converted to JSON")

        update_timestamp("last_ingested_timestamp", current_timestamp)

    except KeyError as k:
        logger.error(f"Error in extraction functions {k}")
    except Exception as e:
        logger.error(e)
        raise RuntimeError
