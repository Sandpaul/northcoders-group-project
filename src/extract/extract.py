"""This module contains the definitions for create_current_timestamp(),
get_timestamp(), update_timestamp(), connect_to_totesys(),
retrieve_data_from_table() and retrieve_data_from_totesys()"""

from datetime import datetime
import json
import logging
import boto3
import pg8000


logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def create_current_timestamp():
    """Creates a new timestamp.

    Returns:
        timestamp (str): timestamp in format `YYYY-MM-DD HH:MM:SS.000000`
    """
    current_timestamp = str(datetime.utcnow())
    logger.info(f"New timestamp created - {current_timestamp}")
    return current_timestamp


def get_timestamp(parameter_name):
    """Retrieves a timestamp from AWS System Manager's Parameter Store.

    Args:
        parameter_name (str): name of the parameter in AWS.

    Returns:
        timestamp (str): timestamp in format `YYYY-MM-DD HH:MM:SS.000000`
    """
    ssm_client = boto3.client("ssm", region_name="eu-west-2")

    timestamp = ssm_client.get_parameter(Name=parameter_name)
    logger.info(f"Timestamp retrieved - {parameter_name}: {timestamp}")
    return timestamp


def update_timestamp(parameter_name, value):
    """Updates a timestamp in AWS System Manager's Parameter Store.

    Args:
        parameter_name (str): name of the parameter in AWS.
        value (str): the value of the paremeter to be saved.
    """
    ssm_client = boto3.client("ssm", region_name="eu-west-2")

    ssm_client.put_parameter(
        Name=parameter_name, Type="String", Value=value, Overwrite=True
    )
    logger.info(f"{parameter_name} updated to {value}")


def connect_to_totesys():
    """Retrieves database credentials from AWS Secrets Manager and creates pg8000 connection to totesys database.

    Returns:
        class: Connection to the totesys database.
    """

    sm = boto3.client("secretsmanager")
    
    db_secret = sm.get_secret_value(SecretId="db_credentials")
    db_credentials = db_secret["SecretString"]
    db_dict = json.loads(db_credentials)
    print(db_dict, type(db_dict))
    
    conn = pg8000.connect(**db_dict)
    logger.info("Connected to totesys")
    return conn


def retrieve_data_from_table(
    table_name,
    current_timestamp,
    conn,
    last_ingested_timestamp,
):
    """Retrieves data from a specified table in the totesys database.

    Args:
        table_name (str): name of the table that data is to be retrieved from.
        current_timestamp (str): the current timestamp.
        conn (class, optional): Connection to a database.
        Defaults to connect_to_totesys().
        last_ingested_timestamp (str, optional):
        The timestamp from when data was last extracted - used in SQL WHERE
        statment to filter results. Defaults to get_timestamp
        ("last_ingested_timestamp").

    Returns:
        result (dict): the result of the SQL extraction
        from the passed database.
        e.g. - {
            "timestamp": current_timestamp,
            "table_name": table_name,
            "table_columns": column_names,
            "table_rows": rows,
        } """
    try:
        last_ingested_timestamp_value = \
         last_ingested_timestamp["Parameter"]["Value"]

        if last_ingested_timestamp_value == "None":
            query = f"SELECT * FROM {table_name};"
        else:
            query = f"SELECT * FROM {table_name} WHERE last_updated > '{last_ingested_timestamp_value}';"  # noqa

        cursor = conn.cursor()
        cursor.execute(query)

        column_names = [i[0] for i in cursor.description]
        rows = cursor.fetchall()

        cursor.close()

        if len(rows) != 0:
            result = {
                "timestamp": current_timestamp,
                "table_name": table_name,
                "table_columns": column_names,
                "table_rows": rows,
            }
            return result
        else:
            return None


    except KeyError as ke_err:
        logger.error(f"KeyError Error occurred: {ke_err}")
        raise KeyError(f"KeyError occurred: {ke_err}") from ke_err

    except pg8000.ProgrammingError as pg_err:
        logger.error(f"Programming Error occurred: {pg_err}")
        raise pg8000.ProgrammingError(f"Programming Error occurred:{pg_err}") from pg_err  # noqa

    except Exception as e:
        logger.error(f"Unexpected Error occurred: {e}")
        raise RuntimeError(f"An unexpected error occurred: {e}") from e


def retrieve_data_from_totesys(
    **kwargs,
):
    """Retrieves all new data from all tables in totesys db
    (i.e. data added since last_ingested_timestamp.)

    Args:
        current_timestamp (str, optional): The current timestamp where
        data is to be saved. Defaults to create_current_timestamp().
        last_ingested_timestamp (str, optional): The timestamp of when data was
        last ingested from totesys db.
        Defaults to get_timestamp("last_ingested_timestamp").

    Returns:
        data_update (list of dicts): list of dicts representing
        data extracted from totesys db.
    """
    
    if kwargs.get("conn", None) is None:
        conn = connect_to_totesys()
    else:
        conn = kwargs["conn"]

    if kwargs.get("last_ingested_timestamp", None) is None:
        last_ingested_timestamp = get_timestamp("last_ingested_timestamp")
    else:
        last_ingested_timestamp = kwargs["last_ingested_timestamp"]
    
    if kwargs.get("current_timestamp", None) is None:
        current_timestamp = create_current_timestamp()
    else:
        current_timestamp = kwargs["current_timestamp"]

    table_names = [
        "counterparty",
        "currency",
        "address",
        "department",
        "design",
        "staff",
        "sales_order",
        "payment",
        "payment_type",
        "purchase_order",
        "transaction",
    ]
    try:
        data_update = [
            retrieve_data_from_table(
                table, current_timestamp, conn,
                last_ingested_timestamp=last_ingested_timestamp
            )
            for table in table_names
        ]

        logger.info(f"Data extracted from totesys: {data_update}")
        return data_update

    except ValueError as v:
        logger.error(f"ValueError occured: {v}")
        raise RuntimeError(f"ValueError occurred: {v}") from v

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise RuntimeError(f"An unexpected error occurred: {e}") from e
