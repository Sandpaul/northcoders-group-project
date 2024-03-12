"""This module contains the definition for the lambda_handler() function
for the transformation lambda.
"""
import logging
from src.transform.DF_to_parquet import DF_to_parquet
from src.transform.read_ingestion_file_data import read_ingestion_file_data
from src.transform.fact_sales_order import transform_sales_order
from src.transform.dim_counterparty import transform_counterparty
from src.transform.dim_currency import transform_currency
from src.transform.dim_design import transform_design
from src.transform.fact_payment import transform_payment
from src.transform.dim_payment_type import transform_payment_type
from src.transform.dim_transaction import transform_transaction
from src.transform.fact_purchase_order import transform_purchase_order
from src.transform.dim_staff import transform_staff
from src.transform.dim_location import transform_location

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def lambda_handler(event, context, bucket_name='totesys-etl-processed-data-bucket-teamness-120224'):  # noqa
    """
    Function to run on event of JSON file being put in s3 ingestion bucket.
    1) Will take JSON file out of the s3 bucket
    2) Run the correct function to transform the data into pandas DataFrame
    and join with other data (if necessary) to match the star schema
    3) Converted into parquet file format and pushed into s3 processed bucket

    Args:
        event:
            a valid S3 PutObject event

    Return:
        Should return a message to confirm file has been sent to
        s3 processed bucket
    """

    file_name = grab_file_name(event['Records'])
    formatted_file_name = file_name.replace("%3A", ":")
    logger.info(f'File name is {formatted_file_name}!')

    parsed_data = read_ingestion_file_data(formatted_file_name)
    logger.info('JSON file taken from S3 ingestion bucket!')
    logger.info(f'Parsed Data: {parsed_data}')

    table_name = None

    keys = list(parsed_data.keys())

    for key in keys:
        if key != 'timestamp':
            table_name = key
            break

    match table_name:
        case 'counterparty':
            result = transform_counterparty(parsed_data)
            logger.info('Counterparty data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Counterparty data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'currency':
            result = transform_currency(parsed_data)
            logger.info('Currency data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Currency data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'design':
            result = transform_design(parsed_data)
            logger.info('Design data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Design data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'payment':
            result = transform_payment(parsed_data)
            logger.info('Payment data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Payment data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'payment_type':
            result = transform_payment_type(parsed_data)
            logger.info('Payment type data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Payment type data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'transaction':
            result = transform_transaction(parsed_data)
            logger.info('Transaction data has been transformed!')
            DF_to_parquet(result, bucket_name)
            logger.info(
                'Transaction data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'purchase_order':
            result = transform_purchase_order(parsed_data)
            logger.info('Purchase order data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Purchase order data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'staff':
            result = transform_staff(parsed_data)
            logger.info('Staff data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Staff data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'sales_order':
            result = transform_sales_order(parsed_data)
            logger.info('Sales order data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Sales order data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa
        case 'address':
            result = transform_location(parsed_data)
            logger.info('Address data has been transformed!')
            DF_to_parquet(result)
            logger.info(
                'Address data has been converted to Parquet and sent to the s3 processed bucket!')  # noqa


def grab_file_name(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]['s3']['object']['key']
