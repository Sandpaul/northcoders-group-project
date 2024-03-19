"""This module contains the definition for the lambda_handler() function
for the transformation lambda.
"""

import logging
from src.transform.df_to_parquet import df_to_parquet
from src.transform.dim_counterparty import dim_counterparty
from src.transform.dim_currency import dim_currency
from src.transform.dim_location import dim_location
from src.transform.dim_staff import dim_staff
from src.transform.fact_payment import fact_payment
from src.transform.fact_sales_order import fact_sales_order
from src.utils.drop_created_and_updated import drop_created_and_updated
from src.utils.split_created_and_updated import split_created_and_updated
from src.utils.get_bucket_name import get_bucket_name
from src.utils.parquet_file_reader import parquet_file_reader

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    file_name = event["Records"][0]["s3"]["object"]["key"]
    formatted_file_name = file_name.replace("%3A", ":")
    logger.info(f"File name is {formatted_file_name}!")

    bucket_name = get_bucket_name("ingestion")
    df = parquet_file_reader(formatted_file_name, bucket_name)
    logger.info(f"{formatted_file_name} retrieved from {bucket_name}")
    
    table_name = formatted_file_name.split("/")[0]
    logger.info(f"{table_name} data: {df}")


    match table_name:
        case "address":
            transformed_df = dim_location(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "counterparty":
            transformed_df = dim_counterparty(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "currency":
            transformed_df = dim_currency(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "design" | "payment_type" | "transaction":
            transformed_df = drop_created_and_updated(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "staff":
            transformed_df = dim_staff(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "payment":
            transformed_df = fact_payment(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "purchase_order":
            transformed_df = split_created_and_updated(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
        case "sales_order":
            transformed_df = fact_sales_order(df)
            df_to_parquet(
                transformed_df,
                formatted_file_name,
            )
