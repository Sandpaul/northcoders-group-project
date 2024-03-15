"""This file contains the function to convert DataFrames to Parquet
format files"""

import logging
import boto3
import pandas as pd

from src.utils.get_bucket_name import get_bucket_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def df_to_parquet(df, file_name):
    """
    
    2. Set up s3 key
    3. Set up parquet file
    4. Send to bucket
    """
    
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Invalid Input: {df} is not a data frame.")
    
    table_name = file_name.split("/")[0]
    
    match table_name:
        case "address":
            split = file_name.split("/")
            new_file_name = f"dim_location/{split[1]}/{split[2]}"
        case "counterparty" | "currency" | "design" | "payment_type" | "staff" | "transaction":
            new_file_name = f"dim_{file_name}"
        case "payment" | "purchase_order" | "sales_order":
            new_file_name = f"fact_{file_name}"
    
    bucket_name = get_bucket_name("processed")
    
    s3 = boto3.client("s3")
    
    parquet_file = pd.DataFrame.to_parquet(df)
    
    response = s3.put_object(Bucket=bucket_name, Body=parquet_file, Key=new_file_name)
    
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"{new_file_name} successfully saved to {bucket_name}")
