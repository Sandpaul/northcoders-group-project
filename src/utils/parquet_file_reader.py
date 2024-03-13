"""This module contains the definition for `parquet_file_reader()`."""

import io

import boto3
import pandas as pd

def parquet_file_reader(file_path, bucket_name):
    """A function to retrieve a data frame from a parquet file.

    Args:
        file_path (str): string of the file path to the required file.
        e.g. `tablename/YYYY-MM-DD/HH.MM.SS.SSSSSSS`
        
        bucket_name (str): name of the s3 bucket where the required parquet file is stored.
        e.g. `totesys-etl-ingestion-bucket-teamness-120224`

    Returns:
        df (data frame): the data frame from the read parquet file.
        
    Raises:
    
    
    """
    s3 = boto3.client("s3")
    
    response = s3.get_object(
        Bucket=bucket_name,
        Key=file_path
    )
    
    file_contents = response["Body"].read()
    content_in_bytes = io.BytesIO(file_contents)
    df = pd.read_parquet(content_in_bytes)
    
    return df