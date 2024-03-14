"""This module contains the definition for `get_bucket_name()`."""

import boto3
from pprint import pprint

import re


def get_bucket_name(bucket):
    """A function to get the name of an s3 bucket.

    Args:
        bucket (str): the bucket that you want to access the name of (either ingestion or processed_data)
    
    Returns:
        bucket_name (str): a string of the name of the bucket.
    """

    if bucket == "ingestion":
        pattern = "totesys-etl-ingestion-bucket-*"
    elif bucket == "processed":
        pattern = "totesys-etl-processed_data-bucket-*"

    s3 = boto3.client("s3")

    response = s3.list_buckets()

    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]

    for b in bucket_names:
        if re.search(pattern, b):
            return b
    