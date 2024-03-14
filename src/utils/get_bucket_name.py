"""This module contains the definition for `get_bucket_name()`."""

import logging
import re

import boto3


def get_bucket_name(bucket):
    """A function to get the name of an s3 bucket.

    Args:
        bucket (str): the bucket that you want to access the name of (either ingestion or processed_data)

    Returns:
        bucket_name (str): a string of the name of the bucket.
    """

    logger = logging.getLogger("MyLogger")
    logger.setLevel(logging.INFO)

    if bucket == "ingestion":
        pattern = "totesys-etl-ingestion-bucket-*"
    elif bucket == "processed":
        pattern = "totesys-etl-processed_data-bucket-*"
    else:
        raise InvalidArgumentError(
            logging.error(
                f"InvalidArgumentError: {bucket}. Valid arguments are `ingestion` or `processed`."
            )
        )

    s3 = boto3.client("s3")

    response = s3.list_buckets()

    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]

    bucket_name = ""

    for b in bucket_names:
        if re.search(pattern, b):
            bucket_name = b

    if len(bucket_name) == 0:
        raise BucketNotFoundError(
            logging.error(f"BucketNotFoundError: {bucket} bucket not found.")
        )

    return bucket_name


class InvalidArgumentError(Exception):
    """Catches arguments other than `ingestion` and `processed`."""


class BucketNotFoundError(Exception):
    """Catches incidents where bucket is not found."""
