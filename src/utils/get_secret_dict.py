"""This module contains the definition for `get_secret_dict()`."""

import json
import logging

import boto3


def get_secret_dict(secret_name: str) -> dict:
    """A function to retrieve a secret from AWS Secrets Manager and return it as a dictionary.

    Args:
        secret_name (str): string of the name of the secret to be retrieved.

    Returns:
        secret_dict (dict): dictionary of the retrieved secret.
    """

    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=secret_name)
    secret_string = secret["SecretString"]
    secret_dict = json.loads(secret_string)

    return secret_dict
