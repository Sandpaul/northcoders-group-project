"""This module contains the definition for transform_counterparty()."""

import json
import pandas as pd
import boto3


def transform_counterparty(counterparty_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from counterparty table in totesys.

    Args:
        counterparty_data (dict): dict of data from counterparty file from
        ingestion bucket

    Returns:
        counterparty_data_copy (dict): copy of counterparty_data with data dict
        replaced by dataframe

    """
    s3 = boto3.client("s3")

    response = s3.list_objects(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224", Prefix="address/", Delimiter="/")  # noqa

    subfolders = [common_prefix["Prefix"] for common_prefix in response.get("CommonPrefixes", [])]  # noqa

    address_merged_df = pd.DataFrame()

    files_list = []

    for folder in subfolders:
        folder_response = s3.list_objects(
            Bucket="totesys-etl-ingestion-bucket-teamness-120224", Prefix=folder)  # noqa
        folder_objects = folder_response.get("Contents")

        address_file = [item["Key"] for item in folder_objects]

        files_list.append(address_file[0])

    for file in files_list:
        file_content = s3.get_object(
            Bucket="totesys-etl-ingestion-bucket-teamness-120224", Key=file)["Body"].read().decode("utf-8")  # noqa

        parsed_data = json.loads(file_content)

        address_data = parsed_data["address"]

        file_df = pd.DataFrame.from_records(address_data)

        address_merged_df = pd.concat(
            [address_merged_df, file_df], ignore_index=True)

    counterparty_data_copy = counterparty_data.copy()

    counterparty_rows = counterparty_data_copy['counterparty']

    counterparty_df = pd.DataFrame.from_records(counterparty_rows)

    counterparty_df = counterparty_df[['counterparty_id',
                                       'counterparty_legal_name',
                                       'legal_address_id']]

    address_merged_df.drop(columns=['created_at',
                                    'last_updated'], inplace=True)

    dim_counterparty_df = pd.merge(counterparty_df,
                                   address_merged_df, left_on='legal_address_id',  # noqa
                                   right_on='address_id')

    dim_counterparty_df.drop(columns=['legal_address_id',
                                      'address_id'], inplace=True)

    dim_counterparty_df.rename(columns={
        "address_line_1": 'counterparty_legal_address_line_1',
        'address_line_2': 'counterparty_legal_address_line_2',
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone_number"
    }, inplace=True)

    dim_counterparty_df["last_updated_date"] = "1970-01-01"
    dim_counterparty_df["last_updated_time"] = "00:00"

    dim_counterparty_df['last_updated_date'] = pd.to_datetime(
        dim_counterparty_df['last_updated_date'], format="%Y-%m-%d").dt.date
    dim_counterparty_df['last_updated_time'] = pd.to_datetime(
        dim_counterparty_df['last_updated_time'], format="%H:%M").dt.time

    counterparty_data_copy['counterparty'] = dim_counterparty_df

    counterparty_data_copy["counterparty"].insert(
        0, 'counterparty_record_id', range(
            1, len(
                counterparty_data_copy["counterparty"]) + 1))

    return counterparty_data_copy
