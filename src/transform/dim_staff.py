# """This module contains the definition for transform_staff()."""

# import json
# import pandas as pd
# import boto3


# def transform_staff(staff_data):
#     """Function to transform data stored in ingestion bucket that was extracted
#     from staff table in totesys.

#     Args:
#         staff_data (dict): dict of data from staff file from
#         ingestion bucket

#     Returns:
#         staff_data_copy (dict): copy of staff_data with data dict
#         replaced by dataframe
#     """
#     s3 = boto3.client("s3")

#     response = s3.list_objects(
#         Bucket="totesys-etl-ingestion-bucket-teamness-120224",
#         Prefix="department/",
#         Delimiter="/",
#     )  # noqa

#     subfolders = [
#         common_prefix["Prefix"] for common_prefix in response.get("CommonPrefixes", [])
#     ]  # noqa

#     department_merged_df = pd.DataFrame()

#     files_list = []

#     for folder in subfolders:
#         folder_response = s3.list_objects(
#             Bucket="totesys-etl-ingestion-bucket-teamness-120224", Prefix=folder
#         )  # noqa
#         folder_objects = folder_response.get("Contents", [])

#         department_file = [item["Key"] for item in folder_objects]

#         files_list.append(department_file[0])

#         for file in files_list:
#             file_content = (
#                 s3.get_object(
#                     Bucket="totesys-etl-ingestion-bucket-teamness-120224", Key=file
#                 )["Body"]
#                 .read()
#                 .decode("utf-8")
#             )  # noqa

#         parsed_data = json.loads(file_content)

#         department_data = parsed_data["department"]

#         file_df = pd.DataFrame.from_records(department_data)

#         department_merged_df = pd.concat(
#             [department_merged_df, file_df], ignore_index=True
#         )

#     staff_data_copy = staff_data.copy()

#     staff_rows = staff_data_copy["staff"]

#     staff_df = pd.DataFrame.from_records(staff_rows)

#     dim_staff_df = pd.merge(
#         staff_df,
#         department_merged_df,
#         left_on="department_id",
#         right_on="department_id",
#     )

#     dim_staff_df = dim_staff_df[
#         [
#             "staff_id",
#             "first_name",
#             "last_name",
#             "department_name",
#             "location",
#             "email_address",
#         ]
#     ]

#     dim_staff_df["last_updated_date"] = "1970-01-01"
#     dim_staff_df["last_updated_time"] = "00:00"

#     dim_staff_df["last_updated_date"] = pd.to_datetime(
#         dim_staff_df["last_updated_date"], format="%Y-%m-%d"
#     ).dt.date
#     dim_staff_df["last_updated_time"] = pd.to_datetime(
#         dim_staff_df["last_updated_time"], format="%H:%M"
#     ).dt.time

#     staff_data_copy["staff"] = dim_staff_df

#     return staff_data_copy

import pandas as pd

from src.utils.get_archived_table_data import get_archived_table_data
from src.utils.get_bucket_name import get_bucket_name


def dim_staff(staff_data):

    bucket_name = get_bucket_name("ingestion")

    department_df = get_archived_table_data(
        table_name="department",
        bucket_name=bucket_name,
    )

    staff_df = staff_data.copy(deep=True)

    dim_staff_df = pd.merge(
        staff_df, department_df, left_on="department_id", right_on="department_id"
    )

    dim_staff_df = dim_staff_df[
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    ]

    return dim_staff_df
