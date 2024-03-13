"""This module contains the test suite for transform_sales_order()"""

import os
import boto3
from moto import mock_aws
import pytest

from src.transform.fact_sales_order import transform_sales_order
from src.transform.read_ingestion_file_data import read_ingestion_file_data


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Create mock s3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def bucket(s3):
    """Create mock s3 bucket."""
    s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("test/test_transform/test_data/test_sales_order_data.json") as f:
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="2022-11-03/14:20:51.563/sales_order.json",
        )
    with open("test/test_transform/test_data/test_sales_order_data2.json") as f:  # noqa
        data_to_write = f.read()
        s3.put_object(
            Body=data_to_write,
            Bucket="totesys-etl-ingestion-bucket-teamness-120224",
            Key="2022-11-02/14:20:51.563/sales_order.json",
        )


@pytest.mark.describe("transform_sales_order()")
@pytest.mark.it("should return dictionary with a dataframe")
def test_transform_sales_order_returns_df(s3, bucket):
    """should return a dictionary with a dataframe"""
    file_path = "2022-11-03/14:20:51.563/sales_order.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_sales_order(test_data)
    assert type(result["sales_order"]).__name__ == "DataFrame"


@pytest.mark.describe("transform_sales_order()")
@pytest.mark.it(
    "dataframe should contain correct last_updated_date and created_time"
)  # noqa
def test_transform_sales_order_returns_correct_created(s3, bucket):
    """should return the correct last_updated_date and created_time"""
    file_path = "2022-11-03/14:20:51.563/sales_order.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_sales_order(test_data)
    assert result["sales_order"].get("last_updated_date").get(0) == "2024-02-20"
    assert result["sales_order"].get("created_time").get(0) == "15:07:09.880000"


@pytest.mark.describe("transform_sales_order()")
@pytest.mark.it(
    "dataframe should contain correct last_updated_date and last_updated_time"
)  # noqa
def test_transform_sales_order_returns_correct_last_updated(s3, bucket):
    """should return the correct last_updated_date and last_updated_time"""
    file_path = "2022-11-03/14:20:51.563/sales_order.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_sales_order(test_data)
    assert result["sales_order"].get("last_updated_date").get(0) == "2024-02-20"
    assert result["sales_order"].get("last_updated_time").get(0) == "15:07:09.880000"


@pytest.mark.describe("transform_sales_order()")
@pytest.mark.it(
    "should work when passed sales order list with more than one dict"
)  # noqa
def test_transform_sales_order_works_on_multiple_dicts(s3, bucket):
    """should return the correct last_updated_date and last_updated_time"""
    file_path = "2022-11-02/14:20:51.563/sales_order.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_sales_order(test_data)
    assert result["sales_order"].get("last_updated_date").get(0) == "2024-02-20"
    assert result["sales_order"].get("last_updated_time").get(0) == "15:07:09.880000"
    assert result["sales_order"].get("last_updated_date").get(1) == "2024-02-19"
    assert result["sales_order"].get("last_updated_time").get(1) == "15:07:09.880005"


@pytest.mark.describe("transform_sales_order()")
@pytest.mark.it("dataframe should contain correct column names")  # noqa
def test_dataframe_returns_correct_column_names(s3, bucket):
    file_path = "2022-11-02/14:20:51.563/sales_order.json"
    test_data = read_ingestion_file_data(file_path)
    result = transform_sales_order(test_data)
    assert set(result["sales_order"].keys()) == set(
        [
            "sales_order_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_records_id",
            "units_sold",
            "unit_price",
            "currency_record_id",
            "design_record_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]
    )
