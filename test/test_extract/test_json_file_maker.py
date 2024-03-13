"""This module contains the test suite for `json_file_maker()`"""

import json
import os
import boto3
from moto import mock_aws
import pytest
from src.extract.json_file_maker import json_file_maker


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
    return s3.create_bucket(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def example_data():
    """Create mock data."""
    return {
        "timestamp": "2022-11-03 14:20:51.563",
        "cars": [
            {"id": 1, "make": "Ford", "model": "Mustang"},
            {"id": 2, "make": "Toyota", "model": "Yaris"},
            {"id": 3, "make": "Honda", "model": "Civic"},
            {"id": 4, "make": "BMW", "model": "X5"},
        ],
    }


@pytest.mark.describe("json_file_maker()")
@pytest.mark.it("successfully saves a file to an s3 bucket")
@mock_aws
def test_saves_file_to_bucket(bucket, s3, example_data):
    """json_file_maker() should successfully
    save files to the s3 ingestion bucket"""
    response1 = s3.list_objects_v2(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224"
    )
    assert response1["KeyCount"] == 0
    json_file_maker(example_data)
    response2 = s3.list_objects_v2(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224"
    )
    assert response2["KeyCount"] == 1


@pytest.mark.describe("json_file_maker()")
@pytest.mark.it(
    """successfully saves a file to
                an s3 bucket with the correct name"""
)
@mock_aws
def test_correct_file_name(bucket, s3, example_data):
    """json_file_maker() should successfully save files
    to the s3 ingestion bucket with the correct name"""
    json_file_maker(example_data)
    response = s3.list_objects_v2(Bucket="totesys-etl-ingestion-bucket-teamness-120224")
    assert response["Contents"][0]["Key"] == "cars/2022-11-03/14:20:51.563.json"  # noqa


@pytest.mark.describe("json_file_maker()")
@pytest.mark.it("saves the correct data in the file")
@mock_aws
def test_correct_file_contents(bucket, s3, example_data):
    """json_file_maker() should save the
    correct data in the files it creates"""
    json_file_maker(example_data)
    test_object = s3.get_object(
        Bucket="totesys-etl-ingestion-bucket-teamness-120224",
        Key="cars/2022-11-03/14:20:51.563.json",
    )

    file_contents = json.loads(test_object["Body"].read())
    assert file_contents == example_data


@pytest.mark.describe("json_file_maker() raises:")
@pytest.mark.it("ValueError if list of dicts is empty")
def test_if_list_of_dictionaries_is_empty(bucket):
    """should raise a ValueError if list of dicts is empty"""
    data = {"timestamp": "2022-11-03 14:20:51.563", "cars": []}
    with pytest.raises(ValueError):
        json_file_maker(data)


@pytest.mark.describe("json_file_maker() raises:")
@pytest.mark.it("TypeError if there is an element in list that is not a dict")
def test_if_there_is_a_list_of_things_othe_than_dict(bucket):
    """should raise TypeError if theres something other than dict in data"""
    data = {
        "timestamp": "2022-11-03 14:20:51.563",
        "cars": [
            {"id": 1, "make": "Ford", "model": "Mustang"},
            {"id": 2, "make": "Toyota", "model": "Yaris"},
            "Honda Civic",
            {"id": 4, "make": "BMW", "model": "X5"},
        ],
    }
    with pytest.raises(TypeError):
        json_file_maker(data)


@pytest.mark.describe("json_file_maker() raises:")
@pytest.mark.it("KeyError when timestamp key is not present")
def test_if_no_time_stamp(bucket):
    data = {
        "cars": [
            {"id": 1, "make": "Ford", "model": "Mustang"},
            {"id": 2, "make": "Toyota", "model": "Yaris"},
            "Honda Civic",
            {"id": 4, "make": "BMW", "model": "X5"},
        ]
    }
    with pytest.raises(KeyError):
        json_file_maker(data)
