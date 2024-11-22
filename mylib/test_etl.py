import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from extract import extract
from unittest import mock


@patch("extract.mkdirs")
@patch("extract.upload_file")
def test_extract(mock_upload_file, mock_mkdirs):
    """
    Test the extract step: directory creation and file upload to DBFS.
    """
    urls = ["https://github.com/fivethirtyeight/data/raw/master/airline-safety/airline-safety.csv"]
    dbfs_paths = ["dbfs:/FileStore/tr2bx_mini_project11/airline_safety.csv"]
    directory = "dbfs:/FileStore/tr2bx_mini_project11"

    extract(urls=urls, dbfs_paths=dbfs_paths, directory=directory)

    mock_mkdirs.assert_called_once()
    mock_upload_file.assert_called_once_with(urls[0], dbfs_paths[0], mock.ANY)


def test_transform_load(spark_session):
    """
    Test the transform and load step.
    """
    spark = spark_session

    data = [("Airline A", 1000, 1, 0, 0, 1, 0, 0)]
    schema = ["airline", "avail_seat_km_per_week", "incidents_85_99",
            "fatal_accidents_85_99",
            "fatalities_85_99", "incidents_00_14", "fatal_accidents_00_14",
            "fatalities_00_14"]
    df = spark.createDataFrame(data, schema=schema)

    # Mock Delta table write (example)
    df.createOrReplaceTempView("airline_safety_delta")
    row_count = df.count()
    assert row_count == 1


def test_query(spark_session):
    """
    Test the query step.
    """
    spark = spark_session

    data = [("Airline A", 1, 0, 1, 0, 2, 0)]
    schema = ["airline", "incidents_85_99", "fatalities_85_99", "incidents_00_14",
              "fatalities_00_14", "total_incidents", "total_fatalities"]
    df = spark.createDataFrame(data, schema=schema)

    df.createOrReplaceTempView("airline_safety_delta")
    result = spark.sql("SELECT * FROM airline_safety_delta").collect()

    assert len(result) == 1
    assert result[0]["airline"] == "Airline A"


@pytest.fixture(scope="module")
def spark_session():
    """
    Fixture to set up and tear down a Spark session for testing.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Pytest ETL Tests") \
        .getOrCreate()
    yield spark
    spark.stop()
