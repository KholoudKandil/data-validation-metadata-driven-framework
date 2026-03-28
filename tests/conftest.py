"""
Pytest configuration and shared fixtures.

Provides SparkSession and test data for all test modules.
"""

import pytest
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    
    Scope: session - reused across all tests for performance.
    """
    spark = SparkSession.builder \
        .appName("MetadataFrameworkTests") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_data():
    """
    Sample person data matching the test specification.
    
    Used for creating DataFrames in tests.
    """
    return [
        {"name": "Xabier", "age": 39, "office": ""},
        {"name": "Miguel", "age": None, "office": "RIO"},
        {"name": "Fran", "age": 31, "office": "RIO"},
    ]


@pytest.fixture
def sample_df(spark, sample_data):
    """
    Sample DataFrame created from sample_data.
    
    Provides a clean DataFrame for transformation tests.
    """
    return spark.createDataFrame(sample_data)


@pytest.fixture
def sample_data_with_duplicates():
    """
    Sample data with duplicate records (same name, different data).
    
    Used for testing deduplication logic.
    """
    return [
        {"name": "Alice", "age": 30, "office": "NYC"},
        {"name": "Alice", "age": 31, "office": "BOSTON"},
        {"name": "Bob", "age": 25, "office": "NYC"},
    ]


@pytest.fixture
def sample_df_with_duplicates(spark, sample_data_with_duplicates):
    """
    DataFrame with duplicate records.
    
    Used for testing MERGE deduplication behavior.
    """
    return spark.createDataFrame(sample_data_with_duplicates)


@pytest.fixture
def sample_df_with_timestamp(spark, sample_data):
    """
    Sample DataFrame with timestamp column added.
    
    Simulates output of add_fields transformation.
    """
    df = spark.createDataFrame(sample_data)
    return df.withColumn("dt", F.current_timestamp())