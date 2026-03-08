"""Test fixtures and sample data."""

import pytest
from pyspark.sql import SparkSession
import json
import tempfile
from pathlib import Path


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("MetadataFrameworkTests") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data():
    """Sample test data matching the spec."""
    return [
        {"name": "Xabier", "age": 39, "office": ""},
        {"name": "Miguel", "age": None, "office": "RIO"},
        {"name": "Fran", "age": 31, "office": "RIO"},
    ]


@pytest.fixture
def sample_df(spark, sample_data):
    """Create DataFrame from sample data."""
    return spark.createDataFrame(sample_data)


@pytest.fixture
def temp_config():
    """Create temporary metadata YAML file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
dataflows:
  - name: test_flow
    sources:
      - name: test_input
        path: "data/input/test.json"
        format: JSON
    transformations: []
    sinks: []
""")
        yield f.name
    Path(f.name).unlink()
