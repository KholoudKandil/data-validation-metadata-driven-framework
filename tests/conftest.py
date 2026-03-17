"""Test fixtures and sample data."""

import pytest
from pyspark.sql import SparkSession
import tempfile
from pathlib import Path


@pytest.fixture(scope="session")
def spark():
    """
    Create Spark session for tests.
    
    Session scope means one Spark session for all tests (efficient).
    """
    spark_session = SparkSession.builder \
        .appName("MetadataFrameworkTests") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_data():
    """
    Sample test data matching the SDG specification.
    
    Three records:
    - Xabier: empty office (fails validation)
    - Miguel: null age (fails validation)
    - Fran: valid data (passes validation)
    """
    return [
        {"name": "Xabier", "age": 39, "office": ""},
        {"name": "Miguel", "age": None, "office": "RIO"},
        {"name": "Fran", "age": 31, "office": "RIO"},
    ]


@pytest.fixture
def sample_df(spark, sample_data):
    """
    Create DataFrame from sample data.
    
    This DataFrame is used in most tests.
    """
    return spark.createDataFrame(sample_data)


@pytest.fixture
def temp_yaml_config():
    """
    Create temporary YAML config file for testing.
    
    Lifecycle:
    1. Create temp file
    2. Write valid YAML
    3. yield filename to test
    4. Delete after test
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        f.write("""
dataflows:
  - name: test_dataflow
    sources:
      - name: test_source
        path: "data/input/test.json"
        format: JSON
    transformations: []
    sinks: []
""")
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)