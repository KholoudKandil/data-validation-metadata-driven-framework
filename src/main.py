"""
Entry point for metadata-driven pipeline.

Usage:
    python src/main.py config/metadata.yaml
"""

import sys
import logging
from pyspark.sql import SparkSession

from src.config import ConfigLoader
from src.pipeline import DataFlowExecutor
from src.exceptions import MetadataFrameworkError


# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    # Validate arguments
    if len(sys.argv) < 2:
        print("Usage: python src/main.py <config.yaml>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from: {config_path}")
        loader = ConfigLoader(config_path)
        config = loader.load()
        
        # Initialize Spark
        spark = SparkSession.builder \
            .appName("MetadataFramework") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        # Execute pipelines
        executor = DataFlowExecutor(spark)
        executor.execute(config)
        
        logger.info("✓ All dataflows completed successfully")
        spark.stop()
    
    except MetadataFrameworkError as e:
        logger.error(f"✗ Framework error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
