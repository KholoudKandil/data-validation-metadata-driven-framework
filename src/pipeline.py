"""
Pipeline orchestration: execute dataflow from metadata.

Purpose: Coordinate sources → transformations → sinks
in the correct order.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import logging

from src.config import ConfigLoader
from src.exceptions import TransformationError
from src.transformations import get_transformation
from src.sinks import write_sink


logger = logging.getLogger(__name__)


class DataFlowExecutor:
    """Execute a dataflow defined in metadata."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize executor.
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        self.dataframes: Dict[str, DataFrame] = {}
    
    def execute(self, config: Dict[str, Any]) -> None:
        """
        Execute dataflows from configuration.
        
        Args:
            config: Configuration dict (from ConfigLoader.load())
            
        Raises:
            TransformationError: If any transformation fails
        """
        for dataflow in config['dataflows']:
            self._execute_dataflow(dataflow)
    
    def _execute_dataflow(self, dataflow: Dict[str, Any]) -> None:
        """
        Execute single dataflow.
        
        Process:
        1. Load sources
        2. Apply transformations in order
        3. Write sinks
        
        Args:
            dataflow: Dataflow configuration
        """
        dataflow_name = dataflow['name']
        logger.info(f"Starting dataflow: {dataflow_name}")
        
        try:
            # Load sources
            logger.info("Loading sources...")
            for source in dataflow['sources']:
                self._load_source(source)
            
            # Apply transformations
            logger.info("Applying transformations...")
            for transformation in dataflow['transformations']:
                self._apply_transformation(transformation)
            
            # Write sinks
            logger.info("Writing sinks...")
            for sink in dataflow['sinks']:
                self._write_sink(sink)
            
            logger.info(f"✓ Dataflow '{dataflow_name}' completed successfully")
        
        except Exception as e:
            logger.error(f"✗ Dataflow '{dataflow_name}' failed: {e}")
            raise
    
    def _load_source(self, source: Dict[str, Any]) -> None:
        """
        Load data from source.
        
        Args:
            source: Source configuration
        """
        source_name = source['name']
        path = source['path']
        format = source['format'].lower()
        options = source.get('options', {})  
        
        logger.info(f"  Loading {source_name} from {path} (format: {format})")
        
        try:
            df = self.spark.read.format(format).options(**options).load(path)
            self.dataframes[source_name] = df
            
            row_count = df.count()
            logger.info(f"    ✓ Loaded {row_count} rows")
        
        except Exception as e:
            raise TransformationError(
                f"Failed to load source '{source_name}': {e}"
            )
    
    def _apply_transformation(self, transformation: Dict[str, Any]) -> None:
        """
        Apply transformation to DataFrame.
        
        Args:
            transformation: Transformation configuration
        """
        trans_name = transformation['name']
        trans_type = transformation['type']
        params = transformation['params']
        
        logger.info(f"  Applying transformation: {trans_name} ({trans_type})")
        
        try:
            # Get transformation function
            transform_func = get_transformation(trans_type)
            
            # Get input DataFrame
            input_name = params['input']
            if input_name not in self.dataframes:
                raise TransformationError(
                    f"Input '{input_name}' not found. Available: "
                    f"{list(self.dataframes.keys())}"
                )
            
            input_df = self.dataframes[input_name]
            
            # Execute transformation
            # Pass transformation name for proper output naming
            if trans_type == 'add_fields':
                output_dfs = transform_func(input_df, params, self.dataframes, trans_name)
            else:
                output_dfs = transform_func(input_df, params, self.dataframes)
            
            # Store output DataFrames
            for output_name, output_df in output_dfs.items():
                self.dataframes[output_name] = output_df
                logger.info(f"    ✓ Created DataFrame: {output_name}")
        
        except Exception as e:
            raise TransformationError(
                f"Transformation '{trans_name}' failed: {e}"
            )
    
    def _write_sink(self, sink: Dict[str, Any]) -> None:
        """
        Write DataFrame to sink.
        
        Args:
            sink: Sink configuration
        """
        sink_name = sink['name']
        input_name = sink['input']
        paths = sink['paths']
        format = sink['format']
        mode = sink.get('saveMode', 'OVERWRITE')
        unique_key = sink.get('uniqueKey')
        
        logger.info(f"  Writing sink: {sink_name}")
        
        try:
            # Get input DataFrame
            if input_name not in self.dataframes:
                raise TransformationError(
                    f"Input '{input_name}' not found for sink '{sink_name}'"
                )
            
            df = self.dataframes[input_name]
            
            # Log deduplication info
            if unique_key and mode == 'APPEND':
                logger.info(f"    Deduplicating on keys: {unique_key}")
            
            # Write to all paths
            for path in paths:
                logger.info(f"    Writing to {path} ({format}, mode={mode})")
                write_sink(df, format, [path], mode, unique_key)
            
            logger.info(f"    ✓ Sink '{sink_name}' completed")
        
        except Exception as e:
            raise TransformationError(
                f"Sink '{sink_name}' failed: {e}"
            )