"""Test pipeline execution.

Tests the pipeline module which provides:
- DataFlowExecutor: Orchestrates sources → transformations → sinks
"""

import pytest
import tempfile
from pathlib import Path
import json

from src.pipeline import DataFlowExecutor
from src.exceptions import TransformationError
from src.config import ConfigLoader


class TestDataFlowExecutor:
    """Test DataFlowExecutor - orchestrates complete dataflows."""
    
    def test_executor_initialization(self, spark):
        """DataFlowExecutor should initialize with SparkSession."""
        executor = DataFlowExecutor(spark)
        
        assert executor.spark is spark
        assert executor.dataframes == {}
    
    def test_load_source_json(self, spark, tmp_path):
        """Test loading JSON source."""
        # Create temporary JSON file
        test_data = [
            {"name": "Test1", "value": 100},
            {"name": "Test2", "value": 200},
        ]
        
        json_file = tmp_path / "test.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Create executor and load source
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'test_source',
            'path': str(json_file),
            'format': 'JSON',
            'options': {'multiLine': 'true'}
        }
        
        executor._load_source(source_config)
        
        # Verify DataFrame loaded
        assert 'test_source' in executor.dataframes
        df = executor.dataframes['test_source']
        assert df.count() == 2
        assert 'name' in df.columns
        assert 'value' in df.columns
    
    def test_source_with_options(self, spark, tmp_path):
        """Test loading source with options."""
        test_data = [
            {"name": "A", "value": 1},
            {"name": "B", "value": 2},
        ]
        
        json_file = tmp_path / "data.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'my_source',
            'path': str(json_file),
            'format': 'JSON',
            'options': {'multiLine': 'true'}
        }
        
        executor._load_source(source_config)
        
        assert 'my_source' in executor.dataframes
        assert executor.dataframes['my_source'].count() == 2
    
    def test_missing_source_file_raises_error(self, spark):
        """Loading non-existent source should raise TransformationError."""
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'missing_source',
            'path': '/nonexistent/path/file.json',
            'format': 'JSON'
        }
        
        with pytest.raises(TransformationError) as exc_info:
            executor._load_source(source_config)
        
        assert "Failed to load source" in str(exc_info.value)
    
    def test_apply_transformation_validate_fields(self, spark, sample_df):
        """Test applying validate_fields transformation."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input_data'] = sample_df
        
        transformation_config = {
            'name': 'validate_step',
            'type': 'validate_fields',
            'params': {
                'input': 'input_data',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']},
                ]
            }
        }
        
        executor._apply_transformation(transformation_config)
        
        # Verify both DataFrames created
        assert 'validation_ok' in executor.dataframes
        assert 'validation_ko' in executor.dataframes
        
        # Verify counts
        assert executor.dataframes['validation_ok'].count() == 1
        assert executor.dataframes['validation_ko'].count() == 2
    
    def test_apply_transformation_add_fields(self, spark, sample_df):
        """Test applying add_fields transformation."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['base_data'] = sample_df
        
        transformation_config = {
            'name': 'add_timestamp',
            'type': 'add_fields',
            'params': {
                'input': 'base_data',
                'addFields': [
                    {'name': 'dt', 'function': 'current_timestamp'}
                ]
            }
        }
        
        executor._apply_transformation(transformation_config)
        
        # Verify output DataFrame created with transformation name
        assert 'add_timestamp' in executor.dataframes
        
        # Verify column added
        df_result = executor.dataframes['add_timestamp']
        assert 'dt' in df_result.columns
        assert df_result.count() == 3
    
    def test_transformation_with_missing_input_raises_error(self, spark):
        """Transformation with non-existent input should raise error."""
        executor = DataFlowExecutor(spark)
        # No input data loaded
        
        transformation_config = {
            'name': 'bad_transform',
            'type': 'validate_fields',
            'params': {
                'input': 'nonexistent_input',
                'validations': [{'field': 'field', 'validations': ['notNull']}]
            }
        }
        
        with pytest.raises(TransformationError) as exc_info:
            executor._apply_transformation(transformation_config)
        
        assert "not found" in str(exc_info.value).lower()


class TestDataFlowExecution:
    """Test complete dataflow execution."""
    
    def test_execute_simple_dataflow(self, spark, tmp_path):
        """Test executing a simple dataflow: load -> validate -> (no sinks)."""
        # Create test JSON file
        test_data = [
            {"name": "Alice", "age": 30, "office": "NYC"},
            {"name": "Bob", "age": None, "office": ""},
            {"name": "Carol", "age": 28, "office": "LA"},
        ]
        
        json_file = tmp_path / "people.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Create config
        config = {
            'dataflows': [
                {
                    'name': 'test_flow',
                    'sources': [
                        {
                            'name': 'people',
                            'path': str(json_file),
                            'format': 'JSON',
                            'options': {'multiLine': 'true'}
                        }
                    ],
                    'transformations': [
                        {
                            'name': 'validate',
                            'type': 'validate_fields',
                            'params': {
                                'input': 'people',
                                'validations': [
                                    {'field': 'office', 'validations': ['notEmpty']},
                                    {'field': 'age', 'validations': ['notNull']},
                                ]
                            }
                        }
                    ],
                    'sinks': []
                }
            ]
        }
        
        # Execute
        executor = DataFlowExecutor(spark)
        executor.execute(config)
        
        # Verify results
        assert 'validation_ok' in executor.dataframes
        assert 'validation_ko' in executor.dataframes
        
        # Alice and Carol pass, Bob fails
        ok_count = executor.dataframes['validation_ok'].count()
        ko_count = executor.dataframes['validation_ko'].count()
        
        assert ok_count == 2  # Alice, Carol
        assert ko_count == 1  # Bob


class TestDataFlowOrchestration:
    """Test dataflow orchestration patterns."""
    
    def test_multiple_transformations_in_sequence(self, spark, sample_df):
        """Test applying multiple transformations in sequence."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        # First: validate
        validate_config = {
            'name': 'validate',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']},
                ]
            }
        }
        executor._apply_transformation(validate_config)
        
        # Second: add timestamp to OK records
        add_config = {
            'name': 'enrich',
            'type': 'add_fields',
            'params': {
                'input': 'validation_ok',
                'addFields': [
                    {'name': 'processed_at', 'function': 'current_timestamp'}
                ]
            }
        }
        executor._apply_transformation(add_config)
        
        # Verify final state
        assert 'enrich' in executor.dataframes
        final_df = executor.dataframes['enrich']
        
        assert 'processed_at' in final_df.columns
        assert final_df.count() == 1  # Only Fran (validation_ok)
        
        # Verify original columns still present
        assert 'name' in final_df.columns
        assert 'age' in final_df.columns
        assert 'office' in final_df.columns
    
    def test_dataframes_accumulate_in_executor(self, spark, sample_df):
        """Test that DataFrames accumulate in executor state."""
        executor = DataFlowExecutor(spark)
        
        # Load source
        executor.dataframes['source1'] = sample_df
        assert len(executor.dataframes) == 1
        
        # Apply first transformation
        config1 = {
            'name': 'transform1',
            'type': 'validate_fields',
            'params': {
                'input': 'source1',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                ]
            }
        }
        executor._apply_transformation(config1)
        
        # Now should have 3 DataFrames: source1, validation_ok, validation_ko
        assert 'source1' in executor.dataframes
        assert 'validation_ok' in executor.dataframes
        assert 'validation_ko' in executor.dataframes
        assert len(executor.dataframes) == 3


class TestDataFlowErrorHandling:
    """Test error handling in dataflow execution."""
    
    def test_invalid_transformation_type_raises_error(self, spark, sample_df):
        """Invalid transformation type should raise error."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        transform_config = {
            'name': 'bad_transform',
            'type': 'unknown_transformation',
            'params': {'input': 'input'}
        }
        
        with pytest.raises(TransformationError):
            executor._apply_transformation(transform_config)
    
    def test_transformation_with_invalid_field_raises_error(self, spark, sample_df):
        """Transformation referencing invalid field should raise error."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        transform_config = {
            'name': 'bad_validate',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'nonexistent_field', 'validations': ['notEmpty']},
                ]
            }
        }
        
        with pytest.raises(TransformationError):
            executor._apply_transformation(transform_config)