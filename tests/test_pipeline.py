"""
Pipeline tests: dataflow execution, transformations, and sinks.

Tests the orchestration of sources → transformations → sinks.
"""

import pytest
from pyspark.sql import functions as F
from pathlib import Path
import tempfile
import os

from src.pipeline import DataFlowExecutor
from src.exceptions import TransformationError


class TestDataFlowExecutor:
    """Test basic dataflow executor functionality."""
    
    def test_executor_initialization(self, spark):
        """Test that executor initializes correctly."""
        executor = DataFlowExecutor(spark)
        assert executor.spark is not None
        assert isinstance(executor.dataframes, dict)
        assert len(executor.dataframes) == 0
    
    def test_load_source_json(self, spark, tmp_path):
        """Test loading JSON source."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"name": "Alice", "age": 30}\n')
        
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'test_source',
            'path': str(json_file),
            'format': 'JSON',
            'options': {'multiLine': 'true'}
        }
        
        executor._load_source(source_config)
        
        assert 'test_source' in executor.dataframes
        assert executor.dataframes['test_source'].count() == 1
    
    def test_source_with_options(self, spark, tmp_path):
        """Test loading source with custom options."""
        json_file = tmp_path / "multiline.json"
        json_file.write_text('[\n{"name": "Alice"},\n{"name": "Bob"}\n]')
        
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'test_source',
            'path': str(json_file),
            'format': 'JSON',
            'options': {'multiLine': 'true'}
        }
        
        executor._load_source(source_config)
        
        assert executor.dataframes['test_source'].count() == 2
    
    def test_missing_source_file_raises_error(self, spark):
        """Test that missing source file raises error."""
        executor = DataFlowExecutor(spark)
        source_config = {
            'name': 'missing',
            'path': '/nonexistent/path/file.json',
            'format': 'JSON'
        }
        
        with pytest.raises(TransformationError):
            executor._load_source(source_config)
    
    def test_apply_transformation_validate_fields(self, spark, sample_df):
        """Test applying validate_fields transformation."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        transformation_config = {
            'name': 'validation',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']}
                ]
            }
        }
        
        executor._apply_transformation(transformation_config)
        
        assert 'validation_ok' in executor.dataframes
        assert 'validation_ko' in executor.dataframes
        assert executor.dataframes['validation_ok'].count() == 1
        assert executor.dataframes['validation_ko'].count() == 2
    
    def test_apply_transformation_add_fields(self, spark, sample_df):
        """Test applying add_fields transformation."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        transformation_config = {
            'name': 'add_timestamp',
            'type': 'add_fields',
            'params': {
                'input': 'input',
                'addFields': [
                    {'name': 'dt', 'function': 'current_timestamp'}
                ]
            }
        }
        
        executor._apply_transformation(transformation_config)
        
        assert 'add_timestamp' in executor.dataframes
        assert 'dt' in executor.dataframes['add_timestamp'].columns
        assert executor.dataframes['add_timestamp'].count() == 3
    
    def test_transformation_with_missing_input_raises_error(self, spark):
        """Test that transformation with missing input raises error."""
        executor = DataFlowExecutor(spark)
        
        transformation_config = {
            'name': 'test',
            'type': 'validate_fields',
            'params': {
                'input': 'nonexistent',
                'validations': []
            }
        }
        
        with pytest.raises(TransformationError):
            executor._apply_transformation(transformation_config)


class TestDataFlowExecution:
    """Test complete dataflow execution."""
    
    def test_execute_simple_dataflow(self, spark, sample_df):
        """Test executing a complete dataflow."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['person_inputs'] = sample_df
        
        config = {
            'dataflows': [
                {
                    'name': 'test_flow',
                    'sources': [],
                    'transformations': [
                        {
                            'name': 'validation',
                            'type': 'validate_fields',
                            'params': {
                                'input': 'person_inputs',
                                'validations': [
                                    {'field': 'office', 'validations': ['notEmpty']},
                                    {'field': 'age', 'validations': ['notNull']}
                                ]
                            }
                        },
                        {
                            'name': 'ok_with_date',
                            'type': 'add_fields',
                            'params': {
                                'input': 'validation_ok',
                                'addFields': [
                                    {'name': 'dt', 'function': 'current_timestamp'}
                                ]
                            }
                        }
                    ],
                    'sinks': []
                }
            ]
        }
        
        executor.execute(config)
        
        assert 'validation_ok' in executor.dataframes
        assert 'ok_with_date' in executor.dataframes
        assert 'dt' in executor.dataframes['ok_with_date'].columns


class TestDataFlowOrchestration:
    """Test coordination of multiple operations."""
    
    def test_multiple_transformations_in_sequence(self, spark, sample_df):
        """Test chaining multiple transformations."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        trans1 = {
            'name': 'validation',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']}
                ]
            }
        }
        executor._apply_transformation(trans1)
        
        trans2 = {
            'name': 'ok_with_date',
            'type': 'add_fields',
            'params': {
                'input': 'validation_ok',
                'addFields': [
                    {'name': 'dt', 'function': 'current_timestamp'}
                ]
            }
        }
        executor._apply_transformation(trans2)
        
        assert 'ok_with_date' in executor.dataframes
        assert executor.dataframes['ok_with_date'].count() == 1
        assert 'dt' in executor.dataframes['ok_with_date'].columns
    
    def test_dataframes_accumulate_in_executor(self, spark, sample_df):
        """Test that DataFrames accumulate correctly in executor."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        assert len(executor.dataframes) == 1
        
        trans = {
            'name': 'output1',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']}
                ]
            }
        }
        executor._apply_transformation(trans)
        
        assert len(executor.dataframes) == 3
        assert 'input' in executor.dataframes
        assert 'validation_ok' in executor.dataframes
        assert 'validation_ko' in executor.dataframes


class TestDataFlowErrorHandling:
    """Test error handling in dataflow execution."""
    
    def test_invalid_transformation_type_raises_error(self, spark, sample_df):
        """Test that invalid transformation type raises error."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        invalid_trans = {
            'name': 'invalid',
            'type': 'nonexistent_type',
            'params': {'input': 'input'}
        }
        
        with pytest.raises(TransformationError):
            executor._apply_transformation(invalid_trans)
    
    def test_transformation_with_invalid_field_raises_error(self, spark, sample_df):
        """Test that validating nonexistent field raises error."""
        executor = DataFlowExecutor(spark)
        executor.dataframes['input'] = sample_df
        
        trans = {
            'name': 'validation',
            'type': 'validate_fields',
            'params': {
                'input': 'input',
                'validations': [
                    {'field': 'nonexistent_field', 'validations': ['notEmpty']}
                ]
            }
        }
        
        with pytest.raises(TransformationError):
            executor._apply_transformation(trans)