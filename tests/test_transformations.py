"""Test transformation logic.

Tests the transformations module which provides:
- transform_validate_fields(): Validate and split into OK/KO
- transform_add_fields(): Add new columns to DataFrame
"""

import pytest
from pyspark.sql import functions as F

from src.transformations import transform_validate_fields, transform_add_fields
from src.exceptions import ConfigurationError, TransformationError


class TestTransformValidateFields:
    """Test transform_validate_fields() - validates fields and splits into OK/KO."""
    
    def test_validate_fields_splits_ok_ko(self, spark, sample_df):
        """
        Validate fields should split DataFrame into validation_ok and validation_ko.
        
        Input (3 records):
        - Xabier: office="" (fails)
        - Miguel: age=null (fails)
        - Fran: both valid (passes)
        
        Output:
        - validation_ok: 1 row (Fran)
        - validation_ko: 2 rows (Xabier, Miguel)
        """
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        
        result = transform_validate_fields(sample_df, params, {'test_input': sample_df})
        
        # Verify both DataFrames exist
        assert 'validation_ok' in result
        assert 'validation_ko' in result
        
        # Verify counts
        ok_count = result['validation_ok'].count()
        ko_count = result['validation_ko'].count()
        
        assert ok_count == 1, f"Expected 1 row in validation_ok (Fran), got {ok_count}"
        assert ko_count == 2, f"Expected 2 rows in validation_ko (Xabier, Miguel), got {ko_count}"
    
    def test_validation_ok_contains_fran(self, spark, sample_df):
        """Fran should be in the OK set (passes all validations)."""
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        
        result = transform_validate_fields(sample_df, params, {'test_input': sample_df})
        ok_df = result['validation_ok']
        
        # Get the name from ok_df
        names = ok_df.select('name').collect()
        names_list = [row['name'] for row in names]
        
        assert 'Fran' in names_list
        assert len(names_list) == 1
    
    def test_validation_ko_contains_xabier_and_miguel(self, spark, sample_df):
        """Xabier and Miguel should be in KO set (failed validations)."""
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        
        result = transform_validate_fields(sample_df, params, {'test_input': sample_df})
        ko_df = result['validation_ko']
        
        # Get names from ko_df
        names = ko_df.select('name').collect()
        names_list = [row['name'] for row in names]
        
        assert 'Xabier' in names_list
        assert 'Miguel' in names_list
        assert len(names_list) == 2
    
    def test_ko_has_error_code_column(self, spark, sample_df):
        """KO DataFrame should have error_code column with error messages."""
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        
        result = transform_validate_fields(sample_df, params, {'test_input': sample_df})
        ko_df = result['validation_ko']
        
        # Verify error_code column exists
        assert 'error_code' in ko_df.columns
        
        # Verify all KO rows have error messages
        error_codes = ko_df.select('error_code').collect()
        for row in error_codes:
            assert row['error_code'] is not None
            assert row['error_code'] != ""
    
    def test_error_messages_explain_failures(self, spark, sample_df):
        """Error messages should explain why validation failed."""
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        
        result = transform_validate_fields(sample_df, params, {'test_input': sample_df})
        ko_df = result['validation_ko']
        
        # Get error messages
        errors_with_names = ko_df.select('name', 'error_code').collect()
        
        # Find Xabier's error (empty office)
        xabier_errors = [row for row in errors_with_names if row['name'] == 'Xabier']
        assert len(xabier_errors) > 0
        assert "office" in xabier_errors[0]['error_code'].lower()
        
        # Find Miguel's error (null age)
        miguel_errors = [row for row in errors_with_names if row['name'] == 'Miguel']
        assert len(miguel_errors) > 0
        assert "age" in miguel_errors[0]['error_code'].lower()
    
    def test_missing_field_raises_error(self, spark, sample_df):
        """Validation on non-existent field should raise ConfigurationError."""
        params = {
            'input': 'test_input',
            'validations': [
                {'field': 'nonexistent_field', 'validations': ['notEmpty']},
            ]
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            transform_validate_fields(sample_df, params, {'test_input': sample_df})
        
        assert "not found" in str(exc_info.value).lower()
    
    def test_missing_validations_param_raises_error(self, spark, sample_df):
        """Missing 'validations' parameter should raise ConfigurationError."""
        params = {
            'input': 'test_input',
            # No 'validations' key
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            transform_validate_fields(sample_df, params, {'test_input': sample_df})
        
        assert "requires 'validations'" in str(exc_info.value)


class TestTransformAddFields:
    """Test transform_add_fields() - adds new columns to DataFrame."""
    
    def test_add_current_timestamp(self, spark, sample_df):
        """Add current_timestamp field to DataFrame."""
        params = {
            'input': 'validation_ok',
            'addFields': [
                {'name': 'dt', 'function': 'current_timestamp'}
            ]
        }
        
        result = transform_add_fields(sample_df, params, {}, transformation_name='add_timestamp')
        
        # Verify transformation name is used as key
        assert 'add_timestamp' in result
        
        df_with_timestamp = result['add_timestamp']
        
        # Verify dt column exists
        assert 'dt' in df_with_timestamp.columns
        
        # Verify all rows have timestamp
        dt_values = df_with_timestamp.select('dt').collect()
        assert len(dt_values) == 3  # 3 input rows
        
        # All timestamps should be non-null
        for row in dt_values:
            assert row['dt'] is not None
    
    def test_add_multiple_fields(self, spark, sample_df):
        """Add multiple fields in one transformation."""
        params = {
            'input': 'test_input',
            'addFields': [
                {'name': 'dt', 'function': 'current_timestamp'},
                {'name': 'dt2', 'function': 'current_timestamp'},
            ]
        }
        
        result = transform_add_fields(sample_df, params, {}, transformation_name='add_dates')
        df_result = result['add_dates']
        
        # Both columns should exist
        assert 'dt' in df_result.columns
        assert 'dt2' in df_result.columns
        
        # Original columns should still exist
        assert 'name' in df_result.columns
        assert 'age' in df_result.columns
    
    def test_unknown_function_raises_error(self, spark, sample_df):
        """Unknown field function should raise ConfigurationError."""
        params = {
            'input': 'test_input',
            'addFields': [
                {'name': 'new_field', 'function': 'unknown_function'}
            ]
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            transform_add_fields(sample_df, params, {}, transformation_name='test')
        
        assert "Unknown function" in str(exc_info.value)
    
    def test_missing_addFields_param_raises_error(self, spark, sample_df):
        """Missing 'addFields' parameter should raise ConfigurationError."""
        params = {
            'input': 'test_input',
            # No 'addFields' key
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            transform_add_fields(sample_df, params, {}, transformation_name='test')
        
        assert "requires 'addFields'" in str(exc_info.value)
    
    def test_output_uses_transformation_name(self, spark, sample_df):
        """Output DataFrame should use transformation name as key."""
        params = {
            'input': 'test_input',
            'addFields': [
                {'name': 'dt', 'function': 'current_timestamp'}
            ]
        }
        
        # Call with different transformation names
        result1 = transform_add_fields(sample_df, params, {}, transformation_name='transform1')
        result2 = transform_add_fields(sample_df, params, {}, transformation_name='transform2')
        
        # Keys should match transformation names
        assert 'transform1' in result1
        assert 'transform2' in result2
        assert 'transform1' not in result2
        assert 'transform2' not in result1


class TestEndToEndValidateAndAdd:
    """Test combining validate_fields and add_fields transformations."""
    
    def test_validate_then_add_timestamp(self, spark, sample_df):
        """
        End-to-end: Validate fields, then add timestamp to OK records.
        
        Flow:
        1. Validate all records
        2. Take validation_ok records
        3. Add timestamp to those records
        """
        # Step 1: Validate
        validate_params = {
            'input': 'raw_input',
            'validations': [
                {'field': 'office', 'validations': ['notEmpty']},
                {'field': 'age', 'validations': ['notNull']},
            ]
        }
        validate_result = transform_validate_fields(
            sample_df, 
            validate_params, 
            {'raw_input': sample_df}
        )
        
        # Step 2: Add timestamp to OK records
        add_params = {
            'input': 'validation_ok',
            'addFields': [
                {'name': 'process_timestamp', 'function': 'current_timestamp'}
            ]
        }
        add_result = transform_add_fields(
            validate_result['validation_ok'],
            add_params,
            {},
            transformation_name='with_timestamp'
        )
        
        final_df = add_result['with_timestamp']
        
        # Verify result
        assert 'process_timestamp' in final_df.columns
        assert final_df.count() == 1  # Only Fran passes validation
        
        # Get the final record
        final_record = final_df.select('name', 'age', 'office', 'process_timestamp').collect()[0]
        assert final_record['name'] == 'Fran'
        assert final_record['age'] == 31
        assert final_record['office'] == 'RIO'
        assert final_record['process_timestamp'] is not None