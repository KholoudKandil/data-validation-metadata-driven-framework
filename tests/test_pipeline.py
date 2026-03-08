"""Test pipeline execution."""

import pytest
from pyspark.sql import SparkSession
from src.transformations import transform_validate_fields


class TestTransformations:
    """Test transformation logic."""
    
    def test_validate_fields_splits_ok_ko(self, sample_df):
        """validate_fields should split rows into OK and KO."""
        result = transform_validate_fields(
            sample_df,
            {
                'input': 'test_input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']},
                ]
            },
            {'test_input': sample_df}
        )
        
        # Should have _ok and _ko versions
        assert 'test_input_ok' in result
        assert 'test_input_ko' in result
        
        # Only Fran passes both (office="RIO", age=31)
        ok_count = result['test_input_ok'].count()
        ko_count = result['test_input_ko'].count()
        
        assert ok_count == 1
        assert ko_count == 2
    
    def test_ko_has_error_messages(self, sample_df):
        """KO DataFrame should have error code column."""
        result = transform_validate_fields(
            sample_df,
            {
                'input': 'test_input',
                'validations': [
                    {'field': 'office', 'validations': ['notEmpty']},
                    {'field': 'age', 'validations': ['notNull']},
                ]
            },
            {'test_input': sample_df}
        )
        
        ko_df = result['test_input_ko']
        
        # Should have error code column
        assert 'error code' in ko_df.columns
        
        # Error codes should be populated
        errors = ko_df.select('error code').collect()
        for row in errors:
            assert row['error code'] is not None
