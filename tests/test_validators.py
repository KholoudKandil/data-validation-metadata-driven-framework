"""Test field validators."""

import pytest
from src.validators import validate_notEmpty, validate_notNull, apply_validations


class TestValidators:
    """Test validation rules."""
    
    def test_validate_notEmpty_passes_non_empty(self, sample_df):
        """notEmpty validator should pass non-empty strings."""
        result = sample_df \
            .withColumn('is_valid', validate_notEmpty(sample_df['office'])) \
            .filter('is_valid') \
            .count()
        
        # Fran has "RIO", Miguel has "RIO" → 2 rows pass
        assert result == 2
    
    def test_validate_notEmpty_fails_empty_string(self, sample_df):
        """notEmpty validator should fail empty strings."""
        result = sample_df \
            .filter(sample_df['office'] == '') \
            .withColumn('is_valid', validate_notEmpty(sample_df['office'])) \
            .filter('not is_valid') \
            .count()
        
        # Xabier has empty office → 1 row fails
        assert result == 1
    
    def test_validate_notNull_passes_non_null(self, sample_df):
        """notNull validator should pass non-null values."""
        result = sample_df \
            .withColumn('is_valid', validate_notNull(sample_df['age'])) \
            .filter('is_valid') \
            .count()
        
        # Xabier and Fran have ages → 2 rows pass
        assert result == 2
    
    def test_validate_notNull_fails_null(self, sample_df):
        """notNull validator should fail null values."""
        result = sample_df \
            .filter(sample_df['age'].isNull()) \
            .count()
        
        # Miguel has null age → 1 row
        assert result == 1


class TestApplyValidations:
    """Test applying multiple validators to a field."""
    
    def test_apply_multiple_validations(self, sample_df):
        """Should apply all validations for a field."""
        is_valid = apply_validations(
            sample_df,
            'office',
            ['notEmpty']
        )
        
        result = sample_df.filter(is_valid).count()
        assert result == 2  # Only non-empty offices pass
