"""Test field validators.

Tests the validators module which provides:
- apply_validations(): Apply multiple validators to a field
- get_validation_expression(): Get Spark expression for a validation rule
"""

import pytest
from pyspark.sql import functions as F

from src.validators import apply_validations, get_validation_expression
from src.exceptions import ConfigurationError


class TestGetValidationExpression:
    """Test get_validation_expression() - converts validation names to Spark expressions."""
    
    def test_notEmpty_expression(self, spark, sample_df):
        """
        notEmpty should create expression that checks non-empty AND non-null.
        
        Valid: "RIO", "London", "123"
        Invalid: "", None
        """
        # Get the Spark expression for notEmpty on 'office' field
        expr = get_validation_expression('office', 'notEmpty')
        
        # Apply it to the DataFrame
        result_df = sample_df.withColumn('is_valid', expr)
        
        # Count valid rows
        valid_count = result_df.filter('is_valid').count()
        
        # Expected: Fran and Miguel (both have non-empty office)
        # Xabier has empty string "" so fails
        assert valid_count == 2, f"Expected 2 valid non-empty offices, got {valid_count}"
    
    def test_notNull_expression(self, spark, sample_df):
        """
        notNull should create expression that checks for non-null values.
        
        Valid: 39, 31
        Invalid: None
        """
        # Get the Spark expression for notNull on 'age' field
        expr = get_validation_expression('age', 'notNull')
        
        # Apply it
        result_df = sample_df.withColumn('is_valid', expr)
        
        # Count valid rows
        valid_count = result_df.filter('is_valid').count()
        
        # Expected: Xabier and Fran have ages, Miguel's is null
        assert valid_count == 2, f"Expected 2 non-null ages, got {valid_count}"
    
    def test_unknown_validation_raises_error(self):
        """Unknown validation name should raise ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            get_validation_expression('field', 'unknown_validator')
        
        assert "Unknown validator 'unknown_validator'" in str(exc_info.value)
        assert "notEmpty" in str(exc_info.value)
        assert "notNull" in str(exc_info.value)


class TestApplyValidations:
    """Test apply_validations() - applies multiple validators to a field."""
    
    def test_single_notEmpty_validation(self, spark, sample_df):
        """Apply single notEmpty validator to office field."""
        # Apply notEmpty validation to office field
        is_valid = apply_validations(sample_df, 'office', ['notEmpty'])
        
        # Count rows that pass
        result_count = sample_df.filter(is_valid).count()
        
        # Expected: Miguel and Fran (both have non-empty office)
        assert result_count == 2, f"Expected 2 rows with non-empty office, got {result_count}"
    
    def test_single_notNull_validation(self, spark, sample_df):
        """Apply single notNull validator to age field."""
        # Apply notNull validation to age field
        is_valid = apply_validations(sample_df, 'age', ['notNull'])
        
        # Count rows that pass
        result_count = sample_df.filter(is_valid).count()
        
        # Expected: Xabier and Fran (both have non-null age)
        assert result_count == 2, f"Expected 2 rows with non-null age, got {result_count}"
    
    def test_multiple_validations_combined(self, spark, sample_df):
        """
        Apply multiple validators to same field.
        
        Both validators must be true (AND logic).
        """
        # For office: both notEmpty (would pass 2) but we're testing combination
        is_valid = apply_validations(sample_df, 'office', ['notEmpty'])
        
        # All validators combined should use AND logic
        result_count = sample_df.filter(is_valid).count()
        assert result_count == 2
    
    def test_field_not_found_raises_error(self, spark, sample_df):
        """Non-existent field should raise ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            apply_validations(sample_df, 'nonexistent_field', ['notEmpty'])
        
        assert "not found in DataFrame" in str(exc_info.value)
    
    def test_empty_validation_list(self, spark, sample_df):
        """Empty validation list should return True for all rows."""
        # No validations = everything is valid
        is_valid = apply_validations(sample_df, 'office', [])
        
        # Should pass all rows
        result_count = sample_df.filter(is_valid).count()
        assert result_count == 3  # All 3 rows pass


class TestValidatorDetails:
    """Test specific validator behavior with sample data."""
    
    def test_notEmpty_with_empty_string(self, spark):
        """notEmpty specifically rejects empty strings."""
        data = [
            {"field": ""},      # Empty - should fail
            {"field": "value"}, # Non-empty - should pass
            {"field": None},    # Null - should fail (notEmpty also checks isNotNull)
        ]
        df = spark.createDataFrame(data)
        
        expr = get_validation_expression('field', 'notEmpty')
        valid_count = df.filter(expr).count()
        
        # Only "value" passes
        assert valid_count == 1
    
    def test_notNull_with_null(self, spark):
        """notNull specifically rejects null values."""
        data = [
            {"field": None},    # Null - should fail
            {"field": 0},       # 0 is valid (not null)
            {"field": ""},      # Empty string is valid (not null)
        ]
        df = spark.createDataFrame(data)
        
        expr = get_validation_expression('field', 'notNull')
        valid_count = df.filter(expr).count()
        
        # 0 and "" both pass (not null)
        assert valid_count == 2