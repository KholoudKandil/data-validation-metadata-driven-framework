"""
Field validation rules: define what "valid" means for different fields.

Purpose: Encapsulate validation logic in reusable functions.
Registry pattern: easy to add new validators without changing core logic.
"""

from pyspark.sql import functions as F
from typing import Dict, Callable

from src.exceptions import ConfigurationError


def apply_validations(df, field_name: str, validations: list):
    """
    Apply all validations to a field.
    
    Args:
        df: Spark DataFrame
        field_name: Column to validate
        validations: List of validation names (e.g., ['notEmpty', 'notNull'])
        
    Returns:
        Boolean column indicating if row passed all validations
        
    Raises:
        ConfigurationError: If field not found or validation unknown
    """
    if field_name not in df.columns:
        raise ConfigurationError(
            f"Field '{field_name}' not found in DataFrame"
        )
    
    col = df[field_name]
    result = None
    
    for validation_name in validations:
        # Get the validation check expression using registry
        check = get_validation_expression(field_name, validation_name)
        
        if result is None:
            result = check
        else:
            # Combine with AND logic (must pass ALL validations)
            result = result & check
    
    return result if result is not None else F.lit(True)


# ============================================================================
# Validation Expression Functions (registered in VALIDATION_EXPRESSIONS)
# ============================================================================

def validate_notempty_expr(field_name: str):
    """
    Validation: field cannot be empty string AND must not be null.
    
    Args:
        field_name: Column name to validate
        
    Returns:
        Spark Column expression (boolean)
    """
    return (F.col(field_name).isNotNull()) & (F.col(field_name) != "")


def validate_notnull_expr(field_name: str):
    """
    Validation: field cannot be null.
    
    Args:
        field_name: Column name to validate
        
    Returns:
        Spark Column expression (boolean)
    """
    return F.col(field_name).isNotNull()


# ============================================================================
# Registry Pattern: Validation Functions
# ============================================================================

VALIDATION_EXPRESSIONS: Dict[str, Callable] = {
    'notEmpty': validate_notempty_expr,
    'notNull': validate_notnull_expr,
    # Extensible: add new validators here
    # 'regex': validate_regex_expr,
    # 'range': validate_range_expr,
}


def get_validation_expression(field_name: str, validation_name: str):
    """
    Get a Spark expression for a validation rule (using registry).
    
    This function implements the Factory pattern: takes a validation name
    and returns the appropriate validation expression function.
    
    Args:
        field_name: Column name to validate
        validation_name: Name of validation (notEmpty, notNull, etc.)
        
    Returns:
        Spark Column expression (boolean)
        
    Raises:
        ConfigurationError: If validation not found
    """
    if validation_name not in VALIDATION_EXPRESSIONS:
        available = ', '.join(VALIDATION_EXPRESSIONS.keys())
        raise ConfigurationError(
            f"Unknown validator '{validation_name}'. Available: {available}"
        )
    
    # Get validation function from registry and apply it
    validation_func = VALIDATION_EXPRESSIONS[validation_name]
    return validation_func(field_name)