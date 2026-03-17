"""
Field validation rules.

Purpose: Define what "valid" means for different field types.
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
        validations: List of validation names
        
    Returns:
        Boolean column indicating if row passed all validations
    """
    if field_name not in df.columns:
        raise ConfigurationError(
            f"Field '{field_name}' not found in DataFrame"
        )
    
    col = df[field_name]
    result = None
    
    for validation_name in validations:
        # Get the validation check expression
        check = get_validation_expression(field_name, validation_name)
        
        if result is None:
            result = check
        else:
            result = result & check
    
    return result if result is not None else F.lit(True)


def get_validation_expression(field_name: str, validation_name: str):
    """
    Get a Spark expression for a validation rule.
    
    Args:
        field_name: Column name to validate
        validation_name: Name of validation (notEmpty, notNull, etc.)
        
    Returns:
        Spark Column expression (boolean)
        
    Raises:
        ConfigurationError: If validation not found
    """
    if validation_name == 'notEmpty':
        # Field cannot be empty string AND must not be null
        return (F.col(field_name).isNotNull()) & (F.col(field_name) != "")
    
    elif validation_name == 'notNull':
        # Field cannot be null
        return F.col(field_name).isNotNull()
    
    else:
        available = ['notEmpty', 'notNull']
        raise ConfigurationError(
            f"Unknown validator '{validation_name}'. Available: {', '.join(available)}"
        )