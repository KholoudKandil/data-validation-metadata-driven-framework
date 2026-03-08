"""
Field validation rules.

Purpose: Define what "valid" means for different field types.
Registry pattern: easy to add new validators without changing core logic.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Callable, Dict

from src.exceptions import ConfigurationError


# Validator functions: take a Spark column, return boolean column
def validate_notEmpty(col):
    """
    Field cannot be empty string.
    
    Valid: "RIO", "London", "123"
    Invalid: "", None
    
    Args:
        col: Spark Column
        
    Returns:
        Boolean column (True = valid, False = invalid)
    """
    return col.isNotNull() & (F.col(col.name) != "")


def validate_notNull(col):
    """
    Field cannot be null.
    
    Valid: "RIO", 39, ""
    Invalid: None
    
    Args:
        col: Spark Column
        
    Returns:
        Boolean column (True = valid, False = invalid)
    """
    return col.isNotNull()


# Registry: maps validator name to function
# Easy to extend: add new function + add to dict
VALIDATORS: Dict[str, Callable] = {
    'notEmpty': validate_notEmpty,
    'notNull': validate_notNull,
    # To add more: 'regex': validate_regex, 'positive': validate_positive
}


def get_validator(name: str) -> Callable:
    """
    Get validator function by name.
    
    Args:
        name: Validator name (e.g., 'notEmpty')
        
    Returns:
        Validator function
        
    Raises:
        ConfigurationError: If validator not found
    """
    if name not in VALIDATORS:
        available = ', '.join(VALIDATORS.keys())
        raise ConfigurationError(
            f"Unknown validator '{name}'. Available: {available}"
        )
    return VALIDATORS[name]


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
    if not df.schema[field_name]:
        raise ConfigurationError(
            f"Field '{field_name}' not found in DataFrame"
        )
    
    col = df[field_name]
    result = None
    
    for validation_name in validations:
        validator = get_validator(validation_name)
        check = validator(col)
        
        if result is None:
            result = check
        else:
            result = result & check
    
    return result if result is not None else F.lit(True)
