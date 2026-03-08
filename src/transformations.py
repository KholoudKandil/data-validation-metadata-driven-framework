"""
Transformation logic: how to modify data based on metadata.

Two transformation types:
1. validate_fields: Apply validation rules, split into OK/KO
2. add_fields: Add new columns based on functions
"""

from pyspark.sql import DataFrame, functions as F
from typing import Dict, Any, Callable, List

from src.exceptions import TransformationError, ConfigurationError
from src.validators import apply_validations


def transform_validate_fields(
    df: DataFrame,
    params: Dict[str, Any],
    dataframes: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    """
    Validate specified fields, split into passed/failed.
    
    Input:
        - dataframe with rows
        - validation rules per field
    
    Output:
        - '{name}_ok': rows that passed all validations
        - '{name}_ko': rows that failed, with error messages
    
    Args:
        df: Input DataFrame
        params: Transformation parameters
            - input: DataFrame reference name
            - validations: list of {field, validations: [rule1, rule2]}
        dataframes: Dict of available DataFrames
    
    Returns:
        Dict with '_ok' and '_ko' versions of DataFrame
    """
    validations_config = params.get('validations', [])
    
    if not validations_config:
        raise ConfigurationError("validate_fields requires 'validations' parameter")
    
    # Build validation expression: check all fields
    validation_results = {}
    error_cols = []
    
    for field_config in validations_config:
        field_name = field_config['field']
        field_validations = field_config['validations']
        
        # Apply all validations for this field
        is_valid = apply_validations(df, field_name, field_validations)
        validation_results[field_name] = is_valid
        
        # Build error message for this field
        error_msg = F.when(~is_valid, f"{field_name} can't be blank")
        error_cols.append(error_msg)
    
    # Combine all field checks: valid if ALL pass
    all_valid = None
    for is_valid in validation_results.values():
        if all_valid is None:
            all_valid = is_valid
        else:
            all_valid = all_valid & is_valid
    
    # Split into OK and KO
    df_ok = df.filter(all_valid)
    
    # Add error message column to failed rows
    df_ko = df.filter(~all_valid).withColumn(
        'error code',
        F.concat_ws(' | ', *[col for col in error_cols if col is not None])
    )
    
    return {
        f"{params['input']}_ok": df_ok,
        f"{params['input']}_ko": df_ko,
    }


def transform_add_fields(
    df: DataFrame,
    params: Dict[str, Any],
    dataframes: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    """
    Add new columns to DataFrame.
    
    Supported functions:
    - current_timestamp: Spark's current_timestamp()
    - (extensible: add more functions here)
    
    Args:
        df: Input DataFrame
        params: Transformation parameters
            - input: DataFrame reference name
            - addFields: list of {name, function, params?}
        dataframes: Dict of available DataFrames
    
    Returns:
        Dict with new DataFrame (same name as input)
    """
    add_fields_config = params.get('addFields', [])
    
    if not add_fields_config:
        raise ConfigurationError("add_fields requires 'addFields' parameter")
    
    # Available field functions (extensible)
    field_functions = {
        'current_timestamp': lambda: F.current_timestamp(),
        # Can add: 'uuid': lambda: F.expr('uuid()'), etc.
    }
    
    result_df = df
    
    for field_spec in add_fields_config:
        field_name = field_spec['name']
        function_name = field_spec['function']
        
        if function_name not in field_functions:
            available = ', '.join(field_functions.keys())
            raise ConfigurationError(
                f"Unknown function '{function_name}'. Available: {available}"
            )
        
        field_func = field_functions[function_name]
        result_df = result_df.withColumn(field_name, field_func())
    
    return {params['input']: result_df}


# Transformation registry (extensible)
TRANSFORMATIONS: Dict[str, Callable] = {
    'validate_fields': transform_validate_fields,
    'add_fields': transform_add_fields,
    # Can add: 'deduplicate': transform_deduplicate, etc.
}


def get_transformation(name: str) -> Callable:
    """
    Get transformation function by name.
    
    Args:
        name: Transformation name
        
    Returns:
        Transformation function
        
    Raises:
        ConfigurationError: If transformation not found
    """
    if name not in TRANSFORMATIONS:
        available = ', '.join(TRANSFORMATIONS.keys())
        raise ConfigurationError(
            f"Unknown transformation '{name}'. Available: {available}"
        )
    return TRANSFORMATIONS[name]
