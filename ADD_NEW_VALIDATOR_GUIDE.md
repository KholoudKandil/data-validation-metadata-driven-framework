# How to Add a New Validator: Age Range (20-30)

## Overview

We'll add a new validator called `ageRange` that checks if age is between 20 and 30 (inclusive).

**Files to Edit:**
1. `src/validators.py` - Add validator function and register it
2. `config/metadata.yaml` - Update configuration to use it
3. `tests/test_validators.py` - Add unit tests
4. `README.md` - Update documentation (optional)

---

## Step 1: Add Validator Function to `src/validators.py`

### Location: Lines 78-91 (after `validate_notnull_expr`)

**Current code (lines 67-78):**
```python
def validate_notnull_expr(field_name: str):
    """
    Validation: field cannot be null.
    
    Args:
        field_name: Column name to validate
        
    Returns:
        Spark Column expression (boolean)
    """
    return F.col(field_name).isNotNull()
```

**Add this new function after line 78:**
```python

def validate_agerange_expr(field_name: str, min_age: int = 20, max_age: int = 30):
    """
    Validation: field value must be between min_age and max_age (inclusive).
    
    Args:
        field_name: Column name to validate
        min_age: Minimum allowed age (default: 20)
        max_age: Maximum allowed age (default: 30)
        
    Returns:
        Spark Column expression (boolean)
        
    Example:
        validate_agerange_expr('age', min_age=18, max_age=65)
        → Returns: (age >= 18) AND (age <= 65)
    """
    return (F.col(field_name) >= min_age) & (F.col(field_name) <= max_age)
```

---

## Step 2: Register Validator in Registry

### Location: Lines 84-90 (VALIDATION_EXPRESSIONS dictionary)

**Current code:**
```python
VALIDATION_EXPRESSIONS: Dict[str, Callable] = {
    'notEmpty': validate_notempty_expr,
    'notNull': validate_notnull_expr,
    # Extensible: add new validators here
    # 'regex': validate_regex_expr,
    # 'range': validate_range_expr,
}
```

**Updated code (add new entry):**
```python
VALIDATION_EXPRESSIONS: Dict[str, Callable] = {
    'notEmpty': validate_notempty_expr,
    'notNull': validate_notnull_expr,
    'ageRange': validate_agerange_expr,  # ← NEW: Added age range validator
    # Extensible: add new validators here
    # 'regex': validate_regex_expr,
    # 'range': validate_range_expr,
}
```

---

## Step 3: Update Configuration File

### Location: `config/metadata.yaml` (Update transformations section)

**Current validations:**
```yaml
transformations:
  - name: validation
    type: validate_fields
    params:
      input: person_inputs
      validations:
        - field: office
          validations:
            - notEmpty
        - field: age
          validations:
            - notNull
```

**Updated validations (add ageRange check):**
```yaml
transformations:
  - name: validation
    type: validate_fields
    params:
      input: person_inputs
      validations:
        - field: office
          validations:
            - notEmpty
        - field: age
          validations:
            - notNull
            - name: ageRange           # ← NEW: Added age range validation
              params:                  # ← NEW: Parameters for age range
                min_age: 20            # ← NEW: Minimum age
                max_age: 30            # ← NEW: Maximum age
```

**Note:** This requires updating `apply_validations()` function to handle parameterized validators.

---

## Step 4: Update `apply_validations()` Function (Important!)

### Location: `src/validators.py` (Lines 14-47)

**Current code (doesn't support parameters):**
```python
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
```

**Updated code (supports parameters):**
```python
def apply_validations(df, field_name: str, validations: list):
    """
    Apply all validations to a field.
    
    Args:
        df: Spark DataFrame
        field_name: Column to validate
        validations: List of validation names or dicts with parameters
                    Examples:
                    - ['notEmpty']
                    - ['notNull', 'ageRange']
                    - [{'name': 'ageRange', 'params': {'min_age': 25, 'max_age': 65}}]
        
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
    
    for validation in validations:
        # Support both simple string and dict with parameters
        if isinstance(validation, str):
            validation_name = validation
            params = {}
        elif isinstance(validation, dict):
            validation_name = validation['name']
            params = validation.get('params', {})
        else:
            raise ConfigurationError(
                f"Invalid validation format: {validation}. "
                f"Expected string or dict with 'name' and 'params'."
            )
        
        # Get the validation check expression with parameters
        check = get_validation_expression(field_name, validation_name, **params)
        
        if result is None:
            result = check
        else:
            # Combine with AND logic (must pass ALL validations)
            result = result & check
    
    return result if result is not None else F.lit(True)
```

---

## Step 5: Update `get_validation_expression()` Function

### Location: `src/validators.py` (Lines 93-118)

**Current code (no parameters):**
```python
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
```

**Updated code (supports parameters):**
```python
def get_validation_expression(field_name: str, validation_name: str, **kwargs):
    """
    Get a Spark expression for a validation rule (using registry).
    
    This function implements the Factory pattern: takes a validation name
    and returns the appropriate validation expression function.
    
    Args:
        field_name: Column name to validate
        validation_name: Name of validation (notEmpty, notNull, ageRange, etc.)
        **kwargs: Optional parameters for validators
                 Examples:
                 - get_validation_expression('age', 'ageRange', min_age=20, max_age=30)
        
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
    
    # Pass parameters if provided
    if kwargs:
        return validation_func(field_name, **kwargs)
    else:
        return validation_func(field_name)
```

---

## Step 6: Add Unit Tests for New Validator

### Location: `tests/test_validators.py` (Add new test class at end of file)

**Add this new test class (after existing test classes, before EOF):**

```python

class TestAgeRangeValidator:
    """Test ageRange validator: values between 20 and 30."""
    
    def test_agerange_expression_basic(self, spark):
        """Test that ageRange validator accepts values between 20 and 30."""
        df = spark.createDataFrame([
            {"age": 19},    # Below range
            {"age": 20},    # At min
            {"age": 25},    # In range
            {"age": 30},    # At max
            {"age": 31},    # Above range
            {"age": None},  # Null
        ])
        
        expr = get_validation_expression("age", "ageRange", min_age=20, max_age=30)
        result = df.filter(expr)
        
        # Only 20, 25, 30 should pass
        assert result.count() == 3
        assert result.filter(F.col("age") == 20).count() == 1
        assert result.filter(F.col("age") == 25).count() == 1
        assert result.filter(F.col("age") == 30).count() == 1
    
    def test_agerange_excludes_below_minimum(self, spark):
        """Test that ageRange rejects values below minimum."""
        df = spark.createDataFrame([
            {"age": 19},
            {"age": 20},
        ])
        
        expr = get_validation_expression("age", "ageRange", min_age=20, max_age=30)
        result = df.filter(expr)
        
        assert result.count() == 1
        assert result.select("age").collect()[0][0] == 20
    
    def test_agerange_excludes_above_maximum(self, spark):
        """Test that ageRange rejects values above maximum."""
        df = spark.createDataFrame([
            {"age": 30},
            {"age": 31},
        ])
        
        expr = get_validation_expression("age", "ageRange", min_age=20, max_age=30)
        result = df.filter(expr)
        
        assert result.count() == 1
        assert result.select("age").collect()[0][0] == 30
    
    def test_agerange_with_custom_range(self, spark):
        """Test ageRange with custom min/max values."""
        df = spark.createDataFrame([
            {"age": 24},
            {"age": 25},
            {"age": 26},
        ])
        
        # Custom range: 25-26
        expr = get_validation_expression("age", "ageRange", min_age=25, max_age=26)
        result = df.filter(expr)
        
        assert result.count() == 2
        assert result.filter(F.col("age") == 25).count() == 1
        assert result.filter(F.col("age") == 26).count() == 1
    
    def test_agerange_with_null_values(self, spark):
        """Test that ageRange rejects null values."""
        df = spark.createDataFrame([
            {"age": 25},
            {"age": None},
        ])
        
        expr = get_validation_expression("age", "ageRange", min_age=20, max_age=30)
        result = df.filter(expr)
        
        assert result.count() == 1
        assert result.select("age").collect()[0][0] == 25
    
    def test_combined_validations_with_agerange(self, spark):
        """Test combining notNull with ageRange."""
        df = spark.createDataFrame([
            {"age": 19},
            {"age": 25},
            {"age": 31},
            {"age": None},
        ])
        
        # Both notNull AND ageRange
        null_check = apply_validations(df, "age", ["notNull"])
        range_check = get_validation_expression("age", "ageRange", min_age=20, max_age=30)
        
        result = df.filter(null_check & range_check)
        
        # Only 25 passes both
        assert result.count() == 1
        assert result.select("age").collect()[0][0] == 25
```

---

## Step 7: Update `apply_validations()` in Test (If Needed)

### Location: `tests/test_validators.py` - Update existing test if using new format

**If your test uses the new dict format with parameters:**

```python
def test_validate_fields_with_age_range(self, spark, sample_df):
    """Test validate_fields with age range validation."""
    params = {
        'input': 'input',
        'validations': [
            {'field': 'office', 'validations': ['notEmpty']},
            {
                'field': 'age', 
                'validations': [
                    'notNull',
                    {'name': 'ageRange', 'params': {'min_age': 20, 'max_age': 30}}  # ← NEW
                ]
            }
        ]
    }
    
    result = transform_validate_fields(sample_df, params, {})
    
    # Only Fran (age 31) passes (wait, he's 31, so he fails)
    # Only records with age between 20-30 pass
    ok_df = result['validation_ok']
    ko_df = result['validation_ko']
    
    # Verify the split
    assert ok_df.count() + ko_df.count() == sample_df.count()
```

---

## Summary of All Changes

| File | Change | Location | Type |
|------|--------|----------|------|
| `src/validators.py` | Add `validate_agerange_expr()` | Line 79-91 | New Function |
| `src/validators.py` | Register in `VALIDATION_EXPRESSIONS` | Line 87 | Edit Dictionary |
| `src/validators.py` | Update `apply_validations()` | Lines 14-47 | Edit Function |
| `src/validators.py` | Update `get_validation_expression()` | Lines 93-118 | Edit Function |
| `config/metadata.yaml` | Add ageRange to validations | transformations.params | Edit Config |
| `tests/test_validators.py` | Add `TestAgeRangeValidator` class | End of file | New Test Class |
| `tests/test_validators.py` | Add 6 new test methods | New tests | New Tests |

---

## Step-by-Step Execution Checklist

```
[  ] 1. Edit src/validators.py - Add validate_agerange_expr() function
[  ] 2. Edit src/validators.py - Add 'ageRange' to VALIDATION_EXPRESSIONS
[  ] 3. Edit src/validators.py - Update apply_validations() to handle params
[  ] 4. Edit src/validators.py - Update get_validation_expression() to accept **kwargs
[  ] 5. Edit config/metadata.yaml - Add ageRange validation to age field
[  ] 6. Edit tests/test_validators.py - Add TestAgeRangeValidator class
[  ] 7. Edit tests/test_validators.py - Add 6 new test methods
[  ] 8. Run tests: pytest tests/test_validators.py -v
[  ] 9. Expected: 12 + 6 = 18 tests passing in test_validators.py
[  ] 10. Run full suite: pytest tests/ -v
[  ] 11. Expected: 35 + 6 = 41 tests passing total
[  ] 12. Run pipeline: python src/main.py config/metadata.yaml
[  ] 13. Expected: Pipeline completes, uses new validator
[  ] 14. Git commit: git add . && git commit -m "Add ageRange validator"
```

---

## Testing Your New Validator

### Test 1: Run Unit Tests Only
```powershell
docker-compose run --rm spark-dev pytest tests/test_validators.py::TestAgeRangeValidator -v
```

Expected output:
```
test_agerange_expression_basic .............. PASSED
test_agerange_excludes_below_minimum ........ PASSED
test_agerange_excludes_above_maximum ........ PASSED
test_agerange_with_custom_range ............ PASSED
test_agerange_with_null_values ............. PASSED
test_combined_validations_with_agerange .... PASSED

====== 6 passed in 2.34s ======
```

### Test 2: Run All Tests
```powershell
docker-compose run --rm spark-dev pytest tests/ -v
```

Expected output:
```
test_validators.py ...................... 18 passed (12 old + 6 new)
test_transformations.py ................. 13 passed
test_pipeline.py ....................... 10 passed

====== 41 passed in 48.32s ======
```

### Test 3: Run Pipeline
```powershell
docker-compose run --rm spark-dev python src/main.py config/metadata.yaml
```

Expected output:
```
INFO - Starting dataflow: dataflow1
INFO - Loading sources...
INFO -   Loading person_inputs from data/input/events/person/* (format: json)
INFO -     ✓ Loaded 3 rows
INFO - Applying transformations...
INFO -   Applying transformation: validation (validate_fields)
INFO -     ✓ Created DataFrame: validation_ok
INFO -     ✓ Created DataFrame: validation_ko
INFO -   Applying transformation: ok_with_date (add_fields)
INFO -     ✓ Created DataFrame: ok_with_date
INFO - Writing sinks...
INFO -   Writing sink: raw-ok
INFO -     ✓ Sink 'raw-ok' completed
INFO -   Writing sink: raw-ko
INFO -     ✓ Sink 'raw-ko' completed
INFO - ✓ Dataflow 'dataflow1' completed successfully
```

---

## Verification Steps

### Step 1: Verify validators.py Changes
```powershell
# Check that function exists
grep -n "validate_agerange_expr" src/validators.py

# Should show:
# 79:def validate_agerange_expr(field_name: str, min_age: int = 20, max_age: int = 30):
# 87:'ageRange': validate_agerange_expr,
```

### Step 2: Verify metadata.yaml Changes
```powershell
# Check that ageRange is in config
grep -A5 "age:" config/metadata.yaml | grep -A3 "validations:"

# Should show ageRange in the validation list
```

### Step 3: Verify Tests Exist
```powershell
# Check test class exists
grep -n "class TestAgeRangeValidator" tests/test_validators.py

# Should show the line number
```

### Step 4: Run Quick Validation
```powershell
# Quick test to ensure no syntax errors
docker-compose run --rm spark-dev python -c "from src.validators import validate_agerange_expr; print('✓ Validator loaded successfully')"
```

---

## Common Issues & Solutions

### Issue 1: Tests fail with "TypeError: validate_agerange_expr() missing 1 required positional argument"

**Cause:** `get_validation_expression()` not passing parameters

**Fix:** Ensure you updated `get_validation_expression()` to accept and pass `**kwargs`

```python
# Wrong
return validation_func(field_name)

# Right
if kwargs:
    return validation_func(field_name, **kwargs)
else:
    return validation_func(field_name)
```

### Issue 2: YAML parsing fails

**Cause:** Invalid YAML indentation or formatting

**Fix:** Ensure proper indentation (2 spaces, not tabs)

```yaml
# Wrong - tabs
→ name: ageRange

# Right - 2 spaces
  name: ageRange
```

### Issue 3: Validator not in registry error

**Cause:** Forgot to add validator to `VALIDATION_EXPRESSIONS` dictionary

**Fix:** Make sure entry is in the dict:

```python
VALIDATION_EXPRESSIONS: Dict[str, Callable] = {
    'notEmpty': validate_notempty_expr,
    'notNull': validate_notnull_expr,
    'ageRange': validate_agerange_expr,  # ← Must be here
}
```

---

## After Adding the Validator

### Update Test Sample Data (Optional)

If you want sample data that includes ages in the range:

```python
# In conftest.py
@pytest.fixture
def sample_data():
    return [
        {"name": "Xabier", "age": 19, "office": ""},      # Below range
        {"name": "Miguel", "age": None, "office": "RIO"}, # Null age
        {"name": "Fran", "age": 25, "office": "RIO"},     # In range
    ]
```

---

## Final Checklist

✅ **Before Committing:**
- [ ] All 41 tests passing (35 old + 6 new)
- [ ] No syntax errors
- [ ] Pipeline executes successfully
- [ ] metadata.yaml is valid YAML
- [ ] New validator is registered in VALIDATION_EXPRESSIONS
- [ ] apply_validations() handles dict format
- [ ] get_validation_expression() accepts **kwargs

✅ **After Committing:**
```powershell
git add src/validators.py config/metadata.yaml tests/test_validators.py
git commit -m "feat: Add ageRange validator (20-30 age check)"
git push
```

---

## Summary

You've successfully added a **parameterized validator** that:
- ✅ Checks if age is between min and max values
- ✅ Defaults to 20-30 but is customizable
- ✅ Works with existing validation system
- ✅ Has 6 comprehensive unit tests
- ✅ Integrates with metadata.yaml configuration
- ✅ Maintains the registry pattern
- ✅ Doesn't break existing validators

**Total time to implement:** ~30-45 minutes
**Tests added:** 6
**Files modified:** 3
**New code lines:** ~80

