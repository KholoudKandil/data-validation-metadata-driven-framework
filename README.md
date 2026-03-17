# Metadata-Driven Data Framework

A modular, extensible PySpark framework for metadata-driven data pipelines.

## Quick Start

### Option 1: Using Docker (Recommended)

```bash
# Build Docker image
docker-compose build

# Run pipeline in container
docker-compose run --rm spark-dev python src/main.py config/metadata.yaml

# Run tests
docker-compose run --rm spark-dev pytest tests/ -v
```

### Option 2: Local Python Installation

```bash
# Run setup script
./setup.sh

# Activate environment
source venv/bin/activate  # Windows Git Bash: source venv/Scripts/activate

# Run pipeline
python src/main.py config/metadata.yaml

# Run tests
pytest tests/ -v
```

## Architecture

### Design Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Dict-based registries | Simple, extensible, no boilerplate | Less strict than factory patterns |
| Function-based transformers | Composable, testable, Pythonic | No inheritance structure |
| Fail-fast validation | Catch config errors at startup | More upfront checking |
| Spark lazy evaluation | Lets Spark optimize DAG | Debug requires explicit .count() |

### Modules

```
src/
├── config.py          # Load & validate YAML metadata
├── validators.py      # Field validation rules (notEmpty, notNull, etc.)
├── transformations.py # Transform logic (validate_fields, add_fields)
├── sinks.py           # Output writers (Delta, Parquet, CSV)
├── pipeline.py        # Orchestrate: load → transform → write
├── exceptions.py      # Custom exceptions
└── main.py            # Entry point
```

### How It Works

1. **ConfigLoader**: Reads YAML, validates structure & paths (security)
2. **DataFlowExecutor**: Orchestrates dataflow
   - Loads sources using Spark
   - Applies transformations via registry
   - Writes sinks in specified format
3. **Registries**: Dict-based, extensible
   - `validators.py`: Add `validate_regex`? Define function + add to dict
   - `transformations.py`: Add `transform_deduplicate`? Same process
   - `sinks.py`: Add Iceberg support? Define function + add to dict

## Extensibility Examples

### Add a New Validator

```python
# In src/validators.py

def validate_regex(col, pattern):
    """Validate field matches regex pattern."""
    return col.rlike(pattern)

# Add to registry
VALIDATORS['regex'] = validate_regex

# Use in metadata.yaml
validations:
  - field: email
    validations:
      - regex  # Now available!
```

### Add a New Transformation

```python
# In src/transformations.py

def transform_deduplicate(df, params, dataframes):
    """Remove duplicate rows."""
    subset = params.get('subset')
    result = df.dropDuplicates(subset)
    return {params['input']: result}

# Add to registry
TRANSFORMATIONS['deduplicate'] = transform_deduplicate
```

## Security Features

- **Path validation**: Prevents `../../../etc/passwd` traversal attacks
- **Transformation whitelist**: Only allowed types in metadata
- **Input validation**: Extensive config validation at startup
- **Error messages**: No sensitive info leaked

## Performance Optimizations

- **Lazy evaluation**: Spark optimizes the DAG
- **Delta format**: ACID + schema enforcement for reliability
- **Columnar storage**: Parquet format for analytics
- **Broadcast**: Small dataframes shared to workers efficiently

## Example

Input data (`data/input/person.json`):
```json
[
  {"name": "Xabier", "age": 39, "office": ""},
  {"name": "Miguel", "age": null, "office": "RIO"},
  {"name": "Fran", "age": 31, "office": "RIO"}
]
```

Output (`data/output/person_ok`):
```
name   | age | office | dt
-------|-----|--------|--------------------
Fran   | 31  | RIO    | 2024-01-15 10:30:00
```

Output (`data/output/person_ko`):
```
name    | age  | office | error code
--------|------|--------|---------------------
Xabier  | 39   |        | office can't be blank
Miguel  | null | RIO    | age can't be null
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src

# Run specific test
pytest tests/test_validators.py::TestValidators::test_validate_notEmpty_passes_non_empty -v
```

## Portability & Delivery

This project is portable and executable on any machine:

**Docker Approach (Guaranteed):**
- Everything packaged: Python 3.10 + Java + Spark + dependencies
- Run: `docker-compose up` and works everywhere
- Recommended for production/evaluation

**Local Approach (Flexible):**
- Run `./setup.sh` to set up environment automatically
- Works if Python 3.9+ installed
- Lightweight, no Docker overhead

## Contact

Questions? Check the code comments or review the test cases.
Find me at kholoudkandel@gmail.com
