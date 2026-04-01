# Code Flow Explanation: Complete Walkthrough

This document traces the entire execution flow from entry point to completion.

---

## **Entry Point: main.py**

```python
# main.py (assumed structure based on usage)

from pyspark.sql import SparkSession
from src.config import ConfigLoader
from src.pipeline import DataFlowExecutor
import logging

logging.basicConfig(level=logging.INFO)

def main():
    """
    Entry point for the framework.
    
    Command: python src/main.py config/metadata.yaml
    """
    
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MetadataDrivenFramework") \
        .config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension") \
        .getOrCreate()
    
    # Step 2: Load configuration from YAML
    loader = ConfigLoader()
    config = loader.load("config/metadata.yaml")
    
    # Step 3: Create executor and execute
    executor = DataFlowExecutor(spark)
    executor.execute(config)
```

---

## **Complete Execution Flow**

### **PHASE 1: Load Sources**

```
Input JSON (3 rows)
├─ {"name": "Xabier", "age": 39, "office": ""}
├─ {"name": "Miguel", "age": null, "office": "RIO"}
└─ {"name": "Fran", "age": 31, "office": "RIO"}
    │
    ▼ spark.read.format('json').load('data/input/events/person/*')
    │
    ▼
self.dataframes['person_inputs'] = DataFrame (3 rows)
```

**Function Call Chain:**
1. `main()` creates SparkSession
2. `DataFlowExecutor(spark).execute(config)`
3. `_execute_dataflow(dataflow)`
4. `_load_source(source)` ← Loads JSON
5. Stores in `self.dataframes['person_inputs']`

---

### **PHASE 2: Apply Transformations**

#### **Transformation 1: validate_fields**

```
Input: self.dataframes['person_inputs'] (3 rows)

VALIDATION LOGIC:
├─ Field 'office' with validator 'notEmpty'
│  └─ apply_validations(df, 'office', ['notEmpty'])
│     └─ get_validation_expression('office', 'notEmpty')
│        └─ VALIDATION_EXPRESSIONS['notEmpty']('office')
│           └─ (F.col('office').isNotNull()) & (F.col('office') != "")
│              Results: [false, true, true]
│              - Xabier (office=''): FALSE
│              - Miguel (office='RIO'): TRUE
│              - Fran (office='RIO'): TRUE
│
├─ Field 'age' with validator 'notNull'
│  └─ apply_validations(df, 'age', ['notNull'])
│     └─ get_validation_expression('age', 'notNull')
│        └─ VALIDATION_EXPRESSIONS['notNull']('age')
│           └─ F.col('age').isNotNull()
│              Results: [true, false, true]
│              - Xabier (age=39): TRUE
│              - Miguel (age=null): FALSE
│              - Fran (age=31): TRUE
│
└─ Combine with AND: [false, true, true] & [true, false, true] = [false, false, true]

SPLIT RESULTS:
├─ validation_ok: df.filter([false, false, true])
│  Result: 1 row (Fran)
│
└─ validation_ko: df.filter(~[...])
   Result: 2 rows (Xabier, Miguel) with error_code column

Output Stored:
├─ self.dataframes['validation_ok'] = 1 row
└─ self.dataframes['validation_ko'] = 2 rows
```

**Function Call Chain:**
1. `_apply_transformation(transformation)` 
2. `get_transformation('validate_fields')`
3. Returns: `transform_validate_fields` function
4. `transform_validate_fields(df, params, dataframes)`
   - For each field in params['validations']:
     - `apply_validations(df, field_name, validators)`
       - For each validator:
         - `get_validation_expression(field, validator)`
           - Lookup in `VALIDATION_EXPRESSIONS` registry
           - Call validator function: `validate_notempty_expr('office')`
           - Returns: Spark Column (boolean)
5. Combine all validation results with AND
6. Split into `validation_ok` and `validation_ko`
7. Store both in `self.dataframes`

---

#### **Transformation 2: add_fields**

```
Input: self.dataframes['validation_ok'] (1 row: Fran)

FIELD ADDITION LOGIC:
├─ Field name: 'dt'
├─ Function: 'current_timestamp'
└─ withColumn('dt', F.current_timestamp())
   Result: New column added with timestamp value

Output:
self.dataframes['validation_ok'] = 1 row with dt column
{'name': 'Fran', 'age': 31, 'office': 'RIO', 'dt': '2026-03-29 19:24:43'}
```

**Function Call Chain:**
1. `_apply_transformation(transformation)`
2. `get_transformation('add_fields')`
3. Returns: `transform_add_fields` function
4. `transform_add_fields(df, params, dataframes)`
   - For each field in params['addFields']:
     - Lookup function in field_functions dict
     - `withColumn(field_name, function())`
5. Return updated DataFrame
6. Store in `self.dataframes['validation_ok']`

---

### **PHASE 3: Write Sinks**

#### **Sink 1: raw-ok**

```
Input: self.dataframes['ok_with_date']
       (From metadata, refers to 'validation_ok' after add_fields)
       1 row (Fran with timestamp)

CONFIG:
├─ format: 'DELTA'
├─ path: 'data/output/events/person'
├─ saveMode: 'APPEND'
└─ uniqueKey: ['name']

WRITE LOGIC (First Run):
├─ Try: DeltaTable.forPath(spark, 'data/output/events/person')
├─ Exception: "not a delta table" (table doesn't exist)
├─ Catch: Exception handler
├─ Check: Error message contains "not found"
├─ Action: Create new table
│  └─ df.write.format('delta').mode('overwrite').save(...)
│
└─ Result: Delta table created at 'data/output/events/person'
           Contains: 1 row (Fran)

WRITE LOGIC (Second Run):
├─ Table exists: DeltaTable.forPath succeeds
├─ Build MERGE condition: "t.name = s.name"
├─ Build column mappings:
│  ├─ update_dict = {'name': 's.name', 'age': 's.age', 'office': 's.office', 'dt': 's.dt'}
│  └─ insert_dict = same
├─ Execute MERGE:
│  └─ When key (name) matches: UPDATE
│  └─ When key doesn't match: INSERT
├─ Result: Idempotent (same data, no duplicates)
└─ VACUUM: Clean up old files
```

**Function Call Chain:**
1. `_write_sink(sink)`
2. Extract sink config (format, path, mode, uniqueKey)
3. Get input DataFrame: `self.dataframes['ok_with_date']`
4. `write_sink(df, 'DELTA', ['data/output/events/person'], 'APPEND', ['name'])`
   - Lookup writer: `SINK_WRITERS['DELTA']` = `write_delta`
   - Validate path (no '..')
   - `write_delta(df, path, 'APPEND', ['name'])`
     - Check: mode='APPEND' and unique_key provided
     - Try MERGE:
       - `DeltaTable.forPath(spark, path)`
       - Build merge condition
       - `delta_table.merge(...).whenMatchedUpdate(...).whenNotMatchedInsert(...).execute()`
     - On exception: Create new table with `df.write.format('delta').save(...)`
     - VACUUM cleanup

---

#### **Sink 2: raw-ko**

```
Input: self.dataframes['validation_ko']
       2 rows (Xabier, Miguel) with error_code

CONFIG:
├─ format: 'DELTA'
├─ path: 'data/output/discards/person'
├─ saveMode: 'APPEND'
└─ uniqueKey: ['name']

WRITE LOGIC: (Same as raw-ok)
└─ Result: Delta table created at 'data/output/discards/person'
           Contains: 2 rows with error messages
```

---

## **Data Structure Changes Through Flow**

```
START: JSON File
├─ {"name": "Xabier", "age": 39, "office": ""}
├─ {"name": "Miguel", "age": null, "office": "RIO"}
└─ {"name": "Fran", "age": 31, "office": "RIO"}

AFTER Load:
self.dataframes = {
    'person_inputs': [Xabier, Miguel, Fran] (3 rows)
}

AFTER Validation:
self.dataframes = {
    'person_inputs': [Xabier, Miguel, Fran],
    'validation_ok': [Fran],
    'validation_ko': [Xabier, Miguel] + error_code
}

AFTER Add Fields:
self.dataframes = {
    'person_inputs': [Xabier, Miguel, Fran],
    'validation_ok': [Fran + dt],
    'validation_ko': [Xabier, Miguel] + error_code
}

AFTER Write Sinks:
Files Created:
├─ data/output/events/person/ (Delta)
│  └─ [Fran + dt] (1 row)
│
└─ data/output/discards/person/ (Delta)
   └─ [Xabier, Miguel] + error_code (2 rows)
```

---

## **Key Function Interactions**

### **Registry Pattern in Validators**

```
When validate_fields calls: apply_validations(df, 'office', ['notEmpty'])

Flow:
1. apply_validations('office', ['notEmpty'])
   │
   ├─ For each validator in ['notEmpty']:
   │  │
   │  └─ get_validation_expression('office', 'notEmpty')
   │     │
   │     ├─ Check: 'notEmpty' in VALIDATION_EXPRESSIONS
   │     │
   │     ├─ Get: validation_func = VALIDATION_EXPRESSIONS['notEmpty']
   │     │                        = validate_notempty_expr
   │     │
   │     └─ Call: validate_notempty_expr('office')
   │        │
   │        └─ Returns: (F.col('office').isNotNull()) & (F.col('office') != "")
   │
   └─ Result: Column[false, true, true]
```

### **Strategy Pattern in Sinks**

```
When _write_sink calls: write_sink(df, 'DELTA', ...)

Flow:
1. write_sink(df, format='DELTA', ...)
   │
   ├─ Check: format in SINK_WRITERS
   │
   ├─ Get: writer = SINK_WRITERS['DELTA']
   │                = write_delta
   │
   └─ Call: write_delta(df, path, mode, unique_key)
      │
      └─ Returns: Data written to path
```

### **Factory Pattern in Transformations**

```
When _apply_transformation calls: get_transformation('validate_fields')

Flow:
1. get_transformation('validate_fields')
   │
   ├─ Check: 'validate_fields' in TRANSFORMATIONS
   │
   ├─ Get: transformation_func = TRANSFORMATIONS['validate_fields']
   │                            = transform_validate_fields
   │
   └─ Return: transform_validate_fields function
      │
      └─ Caller can now: result = transform_validate_fields(df, params, dataframes)
```

---

## **Error Handling Flow**

### **Unknown Validator**

```
Code: apply_validations(df, 'age', ['unknown_validator'])
  │
  └─ get_validation_expression('age', 'unknown_validator')
     │
     ├─ Check: 'unknown_validator' in VALIDATION_EXPRESSIONS
     │
     └─ Not found!
        └─ Raise ConfigurationError:
           "Unknown validator 'unknown_validator'. Available: notEmpty, notNull"
```

### **Invalid Delta Table State**

```
Code: write_delta(df, 'data/output/events/person', 'APPEND', ['name'])
  │
  ├─ Try: DeltaTable.forPath(spark, 'data/output/events/person')
  │
  ├─ Exception: [DELTA_MISSING_DELTA_TABLE] not a delta table
  │
  └─ Catch:
     ├─ Check error message
     ├─ If "not found" or "not a delta table":
     │  └─ Create new table: df.write.format('delta').save(...)
     │
     └─ Else: re-raise exception
```

---

## **State Management**

### **self.dataframes Dictionary**

Throughout execution, `self.dataframes` holds all intermediate DataFrames:

```
Initial: {}

After loading person_inputs: 
{
    'person_inputs': DataFrame (3 rows)
}

After validate_fields:
{
    'person_inputs': DataFrame (3 rows),
    'validation_ok': DataFrame (1 row),
    'validation_ko': DataFrame (2 rows)
}

After add_fields:
{
    'person_inputs': DataFrame (3 rows),
    'validation_ok': DataFrame (1 row, with 'dt' column),
    'validation_ko': DataFrame (2 rows)
}

After sinks:
{
    (same, but data is written to Delta Lake)
}
```

---

## **Complete Call Stack Example**

For a complete trace of one validation:

```
main()
└─ DataFlowExecutor(spark).execute(config)
   └─ for dataflow in config['dataflows']:
      └─ _execute_dataflow(dataflow)
         └─ for transformation in dataflow['transformations']:
            └─ _apply_transformation(transformation)
               └─ get_transformation('validate_fields')
                  └─ TRANSFORMATIONS['validate_fields']
                     └─ transform_validate_fields(df, params, dataframes)
                        └─ for field_config in params['validations']:
                           └─ apply_validations(df, 'office', ['notEmpty'])
                              └─ for validation_name in ['notEmpty']:
                                 └─ get_validation_expression('office', 'notEmpty')
                                    └─ VALIDATION_EXPRESSIONS['notEmpty']
                                       └─ validate_notempty_expr('office')
                                          └─ (F.col('office').isNotNull()) & (F.col('office') != "")
                                             └─ Column[false, true, true]
                        └─ df_ok = df.filter(combined_validations)
                        └─ df_ko = df.filter(~combined_validations).withColumn('error code', ...)
                        └─ return {'validation_ok': df_ok, 'validation_ko': df_ko}
                           └─ stored in self.dataframes['validation_ok']
                           └─ stored in self.dataframes['validation_ko']
```

---

This is the complete code flow! Each layer calls the next, passes data through, and builds on the results.
