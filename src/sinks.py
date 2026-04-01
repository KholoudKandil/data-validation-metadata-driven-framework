"""
Output sinks: how to write data to different formats.

Purpose: Abstract away format-specific details.
Registry pattern: easy to add formats.

Deduplication strategy:
- DELTA + APPEND + uniqueKey: Use Delta MERGE (upsert) for deduplication
- Other formats: Standard write (dedup not supported)
"""

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pathlib import Path
from typing import Dict, Callable, Any, List, Optional
import logging

from src.exceptions import ConfigurationError, SecurityError

logger = logging.getLogger(__name__)


def write_delta(
    df: DataFrame, 
    path: str, 
    mode: str,
    unique_key: Optional[List[str]] = None
) -> None:
    """
    Write DataFrame to Delta Lake format.
    
    If APPEND mode + unique_key specified: Use MERGE (upsert) for deduplication.
    This implements key-based deduplication: newer records overwrite older ones.
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode (OVERWRITE, APPEND, etc.)
        unique_key: List of columns that identify unique records (for APPEND dedup)
    """
    spark = df.sparkSession
    
    # Strategy 1: APPEND + unique_key → Use Delta MERGE (upsert)
    if mode == 'APPEND' and unique_key:
        # BETTER: Proactive Check + Reactive Fallback

        try:
            # CHECK if table exists and is valid BEFORE attempting merge
            if DeltaTable.isDeltaTable(spark, path):
                # Table exists and is valid Delta table - proceed with merge
                delta_table = DeltaTable.forPath(spark, path)
                
                merge_conditions = [f"t.{col} = s.{col}" for col in unique_key]
                merge_condition = " AND ".join(merge_conditions)
                
                logger.info(f"    Using Delta MERGE for deduplication on: {unique_key}")
                
                update_dict = {col: f"s.{col}" for col in df.columns}
                insert_dict = {col: f"s.{col}" for col in df.columns}
                
                delta_table.alias("t").merge(
                    df.alias("s"),
                    merge_condition
                ).whenMatchedUpdate(
                    set=update_dict
                ).whenNotMatchedInsert(
                    values=insert_dict
                ).execute()
                
                logger.info(f"    ✓ MERGE completed successfully")
            else:
                # Table doesn't exist or is invalid - create new
                logger.info(f"    Table doesn't exist or is invalid. Creating with initial data.")
                df.write.format('delta').mode('overwrite').save(path)
        
        except Exception as e:
            # Catch unexpected errors (schema mismatches, permissions, etc.)
            logger.error(f"    Unexpected error during Delta write: {str(e)}")
            raise

    
    elif mode == 'OVERWRITE':
        # Strategy 2: OVERWRITE, IGNORE, ERROR, or other modes → Standard write
        logger.info(f" Proceeding with {mode}."
        )
    
        df.write.format('delta').mode(mode).save(path)
    
    else:
        logger.info(f"No uniqueKey provided or invalid write mode.")

    
    # Cleanup: VACUUM to remove old files (Delta best practice)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    file_uri = f"file://{Path(path).resolve()}"
    DeltaTable.forPath(spark, file_uri).vacuum(0.05) # Vacuum after 3 minutes


def write_parquet(
    df: DataFrame, 
    path: str, 
    mode: str,
    unique_key: Optional[List[str]] = None
) -> None:
    """
    Write DataFrame to Parquet format.
    
    Note: Parquet doesn't support MERGE. Deduplication not applied.
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode
        unique_key: Ignored for Parquet (not supported)
    """
    if unique_key:
        logger.warning(
            f"    uniqueKey specified for Parquet format, but Parquet "
            f"doesn't support MERGE deduplication. Proceeding without dedup."
        )
    
    df.write.format('parquet').mode(mode).save(path)


def write_csv(
    df: DataFrame, 
    path: str, 
    mode: str,
    unique_key: Optional[List[str]] = None
) -> None:
    """
    Write DataFrame to CSV format.
    
    Note: CSV doesn't support MERGE. Deduplication not applied.
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode
        unique_key: Ignored for CSV (not supported)
    """
    if unique_key:
        logger.warning(
            f"    uniqueKey specified for CSV format, but CSV "
            f"doesn't support MERGE deduplication. Proceeding without dedup."
        )
    
    df.write.format('csv').option('header', True).mode(mode).save(path)


# Sink writers registry (extensible)
SINK_WRITERS: Dict[str, Callable] = {
    'DELTA': write_delta,
    'PARQUET': write_parquet,
    'CSV': write_csv,
}


def write_sink(
    df: DataFrame,
    format: str,
    paths: list,
    mode: str = 'OVERWRITE',
    unique_key: Optional[List[str]] = None
) -> None:
    """
    Write DataFrame to sink(s) in specified format.
    
    Deduplication strategy (for APPEND mode):
    - Delta: Uses native MERGE (upsert) if uniqueKey provided
    - Parquet/CSV: No deduplication support (standard write)
    
    Args:
        df: DataFrame to write
        format: Output format (DELTA, PARQUET, CSV)
        paths: List of output paths (usually 1, but support multiple)
        mode: SaveMode (OVERWRITE, APPEND, IGNORE, ERROR)
        unique_key: Optional list of columns that identify unique records
                   (for APPEND mode deduplication in Delta)
        
    Raises:
        ConfigurationError: If format not supported or config invalid
        SecurityError: If path is suspicious
    """
    if format not in SINK_WRITERS:
        available = ', '.join(SINK_WRITERS.keys())
        raise ConfigurationError(
            f"Unknown sink format '{format}'. Available: {available}"
        )
    
    writer = SINK_WRITERS[format]
    
    # Security: validate each path (prevent path traversal attacks)
    for path in paths:
        if '..' in path:
            raise SecurityError(f"Path traversal detected: {path}")
    
    # Write to each path
    for path in paths:
        writer(df, path, mode, unique_key)