"""
Output sinks: how to write data to different formats.

Purpose: Abstract away format-specific details.
Registry pattern: easy to add formats.
"""

from pyspark.sql import DataFrame
from pathlib import Path
from typing import Dict, Callable, Any

from src.exceptions import ConfigurationError, SecurityError


def write_delta(df: DataFrame, path: str, mode: str) -> None:
    """
    Write DataFrame to Delta Lake format.
    
    Performance note: Delta is optimized for ACID operations and schema enforcement.
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode (OVERWRITE, APPEND, etc.)
    """
    df.write.format('delta').mode(mode).save(path)


def write_parquet(df: DataFrame, path: str, mode: str) -> None:
    """
    Write DataFrame to Parquet format.
    
    Performance note: Parquet is columnar format, great for analytics.
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode
    """
    df.write.format('parquet').mode(mode).save(path)


def write_csv(df: DataFrame, path: str, mode: str) -> None:
    """
    Write DataFrame to CSV format.
    
    Note: CSV is slower than Parquet/Delta for large files (text-based).
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: SaveMode
    """
    df.write.format('csv').option('header', True).mode(mode).save(path)


# Sink writers registry (extensible)
SINK_WRITERS: Dict[str, Callable] = {
    'DELTA': write_delta,
    'PARQUET': write_parquet,
    'CSV': write_csv,
    # Can add: 'JDBC': write_jdbc, 'KAFKA': write_kafka, etc.
}


def write_sink(
    df: DataFrame,
    format: str,
    paths: list,
    mode: str = 'OVERWRITE'
) -> None:
    """
    Write DataFrame to sink(s) in specified format.
    
    Args:
        df: DataFrame to write
        format: Output format (DELTA, PARQUET, CSV)
        paths: List of output paths (usually 1, but support multiple)
        mode: SaveMode (OVERWRITE, APPEND, IGNORE, ERROR)
        
    Raises:
        ConfigurationError: If format not supported
        SecurityError: If path is suspicious
    """
    if format not in SINK_WRITERS:
        available = ', '.join(SINK_WRITERS.keys())
        raise ConfigurationError(
            f"Unknown sink format '{format}'. Available: {available}"
        )
    
    writer = SINK_WRITERS[format]
    
    # Security: validate each path
    for path in paths:
        if '..' in path:
            raise SecurityError(f"Path traversal detected: {path}")
    
    # Write to each path
    for path in paths:
        writer(df, path, mode)
