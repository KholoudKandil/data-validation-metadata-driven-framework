"""
Configuration loading and validation.

Reads metadata YAML and validates structure against requirements.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List

from src.exceptions import ConfigurationError, SecurityError


class ConfigLoader:
    """Load and validate metadata configuration from YAML."""
    
    # Allowed transformation types (extensible whitelist)
    ALLOWED_TRANSFORMATIONS = {'validate_fields', 'add_fields'}
    
    # Allowed source/sink formats
    ALLOWED_SOURCE_FORMATS = {'JSON', 'CSV', 'PARQUET', 'DELTA'}
    ALLOWED_SINK_FORMATS = {'DELTA', 'PARQUET', 'CSV'}
    
    # Safe base path to prevent path traversal attacks
    SAFE_BASE_PATH = Path('/').resolve()
    
    def __init__(self, config_path: str):
        """
        Initialize config loader.
        
        Args:
            config_path: Path to metadata YAML file
            
        Raises:
            ConfigurationError: If file not found or invalid
            SecurityError: If path is suspicious
        """
        self.config_path = Path(config_path)
        
        # Security: prevent path traversal
        if '..' in str(self.config_path):
            raise SecurityError(
                f"Path traversal detected in config path: {config_path}"
            )
        
        if not self.config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {self.config_path}"
            )
        
        if self.config_path.suffix.lower() not in ['.yaml', '.yml']:
            raise ConfigurationError(
                f"Configuration must be YAML (.yaml/.yml), "
                f"got: {self.config_path.suffix}"
            )
    
    def load(self) -> Dict[str, Any]:
        """
        Load and validate configuration.
        
        Returns:
            Validated configuration dictionary
            
        Raises:
            ConfigurationError: If YAML is invalid or structure wrong
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse YAML: {e}"
            )
        
        # Validate structure
        self._validate_structure(config)
        
        return config
    
    def _validate_structure(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration structure.
        
        Raises:
            ConfigurationError: If structure is invalid
        """
        if not isinstance(config, dict):
            raise ConfigurationError("Configuration must be a dictionary")
        
        if 'dataflows' not in config:
            raise ConfigurationError("Configuration must contain 'dataflows' key")
        
        if not isinstance(config['dataflows'], list):
            raise ConfigurationError("'dataflows' must be a list")
        
        if not config['dataflows']:
            raise ConfigurationError("At least one dataflow must be defined")
        
        # Validate each dataflow
        for i, dataflow in enumerate(config['dataflows']):
            self._validate_dataflow(dataflow, index=i)
    
    def _validate_dataflow(self, dataflow: Dict, index: int) -> None:
        """
        Validate single dataflow structure.
        
        Raises:
            ConfigurationError: If dataflow is invalid
        """
        required_keys = {'name', 'sources', 'transformations', 'sinks'}
        if not all(key in dataflow for key in required_keys):
            raise ConfigurationError(
                f"Dataflow #{index} missing required keys: {required_keys}"
            )
        
        # Validate sources
        if not isinstance(dataflow['sources'], list):
            raise ConfigurationError(
                f"Dataflow #{index}: 'sources' must be a list"
            )
        
        for source in dataflow['sources']:
            self._validate_source(source)
        
        # Validate transformations
        if not isinstance(dataflow['transformations'], list):
            raise ConfigurationError(
                f"Dataflow #{index}: 'transformations' must be a list"
            )
        
        for trans in dataflow['transformations']:
            self._validate_transformation(trans)
        
        # Validate sinks
        if not isinstance(dataflow['sinks'], list):
            raise ConfigurationError(
                f"Dataflow #{index}: 'sinks' must be a list"
            )
        
        for sink in dataflow['sinks']:
            self._validate_sink(sink)
    
    def _validate_source(self, source: Dict) -> None:
        """Validate source configuration."""
        required = {'name', 'path', 'format'}
        if not all(key in source for key in required):
            raise ConfigurationError(
                f"Source missing required keys: {required}"
            )
        
        if source['format'] not in self.ALLOWED_SOURCE_FORMATS:
            raise ConfigurationError(
                f"Unknown source format: {source['format']}"
            )
        
        # Security: validate path
        self._validate_path(source['path'], context="source")
    
    def _validate_transformation(self, trans: Dict) -> None:
        """Validate transformation configuration."""
        required = {'name', 'type', 'params'}
        if not all(key in trans for key in required):
            raise ConfigurationError(
                f"Transformation missing required keys: {required}"
            )
        
        if trans['type'] not in self.ALLOWED_TRANSFORMATIONS:
            raise ConfigurationError(
                f"Unknown transformation type: {trans['type']}"
            )
    
    def _validate_sink(self, sink: Dict) -> None:
        """
        Validate sink configuration.
        
        Raises:
            ConfigurationError: If sink config is invalid
        """
        required = {'input', 'name', 'paths', 'format'}
        if not all(key in sink for key in required):
            raise ConfigurationError(
                f"Sink missing required keys: {required}"
            )
        
        if sink['format'] not in self.ALLOWED_SINK_FORMATS:
            raise ConfigurationError(
                f"Unknown sink format: {sink['format']}"
            )
        
        if not isinstance(sink['paths'], list):
            raise ConfigurationError("Sink 'paths' must be a list")
        
        # NEW: Validate uniqueKey if present
        if 'uniqueKey' in sink:
            if not isinstance(sink['uniqueKey'], list):
                raise ConfigurationError(
                    f"Sink '{sink['name']}': 'uniqueKey' must be a list of column names"
                )
            
            if not sink['uniqueKey']:
                raise ConfigurationError(
                    f"Sink '{sink['name']}': 'uniqueKey' cannot be empty"
                )
            
            # Validate uniqueKey usage: only makes sense with APPEND mode
            save_mode = sink.get('saveMode', 'OVERWRITE')
            if save_mode != 'APPEND':
                raise ConfigurationError(
                    f"Sink '{sink['name']}': 'uniqueKey' is only applicable with "
                    f"saveMode='APPEND'. Current mode: '{save_mode}'. "
                    f"(Remove uniqueKey for {save_mode} mode, or change saveMode to APPEND)"
                )
            
            # Validate uniqueKey only works well with Delta
            if sink['format'] != 'DELTA':
                raise ConfigurationError(
                    f"Sink '{sink['name']}': 'uniqueKey' with APPEND mode is only "
                    f"supported for format='DELTA'. Current format: '{sink['format']}'. "
                    f"(Parquet/CSV don't support MERGE operations)"
                )
        
        # Security: validate all paths
        for path in sink['paths']:
            self._validate_path(path, context="sink")
    
    def _validate_path(self, path: str, context: str) -> None:
        """
        Validate file path for security issues.
        
        Args:
            path: Path to validate
            context: Where path is used (source/sink) for error messages
            
        Raises:
            SecurityError: If path is suspicious
        """
        # Check for path traversal attempts
        if '..' in path:
            raise SecurityError(
                f"{context} path contains '..': {path}"
            )
        
        # Resolve absolute path and ensure it's under safe base
        try:
            resolved = Path(path).resolve()
            # Allow both local and HDFS paths
            if path.startswith('/') and not path.startswith('hdfs://'):
                resolved.relative_to(self.SAFE_BASE_PATH)
        except ValueError:
            # Path is outside safe base
            raise SecurityError(
                f"{context} path outside allowed directory: {path}"
            )