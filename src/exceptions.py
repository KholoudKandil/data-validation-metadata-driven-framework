"""
Custom exceptions for metadata framework.

Purpose: Distinguish framework errors from generic Python errors,
making error handling and debugging easier.
"""


class MetadataFrameworkError(Exception):
    """Base exception for all framework errors."""
    pass


class ConfigurationError(MetadataFrameworkError):
    """Configuration/metadata is invalid."""
    pass


class ValidationError(MetadataFrameworkError):
    """Data validation failed."""
    pass


class TransformationError(MetadataFrameworkError):
    """Transformation execution failed."""
    pass


class SecurityError(MetadataFrameworkError):
    """Security violation detected."""
    pass
