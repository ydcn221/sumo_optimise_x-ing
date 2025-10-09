"""Exception hierarchy shared across the pipeline."""
from __future__ import annotations


class BuildError(Exception):
    """Base class for all build related failures."""


class SpecFileNotFound(BuildError):
    pass


class SchemaFileNotFound(BuildError):
    pass


class UnsupportedVersionError(BuildError):
    pass


class SchemaValidationError(BuildError):
    pass


class SemanticValidationError(BuildError):
    pass


class InvalidConfigurationError(BuildError):
    pass


class IoError(BuildError):
    pass


class NetconvertExecutionError(BuildError):
    pass
