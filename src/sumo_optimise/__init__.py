"""Top-level package for SUMO corridor conversion utilities."""
from __future__ import annotations

from . import conversion, legacy
from .conversion import (
    BuildOptions as ConversionBuildOptions,
    BuildResult,
    OutputDirectoryTemplate,
    build_and_persist,
    build_corridor_artifacts as build_modern_corridor,
)
from .legacy import BuildOptions as LegacyBuildOptions
from .legacy import build_corridor_artifacts as build_legacy_corridor

__all__ = [
    "conversion",
    "legacy",
    "ConversionBuildOptions",
    "LegacyBuildOptions",
    "BuildResult",
    "OutputDirectoryTemplate",
    "build_and_persist",
    "build_modern_corridor",
    "build_legacy_corridor",
]
