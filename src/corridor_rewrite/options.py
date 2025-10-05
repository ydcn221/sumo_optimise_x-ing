"""Configuration object for the wrapper pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class BuildOptions:
    """Options controlling how the legacy builder is executed."""

    schema_path: Path | None = None
    output_dir: Path | None = None
    keep_output: bool = False

    @property
    def default_schema_path(self) -> Path:
        return Path("data/legacy(v1.2)/schema_v1.2.json")
