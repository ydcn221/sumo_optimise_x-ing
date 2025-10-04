"""Runtime options that control corridor building behaviour."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _default_workdir() -> Path:
    return Path("build").absolute()


@dataclass(slots=True)
class BuildOptions:
    """Configuration for the corridor build pipeline."""

    run_netconvert: bool = False
    schema_path: Optional[Path] = None
    workdir: Path = field(default_factory=_default_workdir)
    log_level: str = "INFO"

    def ensure_workdir(self) -> Path:
        self.workdir.mkdir(parents=True, exist_ok=True)
        return self.workdir
