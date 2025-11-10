"""Integration with SUMO's sumo-gui executable."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ..utils.logging import get_logger

LOG = get_logger()


def launch_sumo_gui(config_file: Path) -> None:
    exe = shutil.which("sumo-gui")
    if exe is None:
        LOG.warning("sumo-gui not found in PATH. Skip launching GUI.")
        return

    resolved_cfg = config_file.resolve()
    cmd = [exe, "-c", str(resolved_cfg)]
    LOG.info("Launching sumo-gui: %s", " ".join(cmd))
    try:
        subprocess.Popen(cmd, cwd=str(resolved_cfg.parent))
    except OSError as exc:  # pragma: no cover - defensive
        LOG.error("Failed to launch sumo-gui: %s", exc)
