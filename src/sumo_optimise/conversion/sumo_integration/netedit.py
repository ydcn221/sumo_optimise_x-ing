"""Integration with SUMO's netedit utility."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ..utils.logging import get_logger

LOG = get_logger()


def launch_netedit(network_file: Path) -> None:
    resolved_network_file = network_file.resolve()

    exe = shutil.which("netedit")
    if exe is None:
        LOG.warning("netedit not found in PATH. Skip launching netedit.")
        return

    cmd = [exe, str(resolved_network_file)]
    LOG.info("Launching netedit: %s", " ".join(cmd))
    try:
        subprocess.Popen(cmd, cwd=str(resolved_network_file.parent))
    except OSError as exc:  # pragma: no cover - defensive
        LOG.error("Failed to launch netedit: %s", exc)
