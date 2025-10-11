"""Integration helpers for launching SUMO's netedit GUI."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ..utils.logging import get_logger

LOG = get_logger()


def launch_netedit(network_file: Path) -> None:
    """Open the generated SUMO network in netedit if available.

    Parameters
    ----------
    network_file:
        Absolute path to the SUMO network file produced by ``netconvert``.
    """

    exe = shutil.which("netedit") or shutil.which("netedit.exe")
    if exe is None:
        LOG.warning("netedit not found in PATH. Skip launching netedit.")
        return

    if not network_file.exists():
        LOG.warning("Network file does not exist: %s", network_file)
        return

    cmd = [exe, str(network_file)]
    LOG.info("Launching netedit: %s", " ".join(cmd))

    try:
        subprocess.Popen(cmd)  # noqa: S603,S607 - trusted executable resolved via shutil.which
    except OSError as exc:
        LOG.error("Failed to start netedit: %s", exc)


__all__ = ["launch_netedit"]
