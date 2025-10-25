"""Integration with SUMO's netconvert utility."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Optional

from ..utils.constants import PLAIN_NETCONVERT_PREFIX, NETWORK_FILE_NAME
from ..utils.errors import NetconvertExecutionError
from ..utils.logging import get_logger

LOG = get_logger()


def run_two_step_netconvert(
    outdir: Path,
    nodes_file: Path,
    edges_file: Path,
    connections_file: Path,
    tll_file: Optional[Path],
) -> None:
    """Execute the two-stage netconvert workflow required by SUMO."""

    exe = shutil.which("netconvert")
    if exe is None:
        LOG.warning("netconvert not found in PATH. Skip multi-step conversion.")
        return

    plain_prefix = PLAIN_NETCONVERT_PREFIX

    step1 = [
        exe,
        "--lefthand",
        "--sidewalks.guess",
        "--no-internal-links",
        "--node-files",
        nodes_file.name,
        "--edge-files",
        edges_file.name,
        "--plain-output-prefix",
        plain_prefix,
    ]
    step2 = [
        exe,
        "--lefthand",
        "--node-files",
        f"{plain_prefix}.nod.xml",
        "--edge-files",
        f"{plain_prefix}.edg.xml",
        "--connection-files",
        connections_file.name,
    ]
    if tll_file is not None:
        step2 += [
            "--tllogic-files",
            tll_file.name,
        ]
    step2 += [
        "--output-file",
        NETWORK_FILE_NAME,
    ]

    for idx, cmd in enumerate((step1, step2), start=1):
        LOG.info("netconvert step %d: %s", idx, " ".join(cmd))
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(outdir),
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
            )
            LOG.info("[netconvert %d] rc=%d", idx, proc.returncode)
            if proc.stdout:
                LOG.info("[netconvert %d STDOUT]\n%s", idx, proc.stdout)
            if proc.stderr:
                LOG.info("[netconvert %d STDERR]\n%s", idx, proc.stderr)
        except subprocess.CalledProcessError as exc:
            if exc.stdout:
                LOG.error("[netconvert %d STDOUT]\n%s", idx, exc.stdout)
            if exc.stderr:
                LOG.error("[netconvert %d STDERR]\n%s", idx, exc.stderr)
            raise NetconvertExecutionError(f"netconvert step {idx} failed with rc={exc.returncode}")
