"""Integration with SUMO's netconvert utility."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from ..utils.errors import NetconvertExecutionError
from ..utils.logging import get_logger

LOG = get_logger()


def _cli_path(path: Path, base: Path) -> str:
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        try:
            return Path(os.path.relpath(path, base)).as_posix()
        except ValueError:
            return path.resolve().as_posix()


def _resolve_under_base(token: str, base: Path) -> Path:
    candidate = Path(token)
    if candidate.is_absolute():
        return candidate
    return base / candidate


def run_two_step_netconvert(
    outdir: Path,
    nodes_file: Path,
    edges_file: Path,
    connections_file: Path,
    tll_file: Optional[Path],
    *,
    plain_prefix: str,
    network_output: Path,
) -> None:
    """Execute the two-stage netconvert workflow required by SUMO."""

    exe = shutil.which("netconvert")
    if exe is None:
        LOG.warning("netconvert not found in PATH. Skip multi-step conversion.")
        return

    plain_node = _resolve_under_base(f"{plain_prefix}.nod.xml", outdir)
    plain_edge = _resolve_under_base(f"{plain_prefix}.edg.xml", outdir)

    step1 = [
        exe,
        "--lefthand",
        "--sidewalks.guess",
        "--no-internal-links",
        "--node-files",
        _cli_path(nodes_file, outdir),
        "--edge-files",
        _cli_path(edges_file, outdir),
        "--plain-output-prefix",
        plain_prefix,
    ]
    step2 = [
        exe,
        "--lefthand",
        "--node-files",
        _cli_path(plain_node, outdir),
        "--edge-files",
        _cli_path(plain_edge, outdir),
        "--connection-files",
        _cli_path(connections_file, outdir),
    ]
    if tll_file is not None:
        step2 += [
            "--tllogic-files",
            _cli_path(tll_file, outdir),
        ]
    step2 += [
        "--output-file",
        _cli_path(network_output, outdir),
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
