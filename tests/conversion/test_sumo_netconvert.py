"""Tests for the SUMO netconvert integration helpers."""
from __future__ import annotations

from pathlib import Path

import pytest

from sumo_optimise.conversion.sumo_integration import netconvert
from sumo_optimise.conversion.utils.constants import (
    CONNECTIONS_FILE_NAME,
    EDGES_FILE_NAME,
    NODES_FILE_NAME,
    TLL_FILE_NAME,
)


class _CompletedProcess:
    """Minimal stand-in for ``subprocess.CompletedProcess``."""

    def __init__(self) -> None:
        self.returncode = 0
        self.stdout = ""
        self.stderr = ""


def test_run_two_step_netconvert_includes_tll_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the third netconvert call references the generated ``1-generated.tll.xml`` file."""

    commands: list[list[str]] = []

    monkeypatch.setattr(netconvert.shutil, "which", lambda _: "netconvert")

    def fake_run(cmd: list[str], **_: object) -> _CompletedProcess:
        commands.append(cmd)
        return _CompletedProcess()

    monkeypatch.setattr(netconvert.subprocess, "run", fake_run)

    outdir = tmp_path
    nodes = outdir / NODES_FILE_NAME
    edges = outdir / EDGES_FILE_NAME
    connections = outdir / CONNECTIONS_FILE_NAME
    tll = outdir / TLL_FILE_NAME

    for path in (nodes, edges, connections, tll):
        path.write_text("<xml/>", encoding="utf-8")

    netconvert.run_two_step_netconvert(outdir, nodes, edges, connections, tll)

    step3 = commands[2]
    assert "--tllogic-files" in step3
    assert step3[step3.index("--tllogic-files") + 1] == TLL_FILE_NAME
