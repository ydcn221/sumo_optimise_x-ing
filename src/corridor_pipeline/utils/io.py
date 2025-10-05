"""Filesystem helpers that reproduce the legacy behaviour."""
from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

from .constants import (
    CONNECTIONS_FILE_NAME,
    DATE_DIR_FORMAT,
    EDGES_FILE_NAME,
    MANIFEST_NAME,
    NODES_FILE_NAME,
    OUTPUT_DIR_PREFIX,
)


class BuildArtifacts:
    """Represents materialised file paths for a build run."""

    def __init__(self, outdir: Path) -> None:
        self.outdir = outdir
        self.log_path = outdir / "build.log"
        self.nodes_path = outdir / NODES_FILE_NAME
        self.edges_path = outdir / EDGES_FILE_NAME
        self.connections_path = outdir / CONNECTIONS_FILE_NAME


def ensure_output_directory() -> BuildArtifacts:
    today_str = datetime.datetime.now().strftime(DATE_DIR_FORMAT)
    base_dir = Path(OUTPUT_DIR_PREFIX)
    base_dir.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(rf"^{re.escape(today_str)}_(\d+)$")
    max_seq = 0
    for entry in base_dir.iterdir():
        if entry.is_dir():
            match = pattern.match(entry.name)
            if match:
                max_seq = max(max_seq, int(match.group(1)))

    outdir = base_dir / f"{today_str}_{max_seq + 1:03}"
    outdir.mkdir(parents=True, exist_ok=False)
    return BuildArtifacts(outdir)


def write_manifest(artifacts: BuildArtifacts, payload: dict) -> Path:
    path = artifacts.outdir / MANIFEST_NAME
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def persist_xml(artifacts: BuildArtifacts, *, nodes: str, edges: str, connections: str) -> None:
    artifacts.nodes_path.write_text(nodes, encoding="utf-8")
    artifacts.edges_path.write_text(edges, encoding="utf-8")
    artifacts.connections_path.write_text(connections, encoding="utf-8")


__all__ = [
    "BuildArtifacts",
    "ensure_output_directory",
    "persist_xml",
    "write_manifest",
]
