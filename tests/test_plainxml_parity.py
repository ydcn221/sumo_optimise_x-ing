from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from corridor_pipeline import BuildOptions, build_corridor_artifacts

SAMPLE_JSON = Path("data/legacy(v1.2)/schema_v1.2_sample.json")
SAMPLE_SCHEMA = Path("data/legacy(v1.2)/schema_v1.2.json")
LEGACY_SCRIPT = Path("data/legacy(v1.2)/plainXML_converter_0927_1.2.11.py")


@pytest.fixture()
def legacy_output(tmp_path: Path) -> dict[str, str]:
    workdir = tmp_path / "legacy"
    workdir.mkdir()
    shutil.copy(SAMPLE_JSON, workdir / "v1.2.check.json")
    shutil.copy(SAMPLE_SCHEMA, workdir / "schema_v1.2.json")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path("src").resolve())
    subprocess.run(
        ["python", str(LEGACY_SCRIPT.resolve())],
        cwd=workdir,
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    outdir = next((workdir / "plainXML_out").rglob("net.nod.xml")).parent
    nodes = (outdir / "net.nod.xml").read_text(encoding="utf-8")
    edges = (outdir / "net.edg.xml").read_text(encoding="utf-8")
    connections = (outdir / "net.con.xml").read_text(encoding="utf-8")
    return {"nodes": nodes, "edges": edges, "connections": connections}


def test_plainxml_matches_legacy(legacy_output: dict[str, str]) -> None:
    result = build_corridor_artifacts(
        SAMPLE_JSON.resolve(),
        BuildOptions(schema_path=SAMPLE_SCHEMA.resolve()),
    )

    assert result.nodes_xml == legacy_output["nodes"]
    assert result.edges_xml == legacy_output["edges"]
    assert result.connections_xml == legacy_output["connections"]
