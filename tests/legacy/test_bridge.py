from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from sumo_optimise.legacy import build_corridor_artifacts
from sumo_optimise.legacy.options import BuildOptions
from sumo_optimise.legacy.bridge import LEGACY_SCRIPT_PATH


SPEC_SRC = Path("data/legacy(v1.2)/schema_v1.2_sample.json")
SCHEMA_SRC = Path("data/legacy(v1.2)/schema_v1.2.json")


def _run_legacy_converter(tmp_path: Path) -> dict[str, str]:
    spec_path = tmp_path / "v1.2.check.json"
    schema_path = tmp_path / "schema_v1.2.json"
    spec_path.write_text(SPEC_SRC.read_text(encoding="utf-8"), encoding="utf-8")
    schema_path.write_text(SCHEMA_SRC.read_text(encoding="utf-8"), encoding="utf-8")

    env = os.environ.copy()
    src_path = Path("src").resolve()
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(src_path) if not existing else f"{src_path}{os.pathsep}{existing}"
    subprocess.run(
        [sys.executable, str(LEGACY_SCRIPT_PATH)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    out_base = tmp_path / "plainXML_out"
    out_dirs = sorted(out_base.iterdir())
    assert out_dirs, "legacy converter did not produce any output directory"
    latest = out_dirs[-1]
    return {
        "nodes.nod.xml": (latest / "net.nod.xml").read_text(encoding="utf-8"),
        "edges.edg.xml": (latest / "net.edg.xml").read_text(encoding="utf-8"),
        "connections.con.xml": (latest / "net.con.xml").read_text(encoding="utf-8"),
    }


def test_plainxml_matches_legacy(tmp_path):
    legacy_output = _run_legacy_converter(tmp_path)

    options = BuildOptions(
        schema_path=SCHEMA_SRC,
        output_dir=tmp_path / "rewrite_out",
        keep_output=True,
    )
    artefacts = build_corridor_artifacts(tmp_path / "v1.2.check.json", options=options)

    assert artefacts.as_mapping() == legacy_output

    # Ensure files were written for manual inspection when keep_output=True
    for name in legacy_output:
        assert (options.output_dir / name).exists()
