from __future__ import annotations

from pathlib import Path

from sumo_optimise.conversion.domain.models import BuildOptions
from sumo_optimise.conversion.pipeline import build_and_persist
from sumo_optimise.conversion.utils.constants import SCHEMA_JSON_PATH


def test_build_and_persist_logs_pipeline_steps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    spec_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "reference"
        / "schema_v1.3_sample.json"
    )
    options = BuildOptions(schema_path=SCHEMA_JSON_PATH)

    result = build_and_persist(spec_path, options)

    assert result.manifest_path is not None
    log_path = result.manifest_path.parent / "build.log"
    log_contents = log_path.read_text(encoding="utf-8")

    assert "rendered connections" in log_contents
