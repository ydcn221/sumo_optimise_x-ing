from __future__ import annotations

from pathlib import Path

from sumo_optimise.conversion.domain.models import BuildOptions
from sumo_optimise.conversion.pipeline import build_corridor_artifacts
from sumo_optimise.conversion.utils.constants import SCHEMA_JSON_PATH


def test_build_corridor_artifacts_includes_tllogics() -> None:
    spec_path = Path(__file__).resolve().parents[2] / "data" / "reference" / "schema_v1.3_sample.json"
    options = BuildOptions(schema_path=SCHEMA_JSON_PATH)

    result = build_corridor_artifacts(spec_path, options)

    assert "<tlLogics" in result.tllogics_xml
    assert "<tlLogic" in result.tllogics_xml
