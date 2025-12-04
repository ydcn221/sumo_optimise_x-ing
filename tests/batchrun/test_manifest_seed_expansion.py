from pathlib import Path
from types import SimpleNamespace

from sumo_optimise.batchrun.orchestrator import (
    _collect_artifacts,
    _safe_id_for_filename,
    load_manifest,
)
from sumo_optimise.batchrun.models import OutputFormat


def test_manifest_seed_range_expands_per_seed(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "spec,scenario_id,seed,demand_dir,scale\n"
        "spec.json,S,1001-1003,demand,1\n",
        encoding="utf-8",
    )

    scenarios = load_manifest(manifest)

    assert [sc.scenario_id for sc in scenarios] == ["S-1001", "S-1002", "S-1003"]
    assert [sc.scenario_base_id for sc in scenarios] == ["S", "S", "S"]
    assert [sc.seed for sc in scenarios] == [1001, 1002, 1003]


def test_manifest_seed_list_is_preserved(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "spec,scenario_id,seed,demand_dir,scale\n"
        'spec.json,S,"1001,1002,1004",demand,1\n',
        encoding="utf-8",
    )

    scenarios = load_manifest(manifest)

    assert [sc.seed for sc in scenarios] == [1001, 1002, 1004]
    assert [sc.scenario_id for sc in scenarios] == ["S-1001", "S-1002", "S-1004"]
    assert [sc.scenario_base_id for sc in scenarios] == ["S", "S", "S"]


def test_collect_artifacts_names_include_scenario_id(tmp_path: Path) -> None:
    run_dir = tmp_path / "out" / "001"
    run_dir.mkdir(parents=True)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    result = SimpleNamespace(manifest_path=manifest_path, sumocfg_path=None)

    scenario_id = "S-1001/x"
    artifacts = _collect_artifacts(
        result,
        scenario_id=scenario_id,
        output_format=OutputFormat(),
    )
    safe_id = _safe_id_for_filename(scenario_id)

    assert artifacts.fcd.name == f"fcd_{safe_id}.csv.gz"
    assert artifacts.tripinfo.name == f"vehicle_tripinfo_{safe_id}.csv.gz"
    assert artifacts.personinfo.name == f"person_tripinfo_{safe_id}.csv.gz"
    assert artifacts.summary.name == f"vehicle_summary_{safe_id}.csv.gz"
    assert artifacts.sumo_log.name == f"sumo_{safe_id}.log"
