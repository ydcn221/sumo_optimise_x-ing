from __future__ import annotations

import csv
import json
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, List, Sequence

from sumo_optimise.conversion.domain.models import (
    BuildOptions,
    BuildTask,
    DemandOptions,
    OutputDirectoryTemplate,
    OutputFileTemplates,
)
from sumo_optimise.conversion.pipeline import build_and_persist
from sumo_optimise.conversion.utils.constants import SCHEMA_JSON_PATH

from .models import (
    DEFAULT_BEGIN_FILTER,
    DEFAULT_END_TIME,
    DEFAULT_MAX_WORKERS,
    DemandFiles,
    RunArtifacts,
    ScenarioConfig,
    ScenarioResult,
    TripinfoMetrics,
    WaitingMetrics,
    WaitingThresholds,
)
from .parsers import parse_summary, parse_tripinfo


RESULT_COLUMNS = [
    "scenario_id",
    "seed",
    "scale",
    "begin_filter",
    "end_time",
    "demand_dir",
    "vehicle_mean_timeLoss",
    "vehicle_count",
    "person_mean_timeLoss",
    "person_count",
    "person_mean_routeLength",
    "waiting_threshold_fixed",
    "waiting_threshold_pct",
    "waiting_first_exceed_time_fixed",
    "waiting_first_exceed_value_fixed",
    "waiting_first_exceed_time_pct",
    "waiting_first_exceed_value_pct",
    "waiting_max_value",
    "fcd_note",
    "error_note",
]


def load_manifest(path: Path) -> List[ScenarioConfig]:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    suffix = manifest_path.suffix.lower()
    if suffix == ".json":
        return _load_manifest_json(manifest_path)
    if suffix == ".csv":
        return _load_manifest_csv(manifest_path)
    raise ValueError("Manifest must be .json or .csv")


def _load_manifest_json(path: Path) -> List[ScenarioConfig]:
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, list):
        raise ValueError("JSON manifest must be a list of scenario entries")
    base = path.parent
    return [_row_to_config(entry, base) for entry in data]


def _load_manifest_csv(path: Path) -> List[ScenarioConfig]:
    scenarios: List[ScenarioConfig] = []
    base = path.parent
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if not any(row.values()):
                continue
            scenarios.append(_row_to_config(row, base))
    return scenarios


def _row_to_config(raw: dict, base: Path) -> ScenarioConfig:
    try:
        spec = Path(raw["spec"])
        scenario_id = str(raw["scenario_id"])
        seed = int(raw["seed"])
        demand_dir = Path(raw["demand_dir"])
        scale = float(raw["scale"])
    except KeyError as err:
        raise ValueError(f"Missing manifest field: {err.args[0]}") from err
    begin_filter = float(raw.get("begin_filter", DEFAULT_BEGIN_FILTER))
    end_time = float(raw.get("end_time", DEFAULT_END_TIME))
    spec = spec if spec.is_absolute() else (base / spec)
    demand_dir = demand_dir if demand_dir.is_absolute() else (base / demand_dir)
    return ScenarioConfig(
        spec=spec,
        scenario_id=scenario_id,
        seed=seed,
        demand_dir=demand_dir,
        scale=scale,
        begin_filter=begin_filter,
        end_time=end_time,
    )


def resolve_demand_files(demand_dir: Path) -> DemandFiles:
    def pick(pattern: str) -> Path:
        matches = sorted(demand_dir.glob(pattern))
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one file matching {pattern} in {demand_dir}")
        return matches[0]

    return DemandFiles(
        ped_endpoint=pick("*.pe.csv"),
        ped_junction=pick("*.pj.csv"),
        veh_endpoint=pick("*.ve.csv"),
        veh_junction=pick("*.vj.csv"),
    )


def _build_options_for_scenario(
    scenario: ScenarioConfig,
    *,
    output_root: Path,
) -> BuildOptions:
    output_template = OutputDirectoryTemplate(
        root=str(output_root / scenario.scenario_id),
        run="{seq:03}",
    )
    output_files = OutputFileTemplates()
    demand_files = resolve_demand_files(scenario.demand_dir)
    demand_options = DemandOptions(
        ped_endpoint_csv=demand_files.ped_endpoint,
        ped_junction_turn_weight_csv=demand_files.ped_junction,
        veh_endpoint_csv=demand_files.veh_endpoint,
        veh_junction_turn_weight_csv=demand_files.veh_junction,
        simulation_end_time=scenario.end_time,
    )

    return BuildOptions(
        schema_path=SCHEMA_JSON_PATH,
        run_netconvert=False,
        run_netedit=False,
        run_sumo_gui=False,
        console_log=False,
        output_template=output_template,
        output_files=output_files,
        demand=demand_options,
        generate_demand_templates=False,
        network_input=None,
    )


def _collect_artifacts(result) -> RunArtifacts:
    if result.manifest_path is None:
        raise ValueError("manifest path not recorded by build")
    outdir = result.manifest_path.parent
    sumocfg = result.sumocfg_path or (outdir / "config.sumocfg")
    return RunArtifacts(
        outdir=outdir,
        sumocfg=sumocfg,
        tripinfo=outdir / "tripinfo.xml",
        fcd=outdir / "fcd.xml",
        summary=outdir / "summary.xml",
        person_summary=outdir / "summary.person.xml",
        detector=outdir / "detector.xml",
    )


def _sumo_command(
    artifacts: RunArtifacts,
    scenario: ScenarioConfig,
) -> List[str]:
    cmd = [
        "sumo",
        "-c",
        str(artifacts.sumocfg),
        "--tripinfo-output",
        str(artifacts.tripinfo),
        "--fcd-output",
        str(artifacts.fcd),
        "--summary-output",
        str(artifacts.summary),
        "--person-summary-output",
        str(artifacts.person_summary),
        "--device.fcd.begin",
        str(scenario.begin_filter),
        "--end",
        str(scenario.end_time),
        "--seed",
        str(scenario.seed),
        "--scale",
        str(scenario.scale),
    ]
    return cmd


def _set_affinity_preexec(cpu: int | None):
    if cpu is None:
        return None

    def setter():
        try:
            os.sched_setaffinity(0, {cpu})
        except AttributeError:
            pass

    return setter


def run_scenario(
    scenario: ScenarioConfig,
    *,
    output_root: Path,
    thresholds: WaitingThresholds,
    affinity_cpu: int | None,
) -> ScenarioResult | None:
    try:
        options = _build_options_for_scenario(scenario, output_root=output_root)
        build_result = build_and_persist(scenario.spec, options, task=BuildTask.ALL)
        artifacts = _collect_artifacts(build_result)
    except Exception as exc:  # noqa: BLE001
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            begin_filter=scenario.begin_filter,
            end_time=scenario.end_time,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            waiting=WaitingMetrics(),
            waiting_thresholds=thresholds,
            fcd_note="build failed",
            error=str(exc),
        )

    artifacts.tripinfo.parent.mkdir(parents=True, exist_ok=True)

    cmd = _sumo_command(artifacts, scenario)
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=_set_affinity_preexec(affinity_cpu),
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            begin_filter=scenario.begin_filter,
            end_time=scenario.end_time,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            waiting=WaitingMetrics(),
            waiting_thresholds=thresholds,
            fcd_note="sumo failed",
            error=getattr(exc, "stderr", None) or str(exc),
        )

    tripinfo_metrics = parse_tripinfo(artifacts.tripinfo, begin_filter=scenario.begin_filter)
    waiting_metrics = parse_summary(artifacts.summary, thresholds=thresholds)
    person_waiting = parse_summary(artifacts.person_summary, thresholds=thresholds)
    waiting_metrics = _merge_waiting(waiting_metrics, person_waiting)

    return ScenarioResult(
        scenario_id=scenario.scenario_id,
        seed=scenario.seed,
        scale=scenario.scale,
        begin_filter=scenario.begin_filter,
        end_time=scenario.end_time,
        demand_dir=scenario.demand_dir,
        tripinfo=tripinfo_metrics,
        waiting=waiting_metrics,
        waiting_thresholds=thresholds,
        fcd_note="n/a",
        error=None,
    )


def _affinity_plan(count: int) -> List[int | None]:
    try:
        cpus = sorted(os.sched_getaffinity(0))
    except AttributeError:
        return [None] * count
    if not cpus:
        return [None] * count
    planned: List[int | None] = []
    for idx in range(count):
        planned.append(cpus[idx % len(cpus)])
    return planned


def run_batch(
    scenarios: Iterable[ScenarioConfig],
    *,
    output_root: Path,
    thresholds: WaitingThresholds,
    results_csv: Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    scenario_list = list(scenarios)
    if not scenario_list:
        return

    workers = min(max_workers, len(scenario_list))
    affinity = _affinity_plan(workers)
    results: List[ScenarioResult] = []

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                run_scenario,
                scenario,
                output_root=output_root,
                thresholds=thresholds,
                affinity_cpu=affinity[idx % len(affinity)],
            ): scenario
            for idx, scenario in enumerate(scenario_list)
        }
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue
            if result.error is not None:
                print(
                    f"[skip] scenario={result.scenario_id} seed={result.seed} error={result.error}"
                )
                continue
            results.append(result)

    if results:
        _append_results(results_csv, results)


def _merge_waiting(primary: WaitingMetrics, secondary: WaitingMetrics) -> WaitingMetrics:
    fixed_time, fixed_value = _pick_first(
        primary.first_fixed_time,
        primary.first_fixed_value,
        secondary.first_fixed_time,
        secondary.first_fixed_value,
    )
    pct_time, pct_value = _pick_first(
        primary.first_pct_time,
        primary.first_pct_value,
        secondary.first_pct_time,
        secondary.first_pct_value,
    )
    return WaitingMetrics(
        first_fixed_time=fixed_time,
        first_fixed_value=fixed_value,
        first_pct_time=pct_time,
        first_pct_value=pct_value,
        max_waiting=max(primary.max_waiting, secondary.max_waiting),
    )


def _pick_first(
    t_a: float | None,
    v_a: float | None,
    t_b: float | None,
    v_b: float | None,
) -> tuple[float | None, float | None]:
    if t_a is None:
        return t_b, v_b
    if t_b is None:
        return t_a, v_a
    if t_a <= t_b:
        return t_a, v_a
    return t_b, v_b


def _append_results(path: Path, results: Sequence[ScenarioResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=RESULT_COLUMNS)
        if header_needed:
            writer.writeheader()
        for result in results:
            writer.writerow(_result_to_row(result))


def _result_to_row(result: ScenarioResult) -> dict:
    return {
        "scenario_id": result.scenario_id,
        "seed": result.seed,
        "scale": result.scale,
        "begin_filter": result.begin_filter,
        "end_time": result.end_time,
        "demand_dir": str(result.demand_dir),
        "vehicle_mean_timeLoss": _fmt(result.tripinfo.vehicle_mean_time_loss),
        "vehicle_count": result.tripinfo.vehicle_count,
        "person_mean_timeLoss": _fmt(result.tripinfo.person_mean_time_loss),
        "person_count": result.tripinfo.person_count,
        "person_mean_routeLength": _fmt(result.tripinfo.person_mean_route_length),
        "waiting_threshold_fixed": result.waiting_thresholds.fixed,
        "waiting_threshold_pct": result.waiting_thresholds.pct_of_running,
        "waiting_first_exceed_time_fixed": _fmt(result.waiting.first_fixed_time),
        "waiting_first_exceed_value_fixed": _fmt(result.waiting.first_fixed_value),
        "waiting_first_exceed_time_pct": _fmt(result.waiting.first_pct_time),
        "waiting_first_exceed_value_pct": _fmt(result.waiting.first_pct_value),
        "waiting_max_value": _fmt(result.waiting.max_waiting),
        "fcd_note": result.fcd_note,
        "error_note": result.error or "",
    }


def _fmt(value) -> str | float:
    if value is None:
        return ""
    return round(value, 3) if isinstance(value, float) else value
