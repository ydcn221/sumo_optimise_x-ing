from __future__ import annotations

import csv
import json
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

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
    QueueDurabilityConfig,
    QueueDurabilityMetrics,
    RunArtifacts,
    ScaleProbeConfig,
    ScaleProbeResult,
    ScenarioConfig,
    ScenarioResult,
    TripinfoMetrics,
    WaitingMetrics,
    WaitingThresholds,
)
from .parsers import parse_queue_output, parse_summary, parse_tripinfo


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
    "queue_threshold_steps",
    "queue_threshold_length",
    "queue_first_non_durable_time",
    "queue_max_length",
    "queue_is_durable",
    "scale_probe_enabled",
    "scale_probe_min_failure_scale",
    "scale_probe_attempts",
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
        queue=outdir / "queue.xml",
    )


def _artifacts_for_label(base: RunArtifacts, label: str | None) -> RunArtifacts:
    if not label:
        return base

    outdir = base.outdir / label
    return RunArtifacts(
        outdir=outdir,
        sumocfg=base.sumocfg,
        tripinfo=outdir / base.tripinfo.name,
        fcd=outdir / base.fcd.name,
        summary=outdir / base.summary.name,
        person_summary=outdir / base.person_summary.name,
        detector=outdir / base.detector.name,
        queue=outdir / base.queue.name,
    )


def _sumo_command(
    artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    scale: float,
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
        "--queue-output",
        str(artifacts.queue),
        "--device.fcd.begin",
        str(scenario.begin_filter),
        "--end",
        str(scenario.end_time),
        "--seed",
        str(scenario.seed),
        "--scale",
        str(scale),
    ]
    return cmd


def _normalize_scale(scale: float, resolution: float) -> float:
    if resolution <= 0:
        return scale
    step = round(scale / resolution)
    return round(step * resolution, 10)


def _format_scale_label(scale: float) -> str:
    normalized = f"{scale:.3f}".rstrip("0").rstrip(".")
    safe = normalized.replace("-", "neg").replace(".", "p") or "0"
    return f"scale_{safe}"


def _run_for_scale(
    artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    thresholds: WaitingThresholds,
    queue_config: QueueDurabilityConfig,
    scale: float,
    affinity_cpu: int | None,
    collect_tripinfo: bool,
    collect_waiting: bool,
) -> tuple[TripinfoMetrics, WaitingMetrics, QueueDurabilityMetrics]:
    artifacts.tripinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.queue.parent.mkdir(parents=True, exist_ok=True)

    cmd = _sumo_command(artifacts, scenario, scale=scale)
    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=_set_affinity_preexec(affinity_cpu),
    )

    tripinfo_metrics = (
        parse_tripinfo(artifacts.tripinfo, begin_filter=scenario.begin_filter)
        if collect_tripinfo
        else TripinfoMetrics()
    )

    waiting_metrics = WaitingMetrics()
    if collect_waiting:
        waiting_metrics = parse_summary(artifacts.summary, thresholds=thresholds)
        person_waiting = parse_summary(artifacts.person_summary, thresholds=thresholds)
        waiting_metrics = _merge_waiting(waiting_metrics, person_waiting)

    queue_metrics = parse_queue_output(artifacts.queue, config=queue_config)
    return tripinfo_metrics, waiting_metrics, queue_metrics


def _probe_scale_durability(
    base_artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    thresholds: WaitingThresholds,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    affinity_cpu: int | None,
    queue_cache: Dict[float, QueueDurabilityMetrics],
    attempts: int,
) -> ScaleProbeResult:
    resolution = scale_probe.resolution if scale_probe.resolution > 0 else 0.1
    start_scale = _normalize_scale(scale_probe.start, resolution)
    ceiling = max(scale_probe.ceiling, start_scale)

    def run_scale(raw_scale: float) -> QueueDurabilityMetrics:
        nonlocal attempts
        scale_value = _normalize_scale(raw_scale, resolution)
        if scale_value in queue_cache:
            return queue_cache[scale_value]

        artifacts = _artifacts_for_label(base_artifacts, _format_scale_label(scale_value))
        _, _, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            thresholds=thresholds,
            queue_config=queue_config,
            scale=scale_value,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=False,
            collect_waiting=False,
        )
        queue_cache[scale_value] = queue_metrics
        attempts += 1
        return queue_metrics

    last_durable: float | None = None
    failure_scale: float | None = None

    current = start_scale
    while current <= ceiling:
        metrics = run_scale(current)
        if not metrics.is_durable:
            failure_scale = current
            break
        last_durable = current
        current = _normalize_scale(current + 1.0, resolution)

    if failure_scale is None:
        return ScaleProbeResult(
            enabled=True,
            min_failure_scale=None,
            attempts=attempts,
        )

    if last_durable is None:
        return ScaleProbeResult(
            enabled=True,
            min_failure_scale=failure_scale,
            attempts=attempts,
        )

    low = last_durable
    high = failure_scale

    while high - low > resolution:
        mid = _normalize_scale((low + high) / 2, resolution)
        if mid in {low, high}:
            break

        metrics = run_scale(mid)
        if metrics.is_durable:
            low = mid
        else:
            high = mid

    return ScaleProbeResult(
        enabled=True,
        min_failure_scale=high,
        attempts=attempts,
    )


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
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    affinity_cpu: int | None,
) -> ScenarioResult | None:
    base_queue_metrics = QueueDurabilityMetrics(
        threshold_steps=queue_config.step_window,
        threshold_length=queue_config.length_threshold,
    )
    probe_result = ScaleProbeResult(
        enabled=scale_probe.enabled,
        min_failure_scale=None,
        attempts=0,
    )

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
            queue=base_queue_metrics,
            scale_probe=probe_result,
            fcd_note="build failed",
            error=str(exc),
        )

    try:
        tripinfo_metrics, waiting_metrics, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            thresholds=thresholds,
            queue_config=queue_config,
            scale=scenario.scale,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=True,
            collect_waiting=True,
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
            queue=base_queue_metrics,
            scale_probe=ScaleProbeResult(
                enabled=scale_probe.enabled,
                min_failure_scale=None,
                attempts=1,
            ),
            fcd_note="sumo failed",
            error=getattr(exc, "stderr", None) or str(exc),
        )

    resolution = scale_probe.resolution if scale_probe.resolution > 0 else 0.1
    base_scale_key = _normalize_scale(scenario.scale, resolution)
    queue_cache: Dict[float, QueueDurabilityMetrics] = {base_scale_key: queue_metrics}
    probe_result = ScaleProbeResult(
        enabled=scale_probe.enabled,
        min_failure_scale=None,
        attempts=1,
    )
    if scale_probe.enabled:
        probe_result = _probe_scale_durability(
            artifacts,
            scenario,
            thresholds=thresholds,
            queue_config=queue_config,
            scale_probe=scale_probe,
            affinity_cpu=affinity_cpu,
            queue_cache=queue_cache,
            attempts=1,
        )

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
        queue=queue_metrics,
        scale_probe=probe_result,
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
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
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
                queue_config=queue_config,
                scale_probe=scale_probe,
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
        "queue_threshold_steps": result.queue.threshold_steps,
        "queue_threshold_length": _fmt(result.queue.threshold_length),
        "queue_first_non_durable_time": _fmt(result.queue.first_failure_time),
        "queue_max_length": _fmt(result.queue.max_queue_length),
        "queue_is_durable": str(result.queue.is_durable),
        "scale_probe_enabled": str(result.scale_probe.enabled),
        "scale_probe_min_failure_scale": _fmt(result.scale_probe.min_failure_scale),
        "scale_probe_attempts": result.scale_probe.attempts,
        "fcd_note": result.fcd_note,
        "error_note": result.error or "",
    }


def _fmt(value) -> str | float:
    if value is None:
        return ""
    return round(value, 3) if isinstance(value, float) else value
