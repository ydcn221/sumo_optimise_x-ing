from __future__ import annotations

import csv
import gzip
import json
import math
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, as_completed
from queue import Empty
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
from sumo_optimise.conversion.utils.io import write_sumocfg

from .models import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_SAT_SECONDS,
    DEFAULT_UNSAT_SECONDS,
    DEFAULT_WARMUP_SECONDS,
    PhaseTiming,
    OutputCompression,
    OutputFormat,
    DemandFiles,
    QueueDurabilityConfig,
    QueueDurabilityMetrics,
    RunArtifacts,
    RunTimings,
    ScaleProbeConfig,
    ScaleProbeResult,
    ScenarioConfig,
    ScenarioResult,
    ScaleMode,
    TripinfoMetrics,
    WorkerPhase,
    WorkerStatus,
)
# CSV output layout (grouped by scenario inputs → trip stats → queue durability → probe metadata → notes).
# queue_first_over_saturation_time: first timestep where waiting/running ratio stayed above
# queue_threshold_length for at least queue_threshold_steps consecutive seconds; blank means durable.
from .parsers import parse_tripinfo, parse_waiting_ratio, parse_waiting_percentile


RESULT_COLUMNS = [
    "scenario_id",
    "scenario_base_id",
    "seed",
    "warmup_seconds",
    "unsat_seconds",
    "sat_seconds",
    "ped_unsat_scale",
    "ped_sat_scale",
    "veh_unsat_scale",
    "veh_sat_scale",
    "demand_dir",
    "vehicle_count",
    "person_count",
    "vehicle_mean_timeLoss",
    "person_mean_timeLoss",
    "person_mean_routeLength",
    "waiting_p95_sat",
    "worker_id",
    "build_start",
    "build_end",
    "sumo_start",
    "sumo_end",
    "metrics_start",
    "metrics_end",
]
QUEUE_PROBE_COLUMNS = [
    "queue_threshold_steps",
    "queue_threshold_length",
    "queue_first_over_saturation_time",
    "queue_is_durable",
    "scale_probe_enabled",
    "scale_probe_max_durable_scale",
    "scale_probe_attempts",
    "probe_start",
    "probe_end",
]
RESULT_COLUMNS_PROBE = RESULT_COLUMNS + QUEUE_PROBE_COLUMNS + ["error"]
RESULT_COLUMNS_NO_PROBE = RESULT_COLUMNS + ["error"]


def _debug_log(log_path: Path | None, message: str) -> None:
    """Append a debug line to the per-scenario SUMO log."""
    if log_path is None:
        return
    timestamp = datetime.now().isoformat(timespec="milliseconds")
    try:
        with log_path.open("a", encoding="utf-8") as fp:
            fp.write(f"[batchrun] {timestamp} {message}\\n")
    except OSError:
        # Best-effort logging; skip if the log cannot be written.
        return


def _format_over_saturation_reason(length_threshold: float, step_window: int) -> str:
    """Describe why queue over-saturation was flagged."""
    return (
        f"waiting/running ratio >= {length_threshold:.3f} was detected for at least "
        f"{step_window} consecutive seconds"
    )


def _run_layout(
    scenario: ScenarioConfig, *, output_root: Path, run_label: str | None
) -> tuple[str, Path, Path, str, str]:
    safe_sid = _safe_id_for_filename(scenario.scenario_id)
    safe_base_sid = _safe_id_for_filename(scenario.scenario_base_id or scenario.scenario_id)
    label = run_label or "base"
    safe_label = _safe_id_for_filename(label)
    run_id = f"{safe_sid}-{scenario.seed}-{safe_label}"
    root_dir = output_root / f"scenario-{safe_base_sid}"
    run_dir = root_dir / f"seed-{scenario.seed}" / f"run-{safe_label}"
    return run_id, root_dir, run_dir, safe_sid, safe_label


def _sumo_log_path(run_dir: Path, run_id: str) -> Path:
    return run_dir / f"sumo_{run_id}.log"


def _send_status(
    queue,
    *,
    worker_id: int,
    scenario_id: str = "",
    seed: int = 0,
    scale: float | None = None,
    affinity_cpu: int | None = None,
    phase: WorkerPhase = WorkerPhase.IDLE,
    step: float | None = None,
    label: str = "",
    error: str | None = None,
    probe_scale: float | None = None,
    done: bool = False,
    completed: bool = False,
) -> None:
    if queue is None:
        return
    queue.put(
        {
            "worker_id": worker_id,
            "scenario_id": scenario_id,
            "seed": seed,
            "scale": scale,
            "affinity_cpu": affinity_cpu,
            "phase": phase.value if isinstance(phase, WorkerPhase) else str(phase),
            "step": step,
            "label": label,
            "error": error,
            "probe_scale": probe_scale,
            "done": done,
            "completed": completed,
            "timestamp": time.time(),
        }
    )


def _safe_id_for_filename(scenario_id: str) -> str:
    """Make a scenario identifier safe for file names."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", scenario_id).strip("_")
    return safe or "scenario"


def _colorize(text: str, phase: WorkerPhase) -> str:
    colors = {
        WorkerPhase.IDLE: "37",
        WorkerPhase.BUILD: "33",
        WorkerPhase.SUMO: "34",
        WorkerPhase.PROBE: "35",
        WorkerPhase.PARSE: "36",
        WorkerPhase.DONE: "32",
        WorkerPhase.ERROR: "31",
    }
    code = colors.get(phase)
    if not code:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


def _format_bytes(num: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(num)
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}B"


def _format_seconds(seconds: float) -> str:
    if seconds < 0:
        return "-"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(round(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}h{minutes:02d}m{sec:02d}s"
    return f"{minutes:d}m{sec:02d}s"


def _mark_start(timing: PhaseTiming | None) -> None:
    if timing is None:
        return
    if timing.start is None:
        timing.start = time.time()


def _mark_end(timing: PhaseTiming | None) -> None:
    if timing is None:
        return
    timing.end = time.time()


def _format_timestamp(timestamp: float | None) -> str:
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                continue
    return total


def _render_grid(
    statuses: Dict[int, WorkerStatus],
    *,
    total: int,
    completed: int,
) -> tuple[List[str], int]:
    term_width = shutil.get_terminal_size((160, 24)).columns
    cell_width = 44
    cols = max(1, term_width // cell_width)
    rows: List[str] = []
    ordered = [statuses[k] for k in sorted(statuses.keys())]
    for idx, status in enumerate(ordered):
        scenario = status.scenario_id or "-"
        phase = status.phase
        label_text = status.label
        if not label_text and status.step is not None:
            label_text = f"sumo#{int(status.step)}"
        label = (label_text or phase.value)[:10]
        cpu_text = "--" if status.affinity_cpu is None else f"{status.affinity_cpu:02d}"
        cell = f"[{status.worker_id:02d}@{cpu_text}] {label:<10} {scenario:<20}"
        cell = cell[:cell_width].ljust(cell_width)
        rows.append(_colorize(cell, phase))
        if (idx + 1) % cols == 0:
            rows.append("\n")
    error_count = sum(1 for s in statuses.values() if s.phase == WorkerPhase.ERROR)
    return rows, error_count


def _render_loop(
    status_queue,
    stop_event: threading.Event,
    total: int,
    worker_count: int,
    output_root: Path,
    diag_log_path: Path | None = None,
) -> None:
    statuses: Dict[int, WorkerStatus] = {
        idx: WorkerStatus(worker_id=idx) for idx in range(worker_count)
    }
    last_render = 0.0
    completed = 0
    last_size_time = 0.0
    size_display: str | None = None
    last_height = 0
    use_tty = sys.stdout.isatty()
    task_start: Dict[int, float] = {}
    task_durations: List[float] = []
    batch_start = time.time()

    def _render() -> None:
        nonlocal last_height
        rows, error_count = _render_grid(
            statuses,
            total=total,
            completed=completed,
        )
        parts = [
            f"completed {completed}/{total}",
            f"errors {error_count}",
        ]
        if task_durations:
            avg_task = sum(task_durations) / len(task_durations)
            parts.append(f"avg_run {_format_seconds(avg_task)}")
        else:
            parts.append("avg_run -")
        if completed:
            wall_avg = (time.time() - batch_start) / completed
            parts.append(f"wall_avg {_format_seconds(wall_avg)}")
        else:
            parts.append("wall_avg -")
        if size_display:
            parts.append(f"out {size_display}")
        summary_line = " | ".join(parts)
        grid_text = "".join(rows)
        height = grid_text.count("\n")
        if grid_text and not grid_text.endswith("\n"):
            height += 1
        height += 1  # summary line

        if use_tty:
            if last_height:
                print(f"\x1b[{last_height}F", end="")
                print("\x1b[J", end="")
            print(grid_text, end="")
            if grid_text and not grid_text.endswith("\n"):
                print()
            print(summary_line, end="\n")
            sys.stdout.flush()
            last_height = height
        else:
            print(grid_text)
            print(summary_line)
            last_height = 0

    while not stop_event.is_set() or not status_queue.empty():
        try:
            evt = status_queue.get(timeout=0.1)
        except Empty:
            evt = None
        if evt is not None:
            worker_id = int(evt.get("worker_id", -1))
            status = statuses.get(worker_id, WorkerStatus(worker_id=worker_id))
            status.scenario_id = evt.get("scenario_id", status.scenario_id)
            status.seed = evt.get("seed", status.seed)
            status.scale = evt.get("scale", status.scale)
            status.affinity_cpu = evt.get("affinity_cpu", status.affinity_cpu)
            try:
                status.phase = WorkerPhase(evt.get("phase", status.phase))
            except ValueError:
                status.phase = WorkerPhase.IDLE
            status.step = evt.get("step", status.step)
            status.label = evt.get("label", status.label)
            status.error = evt.get("error")
            status.done = evt.get("done", status.done)
            status.probe_scale = evt.get("probe_scale", status.probe_scale)
            status.last_update = evt.get("timestamp", time.time())
            if status.phase != WorkerPhase.IDLE and worker_id not in task_start:
                task_start[worker_id] = status.last_update
            statuses[worker_id] = status
            if evt.get("completed"):
                completed += 1
                started_at = task_start.pop(worker_id, None)
                if started_at is not None:
                    task_durations.append(max(0.0, status.last_update - started_at))
            if diag_log_path is not None:
                try:
                    with diag_log_path.open("a", encoding="utf-8") as fp:
                        ts = datetime.now().isoformat(timespec="milliseconds")
                        fp.write(
                            f"{ts} worker={worker_id} scenario={status.scenario_id} "
                            f"phase={status.phase.value} label={status.label} "
                            f"step={status.step} scale={status.scale} "
                            f"probe_scale={status.probe_scale} cpu={status.affinity_cpu} "
                            f"done={status.done} error={status.error or ''}\n"
                        )
                except OSError:
                    pass
        now = time.time()
        if now - last_size_time >= 2.0:
            try:
                size_bytes = _dir_size_bytes(output_root)
                size_display = _format_bytes(size_bytes)
            except OSError:
                size_display = None
            last_size_time = now
        if now - last_render >= 0.5:
            _render()
            last_render = now
    try:
        _render()
    except (EOFError, BrokenPipeError):
        return

REQUIRED_MANIFEST_FIELDS = [
    "spec",
    "scenario_id",
    "seed",
    "demand_dir",
    "warmup_seconds",
    "unsat_seconds",
    "sat_seconds",
    "ped_unsat_scale",
    "ped_sat_scale",
    "veh_unsat_scale",
    "veh_sat_scale",
]

LEGACY_MANIFEST_FIELDS = {
    "spec",
    "scenario_id",
    "seed",
    "demand_dir",
    "scale",
    "begin_filter",
    "end_time",
}


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
    if data and all(_is_legacy_manifest_row(entry) for entry in data):
        _emit_legacy_manifest_template(path, data)
        raise ValueError(
            f"Legacy manifest detected (missing required fields). A template with the new format was written next to {path}"
        )
    scenarios: List[ScenarioConfig] = []
    for entry in data:
        scenarios.extend(_row_to_config(entry, base))
    return scenarios


def _load_manifest_csv(path: Path) -> List[ScenarioConfig]:
    scenarios: List[ScenarioConfig] = []
    base = path.parent
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(row)
    if rows and all(_is_legacy_manifest_row(row) for row in rows):
        _emit_legacy_manifest_template(path, rows)
        raise ValueError(
            f"Legacy manifest detected (missing required fields). A template with the new format was written next to {path}"
        )
    for row in rows:
        scenarios.extend(_row_to_config(row, base))
    return scenarios


def _parse_seed_field(raw_seed: object) -> List[int]:
    """Parse a seed field that may contain comma lists or ranges (e.g., 1001-1004)."""
    if isinstance(raw_seed, (int, float)):
        return [int(raw_seed)]
    if isinstance(raw_seed, (list, tuple, set)):
        tokens = list(raw_seed)
    else:
        seed_str = str(raw_seed).strip()
        if not seed_str:
            raise ValueError("Seed value cannot be empty")
        tokens = [token.strip() for token in seed_str.split(",")]

    seeds: List[int] = []
    seen = set()

    for token in tokens:
        if token in ("", None):
            raise ValueError("Seed list contains an empty token")
        if isinstance(token, (int, float)):
            values = [int(token)]
        else:
            token_str = str(token).strip()
            if "-" in token_str:
                bounds = [part.strip() for part in token_str.split("-", maxsplit=1)]
                if len(bounds) != 2 or not all(bounds):
                    raise ValueError(f"Invalid seed range: {token_str}")
                start, end = (int(bound) for bound in bounds)
                if end < start:
                    raise ValueError(f"Seed range must be ascending: {token_str}")
                values = list(range(start, end + 1))
            else:
                values = [int(token_str)]

        for value in values:
            if value not in seen:
                seen.add(value)
                seeds.append(value)

    if not seeds:
        raise ValueError("No seeds parsed from seed field")
    return seeds


def _row_to_config(raw: dict, base: Path) -> List[ScenarioConfig]:
    missing = [field for field in REQUIRED_MANIFEST_FIELDS if field not in raw]
    if missing:
        raise ValueError(f"Missing manifest field(s): {', '.join(missing)}")
    try:
        spec = Path(raw["spec"])
        scenario_id = str(raw["scenario_id"])
        seeds = _parse_seed_field(raw["seed"])
        demand_dir = Path(raw["demand_dir"])
        warmup_seconds = float(raw["warmup_seconds"])
        unsat_seconds = float(raw["unsat_seconds"])
        sat_seconds = float(raw["sat_seconds"])
        ped_unsat_scale = float(raw["ped_unsat_scale"])
        ped_sat_scale = float(raw["ped_sat_scale"])
        veh_unsat_scale = float(raw["veh_unsat_scale"])
        veh_sat_scale = float(raw["veh_sat_scale"])
    except KeyError as err:
        raise ValueError(f"Missing manifest field: {err.args[0]}") from err
    except ValueError as err:
        raise ValueError(f"Invalid manifest value: {err}") from err

    if not scenario_id:
        raise ValueError("scenario_id cannot be empty")
    if warmup_seconds < 0 or unsat_seconds < 0 or sat_seconds < 0:
        raise ValueError("Time window values cannot be negative")

    spec = spec if spec.is_absolute() else (base / spec)
    demand_dir = demand_dir if demand_dir.is_absolute() else (base / demand_dir)
    scenarios: List[ScenarioConfig] = []
    for seed in seeds:
        run_id = f"{scenario_id}-{seed}"
        scenarios.append(
            ScenarioConfig(
                spec=spec,
                scenario_id=run_id,
                scenario_base_id=scenario_id,
                seed=seed,
                demand_dir=demand_dir,
                warmup_seconds=warmup_seconds,
                unsat_seconds=unsat_seconds,
                sat_seconds=sat_seconds,
                ped_unsat_scale=ped_unsat_scale,
                ped_sat_scale=ped_sat_scale,
                veh_unsat_scale=veh_unsat_scale,
                veh_sat_scale=veh_sat_scale,
            )
        )
    return scenarios


def _is_legacy_manifest_row(raw: dict) -> bool:
    return LEGACY_MANIFEST_FIELDS.issubset(raw.keys()) and not all(
        field in raw for field in REQUIRED_MANIFEST_FIELDS
    )


def _emit_legacy_manifest_template(path: Path, rows: List[dict]) -> None:
    template_path = path.with_name(path.name + ".new")
    if template_path.exists():
        return

    header = REQUIRED_MANIFEST_FIELDS
    template_rows: List[dict] = []
    for row in rows:
        try:
            warmup = float(row.get("begin_filter", DEFAULT_WARMUP_SECONDS))
            end_time = float(row.get("end_time", warmup))
            unsat = max(end_time - warmup, 0.0)
            scale = float(row.get("scale", 1.0))
        except Exception:
            warmup = DEFAULT_WARMUP_SECONDS
            end_time = warmup + DEFAULT_UNSAT_SECONDS
            unsat = DEFAULT_UNSAT_SECONDS
            scale = 1.0
        template_rows.append(
            {
                "spec": row.get("spec", ""),
                "scenario_id": row.get("scenario_id", ""),
                "seed": row.get("seed", ""),
                "demand_dir": row.get("demand_dir", ""),
                "warmup_seconds": warmup,
                "unsat_seconds": unsat,
                "sat_seconds": DEFAULT_SAT_SECONDS,
                "ped_unsat_scale": scale,
                "ped_sat_scale": scale,
                "veh_unsat_scale": scale,
                "veh_sat_scale": scale,
            }
        )

    with template_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=header)
        writer.writeheader()
        writer.writerows(template_rows)


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
    run_label: str | None = None,
    network_input: Path | None = None,
) -> BuildOptions:
    run_id, root_dir, _, safe_sid, safe_label = _run_layout(
        scenario, output_root=output_root, run_label=run_label
    )
    output_template = OutputDirectoryTemplate(
        root=str(root_dir),
        run=f"seed-{scenario.seed}/run-{safe_label}",
    )
    output_files = OutputFileTemplates()
    demand_files = resolve_demand_files(scenario.demand_dir)
    sim_end = scenario.sim_end
    demand_options = DemandOptions(
        ped_endpoint_csv=demand_files.ped_endpoint,
        ped_junction_turn_weight_csv=demand_files.ped_junction,
        veh_endpoint_csv=demand_files.veh_endpoint,
        veh_junction_turn_weight_csv=demand_files.veh_junction,
        simulation_end_time=sim_end,
        warmup_seconds=scenario.warmup_seconds,
        unsat_seconds=scenario.unsat_seconds,
        sat_seconds=scenario.sat_seconds,
        ped_unsat_scale=scenario.ped_unsat_scale,
        ped_sat_scale=scenario.ped_sat_scale,
        veh_unsat_scale=scenario.veh_unsat_scale,
        veh_sat_scale=scenario.veh_sat_scale,
    )

    return BuildOptions(
        schema_path=SCHEMA_JSON_PATH,
        run_netconvert=True,
        run_netedit=False,
        run_sumo_gui=False,
        console_log=False,
        output_template=output_template,
        output_files=output_files,
        demand=demand_options,
        generate_demand_templates=False,
        network_input=network_input,
        extra_context={
            "scenario": safe_sid,
            "seed": scenario.seed,
            "label": safe_label,
            "run_id": run_id,
            "id": run_id,
        },
    )


def _collect_artifacts(result, *, scenario: ScenarioConfig, label: str, output_format: OutputFormat) -> RunArtifacts:
    if result.manifest_path is None:
        raise ValueError("manifest path not recorded by build")
    outdir = result.manifest_path.parent
    safe_sid = _safe_id_for_filename(scenario.scenario_id)
    safe_label = _safe_id_for_filename(label or "base")
    run_id = result.run_id or f"{safe_sid}-{scenario.seed}-{safe_label}"
    context = {"id": run_id}
    files = OutputFileTemplates()
    network_path = outdir / files.network.format_map(context)
    routes_path = outdir / files.routes.format_map(context)
    sumocfg = result.sumocfg_path or (outdir / files.sumocfg.format_map(context))
    suffix = output_format.sumo_output_suffix
    write_sumocfg(
        sumocfg_path=sumocfg,
        net_path=network_path,
        routes_path=routes_path,
        sim_end=scenario.sim_end,
        seed=scenario.seed,
        fcd_begin=scenario.unsat_begin,
        tripinfo_path=outdir / f"vehicle_tripinfo_{run_id}{suffix}",
        personinfo_path=outdir / f"person_tripinfo_{run_id}{suffix}",
        fcd_output_path=outdir / f"fcd_{run_id}{suffix}",
        summary_output_path=outdir / f"vehicle_summary_{run_id}{suffix}",
        person_summary_output_path=outdir / f"person_summary_{run_id}{suffix}",
        column_header_value="auto",
        no_warnings=True,
    )
    return RunArtifacts(
        outdir=outdir,
        sumocfg=sumocfg,
        network=network_path,
        tripinfo=outdir / f"vehicle_tripinfo_{run_id}{suffix}",
        personinfo=outdir / f"person_tripinfo_{run_id}{suffix}",
        fcd=outdir / f"fcd_{run_id}{suffix}",
        summary=outdir / f"vehicle_summary_{run_id}{suffix}",
        person_summary=outdir / f"person_summary_{run_id}{suffix}",
        detector=outdir / f"detector_{run_id}.xml",
        queue=outdir / f"queue_{run_id}.xml",
        sumo_log=outdir / f"sumo_{run_id}.log",
        run_id=run_id,
    )


def _artifacts_for_label(base: RunArtifacts, label: str | None) -> RunArtifacts:
    if not label:
        return base

    safe_label = _safe_id_for_filename(label)
    parts = base.run_id.split("-")
    if len(parts) >= 3:
        prefix = "-".join(parts[:-1])
        new_run_id = f"{prefix}-{safe_label}"
    else:
        new_run_id = f"{base.run_id}-{safe_label}"
    outdir = base.outdir.parent / f"run-{safe_label}"

    def _replace(path: Path) -> Path:
        return outdir / path.name.replace(base.run_id, new_run_id)

    return RunArtifacts(
        outdir=outdir,
        sumocfg=base.sumocfg,
        network=base.network,
        tripinfo=_replace(base.tripinfo),
        personinfo=_replace(base.personinfo),
        fcd=_replace(base.fcd),
        summary=_replace(base.summary),
        person_summary=_replace(base.person_summary),
        detector=_replace(base.detector),
        queue=_replace(base.queue),
        sumo_log=_replace(base.sumo_log),
        run_id=new_run_id,
    )


def _sumo_command(
    artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    fcd_begin: float,
) -> List[str]:
    return ["sumo", "-c", str(artifacts.sumocfg)]


def _decompress_gz(src: Path) -> Path:
    if not src.name.endswith(".gz"):
        return src
    dst = src.with_name(src.name[: -len(".gz")])
    dst.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src, "rb") as rfp, dst.open("wb") as wfp:
        shutil.copyfileobj(rfp, wfp)
    return dst


@contextmanager
def _materialize_metrics_inputs(
    artifacts: RunArtifacts,
    *,
    output_format: OutputFormat,
    need_tripinfo: bool,
    need_personinfo: bool,
    need_summary: bool,
):
    temp_plain: List[Path] = []

    def _maybe_decompress(path: Path | None) -> Path | None:
        if path is None or not path.exists():
            return None
        if output_format.compression is OutputCompression.GZ:
            if path.name.endswith(".gz"):
                plain = _decompress_gz(path)
                temp_plain.append(plain)
                return plain
            return path
        return path

    trip_path = _maybe_decompress(artifacts.tripinfo) if need_tripinfo else None
    person_path = _maybe_decompress(artifacts.personinfo) if need_personinfo else None
    summary_path = _maybe_decompress(artifacts.summary) if need_summary else None

    try:
        yield trip_path, person_path, summary_path
    finally:
        if output_format.compression is OutputCompression.GZ:
            for path in temp_plain:
                path.unlink(missing_ok=True)
        elif output_format.compression is OutputCompression.ZST:
            _compress_artifacts(
                artifacts,
                level=output_format.zstd_level,
                log_path=artifacts.sumo_log,
            )


def _run_sumo_streaming(
    cmd: List[str],
    *,
    affinity_cpu: int | None,
    status_queue,
    worker_id: int | None,
    scenario_id: str,
    seed: int,
    phase: WorkerPhase,
    scale: float,
    use_pty: bool,
    log_file=None,
    summary_path: Path | None = None,
    waiting_config: QueueDurabilityConfig | None = None,
    enable_waiting_abort: bool = False,
) -> tuple[bool, QueueDurabilityMetrics | None]:
    log_path: Path | None = Path(log_file.name) if log_file else None
    step_pattern = re.compile(r"Step #([0-9]+(?:\\.\\d+)?)")
    last_step: float | None = None
    last_label = "sumo"

    aborted = False
    abort_event = threading.Event()
    live_waiting_metrics: QueueDurabilityMetrics | None = None
    waiting_state = {"streak": 0, "max_ratio": 0.0, "first_over_saturation": None}

    def debug(message: str) -> None:
        _debug_log(log_path, message)

    def _monitor_summary_ratio() -> None:
        nonlocal aborted, live_waiting_metrics
        if (
            not enable_waiting_abort
            or summary_path is None
            or waiting_config is None
            or waiting_config.length_threshold <= 0
        ):
            return
        debug(
            f"[waiting-monitor] start threshold_ratio={waiting_config.length_threshold} "
            f"step_window={waiting_config.step_window} path={summary_path}"
        )
        buffer = ""
        pos = 0
        streak = 0
        max_ratio = 0.0
        first_over_saturation_time: float | None = None
        try:
            while not abort_event.is_set():
                if not summary_path.exists():
                    time.sleep(0.05)
                    continue
                with summary_path.open("r", encoding="utf-8", errors="ignore") as fp:
                    fp.seek(pos)
                    new = fp.read()
                    pos = fp.tell()
                    if not new:
                        time.sleep(0.05)
                        continue
                    buffer += new
                    while True:
                        start = buffer.find("<step")
                        if start == -1:
                            break
                        end = buffer.find("/>", start)
                        if end == -1:
                            break
                        block = buffer[start : end + len("/>")]
                        buffer = buffer[end + len("/>") :]
                        try:
                            elem = ET.fromstring(block)
                        except ET.ParseError:
                            continue
                        try:
                            waiting = float(elem.attrib.get("waiting") or 0.0)
                        except (TypeError, ValueError):
                            waiting = 0.0
                        try:
                            running = float(elem.attrib.get("running") or 0.0)
                        except (TypeError, ValueError):
                            running = 0.0
                        time_attr = elem.attrib.get("time") or elem.attrib.get("timestep")
                        try:
                            time_value = float(time_attr) if time_attr is not None else None
                        except (TypeError, ValueError):
                            time_value = None
                        total = running
                        ratio = (waiting / total) if total > 0 else 0.0
                        max_ratio = max(max_ratio, ratio)
                        if ratio >= waiting_config.length_threshold:
                            streak += 1
                            if (
                                first_over_saturation_time is None
                                and streak >= waiting_config.step_window
                            ):
                                first_over_saturation_time = time_value
                                abort_event.set()
                                aborted = True
                                reason = _format_over_saturation_reason(
                                    waiting_config.length_threshold,
                                    waiting_config.step_window,
                                )
                                debug(
                                    f"[waiting-monitor] over saturation detected at t={time_value} "
                                    f"({reason}); streak={streak}/{waiting_config.step_window} "
                                    f"ratio={ratio:.3f}"
                                )
                                break
                        else:
                            streak = 0
        except Exception:
            # best-effort
            pass
        finally:
            waiting_state["streak"] = streak
            waiting_state["max_ratio"] = max_ratio
            waiting_state["first_over_saturation"] = first_over_saturation_time
            if first_over_saturation_time is not None:
                live_waiting_metrics = QueueDurabilityMetrics(
                    first_failure_time=first_over_saturation_time,
                    max_queue_length=max_ratio,
                    threshold_steps=waiting_config.step_window if waiting_config else 0,
                    threshold_length=waiting_config.length_threshold if waiting_config else 0.0,
                )
            if streak or first_over_saturation_time is not None:
                reason = ""
                if first_over_saturation_time is not None and waiting_config is not None:
                    reason = (
                        f" over saturation detected because "
                        f"{_format_over_saturation_reason(waiting_config.length_threshold, waiting_config.step_window)}"
                    )
                debug(
                    f"[waiting-monitor] exit streak={streak} "
                    f"max_ratio={max_ratio:.3f} first_over_saturation_time={first_over_saturation_time} "
                    f"aborted={aborted} event_set={abort_event.is_set()}{reason}"
                )

    def handle_line(line: str, redraw: bool) -> None:
        nonlocal last_step, last_label
        step_value: float | None = None
        match = step_pattern.search(line)
        if match:
            try:
                step_value = float(match.group(1))
            except ValueError:
                step_value = None
        if step_value is not None:
            last_step = step_value
            last_label = f"sumo#{int(step_value)}"
        label_text = last_label
        if log_file:
            log_file.write(line + "\n")
            log_file.flush()
        _send_status(
            status_queue,
            worker_id=worker_id or 0,
            scenario_id=scenario_id,
            seed=seed,
            scale=scale,
            affinity_cpu=affinity_cpu,
            phase=phase,
            step=step_value if step_value is not None else last_step,
            label=label_text,
        )

    monitor_thread = None
    if enable_waiting_abort and summary_path is not None and waiting_config is not None:
        monitor_thread = threading.Thread(target=_monitor_summary_ratio, daemon=True)
        monitor_thread.start()

    if use_pty and os.name == "nt":
        from winpty import PtyProcess

        proc = PtyProcess.spawn(cmd)
        buffer = ""
        try:
            while proc.isalive():
                try:
                    chunk = proc.read(1024)
                except EOFError:
                    break
                if not chunk:
                    if abort_event.is_set():
                        proc.close(True)
                        break
                    time.sleep(0.01)
                    continue
                text = chunk.decode(errors="replace") if isinstance(chunk, bytes) else chunk
                buffer += text
                while True:
                    nl = buffer.find("\n")
                    cr = buffer.find("\r")
                    if nl == -1 and cr == -1:
                        break
                    if cr != -1 and (nl == -1 or cr < nl):
                        line = buffer[:cr]
                        buffer = buffer[cr + 1 :]
                        handle_line(line, True)
                        continue
                if nl != -1:
                    line = buffer[:nl]
                    buffer = buffer[nl + 1 :]
                    handle_line(line, False)
                    continue
        finally:
            rc = proc.exitstatus or 0
            if proc.isalive():
                proc.close(True)
        if buffer:
            handle_line(buffer, False)
        if monitor_thread:
            abort_event.set()
            monitor_thread.join(timeout=0.2)
        debug(f"[sumo-stream] winpty exit rc={rc} aborted={aborted} last_step={last_step}")
        if rc != 0:
            raise subprocess.CalledProcessError(rc, cmd)
        return aborted, live_waiting_metrics

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=0,
        preexec_fn=_set_affinity_preexec(affinity_cpu),
    ) as proc:
        buffer = ""
        for chunk in iter(lambda: proc.stdout.read(1024), b""):  # type: ignore[attr-defined]
            text = chunk.decode(errors="replace")
            buffer += text
            while True:
                nl = buffer.find("\n")
                cr = buffer.find("\r")
                if nl == -1 and cr == -1:
                    break
                if cr != -1 and (nl == -1 or cr < nl):
                    line = buffer[:cr]
                    buffer = buffer[cr + 1 :]
                    handle_line(line, True)
                    continue
                if nl != -1:
                    line = buffer[:nl]
                    buffer = buffer[nl + 1 :]
                    handle_line(line, False)
                    continue
            if abort_event.is_set():
                debug(f"[sumo-stream] abort_event set; terminating SUMO (last_step={last_step})")
                proc.terminate()
                break
        if buffer:
            handle_line(buffer, False)
        proc.wait()
        if monitor_thread:
            abort_event.set()
            monitor_thread.join(timeout=0.2)
        debug(f"[sumo-stream] exit rc={proc.returncode} aborted={aborted} last_step={last_step}")
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
    return aborted, live_waiting_metrics


def _normalize_scale(scale: float, fine_step: float) -> float:
    if fine_step <= 0:
        return scale
    step = round(scale / fine_step)
    return round(step * fine_step, 10)


def _format_scale_label(scale: float) -> str:
    normalized = f"{scale:.3f}".rstrip("0").rstrip(".")
    safe = normalized.replace("-", "neg").replace(".", "p") or "0"
    return f"scale_{safe}"


def _run_for_scale(
    artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    queue_config: QueueDurabilityConfig,
    output_format: OutputFormat,
    scale: float,
    sumo_scale: float | None = None,
    affinity_cpu: int | None,
    collect_tripinfo: bool,
    status_queue=None,
    worker_id: int | None = None,
    phase: WorkerPhase = WorkerPhase.SUMO,
    use_pty: bool = False,
    enable_waiting_abort: bool = False,
    metrics_trace: bool = False,
    sumo_timing: PhaseTiming | None = None,
    metrics_timing: PhaseTiming | None = None,
    metrics_phase: WorkerPhase | None = None,
    metrics_label: str | None = None,
    compute_queue_metrics: bool = True,
) -> tuple[TripinfoMetrics, QueueDurabilityMetrics, float | None]:
    artifacts.tripinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.personinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.queue.parent.mkdir(parents=True, exist_ok=True)
    artifacts.sumo_log.parent.mkdir(parents=True, exist_ok=True)

    applied_scale = sumo_scale if sumo_scale is not None else scale
    cmd = _sumo_command(artifacts, scenario, fcd_begin=scenario.unsat_begin)
    with artifacts.sumo_log.open("a", encoding="utf-8") as log_fp:
        log_fp.write(" ".join(cmd) + "\n")
        log_fp.flush()
        aborted, live_waiting_metrics = _run_sumo_streaming(
            cmd,
            affinity_cpu=affinity_cpu,
            status_queue=status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            phase=phase,
            scale=scale,
            use_pty=use_pty,
            log_file=log_fp,
            summary_path=artifacts.summary,
            waiting_config=queue_config if compute_queue_metrics else None,
            enable_waiting_abort=enable_waiting_abort and compute_queue_metrics,
        )

    _mark_end(sumo_timing)
    _send_status(
        status_queue,
        worker_id=worker_id or 0,
        scenario_id=scenario.scenario_id,
        seed=scenario.seed,
        scale=scale,
        affinity_cpu=affinity_cpu,
        phase=metrics_phase or WorkerPhase.PARSE,
        label=metrics_label or "post",
    )

    tripinfo_metrics = TripinfoMetrics()
    waiting_p95_sat: float | None = None
    queue_metrics = QueueDurabilityMetrics(
        threshold_steps=queue_config.step_window,
        threshold_length=queue_config.length_threshold,
    )
    need_summary_for_queue = compute_queue_metrics and live_waiting_metrics is None
    need_summary_for_waiting = scenario.sat_seconds > 0
    need_summary = need_summary_for_queue or need_summary_for_waiting
    _mark_start(metrics_timing)
    with _materialize_metrics_inputs(
        artifacts,
        output_format=output_format,
        need_tripinfo=collect_tripinfo,
        need_personinfo=collect_tripinfo,
        need_summary=need_summary,
    ) as (trip_path, person_path, summary_path):
        if collect_tripinfo and trip_path is not None:
            trip_start = time.time()
            last_trip_progress = {"count": 0, "logged_at": trip_start}

            def _trip_progress(count: int, elapsed: float) -> None:
                last_trip_progress["count"] = count
                now = time.time()
                if now - last_trip_progress["logged_at"] >= 1.0:
                    last_trip_progress["logged_at"] = now
                    _debug_log(
                        artifacts.sumo_log,
                        f"[metrics-trace] tripinfo count={count} elapsed={elapsed:.1f}s rate={count/max(elapsed, 1e-9):.0f}/s",
                    )

            tripinfo_metrics = parse_tripinfo(
                trip_path,
                begin_filter=scenario.unsat_begin,
                end_filter=scenario.unsat_end,
                personinfo=person_path,
                progress_cb=_trip_progress if metrics_trace else None,
            )
            if metrics_trace:
                trip_elapsed = time.time() - trip_start
                trip_size = trip_path.stat().st_size if trip_path.exists() else 0
                _debug_log(
                    artifacts.sumo_log,
                    (
                        f"[metrics-trace] tripinfo done count={last_trip_progress['count']} "
                        f"elapsed={trip_elapsed:.2f}s size_bytes={trip_size}"
                    ),
                )

        queue_start = time.time()
        if compute_queue_metrics:
            queue_metrics = live_waiting_metrics or parse_waiting_ratio(
                summary_path or artifacts.summary,
                config=queue_config,
                progress_cb=(
                    (lambda msg: _debug_log(artifacts.sumo_log, msg)) if metrics_trace else None
                ),
            )
        if scenario.sat_seconds > 0 and (summary_path or artifacts.summary).exists():
            waiting_p95_sat = parse_waiting_percentile(
                summary_path or artifacts.summary,
                begin=scenario.sat_begin,
                end=scenario.sim_end,
                progress_cb=(
                    (lambda msg: _debug_log(artifacts.sumo_log, msg)) if metrics_trace else None
                ),
            )
        if metrics_trace and compute_queue_metrics:
            queue_elapsed = time.time() - queue_start
            summary_size = (
                (summary_path or artifacts.summary).stat().st_size
                if (summary_path or artifacts.summary).exists()
                else 0
            )
            _debug_log(
                artifacts.sumo_log,
                (
                    f"[metrics-trace] waiting_ratio done elapsed={queue_elapsed:.2f}s "
                    f"first_over_saturation_time={queue_metrics.first_failure_time} "
                    f"max_ratio={queue_metrics.max_queue_length} "
                    f"size_bytes={summary_size}"
                ),
            )
    _mark_end(metrics_timing)
    phase_label = phase.name if hasattr(phase, "name") else str(phase)
    if compute_queue_metrics:
        queue_status = "over_saturation_detected" if not queue_metrics.is_durable else "durable"
        queue_reason = ""
        if not queue_metrics.is_durable:
            queue_reason = (
                "; over saturation detected because "
                f"{_format_over_saturation_reason(queue_metrics.threshold_length, queue_metrics.threshold_steps)}"
            )
        _debug_log(
            artifacts.sumo_log,
            (
                f"[scale-run] phase={phase_label} scale={scale:.2f} sumo_scale={applied_scale:.2f} "
                f"aborted={aborted} queue_status={queue_status} "
                f"first_over_saturation_time={queue_metrics.first_failure_time} "
                f"max_ratio={queue_metrics.max_queue_length}"
                f"{queue_reason}"
            ),
        )
    else:
        _debug_log(
            artifacts.sumo_log,
            (
                f"[scale-run] phase={phase_label} scale={scale:.2f} sumo_scale={applied_scale:.2f} "
                "queue_metrics=skipped (probe disabled)"
            ),
        )
    return tripinfo_metrics, queue_metrics, waiting_p95_sat


def _probe_scale_durability(
    base_artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    output_format: OutputFormat,
    affinity_cpu: int | None,
    queue_cache: Dict[float, QueueDurabilityMetrics],
    attempts: int,
    status_queue=None,
    worker_id: int | None = None,
    use_pty: bool = False,
    metrics_trace: bool = False,
    output_root: Path | None = None,
    scale_mode: ScaleMode = ScaleMode.SUMO,
    base_network: Path | None = None,
    timings: RunTimings | None = None,
) -> ScaleProbeResult:
    raise NotImplementedError("Scale probe is not supported with multi-phase scaling.")
    fine_step = scale_probe.fine_step if scale_probe.fine_step > 0 else 0.1
    coarse_step = scale_probe.coarse_step if scale_probe.coarse_step > 0 else fine_step
    coarse_step = max(coarse_step, fine_step)
    start_scale = _normalize_scale(max(scale_probe.start, coarse_step), fine_step)
    ceiling = max(scale_probe.ceiling, start_scale)
    probe_tag = f"[scale-probe scenario={scenario.scenario_id} seed={scenario.seed}]"
    log_path = base_artifacts.sumo_log
    over_saturation_reason_text = _format_over_saturation_reason(
        queue_config.length_threshold, queue_config.step_window
    )
    # Probe progression is now tied to an integer step grid to avoid float drift stalls.
    step_resolution = fine_step if fine_step > 0 else 0.1
    coarse_step = max(coarse_step, step_resolution)
    coarse_step_ticks = max(1, int(round(coarse_step / step_resolution)))
    current_tick = int(round(start_scale / step_resolution))
    ceiling_tick = int(round(ceiling / step_resolution))
    _debug_log(
        log_path,
        (
            f"{probe_tag} start={start_scale:.2f} "
            f"ceiling={ceiling:.2f} fine_step={fine_step:.3f} "
            f"coarse_step={coarse_step:.3f}"
        ),
    )

    def run_scale(raw_scale: float) -> QueueDurabilityMetrics:
        nonlocal attempts
        scale_value = _normalize_scale(raw_scale, fine_step)
        if scale_value in queue_cache:
            cached_metrics = queue_cache[scale_value]
            cached_status = (
                "over_saturation_detected" if not cached_metrics.is_durable else "durable"
            )
            cached_reason = (
                f"; over saturation detected because {over_saturation_reason_text}"
                if not cached_metrics.is_durable
                else ""
            )
            _debug_log(
                log_path,
                (
                    f"{probe_tag} reuse scale={scale_value:.2f} "
                    f"status={cached_status} "
                    f"first_over_saturation_time={cached_metrics.first_failure_time} "
                    f"max_queue_length={cached_metrics.max_queue_length}"
                    f"{cached_reason}"
                ),
            )
            return queue_cache[scale_value]

        run_label = _format_scale_label(scale_value)
        if scale_mode == ScaleMode.VEH_ONLY:
            target_root = output_root or base_artifacts.outdir.parent
            task = BuildTask.DEMAND if base_network is not None else BuildTask.ALL
            options = _build_options_for_scenario(
                scenario,
                output_root=target_root,
                vehicle_flow_scale=scale_value,
                run_label=run_label,
                network_input=base_network,
            )
            build_result = build_and_persist(scenario.spec, options, task=task)
            artifacts = _collect_artifacts(
                build_result,
                scenario=scenario,
                label=run_label,
                output_format=output_format,
            )
            applied_sumo_scale = 1.0
        else:
            artifacts = _artifacts_for_label(base_artifacts, run_label)
            applied_sumo_scale = scale_value

        _send_status(
            status_queue,
            worker_id=worker_id or 0,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scale_value,
            affinity_cpu=affinity_cpu,
            phase=WorkerPhase.PROBE,
            label="probe-sumo",
            step=scale_value,
        )

        _, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            queue_config=queue_config,
            output_format=output_format,
            scale=scale_value,
            sumo_scale=applied_sumo_scale,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=False,
            status_queue=status_queue,
            worker_id=worker_id,
            phase=WorkerPhase.PROBE,
            use_pty=use_pty,
            enable_waiting_abort=scale_probe.abort_on_waiting,
            metrics_trace=metrics_trace,
            metrics_phase=WorkerPhase.PROBE,
            metrics_label="probe-post",
            compute_queue_metrics=True,
        )
        queue_cache[scale_value] = queue_metrics
        attempt_no = attempts + 1
        attempts = attempt_no
        run_status = "over_saturation_detected" if not queue_metrics.is_durable else "durable"
        run_reason = (
            f"; over saturation detected because {over_saturation_reason_text}"
            if not queue_metrics.is_durable
            else ""
        )
        _debug_log(
            log_path,
            (
                f"{probe_tag} run#{attempt_no} scale={scale_value:.2f} "
                f"status={run_status} "
                f"first_over_saturation_time={queue_metrics.first_failure_time} "
                f"max_queue_length={queue_metrics.max_queue_length}"
                f"{run_reason}"
            ),
        )
        return queue_metrics

    last_durable: float | None = None
    while current_tick <= ceiling_tick:
        current = _normalize_scale(current_tick * step_resolution, step_resolution)
        metrics = run_scale(current)
        coarse_status = (
            "over_saturation_detected" if not metrics.is_durable else "durable"
        )
        coarse_reason = (
            f"; over saturation detected because {over_saturation_reason_text}"
            if not metrics.is_durable
            else ""
        )
        _debug_log(
            log_path,
            (
                f"{probe_tag} coarse scale={current:.2f} "
                f"status={coarse_status} "
                f"first_over_saturation_time={metrics.first_failure_time} "
                f"max_queue_length={metrics.max_queue_length}"
                f"{coarse_reason}"
            ),
        )
        if metrics.is_durable:
            last_durable = current
            current_tick += coarse_step_ticks
            continue

        # Enter fine scan between last_durable (or start) and current (first coarse over-saturation).
        fine_start_base = _normalize_scale(max(scale_probe.start, fine_step), fine_step)
        fine_start = (
            _normalize_scale(last_durable + fine_step, fine_step)
            if last_durable is not None
            else fine_start_base
        )
        fine_current = fine_start
        last_fine_durable = last_durable
        while fine_current <= current:
            fine_metrics = run_scale(fine_current)
            fine_status = (
                "over_saturation_detected" if not fine_metrics.is_durable else "durable"
            )
            fine_reason = (
                f"; over saturation detected because {over_saturation_reason_text}"
                if not fine_metrics.is_durable
                else ""
            )
            _debug_log(
                log_path,
                (
                    f"{probe_tag} fine scale={fine_current:.2f} "
                    f"status={fine_status} "
                    f"first_over_saturation_time={fine_metrics.first_failure_time} "
                    f"max_queue_length={fine_metrics.max_queue_length}"
                    f"{fine_reason}"
                ),
            )
            if fine_metrics.is_durable:
                last_fine_durable = fine_current
                fine_current = _normalize_scale(fine_current + fine_step, fine_step)
                continue

            # First fine over-saturation hit: return with previous durable scale.
            return ScaleProbeResult(
                enabled=True,
                max_durable_scale=last_fine_durable,
                attempts=attempts,
            )

        # Fine scan never hit over-saturation; treat current coarse as over-saturation bound.
        return ScaleProbeResult(
            enabled=True,
            max_durable_scale=last_fine_durable,
            attempts=attempts,
        )

    _debug_log(
        log_path,
        (
            f"{probe_tag} no over saturation detected up to ceiling={ceiling:.2f}; "
            f"max durable={last_durable if last_durable is not None else ceiling}"
        ),
    )
    return ScaleProbeResult(
        enabled=True,
        max_durable_scale=last_durable if last_durable is not None else ceiling,
        attempts=attempts,
    )


def _set_affinity_preexec(cpu: int | None):
    # Windows does not support preexec_fn; skip affinity binding there.
    if cpu is None or os.name == "nt":
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
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    output_format: OutputFormat,
    affinity_cpu: int | None,
    worker_id: int,
    status_queue,
    use_pty: bool,
    metrics_trace: bool = False,
) -> ScenarioResult | None:
    if scale_probe.enabled:
        raise ValueError("Scale probing is not supported with multi-phase scaling; please disable it.")

    base_run_id, base_root_dir, base_run_dir, _, _ = _run_layout(
        scenario, output_root=output_root, run_label="base"
    )
    base_queue_metrics = QueueDurabilityMetrics(
        threshold_steps=queue_config.step_window,
        threshold_length=queue_config.length_threshold,
    )
    probe_result = ScaleProbeResult(
        enabled=False,
        max_durable_scale=None,
        attempts=0,
    )
    timings = RunTimings()
    scenario_worker = worker_id

    try:
        _mark_start(timings.build)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.veh_unsat_scale,
            affinity_cpu=affinity_cpu,
            phase=WorkerPhase.BUILD,
            label="build",
        )
        options = _build_options_for_scenario(
            scenario,
            output_root=output_root,
        )
        build_result = build_and_persist(scenario.spec, options, task=BuildTask.ALL)
        artifacts = _collect_artifacts(
            build_result,
            scenario=scenario,
            label="base",
            output_format=output_format,
        )
        _mark_end(timings.build)
    except Exception as exc:  # noqa: BLE001
        _mark_end(timings.build)
        errors = [
            f"build failed: {exc}",
            f"log dir: {base_run_dir}",
        ]
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            scenario_base_id=scenario.scenario_base_id,
            seed=scenario.seed,
            warmup_seconds=scenario.warmup_seconds,
            unsat_seconds=scenario.unsat_seconds,
            sat_seconds=scenario.sat_seconds,
            ped_unsat_scale=scenario.ped_unsat_scale,
            ped_sat_scale=scenario.ped_sat_scale,
            veh_unsat_scale=scenario.veh_unsat_scale,
            veh_sat_scale=scenario.veh_sat_scale,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            queue=base_queue_metrics,
            scale_probe=probe_result,
            waiting_p95_sat=None,
            fcd_note="build failed",
            error="; ".join(errors),
            error_messages=errors,
            worker_id=scenario_worker,
            timings=timings,
        )

    try:
        _mark_start(timings.sumo)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.veh_unsat_scale,
            affinity_cpu=affinity_cpu,
            phase=WorkerPhase.SUMO,
            label="sumo",
        )
        tripinfo_metrics, queue_metrics, waiting_p95_sat = _run_for_scale(
            artifacts,
            scenario,
            queue_config=queue_config,
            output_format=output_format,
            scale=scenario.veh_unsat_scale,
            sumo_scale=1.0,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=True,
            status_queue=status_queue,
            worker_id=worker_id,
            phase=WorkerPhase.SUMO,
            use_pty=use_pty,
            metrics_trace=metrics_trace,
            sumo_timing=timings.sumo,
            metrics_timing=timings.metrics,
            metrics_phase=WorkerPhase.PARSE,
            metrics_label="post",
            compute_queue_metrics=scale_probe.enabled,
        )
        if timings.sumo.end is None:
            _mark_end(timings.sumo)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        _mark_end(timings.sumo)
        errors = [
            f"sumo failed: {getattr(exc, 'stderr', None) or str(exc)}",
            f"log: {artifacts.sumo_log}",
        ]
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            scenario_base_id=scenario.scenario_base_id,
            seed=scenario.seed,
            warmup_seconds=scenario.warmup_seconds,
            unsat_seconds=scenario.unsat_seconds,
            sat_seconds=scenario.sat_seconds,
            ped_unsat_scale=scenario.ped_unsat_scale,
            ped_sat_scale=scenario.ped_sat_scale,
            veh_unsat_scale=scenario.veh_unsat_scale,
            veh_sat_scale=scenario.veh_sat_scale,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            queue=base_queue_metrics,
            scale_probe=probe_result,
            waiting_p95_sat=None,
            fcd_note="sumo failed",
            error="; ".join(errors),
            error_messages=errors,
            worker_id=scenario_worker,
            timings=timings,
        )

    result = ScenarioResult(
        scenario_id=scenario.scenario_id,
        scenario_base_id=scenario.scenario_base_id,
        seed=scenario.seed,
        warmup_seconds=scenario.warmup_seconds,
        unsat_seconds=scenario.unsat_seconds,
        sat_seconds=scenario.sat_seconds,
        ped_unsat_scale=scenario.ped_unsat_scale,
        ped_sat_scale=scenario.ped_sat_scale,
        veh_unsat_scale=scenario.veh_unsat_scale,
        veh_sat_scale=scenario.veh_sat_scale,
        demand_dir=scenario.demand_dir,
        tripinfo=tripinfo_metrics,
        queue=queue_metrics,
        scale_probe=probe_result,
        waiting_p95_sat=waiting_p95_sat,
        fcd_note="n/a",
        error=None,
        worker_id=scenario_worker,
        timings=timings,
    )
    _send_status(
        status_queue,
        worker_id=worker_id,
        scenario_id=scenario.scenario_id,
        seed=scenario.seed,
        scale=scenario.veh_unsat_scale,
        affinity_cpu=affinity_cpu,
        phase=WorkerPhase.DONE,
        label="done",
        done=True,
        completed=True,
    )
    return result


def _affinity_plan(count: int) -> List[int | None]:
    cpu_total = os.cpu_count() or 1
    return [(idx % cpu_total) for idx in range(count)]


def run_batch(
    scenarios: Iterable[ScenarioConfig],
    *,
    output_root: Path,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    results_csv: Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_pty: bool = False,
    metrics_trace: bool = False,
    output_format: OutputFormat = OutputFormat(),
) -> None:
    scenario_list = list(scenarios)
    scenario_order = {sc.scenario_id: idx for idx, sc in enumerate(scenario_list)}
    if not scenario_list:
        return

    workers = min(max_workers, len(scenario_list))
    affinity = _affinity_plan(workers)
    results: List[ScenarioResult] = []
    manager = multiprocessing.Manager()
    status_queue = manager.Queue()
    stop_event = threading.Event()

    for idx, scenario in enumerate(scenario_list):
        _send_status(
            status_queue,
            worker_id=idx % workers,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.veh_unsat_scale,
            affinity_cpu=affinity[idx % workers],
            phase=WorkerPhase.IDLE,
            label="queued",
        )

    render_thread = threading.Thread(
        target=_render_loop,
        args=(
            status_queue,
            stop_event,
            len(scenario_list),
            workers,
            output_root,
            (output_root / "batchrun.log") if metrics_trace else None,
        ),
        daemon=True,
    )
    render_thread.start()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for idx, scenario in enumerate(scenario_list):
            worker_slot = idx % workers
            fut = pool.submit(
                run_scenario,
                scenario,
                output_root=output_root,
                queue_config=queue_config,
                scale_probe=scale_probe,
                output_format=output_format,
                affinity_cpu=affinity[worker_slot],
                worker_id=worker_slot,
                status_queue=status_queue,
                use_pty=use_pty,
                metrics_trace=metrics_trace,
            )
            futures[fut] = worker_slot

        for future in as_completed(futures):
            worker_slot = futures[future]
            result = future.result()
            if result is None:
                continue
            if result.error is not None:
                print(
                    f"[skip] scenario={result.scenario_id} seed={result.seed} error={result.error}"
                )
                _send_status(
                    status_queue,
                    worker_id=worker_slot,
                    scenario_id=result.scenario_id,
                    seed=result.seed,
                    scale=result.veh_unsat_scale,
                    affinity_cpu=affinity[worker_slot],
                    phase=WorkerPhase.ERROR,
                    label="error",
                    error=result.error,
                    done=True,
                    completed=True,
                )
                continue
            results.append(result)

    if results:
        results_sorted = sorted(
            results,
            key=lambda r: scenario_order.get(r.scenario_id, len(scenario_order)),
        )
        _append_results(results_csv, results_sorted)
    stop_event.set()
    render_thread.join(timeout=1.0)
    manager.shutdown()


def _append_results(path: Path, results: Sequence[ScenarioResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not path.exists()
    probe_enabled = any(r.scale_probe.enabled for r in results)
    columns = RESULT_COLUMNS_PROBE if probe_enabled else RESULT_COLUMNS_NO_PROBE
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        if header_needed:
            writer.writeheader()
        for result in results:
            row = _result_to_row(result, include_probe_columns=probe_enabled)
            writer.writerow(row)


def _format_error_field(result: ScenarioResult) -> str:
    errors: List[str] = []
    errors.extend([msg for msg in result.error_messages if msg])
    if result.error and result.error not in errors:
        errors.append(result.error)
    return "; ".join(errors)


def _compress_artifacts(artifacts: RunArtifacts, *, level: int, log_path: Path | None) -> None:
    try:
        import zstandard as zstd
    except ImportError:
        message = (
            "[compress] zstandard not installed; install with "
            "`pip install zstandard` to enable zst compression"
        )
        _debug_log(log_path, message)
        raise RuntimeError(message)

    targets = [
        artifacts.tripinfo,
        artifacts.personinfo,
        artifacts.summary,
        artifacts.person_summary,
        artifacts.detector,
        artifacts.queue,
        artifacts.fcd,
    ]
    cctx = zstd.ZstdCompressor(level=level, threads=1)

    for src in targets:
        if not src.exists():
            continue
        dst = src.with_suffix(src.suffix + ".zst")
        try:
            with src.open("rb") as rfp, dst.open("wb") as wfp:
                wfp.write(cctx.compress(rfp.read()))
            src.unlink(missing_ok=True)
            _debug_log(
                log_path,
                f"[compress] {src.name} -> {dst.name} level={level} bytes={dst.stat().st_size}",
            )
        except Exception as exc:  # noqa: BLE001
            _debug_log(log_path, f"[compress] failed {src.name}: {exc}")


def _result_to_row(result: ScenarioResult, *, include_probe_columns: bool) -> dict:
    build_start, build_end = _timing_to_strings(result.timings.build)
    sumo_start, sumo_end = _timing_to_strings(result.timings.sumo)
    metrics_start, metrics_end = _timing_to_strings(result.timings.metrics)
    probe_start, probe_end = _timing_to_strings(result.timings.probe)
    row = {
        "scenario_id": result.scenario_id,
        "scenario_base_id": result.scenario_base_id,
        "seed": result.seed,
        "warmup_seconds": _fmt(result.warmup_seconds),
        "unsat_seconds": _fmt(result.unsat_seconds),
        "sat_seconds": _fmt(result.sat_seconds),
        "ped_unsat_scale": _fmt(result.ped_unsat_scale),
        "ped_sat_scale": _fmt(result.ped_sat_scale),
        "veh_unsat_scale": _fmt(result.veh_unsat_scale),
        "veh_sat_scale": _fmt(result.veh_sat_scale),
        "demand_dir": str(result.demand_dir),
        "vehicle_count": result.tripinfo.vehicle_count,
        "person_count": result.tripinfo.person_count,
        "vehicle_mean_timeLoss": _fmt(result.tripinfo.vehicle_mean_time_loss),
        "person_mean_timeLoss": _fmt(result.tripinfo.person_mean_time_loss),
        "person_mean_routeLength": _fmt(result.tripinfo.person_mean_route_length),
        "waiting_p95_sat": _fmt(result.waiting_p95_sat),
        "worker_id": result.worker_id if result.worker_id is not None else "",
        "build_start": build_start,
        "build_end": build_end,
        "sumo_start": sumo_start,
        "sumo_end": sumo_end,
        "metrics_start": metrics_start,
        "metrics_end": metrics_end,
        "error": _format_error_field(result),
    }
    if include_probe_columns:
        if result.scale_probe.enabled:
            row.update(
                {
                    "queue_threshold_steps": result.queue.threshold_steps,
                    "queue_threshold_length": _fmt(result.queue.threshold_length),
                    "queue_first_over_saturation_time": _fmt(result.queue.first_failure_time),
                    "queue_is_durable": "True" if result.queue.is_durable else "False",
                    "scale_probe_enabled": str(result.scale_probe.enabled),
                    "scale_probe_max_durable_scale": _fmt(result.scale_probe.max_durable_scale),
                    "scale_probe_attempts": result.scale_probe.attempts,
                    "probe_start": probe_start,
                    "probe_end": probe_end,
                }
            )
        else:
            for key in QUEUE_PROBE_COLUMNS:
                row[key] = ""
    return row


def _fmt(value) -> str | float:
    if value is None:
        return ""
    return round(value, 3) if isinstance(value, float) else value


def _timing_to_strings(timing: PhaseTiming | None) -> tuple[str, str]:
    if timing is None:
        return "", ""
    return _format_timestamp(timing.start), _format_timestamp(timing.end)
