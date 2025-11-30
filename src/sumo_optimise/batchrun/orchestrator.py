from __future__ import annotations

import csv
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

from .models import (
    DEFAULT_BEGIN_FILTER,
    DEFAULT_END_TIME,
    DEFAULT_MAX_WORKERS,
    PhaseTiming,
    DemandFiles,
    QueueDurabilityConfig,
    QueueDurabilityMetrics,
    RunArtifacts,
    RunTimings,
    ScaleProbeConfig,
    ScaleProbeResult,
    ScenarioConfig,
    ScenarioResult,
    TripinfoMetrics,
    CompressionConfig,
    ScaleMode,
    WorkerPhase,
    WorkerStatus,
)
# CSV output layout (grouped by scenario inputs → trip stats → queue durability → probe metadata → notes).
# queue_first_over_saturation_time: first timestep where waiting/running ratio stayed above
# queue_threshold_length for at least queue_threshold_steps consecutive seconds; blank means durable.
from .parsers import parse_tripinfo, parse_waiting_ratio


RESULT_COLUMNS = [
    "scenario_id",
    "scenario_base_id",
    "seed",
    "scale",
    "begin_filter",
    "end_time",
    "demand_dir",
    "vehicle_count",
    "person_count",
    "vehicle_mean_timeLoss",
    "person_mean_timeLoss",
    "person_mean_routeLength",
    "queue_threshold_steps",
    "queue_threshold_length",
    "queue_first_over_saturation_time",
    "queue_is_durable",
    "scale_probe_enabled",
    "scale_probe_max_durable_scale",
    "scale_probe_attempts",
    "fcd_note",
    "error_note",
    "worker_id",
    "build_start",
    "build_end",
    "sumo_start",
    "sumo_end",
    "compress_start",
    "compress_end",
    "probe_start",
    "probe_end",
    "probe_compress_start",
    "probe_compress_end",
]


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


def _send_status(
    queue,
    *,
    worker_id: int,
    scenario_id: str = "",
    seed: int = 0,
    scale: float | None = None,
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
    cols = max(1, term_width // 40)
    rows: List[str] = []
    ordered = [statuses[k] for k in sorted(statuses.keys())]
    for idx, status in enumerate(ordered):
        scenario = status.scenario_id or "-"
        phase = status.phase
        scale_str = f"{status.probe_scale or status.scale or 0:.2f}"
        label_text = status.label
        if not label_text and status.step is not None:
            label_text = f"sumo#{int(status.step)}"
        label = (label_text or phase.value)[:10]
        cell = f"[{status.worker_id:02d}] {label:<10} {scenario:<12} (s {scale_str:>4})"
        cell = cell[:40].ljust(40)
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
                            f"step={status.step} scale={status.scale} probe_scale={status.probe_scale} "
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

def load_manifest(path: Path, *, default_scale_mode: ScaleMode = ScaleMode.SUMO) -> List[ScenarioConfig]:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    suffix = manifest_path.suffix.lower()
    if suffix == ".json":
        return _load_manifest_json(manifest_path, default_scale_mode=default_scale_mode)
    if suffix == ".csv":
        return _load_manifest_csv(manifest_path, default_scale_mode=default_scale_mode)
    raise ValueError("Manifest must be .json or .csv")


def _load_manifest_json(path: Path, *, default_scale_mode: ScaleMode) -> List[ScenarioConfig]:
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, list):
        raise ValueError("JSON manifest must be a list of scenario entries")
    base = path.parent
    scenarios: List[ScenarioConfig] = []
    for entry in data:
        scenarios.extend(_row_to_config(entry, base, default_scale_mode=default_scale_mode))
    return scenarios


def _load_manifest_csv(path: Path, *, default_scale_mode: ScaleMode) -> List[ScenarioConfig]:
    scenarios: List[ScenarioConfig] = []
    base = path.parent
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if not any(row.values()):
                continue
            scenarios.extend(_row_to_config(row, base, default_scale_mode=default_scale_mode))
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


def _parse_scale_mode(raw_mode: object | None, default: ScaleMode) -> ScaleMode:
    if raw_mode is None or raw_mode == "":
        return default
    token = str(raw_mode).strip().lower()
    try:
        return ScaleMode(token)
    except ValueError as err:
        raise ValueError(f"Invalid scale_mode: {raw_mode!r}") from err


def _row_to_config(raw: dict, base: Path, *, default_scale_mode: ScaleMode) -> List[ScenarioConfig]:
    try:
        spec = Path(raw["spec"])
        scenario_id = str(raw["scenario_id"])
        seeds = _parse_seed_field(raw["seed"])
        demand_dir = Path(raw["demand_dir"])
        scale = float(raw["scale"])
        scale_mode = _parse_scale_mode(raw.get("scale_mode"), default_scale_mode)
    except KeyError as err:
        raise ValueError(f"Missing manifest field: {err.args[0]}") from err
    if not scenario_id:
        raise ValueError("scenario_id cannot be empty")
    begin_filter = float(raw.get("begin_filter", DEFAULT_BEGIN_FILTER))
    end_time = float(raw.get("end_time", DEFAULT_END_TIME))
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
                scale=scale,
                scale_mode=scale_mode,
                begin_filter=begin_filter,
                end_time=end_time,
            )
        )
    return scenarios


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
    vehicle_flow_scale: float | None = None,
    run_label: str | None = None,
    network_input: Path | None = None,
) -> BuildOptions:
    output_template = OutputDirectoryTemplate(
        root=str(output_root / scenario.scenario_id),
        run=run_label or "{seq:03}",
    )
    output_files = OutputFileTemplates()
    demand_files = resolve_demand_files(scenario.demand_dir)
    demand_options = DemandOptions(
        ped_endpoint_csv=demand_files.ped_endpoint,
        ped_junction_turn_weight_csv=demand_files.ped_junction,
        veh_endpoint_csv=demand_files.veh_endpoint,
        veh_junction_turn_weight_csv=demand_files.veh_junction,
        simulation_end_time=scenario.end_time,
        vehicle_flow_scale=vehicle_flow_scale if vehicle_flow_scale is not None else 1.0,
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
    )


def _collect_artifacts(result, *, scenario_id: str) -> RunArtifacts:
    if result.manifest_path is None:
        raise ValueError("manifest path not recorded by build")
    outdir = result.manifest_path.parent
    sumocfg = result.sumocfg_path or (outdir / "config.sumocfg")
    files = OutputFileTemplates()
    network_path = outdir / files.network
    safe_id = _safe_id_for_filename(scenario_id)
    return RunArtifacts(
        outdir=outdir,
        sumocfg=sumocfg,
        network=network_path,
        tripinfo=outdir / f"tripinfo_{safe_id}.xml",
        personinfo=outdir / f"personinfo_{safe_id}.xml",
        fcd=outdir / f"fcd_{safe_id}.xml",
        summary=outdir / f"summary_{safe_id}.xml",
        person_summary=outdir / f"summary.person_{safe_id}.xml",
        detector=outdir / f"detector_{safe_id}.xml",
        queue=outdir / f"queue_{safe_id}.xml",
        sumo_log=outdir / f"sumo_{safe_id}.log",
    )


def _artifacts_for_label(base: RunArtifacts, label: str | None) -> RunArtifacts:
    if not label:
        return base

    outdir = base.outdir / label
    return RunArtifacts(
        outdir=outdir,
        sumocfg=base.sumocfg,
        network=base.network,
        tripinfo=outdir / base.tripinfo.name,
        personinfo=outdir / base.personinfo.name,
        fcd=outdir / base.fcd.name,
        summary=outdir / base.summary.name,
        person_summary=outdir / base.person_summary.name,
        detector=outdir / base.detector.name,
        queue=outdir / base.queue.name,
        sumo_log=outdir / base.sumo_log.name,
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
        "--personinfo-output",
        str(artifacts.personinfo),
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
        str(scale),
    ]
    return cmd


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
    phase_timing: PhaseTiming | None = None,
) -> tuple[TripinfoMetrics, QueueDurabilityMetrics]:
    artifacts.tripinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.personinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.queue.parent.mkdir(parents=True, exist_ok=True)
    artifacts.sumo_log.parent.mkdir(parents=True, exist_ok=True)

    applied_scale = sumo_scale if sumo_scale is not None else scale
    cmd = _sumo_command(artifacts, scenario, scale=applied_scale)
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
            waiting_config=queue_config,
            enable_waiting_abort=enable_waiting_abort,
        )

    _mark_end(phase_timing)
    _send_status(
        status_queue,
        worker_id=worker_id or 0,
        scenario_id=scenario.scenario_id,
        seed=scenario.seed,
        scale=scale,
        phase=WorkerPhase.PARSE,
        label="metrics",
    )

    tripinfo_metrics = TripinfoMetrics()
    if collect_tripinfo:
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
            artifacts.tripinfo,
            begin_filter=scenario.begin_filter,
            personinfo=artifacts.personinfo,
            progress_cb=_trip_progress if metrics_trace else None,
        )
        if metrics_trace:
            trip_elapsed = time.time() - trip_start
            _debug_log(
                artifacts.sumo_log,
                (
                    f"[metrics-trace] tripinfo done count={last_trip_progress['count']} "
                    f"elapsed={trip_elapsed:.2f}s size_bytes={artifacts.tripinfo.stat().st_size if artifacts.tripinfo.exists() else 0}"
                ),
            )

    queue_start = time.time()
    queue_metrics = live_waiting_metrics or parse_waiting_ratio(
        artifacts.summary,
        config=queue_config,
        progress_cb=(
            (lambda msg: _debug_log(artifacts.sumo_log, msg)) if metrics_trace else None
        ),
    )
    if metrics_trace:
        queue_elapsed = time.time() - queue_start
        _debug_log(
            artifacts.sumo_log,
            (
                f"[metrics-trace] waiting_ratio done elapsed={queue_elapsed:.2f}s "
                f"first_over_saturation_time={queue_metrics.first_failure_time} "
                f"max_ratio={queue_metrics.max_queue_length} "
                f"size_bytes={artifacts.summary.stat().st_size if artifacts.summary.exists() else 0}"
            ),
    )
    phase_label = phase.name if hasattr(phase, "name") else str(phase)
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
    return tripinfo_metrics, queue_metrics


def _probe_scale_durability(
    base_artifacts: RunArtifacts,
    scenario: ScenarioConfig,
    *,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    compression: CompressionConfig,
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
    fine_step = scale_probe.fine_step if scale_probe.fine_step > 0 else 0.1
    coarse_step = scale_probe.coarse_step if scale_probe.coarse_step > 0 else fine_step
    coarse_step = max(coarse_step, fine_step)
    start_scale = _normalize_scale(max(scale_probe.start, coarse_step), fine_step)
    ceiling = max(scale_probe.ceiling, start_scale)
    probe_tag = f"[scale-probe scenario={scenario.scenario_id} seed={scenario.seed}]"
    log_path = base_artifacts.sumo_log
    _mark_start(timings.probe if timings is not None else None)
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
            artifacts = _collect_artifacts(build_result, scenario_id=scenario.scenario_id)
            applied_sumo_scale = 1.0
        else:
            artifacts = _artifacts_for_label(base_artifacts, run_label)
            applied_sumo_scale = scale_value

        _, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            queue_config=queue_config,
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
        )
        if compression.enabled:
            _mark_start(timings.probe_compress if timings is not None else None)
            _compress_artifacts(
                artifacts,
                level=compression.zstd_level,
                log_path=artifacts.sumo_log,
            )
            _mark_end(timings.probe_compress if timings is not None else None)
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
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    compression: CompressionConfig,
    affinity_cpu: int | None,
    worker_id: int,
    status_queue,
    use_pty: bool,
    metrics_trace: bool = False,
) -> ScenarioResult | None:
    base_queue_metrics = QueueDurabilityMetrics(
        threshold_steps=queue_config.step_window,
        threshold_length=queue_config.length_threshold,
    )
    probe_result = ScaleProbeResult(
        enabled=scale_probe.enabled,
        max_durable_scale=None,
        attempts=0,
    )
    vehicle_flow_scale = scenario.scale if scenario.scale_mode == ScaleMode.VEH_ONLY else None
    timings = RunTimings()
    scenario_worker = worker_id

    try:
        _mark_start(timings.build)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.BUILD,
            label="build",
        )
        options = _build_options_for_scenario(
            scenario,
            output_root=output_root,
            vehicle_flow_scale=vehicle_flow_scale,
        )
        build_result = build_and_persist(scenario.spec, options, task=BuildTask.ALL)
        artifacts = _collect_artifacts(build_result, scenario_id=scenario.scenario_id)
        _mark_end(timings.build)
    except Exception as exc:  # noqa: BLE001
        _mark_end(timings.build)
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            scenario_base_id=scenario.scenario_base_id,
            seed=scenario.seed,
            scale=scenario.scale,
            begin_filter=scenario.begin_filter,
            end_time=scenario.end_time,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            queue=base_queue_metrics,
            scale_probe=probe_result,
            fcd_note="build failed",
            error=str(exc),
            worker_id=scenario_worker,
            timings=timings,
        )

    try:
        sumo_scale = 1.0 if scenario.scale_mode == ScaleMode.VEH_ONLY else scenario.scale
        _mark_start(timings.sumo)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.SUMO,
            label="sumo",
        )
        tripinfo_metrics, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            queue_config=queue_config,
            scale=scenario.scale,
            sumo_scale=sumo_scale,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=True,
            status_queue=status_queue,
            worker_id=worker_id,
            phase=WorkerPhase.SUMO,
            use_pty=use_pty,
            metrics_trace=metrics_trace,
            phase_timing=timings.sumo,
        )
        if timings.sumo.end is None:
            _mark_end(timings.sumo)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        _mark_end(timings.sumo)
        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            scenario_base_id=scenario.scenario_base_id,
            seed=scenario.seed,
            scale=scenario.scale,
            begin_filter=scenario.begin_filter,
            end_time=scenario.end_time,
            demand_dir=scenario.demand_dir,
            tripinfo=TripinfoMetrics(),
            queue=base_queue_metrics,
            scale_probe=ScaleProbeResult(
                enabled=scale_probe.enabled,
                max_durable_scale=None,
                attempts=1,
            ),
            fcd_note="sumo failed",
            error=getattr(exc, "stderr", None) or str(exc),
            worker_id=scenario_worker,
            timings=timings,
        )

    fine_step = scale_probe.fine_step if scale_probe.fine_step > 0 else 0.1
    base_scale_key = _normalize_scale(scenario.scale, fine_step)
    queue_cache: Dict[float, QueueDurabilityMetrics] = {base_scale_key: queue_metrics}
    probe_result = ScaleProbeResult(
        enabled=scale_probe.enabled,
        max_durable_scale=None,
        attempts=1,
    )
    if compression.enabled:
        _mark_start(timings.compress)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.PARSE,
            label="compressing",
        )
        _compress_artifacts(
            artifacts,
            level=compression.zstd_level,
            log_path=artifacts.sumo_log,
        )
        _mark_end(timings.compress)
    if scale_probe.enabled:
        _mark_start(timings.probe)
        probe_result = _probe_scale_durability(
            artifacts,
            scenario,
            queue_config=queue_config,
            scale_probe=scale_probe,
            compression=compression,
            affinity_cpu=affinity_cpu,
            queue_cache=queue_cache,
            attempts=1,
            status_queue=status_queue,
            worker_id=worker_id,
            use_pty=use_pty,
            metrics_trace=metrics_trace,
            output_root=output_root,
            scale_mode=scenario.scale_mode,
            base_network=artifacts.network,
            timings=timings,
        )
        _mark_end(timings.probe)

    if compression.enabled:
        target_timing = timings.probe_compress if scale_probe.enabled else timings.compress
        _mark_start(target_timing)
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.PARSE,
            label="compressing",
        )
        _compress_artifacts(
            artifacts,
            level=compression.zstd_level,
            log_path=artifacts.sumo_log,
        )
        _mark_end(target_timing)

    result = ScenarioResult(
        scenario_id=scenario.scenario_id,
        scenario_base_id=scenario.scenario_base_id,
        seed=scenario.seed,
        scale=scenario.scale,
        begin_filter=scenario.begin_filter,
        end_time=scenario.end_time,
        demand_dir=scenario.demand_dir,
        tripinfo=tripinfo_metrics,
        queue=queue_metrics,
        scale_probe=probe_result,
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
        scale=scenario.scale,
        phase=WorkerPhase.DONE,
        label="done",
        done=True,
        completed=True,
    )
    return result


def _affinity_plan(count: int) -> List[int | None]:
    return [None] * count


def run_batch(
    scenarios: Iterable[ScenarioConfig],
    *,
    output_root: Path,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    results_csv: Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_pty: bool | None = None,
    metrics_trace: bool = False,
    compress_zstd_level: int | None = None,
) -> None:
    scenario_list = list(scenarios)
    compression_level = 10
    if compress_zstd_level is not None:
        compression_level = max(1, min(22, compress_zstd_level))
    compression = CompressionConfig(
        enabled=compress_zstd_level is not None,
        zstd_level=compression_level,
    )
    scenario_order = {sc.scenario_id: idx for idx, sc in enumerate(scenario_list)}
    if not scenario_list:
        return

    if use_pty is None:
        use_pty = bool(os.environ.get("SUMO_BATCH_USE_PTY", "").strip())
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
            scale=scenario.scale,
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
                compression=compression,
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
                    scale=result.scale,
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
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=RESULT_COLUMNS)
        if header_needed:
            writer.writeheader()
        for result in results:
            writer.writerow(_result_to_row(result))


def _compress_artifacts(artifacts: RunArtifacts, *, level: int, log_path: Path | None) -> None:
    try:
        import zstandard as zstd
    except ImportError:
        _debug_log(log_path, "[compress] zstandard not installed; skipping compression")
        return

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


def _result_to_row(result: ScenarioResult) -> dict:
    build_start, build_end = _timing_to_strings(result.timings.build)
    sumo_start, sumo_end = _timing_to_strings(result.timings.sumo)
    compress_start, compress_end = _timing_to_strings(result.timings.compress)
    probe_start, probe_end = _timing_to_strings(result.timings.probe)
    probe_compress_start, probe_compress_end = _timing_to_strings(
        result.timings.probe_compress
    )
    return {
        "scenario_id": result.scenario_id,
        "scenario_base_id": result.scenario_base_id,
        "seed": result.seed,
        "scale": result.scale,
        "begin_filter": result.begin_filter,
        "end_time": result.end_time,
        "demand_dir": str(result.demand_dir),
        "vehicle_count": result.tripinfo.vehicle_count,
        "person_count": result.tripinfo.person_count,
        "vehicle_mean_timeLoss": _fmt(result.tripinfo.vehicle_mean_time_loss),
        "person_mean_timeLoss": _fmt(result.tripinfo.person_mean_time_loss),
        "person_mean_routeLength": _fmt(result.tripinfo.person_mean_route_length),
        "queue_threshold_steps": result.queue.threshold_steps,
        "queue_threshold_length": _fmt(result.queue.threshold_length),
        "queue_first_over_saturation_time": _fmt(result.queue.first_failure_time),
        "queue_is_durable": "True" if result.queue.is_durable else "False",
        "scale_probe_enabled": str(result.scale_probe.enabled),
        "scale_probe_max_durable_scale": _fmt(result.scale_probe.max_durable_scale),
        "scale_probe_attempts": result.scale_probe.attempts,
        "fcd_note": result.fcd_note,
        "error_note": result.error or "",
        "worker_id": result.worker_id if result.worker_id is not None else "",
        "build_start": build_start,
        "build_end": build_end,
        "sumo_start": sumo_start,
        "sumo_end": sumo_end,
        "compress_start": compress_start,
        "compress_end": compress_end,
        "probe_start": probe_start,
        "probe_end": probe_end,
        "probe_compress_start": probe_compress_start,
        "probe_compress_end": probe_compress_end,
    }


def _fmt(value) -> str | float:
    if value is None:
        return ""
    return round(value, 3) if isinstance(value, float) else value


def _timing_to_strings(timing: PhaseTiming | None) -> tuple[str, str]:
    if timing is None:
        return "", ""
    return _format_timestamp(timing.start), _format_timestamp(timing.end)
