from __future__ import annotations

import csv
import json
import math
import multiprocessing
import os
import re
import shutil
import subprocess
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
    WorkerPhase,
    WorkerStatus,
)
from .parsers import parse_summary, parse_tripinfo, parse_waiting_ratio


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
    "scale_probe_max_durable_scale",
    "scale_probe_attempts",
    "fcd_note",
    "error_note",
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
    size_display: str | None = None,
) -> None:
    term_width = shutil.get_terminal_size((160, 24)).columns
    cols = max(1, term_width // 40)
    rows: List[str] = []
    ordered = [statuses[k] for k in sorted(statuses.keys())]
    for idx, status in enumerate(ordered):
        scenario = status.scenario_id or "-"
        phase = status.phase
        scale_str = f"{status.probe_scale or status.scale or 0:.1f}"
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
    size_part = f" | out {size_display}" if size_display else ""
    summary = f"completed {completed}/{total} | errors {error_count}{size_part}"
    print("\x1b[H\x1b[2J", end="")
    print("".join(rows))
    print(summary)


def _render_loop(
    status_queue,
    stop_event: threading.Event,
    total: int,
    worker_count: int,
    output_root: Path,
) -> None:
    statuses: Dict[int, WorkerStatus] = {
        idx: WorkerStatus(worker_id=idx) for idx in range(worker_count)
    }
    last_render = 0.0
    completed = 0
    last_size_time = 0.0
    size_display: str | None = None
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
            statuses[worker_id] = status
            if evt.get("completed"):
                completed += 1
        now = time.time()
        if now - last_size_time >= 2.0:
            try:
                size_bytes = _dir_size_bytes(output_root)
                size_display = _format_bytes(size_bytes)
            except OSError:
                size_display = None
            last_size_time = now
        if now - last_render >= 0.5:
            _render_grid(
                statuses,
                total=total,
                completed=completed,
                size_display=size_display,
            )
            last_render = now
    try:
        _render_grid(
            statuses,
            total=total,
            completed=completed,
            size_display=size_display,
        )
    except (EOFError, BrokenPipeError):
        return

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
        run_netconvert=True,
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
        sumo_log=outdir / "sumo.log",
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
    waiting_state = {"streak": 0, "max_ratio": 0.0, "first_failure": None}

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
        first_failure_time: float | None = None
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
                        total = waiting + running
                        ratio = (waiting / total) if total > 0 else 0.0
                        max_ratio = max(max_ratio, ratio)
                        if ratio >= waiting_config.length_threshold:
                            streak += 1
                            if first_failure_time is None and streak >= waiting_config.step_window:
                                first_failure_time = time_value
                                abort_event.set()
                                aborted = True
                                debug(
                                    f"[waiting-monitor] abort at t={time_value} "
                                    f"streak={streak}/{waiting_config.step_window} "
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
            waiting_state["first_failure"] = first_failure_time
            if first_failure_time is not None:
                live_waiting_metrics = QueueDurabilityMetrics(
                    first_failure_time=first_failure_time,
                    max_queue_length=max_ratio,
                    threshold_steps=waiting_config.step_window if waiting_config else 0,
                    threshold_length=waiting_config.length_threshold if waiting_config else 0.0,
                )
            if streak or first_failure_time is not None:
                debug(
                    f"[waiting-monitor] exit streak={streak} "
                    f"max_ratio={max_ratio:.3f} first_failure={first_failure_time} "
                    f"aborted={aborted} event_set={abort_event.is_set()}"
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
    status_queue=None,
    worker_id: int | None = None,
    phase: WorkerPhase = WorkerPhase.SUMO,
    use_pty: bool = False,
    enable_waiting_abort: bool = False,
) -> tuple[TripinfoMetrics, WaitingMetrics, QueueDurabilityMetrics]:
    artifacts.tripinfo.parent.mkdir(parents=True, exist_ok=True)
    artifacts.queue.parent.mkdir(parents=True, exist_ok=True)
    artifacts.sumo_log.parent.mkdir(parents=True, exist_ok=True)

    cmd = _sumo_command(artifacts, scenario, scale=scale)
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

    _send_status(
        status_queue,
        worker_id=worker_id or 0,
        scenario_id=scenario.scenario_id,
        seed=scenario.seed,
        scale=scale,
        phase=WorkerPhase.PARSE,
        label="metrics",
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

    queue_metrics = live_waiting_metrics or parse_waiting_ratio(artifacts.summary, config=queue_config)
    phase_label = phase.name if hasattr(phase, "name") else str(phase)
    _debug_log(
        artifacts.sumo_log,
        (
            f"[scale-run] phase={phase_label} scale={scale:.2f} "
            f"aborted={aborted} queue_durable={queue_metrics.is_durable} "
            f"first_failure_time={queue_metrics.first_failure_time} "
            f"max_ratio={queue_metrics.max_queue_length}"
        ),
    )
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
    status_queue=None,
    worker_id: int | None = None,
    use_pty: bool = False,
) -> ScaleProbeResult:
    resolution = scale_probe.resolution if scale_probe.resolution > 0 else 0.1
    coarse_step = scale_probe.coarse_step if scale_probe.coarse_step > 0 else resolution
    coarse_step = max(coarse_step, resolution)
    start_scale = _normalize_scale(max(scale_probe.start, resolution), resolution)
    ceiling = max(scale_probe.ceiling, start_scale)
    probe_tag = f"[scale-probe scenario={scenario.scenario_id} seed={scenario.seed}]"
    log_path = base_artifacts.sumo_log
    _debug_log(
        log_path,
        (
            f"{probe_tag} start={start_scale:.2f} "
            f"ceiling={ceiling:.2f} resolution={resolution:.3f} "
            f"coarse_step={coarse_step:.3f}"
        ),
    )

    def run_scale(raw_scale: float) -> QueueDurabilityMetrics:
        nonlocal attempts
        scale_value = _normalize_scale(raw_scale, resolution)
        if scale_value in queue_cache:
            _debug_log(
                log_path,
                (
                    f"{probe_tag} reuse scale={scale_value:.2f} "
                    f"durable={queue_cache[scale_value].is_durable} "
                    f"first_failure_time={queue_cache[scale_value].first_failure_time} "
                    f"max_queue_length={queue_cache[scale_value].max_queue_length}"
                ),
            )
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
            status_queue=status_queue,
            worker_id=worker_id,
            phase=WorkerPhase.PROBE,
            use_pty=use_pty,
            enable_waiting_abort=scale_probe.abort_on_waiting,
        )
        queue_cache[scale_value] = queue_metrics
        attempt_no = attempts + 1
        attempts = attempt_no
        _debug_log(
            log_path,
            (
                f"{probe_tag} run#{attempt_no} scale={scale_value:.2f} "
                f"durable={queue_metrics.is_durable} "
                f"first_failure_time={queue_metrics.first_failure_time} "
                f"max_queue_length={queue_metrics.max_queue_length}"
            ),
        )
        return queue_metrics

    last_durable: float | None = None
    failure_scale: float | None = None

    current = start_scale
    while current <= ceiling:
        metrics = run_scale(current)
        _debug_log(
            log_path,
            (
                f"{probe_tag} coarse scale={current:.2f} "
                f"durable={metrics.is_durable} "
                f"first_failure_time={metrics.first_failure_time} "
                f"max_queue_length={metrics.max_queue_length}"
            ),
        )
        if not metrics.is_durable:
            failure_scale = current
            _debug_log(
                log_path,
                (
                    f"{probe_tag} first failure at scale={current:.2f} "
                    f"(last durable={last_durable})"
                ),
            )
            break
        last_durable = current
        next_coarse = (math.floor(current / coarse_step) + 1) * coarse_step
        current = _normalize_scale(next_coarse, resolution)

    if failure_scale is None:
        _debug_log(
            log_path,
            (
                f"{probe_tag} no failure up to ceiling={ceiling:.2f}; "
                f"max durable={last_durable if last_durable is not None else ceiling}"
            ),
        )
        return ScaleProbeResult(
            enabled=True,
            max_durable_scale=last_durable if last_durable is not None else ceiling,
            attempts=attempts,
        )

    if last_durable is None:
        _debug_log(
            log_path,
            f"{probe_tag} failed at start scale={failure_scale:.2f}",
        )
        return ScaleProbeResult(
            enabled=True,
            max_durable_scale=None,
            attempts=attempts,
        )

    low = last_durable
    high = failure_scale

    while high - low > resolution:
        mid = _normalize_scale((low + high) / 2, resolution)
        if mid in {low, high}:
            break

        metrics = run_scale(mid)
        _debug_log(
            log_path,
            (
                f"{probe_tag} binary mid={mid:.2f} "
                f"durable={metrics.is_durable} "
                f"first_failure_time={metrics.first_failure_time} "
                f"max_queue_length={metrics.max_queue_length}"
            ),
        )
        if metrics.is_durable:
            low = mid
        else:
            high = mid

    result = ScaleProbeResult(
        enabled=True,
        max_durable_scale=low,
        attempts=attempts,
    )
    _debug_log(
        log_path,
        f"{probe_tag} result max_durable_scale={result.max_durable_scale} attempts={result.attempts}",
    )
    return result


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
    worker_id: int,
    status_queue,
    use_pty: bool,
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

    try:
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.BUILD,
            label="build",
        )
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
        _send_status(
            status_queue,
            worker_id=worker_id,
            scenario_id=scenario.scenario_id,
            seed=scenario.seed,
            scale=scenario.scale,
            phase=WorkerPhase.SUMO,
            label="sumo",
        )
        tripinfo_metrics, waiting_metrics, queue_metrics = _run_for_scale(
            artifacts,
            scenario,
            thresholds=thresholds,
            queue_config=queue_config,
            scale=scenario.scale,
            affinity_cpu=affinity_cpu,
            collect_tripinfo=True,
            collect_waiting=True,
            status_queue=status_queue,
            worker_id=worker_id,
            phase=WorkerPhase.SUMO,
            use_pty=use_pty,
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
                max_durable_scale=None,
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
        max_durable_scale=None,
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
            status_queue=status_queue,
            worker_id=worker_id,
            use_pty=use_pty,
        )

    result = ScenarioResult(
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
    thresholds: WaitingThresholds,
    queue_config: QueueDurabilityConfig,
    scale_probe: ScaleProbeConfig,
    results_csv: Path,
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_pty: bool | None = None,
) -> None:
    scenario_list = list(scenarios)
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
        args=(status_queue, stop_event, len(scenario_list), workers, output_root),
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
                thresholds=thresholds,
                queue_config=queue_config,
                scale_probe=scale_probe,
                affinity_cpu=affinity[worker_slot],
                worker_id=worker_slot,
                status_queue=status_queue,
                use_pty=use_pty,
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
        _append_results(results_csv, results)
    stop_event.set()
    render_thread.join(timeout=1.0)
    manager.shutdown()


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
        "scale_probe_max_durable_scale": _fmt(result.scale_probe.max_durable_scale),
        "scale_probe_attempts": result.scale_probe.attempts,
        "fcd_note": result.fcd_note,
        "error_note": result.error or "",
    }


def _fmt(value) -> str | float:
    if value is None:
        return ""
    return round(value, 3) if isinstance(value, float) else value
