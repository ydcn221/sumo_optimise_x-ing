from __future__ import annotations

import csv
import math
import xml.etree.ElementTree as ET
import time
from pathlib import Path
from typing import Callable, Optional, List

from .models import (
    QueueDurabilityConfig,
    QueueDurabilityMetrics,
    TripinfoMetrics,
)


def _as_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_tripinfo_csv_file(
    path: Path,
    metrics: TripinfoMetrics,
    *,
    begin_filter: float,
    end_filter: float | None,
    is_person_file: bool,
    progress_cb: Callable[[int, float], None] | None,
) -> None:
    start_time = time.time()
    total_processed = 0
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter=";")
        for row in reader:
            arrival = _as_float(row.get("arrival"))
            depart = _as_float(row.get("depart"))
            duration = _as_float(row.get("duration"))
            if arrival is None and depart is not None and duration is not None:
                arrival = depart + duration
            if arrival is None or arrival < begin_filter:
                continue
            if end_filter is not None and arrival > end_filter:
                continue

            if is_person_file:
                time_loss = _as_float(row.get("timeLoss"))
                if time_loss is None:
                    time_loss = _as_float(row.get("walk_timeLoss"))
                route_length = _as_float(row.get("routeLength"))
                if route_length is None:
                    route_length = _as_float(row.get("walk_routeLength"))
                if time_loss is not None and not math.isnan(time_loss):
                    metrics.person_time_loss_sum += time_loss
                if route_length is not None and not math.isnan(route_length):
                    metrics.person_route_length_sum += route_length
                metrics.person_count += 1
            else:
                time_loss = _as_float(row.get("timeLoss"))
                if time_loss is not None and not math.isnan(time_loss):
                    metrics.vehicle_time_loss_sum += time_loss
                    metrics.vehicle_count += 1

            total_processed += 1
            if progress_cb:
                elapsed = time.time() - start_time
                if elapsed > 0.5:
                    progress_cb(total_processed, elapsed)
                    start_time = time.time()


def _parse_tripinfo_xml_file(
    path: Path,
    metrics: TripinfoMetrics,
    *,
    begin_filter: float,
    end_filter: float | None,
    progress_cb: Callable[[int, float], None] | None,
) -> None:
    start_time = time.time()
    processed = 0
    total_processed = 0
    for _, elem in ET.iterparse(path, events=("end",)):
        tag = elem.tag.split("}")[-1]
        if tag in {"walk", "ride", "stop", "tranship"}:
            # keep child legs intact so the parent <personinfo> can access their attributes
            continue

        if tag not in {"tripinfo", "personinfo"}:
            elem.clear()
            continue

        arrival = _as_float(elem.attrib.get("arrival"))
        if arrival is None:
            for child in elem:
                child_tag = child.tag.split("}")[-1]
                if child_tag in {"walk", "ride", "stop", "tranship"}:
                    arrival = _as_float(child.attrib.get("arrival"))
                    if arrival is not None:
                        break
        if arrival is None:
            depart = _as_float(elem.attrib.get("depart"))
            duration = _as_float(elem.attrib.get("duration"))
            if depart is not None and duration is not None:
                arrival = depart + duration

        if arrival is None or arrival < begin_filter:
            elem.clear()
            continue
        if end_filter is not None and arrival > end_filter:
            elem.clear()
            continue

        time_loss = _as_float(elem.attrib.get("timeLoss"))
        route_length = _as_float(elem.attrib.get("routeLength"))

        if tag == "tripinfo":
            if time_loss is not None and not math.isnan(time_loss):
                metrics.vehicle_time_loss_sum += time_loss
                metrics.vehicle_count += 1
        else:
            child_time_loss = 0.0
            child_route_length = 0.0
            has_child_time_loss = False
            has_child_route_length = False
            for child in elem:
                child_tag = child.tag.split("}")[-1]
                if child_tag not in {"walk", "ride", "stop", "tranship"}:
                    continue
                child_tl = _as_float(child.attrib.get("timeLoss"))
                child_rl = _as_float(child.attrib.get("routeLength"))
                if child_tl is not None and not math.isnan(child_tl):
                    child_time_loss += child_tl
                    has_child_time_loss = True
                if child_rl is not None and not math.isnan(child_rl):
                    child_route_length += child_rl
                    has_child_route_length = True

            if time_loss is None or math.isnan(time_loss):
                time_loss = child_time_loss if has_child_time_loss else None
            if route_length is None or math.isnan(route_length):
                route_length = child_route_length if has_child_route_length else None

            if time_loss is not None:
                metrics.person_time_loss_sum += time_loss
            if route_length is not None:
                metrics.person_route_length_sum += route_length
            metrics.person_count += 1

        elem.clear()
        processed += 1
        total_processed += 1
        if progress_cb:
            elapsed = time.time() - start_time
            if elapsed > 0.5:  # throttle logs to ~2 Hz
                progress_cb(total_processed, elapsed)
                start_time = time.time()
                processed = 0


def parse_tripinfo(
    path: Path,
    *,
    begin_filter: float,
    end_filter: float | None = None,
    personinfo: Path | None = None,
    progress_cb: Callable[[int, float], None] | None = None,
) -> TripinfoMetrics:
    metrics = TripinfoMetrics()
    paths = [p for p in (path, personinfo) if p is not None]
    if not paths:
        return metrics

    for current_path in paths:
        if not current_path.exists():
            continue
        is_person_file = personinfo is not None and current_path == personinfo
        suffix = current_path.suffix.lower()
        if suffix == ".csv":
            _parse_tripinfo_csv_file(
                current_path,
                metrics,
                begin_filter=begin_filter,
                end_filter=end_filter,
                is_person_file=is_person_file,
                progress_cb=progress_cb,
            )
            continue

        _parse_tripinfo_xml_file(
            current_path,
            metrics,
            begin_filter=begin_filter,
            end_filter=end_filter,
            progress_cb=progress_cb,
        )

    return metrics


def _parse_waiting_ratio_csv_file(
    path: Path,
    config: QueueDurabilityConfig,
    progress_cb: Callable[[str], None] | None,
) -> QueueDurabilityMetrics:
    metrics = QueueDurabilityMetrics(
        threshold_steps=config.step_window,
        threshold_length=config.length_threshold,
    )
    start_time = time.time()
    total_processed = 0
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter=";")
        streak = 0
        for row in reader:
            waiting = _as_float(row.get("waiting")) or 0.0
            running = _as_float(row.get("running")) or 0.0
            time_value = _as_float(row.get("time")) or _as_float(row.get("timestep")) or 0.0

            ratio = (waiting / running) if running > 0 else 0.0
            metrics.max_queue_length = max(metrics.max_queue_length, ratio)

            if ratio >= config.length_threshold:
                streak += 1
                if metrics.first_failure_time is None and streak >= config.step_window:
                    metrics.first_failure_time = time_value
            else:
                streak = 0

            total_processed += 1
            if progress_cb:
                elapsed = time.time() - start_time
                if elapsed > 0.5:
                    progress_cb(
                        f"[metrics-trace] waiting_ratio steps={total_processed} elapsed={elapsed:.1f}s file={path}"
                    )
                    start_time = time.time()

    return metrics


def parse_waiting_ratio(
    path: Path,
    *,
    config: QueueDurabilityConfig,
    progress_cb: Callable[[str], None] | None = None,
) -> QueueDurabilityMetrics:
    """Determine durability from summary output using waiting/running ratio."""
    metrics = QueueDurabilityMetrics(
        threshold_steps=config.step_window,
        threshold_length=config.length_threshold,
    )
    if not path.exists():
        return metrics

    if path.suffix.lower() == ".csv":
        return _parse_waiting_ratio_csv_file(path, config, progress_cb)

    streak = 0
    processed = 0
    total_processed = 0
    start_time = time.time()
    for _, elem in ET.iterparse(path, events=("end",)):
        tag = elem.tag.split("}")[-1]
        if tag != "step":
            elem.clear()
            continue

        waiting = _as_float(elem.attrib.get("waiting")) or 0.0
        running = _as_float(elem.attrib.get("running")) or 0.0
        time_value = (
            _as_float(elem.attrib.get("time"))
            or _as_float(elem.attrib.get("timestep"))
            or 0.0
        )

        # Use waiting/running (not waiting/(waiting+running)) to gauge saturation relative to flow.
        ratio = (waiting / running) if running > 0 else 0.0
        metrics.max_queue_length = max(metrics.max_queue_length, ratio)

        if ratio >= config.length_threshold:
            streak += 1
            if metrics.first_failure_time is None and streak >= config.step_window:
                metrics.first_failure_time = time_value
        else:
            streak = 0

        elem.clear()
        processed += 1
        total_processed += 1
        if progress_cb:
            elapsed = time.time() - start_time
            if elapsed > 0.5:
                progress_cb(
                    f"[metrics-trace] waiting_ratio steps={total_processed} elapsed={elapsed:.1f}s file={path}"
                )
                start_time = time.time()
                processed = 0

    return metrics


def parse_queue_output(
    path: Path, *, config: QueueDurabilityConfig
) -> QueueDurabilityMetrics:
    metrics = QueueDurabilityMetrics(
        threshold_steps=config.step_window,
        threshold_length=config.length_threshold,
    )
    if not path.exists():
        return metrics

    hits = 0

    try:
        for _, elem in ET.iterparse(path, events=("end",)):
            tag = elem.tag.split("}")[-1]
            if tag != "data":
                elem.clear()
                continue

            time_value = (
                _as_float(elem.attrib.get("timestep"))
                or _as_float(elem.attrib.get("time_step"))
                or _as_float(elem.attrib.get("time"))
                or 0.0
            )

            step_max = 0.0
            for lane in elem.iterfind(".//lane"):
                lane_length = _as_float(lane.attrib.get("queueing_length")) or 0.0
                step_max = max(step_max, lane_length)

            metrics.max_queue_length = max(metrics.max_queue_length, step_max)

            if step_max > config.length_threshold:
                hits += 1
                if metrics.first_failure_time is None and hits >= config.step_window:
                    metrics.first_failure_time = time_value

            elem.clear()
    except ET.ParseError:
        # File may be truncated when probe aborts early; treat as non-durable if any hits so far.
        if hits > 0 and metrics.first_failure_time is None:
            metrics.first_failure_time = 0.0
            metrics.max_queue_length = max(metrics.max_queue_length, config.length_threshold + 1)
        return metrics

    return metrics


def parse_waiting_percentile(
    path: Path,
    *,
    begin: float,
    end: float,
    progress_cb: Callable[[str], None] | None = None,
) -> float | None:
    """Compute 95th percentile of waiting (vehicle count), trimming top 5% (ceiling) in [begin, end]."""
    if not path.exists() or end <= begin:
        return None

    waiting_values: List[float] = []
    start_time = time.time()
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp, delimiter=";")
            for idx, row in enumerate(reader, start=1):
                time_value = _as_float(row.get("time")) or _as_float(row.get("timestep")) or 0.0
                if time_value < begin or time_value > end:
                    continue
                waiting = _as_float(row.get("waiting"))
                if waiting is not None and not math.isnan(waiting):
                    waiting_values.append(waiting)
                if progress_cb and idx % 50000 == 0:
                    elapsed = time.time() - start_time
                    progress_cb(
                        f"[metrics-trace] waiting_p95 steps={idx} elapsed={elapsed:.1f}s file={path}"
                    )
    else:
        for idx, (_, elem) in enumerate(ET.iterparse(path, events=("end",)), start=1):
            tag = elem.tag.split("}")[-1]
            if tag != "step":
                elem.clear()
                continue
            time_value = (
                _as_float(elem.attrib.get("time"))
                or _as_float(elem.attrib.get("timestep"))
                or 0.0
            )
            if time_value < begin or time_value > end:
                elem.clear()
                continue
            waiting = _as_float(elem.attrib.get("waiting"))
            if waiting is not None and not math.isnan(waiting):
                waiting_values.append(waiting)
            if progress_cb and idx % 50000 == 0:
                elapsed = time.time() - start_time
                progress_cb(
                    f"[metrics-trace] waiting_p95 steps={idx} elapsed={elapsed:.1f}s file={path}"
                )
            elem.clear()

    if not waiting_values:
        return None

    waiting_values.sort()
    # Trim the highest ceil(5%) samples (more aggressive trimming if not exact).
    trim = math.ceil(len(waiting_values) * 0.05)
    if trim > 0:
        waiting_values = waiting_values[:-trim]
    if not waiting_values:
        return None

    # Return the sample at or above the 95% position without interpolation.
    idx = math.ceil(0.95 * len(waiting_values)) - 1
    idx = max(0, min(idx, len(waiting_values) - 1))
    return waiting_values[idx]
