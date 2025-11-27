from __future__ import annotations

import math
import xml.etree.ElementTree as ET
import time
from pathlib import Path
from typing import Callable, Optional

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


def parse_tripinfo(
    path: Path,
    *,
    begin_filter: float,
    personinfo: Path | None = None,
    progress_cb: Callable[[int, float], None] | None = None,
) -> TripinfoMetrics:
    metrics = TripinfoMetrics()
    paths = [p for p in (path, personinfo) if p is not None]
    if not paths:
        return metrics

    start_time = time.time()
    processed = 0
    total_processed = 0
    for current_path in paths:
        if not current_path.exists():
            continue

        for _, elem in ET.iterparse(current_path, events=("end",)):
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

    return metrics


def parse_waiting_ratio(
    path: Path,
    *,
    config: QueueDurabilityConfig,
    progress_cb: Callable[[str], None] | None = None,
) -> QueueDurabilityMetrics:
    """Determine durability from summary.xml using waiting/running ratio."""
    metrics = QueueDurabilityMetrics(
        threshold_steps=config.step_window,
        threshold_length=config.length_threshold,
    )
    if not path.exists():
        return metrics

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
