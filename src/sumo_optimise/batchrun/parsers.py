from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from .models import (
    QueueDurabilityConfig,
    QueueDurabilityMetrics,
    TripinfoMetrics,
    WaitingMetrics,
    WaitingThresholds,
)


def _as_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_tripinfo(path: Path, *, begin_filter: float) -> TripinfoMetrics:
    metrics = TripinfoMetrics()
    if not path.exists():
        return metrics

    for _, elem in ET.iterparse(path, events=("end",)):
        tag = elem.tag.split("}")[-1]
        if tag not in {"tripinfo", "personinfo"}:
            elem.clear()
            continue

        arrival = _as_float(elem.attrib.get("arrival"))
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
            if time_loss is not None and not math.isnan(time_loss):
                metrics.person_time_loss_sum += time_loss
            if route_length is not None and not math.isnan(route_length):
                metrics.person_route_length_sum += route_length
            metrics.person_count += 1

        elem.clear()

    return metrics


def parse_summary(path: Path, *, thresholds: WaitingThresholds) -> WaitingMetrics:
    metrics = WaitingMetrics()
    if not path.exists():
        return metrics

    fixed = thresholds.fixed
    pct = thresholds.pct_of_running

    for _, elem in ET.iterparse(path, events=("end",)):
        tag = elem.tag.split("}")[-1]
        if tag != "step":
            elem.clear()
            continue

        waiting = _as_float(elem.attrib.get("waiting")) or 0.0
        running = _as_float(elem.attrib.get("running")) or 0.0
        time_value = _as_float(elem.attrib.get("time")) or _as_float(elem.attrib.get("timestep")) or 0.0

        metrics.max_waiting = max(metrics.max_waiting, waiting)

        if metrics.first_fixed_time is None and waiting > fixed:
            metrics.first_fixed_time = time_value
            metrics.first_fixed_value = waiting

        dynamic_threshold = running * pct
        if running > 0 and metrics.first_pct_time is None and waiting > dynamic_threshold:
            metrics.first_pct_time = time_value
            metrics.first_pct_value = waiting

        elem.clear()

    return metrics


def parse_waiting_ratio(path: Path, *, config: QueueDurabilityConfig) -> QueueDurabilityMetrics:
    """
    Determine durability from summary.xml using waiting/(waiting+running) ratio.
    Ratio is treated as queue_max_length; threshold_length is the ratio threshold.
    """
    metrics = QueueDurabilityMetrics(
        threshold_steps=config.step_window,
        threshold_length=config.length_threshold,
    )
    if not path.exists():
        return metrics

    streak = 0
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

        total = waiting + running
        ratio = (waiting / total) if total > 0 else 0.0
        metrics.max_queue_length = max(metrics.max_queue_length, ratio)

        if ratio >= config.length_threshold:
            streak += 1
            if metrics.first_failure_time is None and streak >= config.step_window:
                metrics.first_failure_time = time_value
        else:
            streak = 0

        elem.clear()

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
