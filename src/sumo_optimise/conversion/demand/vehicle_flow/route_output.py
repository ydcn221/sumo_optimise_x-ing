"""Render vehicle <flow> entries for SUMO routes."""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, List, Tuple

from ...domain.models import EndpointDemandRow, PersonFlowPattern
from ...utils.errors import DemandValidationError


def build_vehicle_flow_entries(
    flows: Iterable[Tuple[str, str, float, EndpointDemandRow]],
    *,
    vehicle_pattern: PersonFlowPattern,
    begin_time: float,
    end_time: float,
    segment_tag: str | None = None,
) -> List[str]:
    counter_by_pair = defaultdict(int)
    entries: List[str] = []
    for origin, destination, value, row in flows:
        pair_key = (origin, destination)
        seq = counter_by_pair[pair_key]
        counter_by_pair[pair_key] += 1

        # Keep IDs stable and unique per OD pair and segment: vf_{origin}__{destination}__{segment}__{n}
        seg = f"{segment_tag}__" if segment_tag else ""
        flow_id = f"vf_{origin}__{destination}__{seg}{seq}"
        attr = _format_pattern_attribute(vehicle_pattern, value)
        entry = (
            f'  <flow id="{flow_id}" begin="{begin_time:.2f}" end="{end_time:.2f}" '
            f'fromJunction="{origin}" toJunction="{destination}" '
            f'departLane="best_prob" departSpeed="max" {attr}/>'
        )
        entries.append(entry)
    return entries


def _format_pattern_attribute(pattern: PersonFlowPattern, value: float) -> str:
    if pattern is PersonFlowPattern.STEADY:
        return f'vehsPerHour="{value:.6f}"'
    if pattern is PersonFlowPattern.POISSON:
        lam = value / 3600.0
        return f'period="exp({lam:.6f})"'
    raise DemandValidationError(f"unsupported vehicle flow pattern {pattern}")


__all__ = ["build_vehicle_flow_entries"]
