"""Snapping and distance helpers."""
from __future__ import annotations

import math


def kmh_to_mps(speed_kmh: float) -> float:
    return speed_kmh / 3.6


def round_position(value_m: float, step_m: int, tie_break: str) -> int:
    if step_m <= 0:
        return int(round(value_m))
    q = value_m / float(step_m)
    lo = math.floor(q) * step_m
    hi = math.ceil(q) * step_m
    dl = abs(value_m - lo)
    du = abs(hi - value_m)
    if dl < du:
        return int(lo)
    if dl > du:
        return int(hi)
    return int(lo if tie_break == "toward_west" else hi)


def grid_upper_bound(length_m: float, step_m: int) -> int:
    if step_m <= 0:
        return int(math.floor(length_m))
    return int(math.floor(length_m / float(step_m)) * step_m)


def snap_distance_to_step(distance_m: float, step_m: int) -> int:
    if distance_m <= 0 or step_m <= 0:
        return 0
    return int(round(distance_m / float(step_m)) * step_m)
