"""Geometry helpers."""
from __future__ import annotations

from typing import Tuple

from ..domain.models import MainRoadConfig


def build_main_carriageway_y(main_road: MainRoadConfig) -> Tuple[float, float]:
    y_eb = +main_road.center_gap_m / 2.0
    y_wb = -main_road.center_gap_m / 2.0
    return y_eb, y_wb
