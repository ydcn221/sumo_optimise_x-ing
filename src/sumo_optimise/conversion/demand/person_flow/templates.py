"""Helpers for generating blank demand CSV templates."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Sequence

from ...domain.models import PersonFlowPattern


_ENDPOINT_TEMPLATE_NAME = "DemandPerEndpoint_template.csv"
_JUNCTION_TEMPLATE_NAME = "JunctionTurnWeight_template.csv"

_ENDPOINT_PATTERN_ROW = ["Pattern", PersonFlowPattern.PERSONS_PER_HOUR.value]
_ENDPOINT_HEADER = ["SidewalkEndID", "PedFlow", "Label"]
_JUNCTION_HEADER = [
    "JunctionID",
    "ToNorth_EastSide",
    "ToNorth_WestSide",
    "ToWest_NorthSide",
    "ToWest_SouthSide",
    "ToSouth_WestSide",
    "ToSouth_EastSide",
    "ToEast_SouthSide",
    "ToEast_NorthSide",
]


def _write_csv(path: Path, headers: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def write_demand_templates(outdir: Path, endpoint_ids: Sequence[str], junction_ids: Sequence[str]) -> None:
    """Materialise empty CSV templates for endpoint demand and junction turn weights."""

    endpoint_rows = ((endpoint_id, "", "") for endpoint_id in endpoint_ids)
    junction_rows = ((junction_id, "", "", "", "", "", "", "", "") for junction_id in junction_ids)

    endpoint_path = outdir / _ENDPOINT_TEMPLATE_NAME
    endpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with endpoint_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(_ENDPOINT_PATTERN_ROW)
        writer.writerow(_ENDPOINT_HEADER)
        writer.writerows(endpoint_rows)

    _write_csv(outdir / _JUNCTION_TEMPLATE_NAME, _JUNCTION_HEADER, junction_rows)


__all__ = ["write_demand_templates"]
