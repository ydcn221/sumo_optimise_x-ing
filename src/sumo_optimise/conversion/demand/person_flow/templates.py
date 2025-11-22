"""Helpers for generating blank demand CSV templates."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Sequence

from ...domain.models import PersonFlowPattern

_ENDPOINT_PATTERN_ROW = ["Pattern", PersonFlowPattern.STEADY.value]
_PED_ENDPOINT_HEADER = ["SidewalkEndID", "PedFlow", "Label"]
_PED_JUNCTION_HEADER = ["JunctionID", "Main_L", "Main_T", "Main_R", "Minor_L", "Minor_T", "Minor_R"]
_VEH_ENDPOINT_HEADER = ["EndID", "vehFlow", "Label"]
_VEH_JUNCTION_HEADER = ["JunctionID", "Main_L", "Main_T", "Main_R", "Minor_L", "Minor_T", "Minor_R"]


def _write_csv(path: Path, headers: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def write_demand_templates(
    ped_endpoint_path: Path,
    ped_junction_path: Path,
    veh_endpoint_path: Path,
    veh_junction_path: Path,
    ped_endpoint_ids: Sequence[str],
    veh_endpoint_ids: Sequence[str],
    junction_ids: Sequence[str],
) -> None:
    """Materialise empty CSV templates for both pedestrian and vehicle demand inputs."""

    ped_endpoint_rows = ((endpoint_id, "", "") for endpoint_id in ped_endpoint_ids)
    ped_junction_rows = ((junction_id, "", "", "", "", "", "") for junction_id in junction_ids)

    ped_endpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with ped_endpoint_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(_ENDPOINT_PATTERN_ROW)
        writer.writerow(_PED_ENDPOINT_HEADER)
        writer.writerows(ped_endpoint_rows)

    _write_csv(ped_junction_path, _PED_JUNCTION_HEADER, ped_junction_rows)

    veh_endpoint_rows = ((endpoint_id, "", "") for endpoint_id in veh_endpoint_ids)
    veh_endpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with veh_endpoint_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(_ENDPOINT_PATTERN_ROW)
        writer.writerow(_VEH_ENDPOINT_HEADER)
        writer.writerows(veh_endpoint_rows)

    veh_junction_rows = ((junction_id, "", "", "", "", "", "") for junction_id in junction_ids)
    _write_csv(veh_junction_path, _VEH_JUNCTION_HEADER, veh_junction_rows)


__all__ = ["write_demand_templates"]
