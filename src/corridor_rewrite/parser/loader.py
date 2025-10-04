"""Load and validate corridor JSON specifications."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Mapping

from ..domain import models


class SpecError(RuntimeError):
    """Raised when a specification file fails validation."""


ALLOWED_VERSION = "1.2"


def load_corridor_spec(path: Path, schema_path: Path | None = None) -> models.CorridorSpec:
    """Load a specification file and convert it to domain models."""

    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if data.get("version") != ALLOWED_VERSION:
        raise SpecError(f"Unsupported spec version: {data.get('version')!r}")

    snap = _parse_snap(data["snap"])
    defaults = _parse_defaults(data["defaults"])
    main = _parse_main(data["main_road"])
    junction_templates = _parse_templates(data["junction_templates"])
    signal_profiles = _parse_signal_profiles(data["signal_profiles"])
    layout = [_parse_layout_event(event) for event in data.get("layout", [])]
    layout.sort(key=lambda evt: evt.pos_m)

    return models.CorridorSpec(
        version=data["version"],
        snap=snap,
        defaults=defaults,
        main_road=main,
        junction_templates=junction_templates,
        signal_profiles=signal_profiles,
        layout=layout,
    )


def _parse_snap(data: Mapping[str, object]) -> models.SnapSpec:
    tie_break_raw = data.get("tie_break", models.TieBreak.TOWARD_WEST.value)
    try:
        tie_break = models.TieBreak(tie_break_raw)
    except ValueError as exc:
        raise SpecError(f"Invalid tie_break value: {tie_break_raw!r}") from exc
    step = int(data.get("step_m", 5))
    if step <= 0:
        raise SpecError("snap.step_m must be positive")
    return models.SnapSpec(step_m=step, tie_break=tie_break)


def _parse_defaults(data: Mapping[str, object]) -> models.Defaults:
    return models.Defaults(
        minor_road_length_m=float(data.get("minor_road_length_m", 80.0)),
        ped_crossing_width_m=float(data.get("ped_crossing_width_m", 3.0)),
        sidewalk_width_m=float(data.get("sidewalk_width_m", 2.0)),
        speed_kmh=float(data.get("speed_kmh", 50.0)),
    )


def _parse_main(data: Mapping[str, object]) -> models.MainRoadSpec:
    length = float(data.get("length_m", 1000.0))
    if length <= 0:
        raise SpecError("main_road.length_m must be positive")
    return models.MainRoadSpec(
        length_m=length,
        center_gap_m=float(data.get("center_gap_m", 0.0)),
        lanes=int(data.get("lanes", 2)),
    )


def _parse_templates(data: Mapping[str, Iterable[Mapping[str, object]]]) -> Mapping[models.LayoutKind, Dict[str, models.JunctionTemplate]]:
    templates: Dict[models.LayoutKind, Dict[str, models.JunctionTemplate]] = {}
    for key, items in data.items():
        try:
            kind = models.LayoutKind(key)
        except ValueError as exc:
            raise SpecError(f"Unknown junction template kind: {key}") from exc
        bucket: Dict[str, models.JunctionTemplate] = {}
        for raw in items:
            template = models.JunctionTemplate(
                id=str(raw["id"]),
                main_approach_begin_m=float(raw.get("main_approach_begin_m", 0.0)),
                main_approach_lanes=int(raw.get("main_approach_lanes", 0)),
                minor_lanes_to_main=int(raw.get("minor_lanes_to_main", 1)),
                minor_lanes_from_main=int(raw.get("minor_lanes_from_main", 1)),
                split_ped_crossing_on_main=bool(raw.get("split_ped_crossing_on_main", False)),
                median_continuous=bool(raw.get("median_continuous", False)),
            )
            bucket[template.id] = template
        templates[kind] = bucket
    return templates


def _parse_signal_profiles(data: Mapping[str, Iterable[Mapping[str, object]]]) -> Mapping[models.LayoutKind, Dict[str, models.SignalProfile]]:
    profiles: Dict[models.LayoutKind, Dict[str, models.SignalProfile]] = {}
    for key, items in data.items():
        try:
            kind = models.LayoutKind(key)
        except ValueError as exc:
            raise SpecError(f"Unknown signal profile kind: {key}") from exc
        bucket: Dict[str, models.SignalProfile] = {}
        for raw in items:
            phases = [
                models.SignalPhase(
                    name=str(phase.get("name", "")),
                    duration_s=float(phase.get("duration_s", 0.0)),
                    allow_movements=tuple(str(mv) for mv in phase.get("allow_movements", [])),
                )
                for phase in raw.get("phases", [])
            ]
            profile = models.SignalProfile(
                id=str(raw["id"]),
                cycle_s=float(raw.get("cycle_s", 0.0)),
                phases=tuple(phases),
            )
            bucket[profile.id] = profile
        profiles[kind] = bucket
    return profiles


def _parse_layout_event(data: Mapping[str, object]) -> models.LayoutEvent:
    try:
        kind = models.LayoutKind(data["type"])
    except ValueError as exc:
        raise SpecError(f"Unknown layout event type: {data.get('type')}") from exc
    template_id = data.get("template")
    branch_raw = data.get("branch")
    branch = models.BranchSide(branch_raw) if branch_raw else None
    signal_data = data.get("signal")
    signal = None
    if signal_data:
        signal = models.SignalRef(
            profile_id=str(signal_data["profile_id"]),
            offset_s=float(signal_data.get("offset_s", 0.0)),
        )
    placement_data = data.get("main_ped_crossing_placement")
    placement = None
    if placement_data:
        placement = models.CrossingPlacement(
            west=bool(placement_data.get("west", False)),
            east=bool(placement_data.get("east", False)),
        )
    return models.LayoutEvent(
        kind=kind,
        pos_m=float(data.get("pos_m", 0.0)),
        template_id=str(template_id) if template_id else None,
        branch=branch,
        signalized=bool(data.get("signalized", False)),
        signal=signal,
        split_ped_crossing_on_main=data.get("split_ped_crossing_on_main"),
        main_ped_crossing_placement=placement,
    )
