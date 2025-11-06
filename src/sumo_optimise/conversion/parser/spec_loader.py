"""Load and parse corridor specifications."""
from __future__ import annotations

import itertools
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

from jsonschema import Draft7Validator  # type: ignore

from ..domain.models import (
    Cluster,
    CorridorSpec,
    Defaults,
    EventKind,
    JunctionTemplate,
    LayoutEvent,
    MainRoadConfig,
    PedestrianConflictConfig,
    SideMinor,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
    SnapRule,
)
from ..planner.snap import grid_upper_bound, round_position
from ..utils.errors import (
    InvalidConfigurationError,
    SchemaFileNotFound,
    SchemaValidationError,
    SemanticValidationError,
    SpecFileNotFound,
    UnsupportedVersionError,
)
from ..utils.logging import get_logger

LOG = get_logger()


def load_json_file(json_path: Path) -> Dict:
    if not json_path.exists():
        raise SpecFileNotFound(f"JSON not found: {json_path}")
    with json_path.open("r", encoding="utf-8") as f:
        spec_json = json.load(f)
    LOG.info("loaded JSON: %s", json_path)
    return spec_json


def load_schema_file(schema_path: Path) -> Dict:
    if not schema_path.exists():
        raise SchemaFileNotFound(f"Schema not found: {schema_path}")
    with schema_path.open("r", encoding="utf-8") as f:
        schema_json = json.load(f)
    LOG.info("loaded Schema: %s", schema_path)
    return schema_json


def _format_json_path(path_iterable) -> str:
    parts: List[str] = ["root"]
    for p in path_iterable:
        if isinstance(p, int):
            parts[-1] = parts[-1] + f"[{p}]"
        else:
            parts.append(str(p))
    return ".".join(parts)


def validate_json_schema(spec_json: Dict, schema_json: Dict) -> None:
    validator = Draft7Validator(schema_json)
    errors = sorted(validator.iter_errors(spec_json), key=lambda e: (list(e.path), list(e.schema_path)))
    if not errors:
        LOG.info("schema validation: PASSED")
        return
    LOG.error("[SCH] schema validation: FAILED (count=%d)", len(errors))
    for i, err in enumerate(errors, start=1):
        json_path = _format_json_path(err.path)
        schema_path = "/".join(map(str, err.schema_path))
        LOG.error(
            "[SCH] #%d path=%s | msg=%s | validator=%s | schema_path=%s",
            i,
            json_path,
            err.message,
            err.validator,
            schema_path,
        )
    raise SchemaValidationError(f"schema validation failed with {len(errors)} error(s)")


def ensure_supported_version(spec_json: Dict) -> None:
    version = str(spec_json.get("version", ""))
    if not version.startswith("1.3"):
        raise UnsupportedVersionError(f'unsupported "version": {version} (expected 1.3.*)')


def parse_snap_rule(spec_json: Dict) -> SnapRule:
    s = spec_json["snap"]
    raw = s["step_m"]
    if isinstance(raw, int):
        step = raw
    elif isinstance(raw, float) and abs(raw - round(raw)) < 1e-9:
        step = int(round(raw))
        LOG.warning("snap.step_m is non-integer float; normalized to integer: %d", step)
    else:
        raise InvalidConfigurationError(f"snap.step_m must be integer >= 1 (got: {raw!r})")
    if step < 1:
        raise InvalidConfigurationError(f"snap.step_m must be >= 1 (got: {step})")
    return SnapRule(step_m=step, tie_break=s["tie_break"])


def parse_defaults(spec_json: Dict) -> Defaults:
    d = spec_json["defaults"]
    return Defaults(
        minor_road_length_m=int(d["minor_road_length_m"]),
        ped_crossing_width_m=float(d["ped_crossing_width_m"]),
        speed_kmh=int(d["speed_kmh"]),
        ped_endpoint_offset_m=float(d.get("ped_endpoint_offset_m", 0.10)),
        sidewalk_width_m=float(d["sidewalk_width_m"]) if "sidewalk_width_m" in d else None,
    )


def parse_main_road(spec_json: Dict) -> MainRoadConfig:
    mr = spec_json["main_road"]
    main = MainRoadConfig(
        length_m=float(mr["length_m"]),
        center_gap_m=float(mr["center_gap_m"]),
        lanes=int(mr["lanes"]),
    )
    LOG.info("main_road: L=%.2f gap=%.2f lanes=%d", main.length_m, main.center_gap_m, main.lanes)
    return main


def parse_junction_templates(spec_json: Dict) -> Dict[str, JunctionTemplate]:
    result: Dict[str, JunctionTemplate] = {}
    dup_ids: Dict[str, List[str]] = {}
    jt_root = spec_json.get("junction_templates", {})
    for kind in (EventKind.TEE.value, EventKind.CROSS.value):
        arr = jt_root.get(kind, [])
        if not isinstance(arr, list):
            continue
        for t in arr:
            tpl_id = str(t["id"])
            tpl = JunctionTemplate(
                id=tpl_id,
                main_approach_begin_m=int(t["main_approach_begin_m"]),
                main_approach_lanes=int(t["main_approach_lanes"]),
                minor_lanes_to_main=int(t["minor_lanes_to_main"]),
                minor_lanes_from_main=int(t["minor_lanes_from_main"]),
                median_continuous=bool(t["median_continuous"]),
                kind=EventKind(kind),
            )
            if tpl_id in result:
                dup_ids.setdefault(tpl_id, [result[tpl_id].kind.value]).append(kind)
            else:
                result[tpl_id] = tpl
    if dup_ids:
        for dup_id, kinds in dup_ids.items():
            kinds_str = ",".join(sorted(set(kinds)))
            LOG.error("[VAL] E107 duplicate junction_template id: id=%s kinds=%s", dup_id, kinds_str)
        raise SemanticValidationError(f"duplicate junction_template id(s): {', '.join(sorted(dup_ids.keys()))}")
    LOG.info("junction_templates: %d", len(result))
    return result


def parse_signal_ref(obj: Optional[Dict]) -> Optional[SignalRef]:
    if not obj:
        return None
    return SignalRef(profile_id=str(obj["profile_id"]), offset_s=int(obj["offset_s"]))


def parse_signal_profiles(spec_json: Dict) -> Dict[str, Dict[str, SignalProfileDef]]:
    MOVEMENT_RE = re.compile(r"^(pedestrian|(?:main|EB|WB|minor)_(?:L|T|R))$")

    profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]] = {
        EventKind.TEE.value: {},
        EventKind.CROSS.value: {},
        EventKind.XWALK_MIDBLOCK.value: {},
    }
    sp_root = spec_json.get("signal_profiles", {})
    errors: List[str] = []

    def add_profile(kind: str, p: Dict, idx: int) -> None:
        pid = str(p["id"])
        cycle = int(p["cycle_s"])
        ped_cutoff_required = kind in (EventKind.TEE.value, EventKind.CROSS.value)
        ped_early_cutoff_raw = p.get("ped_early_cutoff_s")
        if ped_cutoff_required:
            if ped_early_cutoff_raw is None:
                errors.append(
                    f"[VAL] E305 ped_early_cutoff_s is required for intersections: profile={pid} kind={kind}"
                )
                ped_early_cutoff = 0
            else:
                ped_early_cutoff = int(ped_early_cutoff_raw)
        else:
            if ped_early_cutoff_raw is not None:
                errors.append(
                    f"[VAL] E305 ped_early_cutoff_s is not allowed for midblock crossings: profile={pid} kind={kind}"
                )
                ped_early_cutoff = int(ped_early_cutoff_raw)
            else:
                ped_early_cutoff = 0
        yellow_duration_raw = p.get("yellow_duration_s")
        if yellow_duration_raw is None:
            errors.append(
                f"[VAL] E306 yellow_duration_s is required: profile={pid} kind={kind}"
            )
            yellow_duration = 0
        else:
            yellow_duration = int(yellow_duration_raw)
        phases_data = p.get("phases", [])
        phases: List[SignalPhaseDef] = []
        sum_dur = 0
        conflicts_raw = p.get("pedestrian_conflicts")
        conflicts = PedestrianConflictConfig(left=False, right=False)
        conflicts_required = kind in (EventKind.TEE.value, EventKind.CROSS.value)
        if conflicts_required and conflicts_raw is None:
            errors.append(
                f"[VAL] E304 pedestrian_conflicts must be provided for intersections: profile={pid} kind={kind}"
            )
        if not conflicts_required and conflicts_raw is not None:
            errors.append(
                f"[VAL] E304 pedestrian_conflicts is not allowed for midblock crossings: profile={pid} kind={kind}"
            )
        if conflicts_raw is not None:
            if not isinstance(conflicts_raw, dict):
                errors.append(
                    f"[VAL] E304 pedestrian_conflicts must be object: profile={pid} kind={kind} value={conflicts_raw!r}"
                )
            else:
                missing = [key for key in ("left", "right") if key not in conflicts_raw]
                if missing:
                    errors.append(
                        f"[VAL] E304 pedestrian_conflicts missing keys={missing}: profile={pid} kind={kind}"
                    )
                left_raw = conflicts_raw.get("left")
                right_raw = conflicts_raw.get("right")
                type_errors = [
                    name
                    for name, val in (("left", left_raw), ("right", right_raw))
                    if name not in missing and not isinstance(val, bool)
                ]
                if type_errors:
                    errors.append(
                        f"[VAL] E304 pedestrian_conflicts keys must be boolean: profile={pid} kind={kind} keys={type_errors}"
                    )
                conflicts = PedestrianConflictConfig(
                    left=left_raw if isinstance(left_raw, bool) else False,
                    right=right_raw if isinstance(right_raw, bool) else False,
                )
        for ph in phases_data:
            dur = int(ph["duration_s"])
            amv_list = list(ph.get("allow_movements", []))
            bad = [m for m in amv_list if not MOVEMENT_RE.match(str(m))]
            if bad:
                errors.append(f"[VAL] E301 invalid movement token(s) in profile={pid} kind={kind}: {bad}")
            phases.append(SignalPhaseDef(duration_s=dur, allow_movements=[str(m) for m in amv_list]))
            sum_dur += dur
        if ped_early_cutoff < 0 or ped_early_cutoff >= cycle:
            errors.append(
                "[VAL] E305 ped_early_cutoff_s must satisfy 0 ≤ value < cycle: "
                f"profile={pid} kind={kind} cycle_s={cycle} value={ped_early_cutoff}"
            )
        if yellow_duration_raw is not None:
            if yellow_duration <= 0:
                errors.append(
                    "[VAL] E306 yellow_duration_s must be positive when provided: "
                    f"profile={pid} kind={kind} value={yellow_duration}"
                )
        if yellow_duration >= cycle:
            errors.append(
                "[VAL] E306 yellow_duration_s must leave room within cycle: "
                f"profile={pid} kind={kind} cycle_s={cycle} value={yellow_duration}"
            )
        effective_cycle = sum_dur
        if effective_cycle != cycle:
            errors.append(
                "[VAL] E302 cycle mismatch in profile="
                f"{pid} kind={kind}: sum(phases)={sum_dur} != cycle_s={cycle}"
            )
        prof = SignalProfileDef(
            id=pid,
            cycle_s=cycle,
            ped_early_cutoff_s=ped_early_cutoff,
            yellow_duration_s=yellow_duration,
            phases=phases,
            kind=EventKind(kind),
            pedestrian_conflicts=conflicts,
        )
        if pid in profiles_by_kind[kind]:
            errors.append(f"[VAL] E303 duplicate signal_profile id within kind: id={pid} kind={kind}")
        else:
            profiles_by_kind[kind][pid] = prof

    for kind in (EventKind.TEE.value, EventKind.CROSS.value, EventKind.XWALK_MIDBLOCK.value):
        arr = sp_root.get(kind, [])
        if not isinstance(arr, list):
            continue
        for i, p in enumerate(arr):
            add_profile(kind, p, i)

    if errors:
        for e in errors:
            LOG.error(e)
        raise SemanticValidationError(f"signal_profiles validation failed with {len(errors)} error(s)")
    LOG.info(
        "signal_profiles: tee=%d cross=%d xwalk_midblock=%d",
        len(profiles_by_kind[EventKind.TEE.value]),
        len(profiles_by_kind[EventKind.CROSS.value]),
        len(profiles_by_kind[EventKind.XWALK_MIDBLOCK.value]),
    )
    return profiles_by_kind


def parse_layout_events(spec_json: Dict, snap_rule: SnapRule, main_road: MainRoadConfig) -> List[LayoutEvent]:
    events: List[LayoutEvent] = []
    length = float(main_road.length_m)
    grid_max = grid_upper_bound(length, snap_rule.step_m)
    LOG.info("snap grid: step=%d, grid_max=%d (length=%.3f)", snap_rule.step_m, grid_max, length)

    for e in spec_json.get("layout", []):
        event_type = e["type"]
        if event_type not in (
            EventKind.TEE.value,
            EventKind.CROSS.value,
            EventKind.XWALK_MIDBLOCK.value,
        ):
            LOG.warning("unknown event type: %s (skip)", event_type)
            continue
        pos_raw = float(e["pos_m"])
        if not (0.0 <= pos_raw <= length):
            LOG.warning("event out of range: type=%s pos_m=%.3f (skip)", event_type, pos_raw)
            continue
        pos_snapped = round_position(pos_raw, snap_rule.step_m, snap_rule.tie_break)
        if not (0 <= pos_snapped <= grid_max):
            LOG.warning(
                "snapped position out of grid range: type=%s raw=%.3f -> snap=%d valid=[0,%d] (skip)",
                event_type,
                pos_raw,
                pos_snapped,
                grid_max,
            )
            continue

        if event_type in (EventKind.TEE.value, EventKind.CROSS.value):
            tpl_raw = e.get("template")
            template_id = str(tpl_raw) if tpl_raw is not None else None
            branch_raw = e.get("branch") if event_type == EventKind.TEE.value else None
            branch = None
            if isinstance(branch_raw, str):
                branch_lower = branch_raw.lower()
                if branch_lower in (SideMinor.NORTH.value, SideMinor.SOUTH.value):
                    branch = SideMinor(branch_lower)
            main_u_turn_allowed = bool(e["main_u_turn_allowed"])
            layout_event = LayoutEvent(
                type=EventKind(event_type),
                pos_m_raw=pos_raw,
                pos_m=pos_snapped,
                template_id=template_id,
                signalized=bool(e.get("signalized")),
                signal=parse_signal_ref(e.get("signal")),
                main_ped_crossing_placement=e.get("main_ped_crossing_placement"),
                branch=branch,
                main_u_turn_allowed=main_u_turn_allowed,
                refuge_island_on_main=bool(e.get("refuge_island_on_main")),
                two_stage_tll_control=bool(e.get("two_stage_tll_control")),
            )
        else:
            layout_event = LayoutEvent(
                type=EventKind.XWALK_MIDBLOCK,
                pos_m_raw=pos_raw,
                pos_m=pos_snapped,
                signalized=bool(e.get("signalized")),
                signal=parse_signal_ref(e.get("signal")),
                refuge_island_on_main=bool(e.get("refuge_island_on_main")),
                two_stage_tll_control=bool(e.get("two_stage_tll_control")),
            )
        events.append(layout_event)
        LOG.info("layout: %s raw=%.3f -> snap=%d", event_type, pos_raw, pos_snapped)
    return events


def build_clusters(layout_events: List[LayoutEvent]) -> List[Cluster]:
    clusters: List[Cluster] = []
    for pos, group in itertools.groupby(sorted(layout_events, key=lambda ev: ev.pos_m), key=lambda ev: ev.pos_m):
        clusters.append(Cluster(pos_m=pos, events=list(group)))
    LOG.info("clusters: %d", len(clusters))
    return clusters


def load_specification(spec_path: Path, schema_path: Path) -> CorridorSpec:
    spec_json = load_json_file(spec_path)
    schema_json = load_schema_file(schema_path)
    validate_json_schema(spec_json, schema_json)
    ensure_supported_version(spec_json)
    snap_rule = parse_snap_rule(spec_json)
    defaults = parse_defaults(spec_json)
    main_road = parse_main_road(spec_json)
    junction_templates = parse_junction_templates(spec_json)
    signal_profiles = parse_signal_profiles(spec_json)
    layout_events = parse_layout_events(spec_json, snap_rule, main_road)
    return CorridorSpec(
        version=str(spec_json.get("version")),
        snap=snap_rule,
        defaults=defaults,
        main_road=main_road,
        junction_templates=junction_templates,
        signal_profiles=signal_profiles,
        layout=layout_events,
    )
