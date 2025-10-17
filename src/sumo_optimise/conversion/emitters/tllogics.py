"""PlainXML ``<tlLogics>`` emission derived from signal profiles."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from ..domain.models import Cluster, SignalPhaseDef, SignalProfileDef
from ..utils.logging import get_logger
from ..utils.signals import cluster_has_signal_reference
from .connections import ClusterLinkIndexing, LinkIndexEntry

LOG = get_logger()


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def _active_indices(allowed: Set[str], links: Iterable[LinkIndexEntry]) -> Set[int]:
    indices: Set[int] = set()
    for entry in links:
        if any(token in allowed for token in entry.tokens):
            indices.add(entry.link_index)
    return indices


def _vehicle_state_char(entry: LinkIndexEntry, is_active: bool) -> str:
    """Return the SUMO state character for a vehicle link."""

    if not is_active:
        return "r"
    return "G" if entry.movement == "T" else "g"


def _pedestrian_forced_red(
    entry: LinkIndexEntry,
    profile: SignalProfileDef,
    turn_left_active: bool,
    turn_right_active: bool,
    main_left_active: bool,
    main_right_active: bool,
    main_direction_straight: Dict[str, bool],
    main_direction_turns: Dict[str, Dict[str, bool]],
) -> bool:
    """Determine whether a pedestrian link must be forced red."""

    if not entry.crossing:
        return False

    if entry.crossing.lane_directions:
        if entry.crossing.two_stage:
            direction = entry.crossing.lane_directions[0]
            straight_active = main_direction_straight.get(direction, False)
            if straight_active:
                return True
            turns = main_direction_turns.get(direction, {})
            left_active_dir = turns.get("L", False)
            right_active_dir = turns.get("R", False)
            if direction == "EB":
                if right_active_dir and not profile.pedestrian_conflicts.right:
                    return True
                if left_active_dir and not profile.pedestrian_conflicts.left:
                    return True
            else:
                if right_active_dir and not profile.pedestrian_conflicts.right:
                    return True
                if left_active_dir and not profile.pedestrian_conflicts.left:
                    return True
            return False
        if main_direction_straight.get("EB", False) or main_direction_straight.get("WB", False):
            return True
        left_conflict_active = False
        right_conflict_active = False
    else:
        left_conflict_active = turn_left_active
        right_conflict_active = turn_right_active

    forced_red = False
    if "left" in entry.conflicts_with and not profile.pedestrian_conflicts.left and left_conflict_active:
        forced_red = True
    if "right" in entry.conflicts_with and not profile.pedestrian_conflicts.right and right_conflict_active:
        forced_red = True
    return forced_red


def _pedestrian_state_char(
    entry: LinkIndexEntry,
    is_active: bool,
    profile: SignalProfileDef,
    turn_left_active: bool,
    turn_right_active: bool,
    main_left_active: bool,
    main_right_active: bool,
    main_direction_straight: Dict[str, bool],
    main_direction_turns: Dict[str, Dict[str, bool]],
) -> str:
    """Return the SUMO state character for a pedestrian link."""

    if not is_active:
        return "r"
    forced_red = _pedestrian_forced_red(
        entry,
        profile,
        turn_left_active,
        turn_right_active,
        main_left_active,
        main_right_active,
        main_direction_straight,
        main_direction_turns,
    )
    return "r" if forced_red else "G"


def _phase_state_chars(
    phase: SignalPhaseDef, indexing: ClusterLinkIndexing, profile: SignalProfileDef
) -> List[str]:
    """Return the base state characters for the given phase."""

    allowed = set(phase.allow_movements)
    active = _active_indices(allowed, indexing.links)

    turn_left_active = False
    turn_right_active = False
    main_left_active = False
    main_right_active = False
    main_direction_straight = {"EB": False, "WB": False}
    main_direction_turns: Dict[str, Dict[str, bool]] = {
        "EB": {"L": False, "R": False},
        "WB": {"L": False, "R": False},
    }

    for entry in indexing.links:
        if entry.kind != "vehicle" or entry.link_index not in active:
            continue
        if entry.movement == "L":
            turn_left_active = True
        if entry.movement == "R":
            turn_right_active = True
        conn = entry.connection
        if conn and conn.approach.road == "main":
            if conn.approach.direction == "EB":
                main_right_active = True
                if entry.movement == "T":
                    main_direction_straight["EB"] = True
                elif entry.movement == "L":
                    main_direction_turns["EB"]["L"] = True
                elif entry.movement in {"R", "U"}:
                    main_direction_turns["EB"]["R"] = True
            elif conn.approach.direction == "WB":
                main_left_active = True
                if entry.movement == "T":
                    main_direction_straight["WB"] = True
                elif entry.movement == "L":
                    main_direction_turns["WB"]["L"] = True
                elif entry.movement in {"R", "U"}:
                    main_direction_turns["WB"]["R"] = True

    chars: List[str] = []
    for entry in indexing.links:
        is_active = entry.link_index in active
        if entry.kind == "vehicle":
            chars.append(_vehicle_state_char(entry, is_active))
        else:
            chars.append(
                _pedestrian_state_char(
                    entry,
                    is_active,
                    profile,
                    turn_left_active,
                    turn_right_active,
                    main_left_active,
                    main_right_active,
                    main_direction_straight,
                    main_direction_turns,
                )
            )
    return chars


def _apply_end_of_phase_modifiers(
    duration: int,
    base_chars: Sequence[str],
    next_chars: Sequence[str],
    indexing: ClusterLinkIndexing,
    profile: SignalProfileDef,
) -> List[Tuple[int, str]]:
    """Split a phase to introduce yellow and pedestrian early-red segments."""

    if duration <= 0:
        return []

    modifier_map: Dict[int, Dict[int, str]] = defaultdict(dict)

    yellow_duration = profile.yellow_duration_s
    if yellow_duration > 0:
        indices: List[int] = []
        for idx, entry in enumerate(indexing.links):
            if entry.kind != "vehicle":
                continue
            curr = base_chars[idx]
            next_char = next_chars[idx]
            if curr in {"G", "g"} and next_char not in {"G", "g"}:
                indices.append(idx)
        if indices:
            clamped = min(yellow_duration, duration)
            if clamped > 0:
                offset = clamped
                modifier_map[offset].update({idx: "y" for idx in indices})

    ped_offset = profile.ped_red_offset_s
    if ped_offset > 0:
        for idx, entry in enumerate(indexing.links):
            if entry.kind != "pedestrian":
                continue
            curr = base_chars[idx]
            next_char = next_chars[idx]
            if curr == "G" and next_char != "G":
                clamped = min(ped_offset, duration)
                if clamped > 0:
                    modifier_map[clamped][idx] = "r"

    if not modifier_map:
        return [(duration, "".join(base_chars))]

    events: List[Tuple[int, Dict[int, str]]] = []
    for offset, changes in modifier_map.items():
        start_time = max(duration - offset, 0)
        events.append((start_time, changes))
    events.sort(key=lambda item: item[0])

    segments: List[Tuple[int, str]] = []
    current_state = list(base_chars)
    last_time = 0

    for start_time, changes in events:
        if start_time > duration:
            continue
        if start_time > last_time:
            segments.append((start_time - last_time, "".join(current_state)))
            last_time = start_time
        for idx, value in changes.items():
            current_state[idx] = value

    if duration > last_time:
        segments.append((duration - last_time, "".join(current_state)))

    return [(seg_duration, state) for seg_duration, state in segments if seg_duration > 0]


def _merge_segments(segments: Iterable[Tuple[int, str]]) -> List[Tuple[int, str]]:
    merged: List[Tuple[int, str]] = []
    for duration, state in segments:
        if duration <= 0:
            continue
        if merged and merged[-1][1] == state:
            prev_duration, _ = merged[-1]
            merged[-1] = (prev_duration + duration, state)
        else:
            merged.append((duration, state))
    return merged


def _build_timeline(
    profile: SignalProfileDef,
    indexing: ClusterLinkIndexing,
) -> List[Tuple[int, str]]:
    """Expand a profile into SUMO phases including yellow/pedestrian offsets."""

    if not profile.phases:
        return []

    base_states: List[List[str]] = [
        _phase_state_chars(phase, indexing, profile) for phase in profile.phases
    ]

    segments: List[Tuple[int, str]] = []
    phase_count = len(profile.phases)

    for idx, phase in enumerate(profile.phases):
        duration = phase.duration_s
        base_chars = base_states[idx]
        next_chars = base_states[(idx + 1) % phase_count]
        segments.extend(
            _apply_end_of_phase_modifiers(duration, base_chars, next_chars, indexing, profile)
        )

    return _merge_segments(segments)


def render_tllogics_xml(
    clusters: List[Cluster],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
    link_indexing: Dict[int, ClusterLinkIndexing],
) -> str:
    """Render ``<tlLogics>`` based on signal references attached to clusters."""

    lines: List[str] = ["<tlLogics>"]
    connection_lines: List[str] = []

    rendered = 0

    for cluster in clusters:
        if not cluster_has_signal_reference(cluster):
            continue
        tl_events = [
            ev
            for ev in cluster.events
            if ev.signalized is True and ev.signal is not None and ev.type.value in signal_profiles_by_kind
        ]
        if not tl_events:
            continue
        index_info = link_indexing.get(cluster.pos_m)
        if index_info is None:
            LOG.warning("[BUILD] signalised cluster lacks connection index mapping: pos=%s", cluster.pos_m)
            continue
        tl_id = index_info.tl_id
        for event in tl_events:
            signal_ref = event.signal
            if signal_ref is None:
                continue
            profiles = signal_profiles_by_kind.get(event.type.value, {})
            profile = profiles.get(signal_ref.profile_id)
            if profile is None:
                LOG.warning(
                    "[BUILD] missing signal profile for tlLogics emission: cluster=%s profile_id=%s kind=%s",
                    tl_id,
                    signal_ref.profile_id,
                    event.type.value,
                )
                continue
            lines.append(
                f'  <tlLogic id="{tl_id}" type="static" programID="{profile.id}" offset="{signal_ref.offset_s}">'  # noqa: E501
            )
            lines.append(f"    <param key=\"event_kind\" value=\"{event.type.value}\"/>")
            lines.append(f'    <param key="cycle_s" value="{profile.cycle_s}"/>')
            lines.append(f'    <param key="ped_red_offset_s" value="{profile.ped_red_offset_s}"/>')
            lines.append(f'    <param key="yellow_duration_s" value="{profile.yellow_duration_s}"/>')
            conflicts = profile.pedestrian_conflicts
            lines.append(f'    <param key="pedestrian_conflicts.left" value="{_bool_str(conflicts.left)}"/>')
            lines.append(f'    <param key="pedestrian_conflicts.right" value="{_bool_str(conflicts.right)}"/>')
            timeline = _build_timeline(profile, index_info)
            for duration, state in timeline:
                lines.append(f'    <phase duration="{duration}" state="{state}"/>')
            lines.append("  </tlLogic>")
            rendered += 1

    for pos in sorted(link_indexing):
        idx = link_indexing[pos]
        for entry in idx.links:
            if entry.kind == "vehicle" and entry.connection is not None:
                conn = entry.connection
                LOG.info(
                    "[BUILD] tl connection mapping: tl=%s tokens=%s movement=%s linkIndex=%d from=%s lane=%d to=%s lane=%d",
                    idx.tl_id,
                    ",".join(entry.tokens),
                    entry.movement,
                    entry.link_index,
                    conn.from_edge,
                    conn.from_lane,
                    conn.to_edge,
                    conn.to_lane,
                )
                connection_lines.append(
                    f'  <connection from="{conn.from_edge}" to="{conn.to_edge}" '
                    f'fromLane="{conn.from_lane}" toLane="{conn.to_lane}" '
                    f'tl="{idx.tl_id}" linkIndex="{entry.link_index}"/>'
                )

    lines.extend(connection_lines)
    lines.append("</tlLogics>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered tlLogics (%d logic(s))", rendered)
    return xml


__all__ = ["render_tllogics_xml"]
