"""PlainXML tlLogic emission."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from ..builder.ids import cluster_id
from ..domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    LayoutEvent,
    MainRoadConfig,
    PedestrianConflictConfig,
    SignalLink,
    SignalProfileDef,
    SnapRule,
)
from ..utils.logging import get_logger

LOG = get_logger()


@dataclass(frozen=True)
class MovementInfo:
    """Metadata describing a controllable signal element."""

    name: str
    kind: str  # "vehicle" or "pedestrian"
    group: Optional[str] = None
    direction: Optional[str] = None
    turn: Optional[str] = None
    ped_category: Optional[str] = None
    ped_side: Optional[str] = None
    ped_direction: Optional[str] = None
    ped_branch: Optional[str] = None


@dataclass(frozen=True)
class PedestrianConflicts:
    """Conflicting vehicle movements for a pedestrian signal."""

    entry: Set[str]
    left: Set[str]
    right: Set[str]


@dataclass
class TlContext:
    """Resolved information required to render a traffic light."""

    tl_id: str
    profile: SignalProfileDef
    offset: int
    events: List[LayoutEvent]
    movements: List[MovementInfo]

    def two_stage_flag(self, category: str) -> bool:
        """Return the two-stage flag for the requested pedestrian category."""

        two_stage = False
        for event in self.events:
            if category == "main" and event.type in (EventKind.TEE, EventKind.CROSS):
                two_stage = two_stage or bool(event.two_stage_tll_control)
            elif category == "mid" and event.type == EventKind.XWALK_MIDBLOCK:
                two_stage = two_stage or bool(event.two_stage_tll_control)
        return two_stage


def _movement_info_from_link(link: SignalLink) -> MovementInfo:
    parts = link.movement.split("_")
    if link.kind == "crossing":
        ped_category = parts[1] if len(parts) > 1 else None
        ped_side = parts[2] if len(parts) > 2 else None
        ped_direction = parts[3] if len(parts) > 3 else None
        ped_branch = parts[2] if ped_category == "minor" and len(parts) > 2 else None
        return MovementInfo(
            name=link.movement,
            kind="pedestrian",
            ped_category=ped_category,
            ped_side=ped_side,
            ped_direction=ped_direction,
            ped_branch=ped_branch,
        )

    group = parts[0] if parts else None
    direction: Optional[str] = None
    turn = parts[-1] if parts else None

    if group in {"main", "minor"}:
        direction = parts[1] if len(parts) >= 3 else None
    elif group in {"EB", "WB"}:
        direction = group
        group = "main"
    elif group in {"N", "S"}:
        direction = group
        group = "minor"
    elif group == "pedestrian":
        return MovementInfo(name=link.movement, kind="pedestrian")

    return MovementInfo(
        name=link.movement,
        kind="vehicle",
        group=group,
        direction=direction,
        turn=turn,
    )


def _group_links_by_tl(links: Sequence[SignalLink]) -> Dict[str, List[SignalLink]]:
    grouped: Dict[str, List[SignalLink]] = defaultdict(list)
    for link in links:
        grouped[link.tl_id].append(link)
    for items in grouped.values():
        items.sort(key=lambda item: item.slot_index)
    return grouped


def _cluster_by_id(clusters: Sequence[Cluster]) -> Dict[str, Cluster]:
    return {cluster_id(cluster.pos_m): cluster for cluster in clusters}


def _resolve_profile(event: LayoutEvent, profiles: Dict[str, SignalProfileDef]) -> Optional[SignalProfileDef]:
    if not event.signalized or not event.signal:
        return None
    profile = profiles.get(event.signal.profile_id)
    if not profile:
        LOG.error(
            "[VAL] E500 tlLogic missing profile: tl=%s profile_id=%s kind=%s",
            cluster_id(event.pos_m),
            event.signal.profile_id if event.signal else None,
            event.type.value,
        )
    return profile


def _select_movements(
    movements: Iterable[MovementInfo],
    *,
    group: Optional[str] = None,
    direction: Optional[str] = None,
    turn: Optional[str] = None,
) -> Set[str]:
    selected: Set[str] = set()
    for info in movements:
        if info.kind != "vehicle":
            continue
        if group and info.group != group:
            continue
        if direction and info.direction != direction:
            continue
        if turn and info.turn != turn:
            continue
        selected.add(info.name)
    return selected


def _build_ped_conflicts(
    movements: Iterable[MovementInfo],
    ped: MovementInfo,
) -> PedestrianConflicts:
    entry: Set[str] = set()
    left: Set[str] = set()
    right: Set[str] = set()

    if ped.ped_category == "main":
        if ped.ped_direction:
            entry = _select_movements(movements, group="main", direction=ped.ped_direction)
        else:
            entry = _select_movements(movements, group="main")
    elif ped.ped_category == "minor":
        branch = ped.ped_branch.upper() if ped.ped_branch else None
        if branch:
            entry = _select_movements(movements, group="minor", direction=branch[0])
        if ped.ped_branch == "north":
            left = _select_movements(movements, group="main", direction="EB", turn="L")
            right = _select_movements(movements, group="main", direction="WB", turn="R")
        elif ped.ped_branch == "south":
            left = _select_movements(movements, group="main", direction="WB", turn="L")
            right = _select_movements(movements, group="main", direction="EB", turn="R")
    elif ped.ped_category == "mid":
        if ped.ped_direction:
            entry = _select_movements(movements, group="main", direction=ped.ped_direction)
        else:
            entry = _select_movements(movements, group="main")

    return PedestrianConflicts(entry=entry, left=left, right=right)


def _expand_token(
    token: str,
    movements: Iterable[MovementInfo],
) -> Set[str]:
    if token == "pedestrian":
        return {info.name for info in movements if info.kind == "pedestrian"}

    if "_" not in token:
        return set()

    prefix, turn = token.rsplit("_", 1)
    selected: Set[str] = set()

    if prefix == "main":
        selected = _select_movements(movements, group="main", turn=turn)
    elif prefix == "minor":
        selected = _select_movements(movements, group="minor", turn=turn)
    elif prefix in {"EB", "WB"}:
        selected = _select_movements(movements, group="main", direction=prefix, turn=turn)
    else:
        selected = {info.name for info in movements if info.name == token}

    return selected


def _phase_timeline(
    profile: SignalProfileDef,
    movements: Iterable[MovementInfo],
) -> List[Dict[str, Set[str]]]:
    timeline: List[Dict[str, Set[str]]] = []
    movement_list = list(movements)
    for phase in profile.phases:
        allowed: Set[str] = set()
        ped_allowed = False
        for token in phase.allow_movements:
            if token == "pedestrian":
                ped_allowed = True
                continue
            allowed.update(_expand_token(token, movement_list))
        for _ in range(phase.duration_s):
            timeline.append({"vehicles": set(allowed), "ped_allowed": ped_allowed})
    return timeline


def _build_tl_contexts(
    clusters: Sequence[Cluster],
    grouped_links: Dict[str, List[SignalLink]],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
) -> List[TlContext]:
    cluster_map = _cluster_by_id(clusters)
    contexts: List[TlContext] = []
    for tl_id, links in grouped_links.items():
        cluster = cluster_map.get(tl_id)
        if not cluster:
            LOG.warning("[VAL] E501 tlLogic cluster missing: tl=%s", tl_id)
            continue

        signal_events = [event for event in cluster.events if event.signalized]
        if not signal_events:
            continue

        profile: Optional[SignalProfileDef] = None
        offset = 0
        resolved_events: List[LayoutEvent] = []
        for event in signal_events:
            profiles = signal_profiles_by_kind.get(event.type.value, {})
            prof = _resolve_profile(event, profiles)
            if not prof:
                continue
            if profile and profile.id != prof.id:
                LOG.error(
                    "[VAL] E502 multiple profiles mapped to tlLogic: tl=%s first=%s other=%s",
                    tl_id,
                    profile.id,
                    prof.id,
                )
                continue
            profile = prof
            offset = event.signal.offset_s if event.signal else 0
            resolved_events.append(event)

        if not profile:
            continue

        movements = [_movement_info_from_link(link) for link in links]
        contexts.append(
            TlContext(
                tl_id=tl_id,
                profile=profile,
                offset=offset,
                events=resolved_events,
                movements=movements,
            )
        )
    return contexts


def _ensure_two_stage_consistency(
    ped_states: Dict[str, List[str]],
    ped_infos: Dict[str, MovementInfo],
    context: TlContext,
    cycle: int,
) -> None:
    groups: Dict[Tuple[str, Optional[str]], List[str]] = defaultdict(list)
    for name, info in ped_infos.items():
        if info.ped_category in {"main", "mid"} and info.ped_direction:
            key = (info.ped_category, info.ped_side)
            groups[key].append(name)

    for (category, _), names in groups.items():
        if len(names) <= 1:
            continue
        if context.two_stage_flag(category):
            continue
        for second in range(cycle):
            char = "G" if all(ped_states[name][second] == "G" for name in names) else "r"
            for name in names:
                ped_states[name][second] = char


def _apply_yellow(states: List[str], yellow: int) -> None:
    if yellow <= 0 or not states:
        return

    def is_green(char: str) -> bool:
        return char in {"G", "g"}

    length = len(states)
    idx = 0
    while idx < length:
        if not is_green(states[idx]):
            idx += 1
            continue
        start = idx
        while idx < length and is_green(states[idx]):
            idx += 1
        end = idx
        if end < length:
            convert = min(yellow, end - start)
            for pos in range(end - convert, end):
                states[pos] = "y"
        else:
            if is_green(states[0]):
                break
            convert = min(yellow, end - start)
            for pos in range(length - convert, length):
                states[pos] = "y"


def _apply_ped_cutoff(states: List[str], cutoff: int) -> None:
    if cutoff <= 0 or not states:
        return

    length = len(states)
    idx = 0
    while idx < length:
        if states[idx] != "G":
            idx += 1
            continue
        start = idx
        while idx < length and states[idx] == "G":
            idx += 1
        end = idx
        if end < length:
            convert = min(cutoff, end - start)
            for pos in range(end - convert, end):
                states[pos] = "r"
        else:
            if states[0] == "G":
                break
            convert = min(cutoff, end - start)
            for pos in range(length - convert, length):
                states[pos] = "r"


def _timeline_per_second(
    context: TlContext,
) -> Dict[str, List[str]]:
    cycle = context.profile.cycle_s
    timeline = _phase_timeline(context.profile, context.movements)
    allowed_vehicles = [entry["vehicles"] for entry in timeline]
    ped_allowed = [bool(entry["ped_allowed"]) for entry in timeline]

    ped_infos = {info.name: info for info in context.movements if info.kind == "pedestrian"}
    vehicle_infos = {info.name: info for info in context.movements if info.kind == "vehicle"}

    ped_states = {name: ["r"] * cycle for name in ped_infos}
    conflicts = {name: _build_ped_conflicts(context.movements, ped_infos[name]) for name in ped_infos}
    ped_conf = context.profile.pedestrian_conflicts or PedestrianConflictConfig(False, False)

    for second in range(cycle):
        if not ped_allowed[second]:
            continue
        active = allowed_vehicles[second]
        for name, info in ped_infos.items():
            conflict = conflicts[name]
            if conflict.entry & active:
                continue
            if (conflict.left & active) and not ped_conf.left:
                continue
            if (conflict.right & active) and not ped_conf.right:
                continue
            ped_states[name][second] = "G"

    for states in ped_states.values():
        _apply_ped_cutoff(states, context.profile.ped_early_cutoff_s)

    _ensure_two_stage_consistency(ped_states, ped_infos, context, cycle)

    vehicle_states = {name: ["r"] * cycle for name in vehicle_infos}
    for second in range(cycle):
        active = allowed_vehicles[second]
        for name in active:
            if name in vehicle_states:
                vehicle_states[name][second] = "G"

    for second in range(cycle):
        active = allowed_vehicles[second]
        for name, conflict in conflicts.items():
            if ped_states[name][second] != "G":
                continue
            if ped_conf.left:
                for movement in conflict.left & active:
                    if movement in vehicle_states:
                        vehicle_states[movement][second] = "g"
            if ped_conf.right:
                for movement in conflict.right & active:
                    if movement in vehicle_states:
                        vehicle_states[movement][second] = "g"

    for states in vehicle_states.values():
        _apply_yellow(states, context.profile.yellow_duration_s)

    return {**vehicle_states, **ped_states}


def _state_sequences(
    context: TlContext,
) -> List[Tuple[int, str]]:
    cycle = context.profile.cycle_s
    states = _timeline_per_second(context)
    slot_order = [info.name for info in context.movements]

    per_second: List[str] = []
    for second in range(cycle):
        chars = []
        for name in slot_order:
            timeline = states.get(name, ["r"] * cycle)
            chars.append(timeline[second] if second < len(timeline) else "r")
        per_second.append("".join(chars))

    phases: List[Tuple[int, str]] = []
    if not per_second:
        return phases

    current = per_second[0]
    duration = 1
    for state in per_second[1:]:
        if state == current:
            duration += 1
        else:
            phases.append((duration, current))
            current = state
            duration = 1
    phases.append((duration, current))
    return phases


def _emit_connection_refs(links: Sequence[SignalLink]) -> List[str]:
    lines: List[str] = []
    for link in links:
        descriptor = link.element_id.replace("\"", "&quot;")
        lines.append(
            f'    <connection linkIndex="{link.link_index}" movement="{link.movement}" ref="{descriptor}"/>'
        )
    return lines


def render_tll_xml(
    *,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    lane_overrides: Sequence[LaneOverride],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
    connection_links: Sequence[SignalLink],
) -> str:
    del defaults, breakpoints, junction_template_by_id, snap_rule, main_road, lane_overrides

    grouped_links = _group_links_by_tl(connection_links)
    contexts = _build_tl_contexts(clusters, grouped_links, signal_profiles_by_kind)

    if not contexts:
        LOG.info("rendered tlLogics (0 tls)")
        return "<tlLogics/>\n"

    lines: List[str] = ["<tlLogics>"]
    for context in sorted(contexts, key=lambda item: item.tl_id):
        lines.append(
            f'  <tlLogic id="{context.tl_id}" type="static" programID="0" offset="{context.offset}">'
        )
        phases = _state_sequences(context)
        for duration, state in phases:
            lines.append(f'    <phase duration="{duration}" state="{state}"/>')
        lines.extend(_emit_connection_refs(grouped_links[context.tl_id]))
        lines.append("  </tlLogic>")
    lines.append("</tlLogics>")

    xml = "\n".join(lines) + "\n"
    LOG.info("rendered tlLogics (%d tls)", len(contexts))
    return xml

