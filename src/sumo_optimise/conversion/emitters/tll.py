"""Emission of ``1-generated.tll.xml`` traffic light programmes."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set

from ..builder.ids import cluster_id
from ..domain.models import (
    Cluster,
    Defaults,
    LaneOverride,
    MainRoadConfig,
    PedestrianConflictConfig,
    ControlledConnection,
    SignalLink,
    SignalProfileDef,
    SnapRule,
)


PEDESTRIAN_PREFIX = "ped_"


@dataclass(frozen=True)
class _TlProgram:
    tl_id: str
    profile: SignalProfileDef
    offset: int
    refuge_island_on_main: bool
    two_stage_tll_control: bool


def _group_links_by_tl(links: Iterable[SignalLink]) -> Dict[str, List[SignalLink]]:
    grouped: Dict[str, List[SignalLink]] = defaultdict(list)
    for link in links:
        grouped[link.tl_id].append(link)
    for arr in grouped.values():
        arr.sort(key=lambda item: item.slot_index)
    return grouped


def _group_controlled_connections(
    connections: Iterable[ControlledConnection],
) -> Dict[str, List[ControlledConnection]]:
    grouped: Dict[str, List[ControlledConnection]] = defaultdict(list)
    for conn in connections:
        grouped[conn.tl_id].append(conn)
    for arr in grouped.values():
        arr.sort(key=lambda item: item.link_index)
    return grouped


def _collect_programs(
    clusters: Sequence[Cluster],
    signal_profiles_by_kind: Mapping[str, Mapping[str, SignalProfileDef]],
) -> Dict[str, _TlProgram]:
    programs: Dict[str, _TlProgram] = {}
    for cluster in clusters:
        tl_id = cluster_id(cluster.pos_m)
        for event in cluster.events:
            if not bool(event.signalized):
                continue
            if not event.signal:
                continue
            profile_map = signal_profiles_by_kind.get(event.type.value)
            if not profile_map:
                continue
            profile = profile_map.get(event.signal.profile_id)
            if not profile:
                continue
            programs[tl_id] = _TlProgram(
                tl_id=tl_id,
                profile=profile,
                offset=event.signal.offset_s,
                refuge_island_on_main=bool(event.refuge_island_on_main),
                two_stage_tll_control=bool(event.two_stage_tll_control),
            )
    return programs


def _expand_vehicle_movements(
    tokens: Iterable[str],
    available_movements: Sequence[str],
) -> Set[str]:
    veh_movements = {mv for mv in available_movements if not mv.startswith(PEDESTRIAN_PREFIX)}
    expanded: Set[str] = set()
    for token in tokens:
        if token == "pedestrian":
            continue
        for movement in veh_movements:
            parts = movement.split("_")
            if len(parts) < 3:
                continue
            prefix, direction, turn = parts[0], parts[1], parts[2]
            if token == movement:
                expanded.add(movement)
                continue
            if token.startswith(prefix) and token.endswith(turn) and len(token.split("_")) == 2:
                # main_L/main_T/main_R, minor_L/minor_T/minor_R
                expanded.add(movement)
                continue
            if prefix == "main" and token == f"{direction}_{turn}":
                expanded.add(movement)
                continue

    if expanded:
        for movement in list(expanded):
            if not movement.startswith("main_"):
                continue
            if not movement.endswith("_R"):
                continue
            prefix = movement.rsplit("_", 1)[0]
            u_turn = f"{prefix}_U"
            if u_turn in veh_movements:
                expanded.add(u_turn)
    return expanded


def _ped_matches(pattern: str, ped_name: str) -> bool:
    if pattern.endswith("*"):
        prefix = pattern[:-1]
        return ped_name.startswith(prefix)
    return pattern == ped_name


def _ped_base(name: str) -> str:
    for suffix in ("_EB", "_WB", "_north", "_south"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


_SEGMENT_TO_PEDS: Mapping[str, Set[str]] = {
    "S-W": {"ped_minor_south"},
    "S-E": {"ped_minor_south"},
    "E-S": {"ped_main_east_south"},
    "E-N": {"ped_main_east_north"},
    "N-E": {"ped_minor_north"},
    "N-W": {"ped_minor_north"},
    "W-N": {"ped_main_west_north"},
    "W-S": {"ped_main_west_south"},
}


_MOVEMENT_SEGMENT_STATUS: Mapping[str, Mapping[str, str]] = {
    "main_EB_L": {"W-N": "never", "N-W": "left"},
    "main_EB_T": {"W-N": "never", "E-N": "never"},
    "main_EB_R": {"W-N": "never", "S-E": "right"},
    "main_EB_U": {"W-N": "never", "W-S": "right"},
    "main_WB_L": {"E-S": "never", "S-E": "left"},
    "main_WB_T": {"E-S": "never", "W-S": "never"},
    "main_WB_R": {"E-S": "never", "N-W": "right"},
    "main_WB_U": {"E-S": "never", "E-N": "right"},
    "minor_N_L": {"N-E": "never", "E-N": "left"},
    "minor_N_T": {"N-E": "never"},
    "minor_N_R": {"N-E": "never", "W-S": "right"},
    "minor_N_U": {"N-E": "never", "N-W": "right"},
    "minor_S_L": {"S-W": "never", "W-S": "left"},
    "minor_S_T": {"S-W": "never"},
    "minor_S_R": {"S-W": "never", "E-N": "right"},
    "minor_S_U": {"S-W": "never", "S-E": "right"},
}


def _vehicle_conflicts() -> Dict[str, Dict[str, Set[str]]]:
    """Return a static conflict map between vehicle movements and crossings."""

    status_to_key = {"never": "mandatory", "left": "left", "right": "right"}

    conflicts: Dict[str, Dict[str, Set[str]]] = {}
    for movement, segment_map in _MOVEMENT_SEGMENT_STATUS.items():
        info: Dict[str, Set[str]] = {"mandatory": set(), "left": set(), "right": set()}
        for segment, status in segment_map.items():
            ped_names = _SEGMENT_TO_PEDS.get(segment)
            if not ped_names:
                continue
            target_key = status_to_key.get(status)
            if not target_key:
                continue
            for ped_name in ped_names:
                info[target_key].add(ped_name)
        conflicts[movement] = info

    for movement in ("main_EB_T", "main_WB_T"):
        info = conflicts.setdefault(movement, {"mandatory": set(), "left": set(), "right": set()})
        info["mandatory"].update({"ped_mid", "ped_mid_north", "ped_mid_south"})

    coupled_segments = {
        "ped_main_west": ("ped_main_west_north", "ped_main_west_south"),
        "ped_main_east": ("ped_main_east_north", "ped_main_east_south"),
    }
    for info in conflicts.values():
        for merged, components in coupled_segments.items():
            for key in ("mandatory", "left", "right"):
                if any(component in info[key] for component in components):
                    info[key].add(merged)

    return conflicts


_VEHICLE_CONFLICTS = _vehicle_conflicts()


def _pedestrian_allowed(
    *,
    ped_name: str,
    vehicle_movements: Iterable[str],
    ped_token_present: bool,
    conflicts: PedestrianConflictConfig,
) -> bool:
    if not ped_token_present:
        return False

    for movement in vehicle_movements:
        info = _VEHICLE_CONFLICTS.get(movement)
        if not info:
            continue
        mandatory = info.get("mandatory", set())
        if any(_ped_matches(pattern, ped_name) for pattern in mandatory):
            return False
        if not conflicts.left:
            left_patterns = info.get("left", set())
            if any(_ped_matches(pattern, ped_name) for pattern in left_patterns):
                return False
        if not conflicts.right:
            right_patterns = info.get("right", set())
            if any(_ped_matches(pattern, ped_name) for pattern in right_patterns):
                return False
    return True


def _apply_tail_substitution(
    states: MutableMapping[str, List[str]],
    *,
    yellow: int,
    ped_cutoff: int,
) -> None:
    if yellow <= 0 and ped_cutoff <= 0:
        return

    cycle = len(next(iter(states.values()))) if states else 0

    for movement, timeline in states.items():
        if movement.startswith(PEDESTRIAN_PREFIX):
            if ped_cutoff <= 0:
                continue
            for idx in range(cycle):
                nxt = timeline[(idx + 1) % cycle]
                if timeline[idx] == "G" and nxt == "r":
                    for back in range(ped_cutoff):
                        pos = (idx - back) % cycle
                        if timeline[pos] != "G":
                            break
                        timeline[pos] = "r"
            continue

        if yellow <= 0:
            continue
        for idx in range(cycle):
            nxt = timeline[(idx + 1) % cycle]
            if timeline[idx] in ("G", "g") and nxt == "r":
                for back in range(yellow):
                    pos = (idx - back) % cycle
                    if timeline[pos] not in ("G", "g"):
                        break
                    timeline[pos] = "y"


def _build_timelines(
    program: _TlProgram,
    links: Sequence[SignalLink],
) -> Dict[str, List[str]]:
    profile = program.profile
    cycle = profile.cycle_s
    if cycle <= 0:
        return {}

    movements = [link.movement for link in links]
    movement_states: Dict[str, List[str]] = {
        movement: ["r"] * cycle for movement in movements
    }

    available_movements = list(movements)
    conflicts = profile.pedestrian_conflicts
    time_cursor = 0

    for phase in profile.phases:
        tokens = list(phase.allow_movements)
        veh_movements = _expand_vehicle_movements(tokens, available_movements)
        ped_token_present = "pedestrian" in tokens

        veh_state_map: Dict[str, str] = {}
        for movement in veh_movements:
            veh_state_map[movement] = "g" if movement.endswith("_R") else "G"

        ped_state_map: Dict[str, str] = {}
        for movement in movements:
            if not movement.startswith(PEDESTRIAN_PREFIX):
                continue
            allow = _pedestrian_allowed(
                ped_name=movement,
                vehicle_movements=veh_movements,
                ped_token_present=ped_token_present,
                conflicts=conflicts,
            )
            if allow:
                ped_state_map[movement] = "G"
            else:
                ped_state_map[movement] = "r"

        if not program.two_stage_tll_control or not program.refuge_island_on_main:
            grouped: Dict[str, List[str]] = defaultdict(list)
            for name in ped_state_map:
                base = _ped_base(name)
                if base == name:
                    continue
                grouped[base].append(name)
            for names in grouped.values():
                if len(names) < 2:
                    continue
                if any(ped_state_map[name] == "r" for name in names):
                    for name in names:
                        ped_state_map[name] = "r"
                else:
                    for name in names:
                        ped_state_map[name] = "G"

        phase_state = {}
        phase_state.update({mv: "r" for mv in movements})
        phase_state.update(ped_state_map)
        phase_state.update(veh_state_map)

        for offset in range(phase.duration_s):
            slot = (time_cursor + offset) % cycle
            for movement in movements:
                movement_states[movement][slot] = phase_state.get(movement, "r")
        time_cursor = (time_cursor + phase.duration_s) % cycle

    _apply_tail_substitution(
        movement_states,
        yellow=profile.yellow_duration_s,
        ped_cutoff=profile.ped_early_cutoff_s,
    )

    return movement_states


def _timelines_to_phases(
    movements: Sequence[str],
    timelines: Mapping[str, List[str]],
) -> List[tuple[int, str]]:
    if not movements:
        return []

    cycle = len(next(iter(timelines.values()))) if timelines else 0
    if cycle == 0:
        return []

    ordered_timelines = [timelines[mv] for mv in movements]
    phases: List[tuple[int, str]] = []
    current_state: str | None = None
    current_duration = 0

    for second in range(cycle):
        state = "".join(timeline[second] for timeline in ordered_timelines)
        if current_state is None:
            current_state = state
            current_duration = 1
            continue
        if state == current_state:
            current_duration += 1
        else:
            phases.append((current_duration, current_state))
            current_state = state
            current_duration = 1

    if current_state is not None:
        phases.append((current_duration, current_state))

    # Merge adjacent duplicates after wrap-around
    if len(phases) >= 2 and phases[0][1] == phases[-1][1]:
        merged_duration = phases[0][0] + phases[-1][0]
        phases = [(merged_duration, phases[0][1])] + phases[1:-1]

    return phases


def _render_tl_logic(program: _TlProgram, links: Sequence[SignalLink]) -> List[str]:
    timelines = _build_timelines(program, links)
    movements = [link.movement for link in links]
    phases = _timelines_to_phases(movements, timelines)

    lines: List[str] = []
    lines.append(
        f'    <tlLogic id="{program.tl_id}" type="static" programID="0" offset="{program.offset}">'  # noqa: E501
    )
    for duration, state in phases:
        lines.append(f'        <phase duration="{duration}" state="{state}"/>')
    lines.append("    </tlLogic>")
    return lines


def render_tll_xml(
    *,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    lane_overrides: Sequence[LaneOverride],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
    connection_links: Sequence[SignalLink],
    controlled_connections: Sequence[ControlledConnection],
) -> str:
    """Render a ``1-generated.tll.xml`` document with deterministic ordering."""

    _ = (defaults, breakpoints, snap_rule, main_road, lane_overrides)

    programs = _collect_programs(clusters, signal_profiles_by_kind)
    links_by_tl = _group_links_by_tl(connection_links)
    controlled_by_tl = _group_controlled_connections(controlled_connections)

    lines: List[str] = [
        '<tlLogics version="1.20" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/tllogic_file.xsd">'
    ]

    for tl_id in sorted(links_by_tl):
        if tl_id not in programs:
            continue
        program = programs[tl_id]
        tl_lines = _render_tl_logic(program, links_by_tl[tl_id])
        lines.extend(tl_lines)

    for tl_id in sorted(controlled_by_tl):
        if tl_id not in programs:
            continue
        for conn in controlled_by_tl[tl_id]:
            lines.append(
                f'    <connection from="{conn.from_edge}" to="{conn.to_edge}" '
                f'fromLane="{conn.from_lane}" toLane="{conn.to_lane}" '
                f'tl="{conn.tl_id}" linkIndex="{conn.link_index}"/>'
            )

    lines.append("</tlLogics>")
    return "\n".join(lines)
