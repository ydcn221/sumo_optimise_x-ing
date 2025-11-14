"""Emission of ``1-generated.tll.xml`` traffic light programmes."""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set

from ..builder.ids import cluster_id
from ..domain.models import (
    Cluster,
    ControlledConnection,
    Defaults,
    EventKind,
    LaneOverride,
    MainRoadConfig,
    SignalLink,
    SignalProfileDef,
    SnapRule,
)
from ..utils.logging import get_logger
from .conflict_table import GLOBAL_CONFLICT_MATRIX

LOG = get_logger()

PEDESTRIAN_PREFIX = "ped_"

_VEHICLE_DIR_MAP = {
    "main_EB": "EB",
    "main_WB": "WB",
    "minor_N": "SB",  # North branch -> southbound traffic
    "minor_S": "NB",  # South branch -> northbound traffic
}

_PED_ORIENTATION_HALVES = {
    "N": ("XN_W-half", "XN_E-half"),
    "E": ("XE_N-half", "XE_S-half"),
    "S": ("XS_E-half", "XS_W-half"),
    "W": ("XW_S-half", "XW_N-half"),
}


@dataclass(frozen=True)
class PhaseToken:
    canonical: str
    movements: Sequence[str]
    raw_token: str


@dataclass(frozen=True)
class _TlProgram:
    tl_id: str
    profile: SignalProfileDef
    offset: int
    refuge_island_on_main: bool
    two_stage_tll_control: bool
    is_midblock: bool


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
                is_midblock=event.type == EventKind.XWALK_MIDBLOCK,
            )
    return programs


def _ped_base(name: str) -> str:
    """Return the grouping key for half-split crossings."""

    if name.startswith(("ped_main_", "ped_mid_")):
        for suffix in ("_north", "_south"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
    for suffix in ("_EB", "_WB"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


class MovementCatalog:
    """Helper that maps canonical movement tokens to actual signal links."""

    def __init__(self, movements: Sequence[str]) -> None:
        self.movements = list(movements)
        self.vehicle_tokens: Dict[str, List[str]] = defaultdict(list)
        self.movement_bases: Dict[str, str] = {}
        self.ped_half_tokens: Dict[str, List[str]] = defaultdict(list)
        self.ped_orientations: Set[str] = set()

        for movement in movements:
            if movement.startswith(PEDESTRIAN_PREFIX):
                halves = _ped_half_tokens_for_movement(movement)
                if not halves:
                    continue
                for half in halves:
                    self.ped_half_tokens[half].append(movement)
                    orientation = half[1]  # e.g. XN_W-half -> N
                    self.ped_orientations.add(orientation)
                continue

            base = _vehicle_base_token(movement)
            if not base:
                continue
            self.vehicle_tokens[base].append(movement)
            self.movement_bases[movement] = base


def _vehicle_base_token(movement: str) -> str | None:
    parts = movement.split("_")
    if len(parts) < 3:
        return None
    prefix = f"{parts[0]}_{parts[1]}"
    turn = parts[2].upper()
    direction = _VEHICLE_DIR_MAP.get(prefix)
    if not direction:
        return None
    if turn not in {"L", "T", "R", "U"}:
        return None
    return f"{direction}_{turn}"


def _ped_half_tokens_for_movement(movement: str) -> Set[str]:
    tokens: Set[str] = set()
    if movement.startswith("ped_main_"):
        _, _, side, *rest = movement.split("_")
        orientation = {"west": "W", "east": "E"}.get(side)
        if not orientation:
            return tokens
        halves = _ped_halves_for_orientation(orientation)
        if rest:
            half_key = rest[0]
            mapped = _ped_half_token_for_suffix(orientation, half_key)
            if mapped:
                tokens.add(mapped)
                return tokens
        tokens.update(halves)
        return tokens

    if movement.startswith("ped_minor_"):
        _, _, branch = movement.split("_")
        orientation = {"north": "N", "south": "S"}.get(branch)
        if not orientation:
            return tokens
        tokens.update(_ped_halves_for_orientation(orientation))
        return tokens

    if movement.startswith("ped_mid"):
        parts = movement.split("_")
        if len(parts) == 3:
            orientation = {"north": "N", "south": "S"}.get(parts[2])
            if orientation:
                tokens.update(_ped_halves_for_orientation(orientation))
                return tokens
        tokens.update(_ped_halves_for_orientation("N"))
        tokens.update(_ped_halves_for_orientation("S"))
        return tokens

    return tokens


def _ped_halves_for_orientation(orientation: str) -> Tuple[str, str]:
    return _PED_ORIENTATION_HALVES.get(orientation.upper(), tuple())


def _ped_half_token_for_suffix(orientation: str, suffix: str | None) -> str | None:
    orientation = orientation.upper()
    raw = (suffix or "").strip()
    normalized = raw.upper()
    mapping = {
        ("E", "N"): "XE_N-half",
        ("E", "S"): "XE_S-half",
        ("W", "N"): "XW_N-half",
        ("W", "S"): "XW_S-half",
        ("N", "W"): "XN_W-half",
        ("N", "E"): "XN_E-half",
        ("S", "E"): "XS_E-half",
        ("S", "W"): "XS_W-half",
    }
    if normalized.endswith("-HALF"):
        key = normalized[:-5]
        token = mapping.get((orientation, key))
        if token:
            return token
    legacy_alias = {
        "north": "N",
        "south": "S",
        "east": "E",
        "west": "W",
    }
    legacy_key = legacy_alias.get(raw.lower())
    if legacy_key:
        return mapping.get((orientation, legacy_key))
    return None


def _parse_ped_descriptor(descriptor: str) -> Tuple[List[str], str | None]:
    descriptor = descriptor.strip()
    if not descriptor:
        return [], None
    if "_" not in descriptor:
        if descriptor.isalpha() and all(ch in "NESW" for ch in descriptor):
            descriptor = descriptor.upper()
            orientations = list(dict.fromkeys(descriptor))
            return orientations, None
        return [], None
    head, tail = descriptor.split("_", 1)
    head = head.strip().upper()
    tail = tail.strip()
    if head not in {"N", "E", "S", "W"} or not tail:
        return [], None
    return [head], tail


_VEHICLE_TOKEN_RE = re.compile(r"^(?P<dir>[NSEW](?:B)?)_(?P<turns>[LTRU]+)$", re.IGNORECASE)
_PED_SUFFIX_RE = re.compile(r"_p([rg])$", re.IGNORECASE)


def _normalize_vehicle_token(token: str) -> List[Tuple[str, str]]:
    """Return (canonical_token, base_token) pairs for vehicle movements."""

    token = token.strip()
    if not token:
        return []

    suffix_match = _PED_SUFFIX_RE.search(token)
    suffix = "_pg"
    core = token
    if suffix_match:
        suffix = f"_p{suffix_match.group(1).lower()}"
        core = token[: suffix_match.start()]

    bare_direction = core.upper()
    if bare_direction in {"EB", "WB", "NB", "SB"}:
        canonical = f"{bare_direction}_T{suffix}"
        base = f"{bare_direction}_T"
        return [(canonical, base)]

    results: List[Tuple[str, str]] = []
    match = _VEHICLE_TOKEN_RE.match(core)
    if match:
        dir_token = match.group("dir").upper()
        if len(dir_token) == 1:
            dir_token += "B"
        turns = match.group("turns").upper()
        if len(set(turns)) != len(turns):
            return results
        for turn in turns:
            if turn not in {"L", "T", "R", "U"}:
                continue
            canonical = f"{dir_token}_{turn}{suffix}"
            base = f"{dir_token}_{turn}"
            results.append((canonical, base))
        return results

    legacy_core = core
    if legacy_core.startswith("main_"):
        _, rest = legacy_core.split("_", 1)
        if rest in {"L", "T", "R", "U"}:
            for direction in ("EB", "WB"):
                canonical = f"{direction}_{rest}{suffix}"
                base = f"{direction}_{rest}"
                results.append((canonical, base))
            return results
        parts = rest.split("_")
        if len(parts) == 2:
            direction, turn = parts[0].upper(), parts[1].upper()
            direction = f"{direction}B" if len(direction) == 1 else direction
            canonical = f"{direction}_{turn}{suffix}"
            base = f"{direction}_{turn}"
            results.append((canonical, base))
            return results

    if legacy_core.startswith("minor_"):
        _, rest = legacy_core.split("_", 1)
        if rest in {"L", "T", "R", "U"}:
            for direction in ("NB", "SB"):
                canonical = f"{direction}_{rest}{suffix}"
                base = f"{direction}_{rest}"
                results.append((canonical, base))
            return results
        parts = rest.split("_")
        if len(parts) == 2:
            branch, turn = parts[0].upper(), parts[1].upper()
            direction = "SB" if branch == "N" else "NB"
            canonical = f"{direction}_{turn}{suffix}"
            base = f"{direction}_{turn}"
            results.append((canonical, base))
            return results

    if legacy_core == "main_R":  # backwards compatibility
        for direction in ("EB", "WB"):
            canonical = f"{direction}_R{suffix}"
            base = f"{direction}_R"
            results.append((canonical, base))
        return results

    return results


def _expand_vehicle_tokens(
    token: str,
    catalog: MovementCatalog,
) -> List[PhaseToken]:
    normalized = _normalize_vehicle_token(token)
    phase_tokens: List[PhaseToken] = []
    for canonical, base in normalized:
        movements = catalog.vehicle_tokens.get(base, [])
        if not movements:
            continue
        phase_tokens.append(PhaseToken(canonical=canonical, movements=movements, raw_token=token))
    return phase_tokens


def _expand_ped_tokens(token: str, catalog: MovementCatalog) -> List[PhaseToken]:
    token = token.strip()
    if not token:
        return []

    phase_tokens: List[PhaseToken] = []

    if token.lower() == "pedestrian":
        halves = set(catalog.ped_half_tokens.keys())
        for half in halves:
            phase_tokens.append(PhaseToken(canonical=half, movements=catalog.ped_half_tokens[half], raw_token=token))
        return phase_tokens

    if token.lower() == "pedx":
        accumulated: Set[str] = set()
        for orientation in ("N", "E", "S", "W"):
            halves = _ped_halves_for_orientation(orientation)
            for half in halves:
                if half in catalog.ped_half_tokens and half not in accumulated:
                    accumulated.add(half)
                    phase_tokens.append(
                        PhaseToken(canonical=half, movements=catalog.ped_half_tokens[half], raw_token=token)
                    )
        return phase_tokens

    if token.startswith("PedX_") or token.startswith("pedx_"):
        descriptor = token.split("_", 1)[1]
        orientations, suffix = _parse_ped_descriptor(descriptor)
        seen_halves: Set[str] = set()
        for orientation in orientations:
            halves = list(_ped_halves_for_orientation(orientation))
            if suffix:
                candidate = _ped_half_token_for_suffix(orientation, suffix)
                if candidate:
                    halves = [candidate]
            for half in halves:
                if half in catalog.ped_half_tokens and half not in seen_halves:
                    seen_halves.add(half)
                    phase_tokens.append(
                        PhaseToken(canonical=half, movements=catalog.ped_half_tokens[half], raw_token=token)
                    )
        return phase_tokens

    if token.startswith("X") and token.endswith("-half"):
        if token in catalog.ped_half_tokens:
            phase_tokens.append(PhaseToken(canonical=token, movements=catalog.ped_half_tokens[token], raw_token=token))
        return phase_tokens

    return phase_tokens


def _expand_phase_tokens(tokens: Iterable[str], catalog: MovementCatalog) -> List[PhaseToken]:
    phase_tokens: List[PhaseToken] = []
    for token in tokens:
        produced = _expand_vehicle_tokens(token, catalog)
        if not produced:
            produced = _expand_ped_tokens(token, catalog)
        if not produced and token:
            LOG.warning("[TLS] unsupported allow_movement token '%s'", token)
        phase_tokens.extend(produced)
    return phase_tokens


def _normalize_conflict_state(token_a: str, token_b: str, state: str) -> str:
    if state == "X":
        LOG.warning(
            "[TLS] conflict table reported 'X' for %s vs %s; falling back to 'Y'",
            token_a,
            token_b,
        )
        return "Y"
    if state in {"P", "Y", "S"}:
        return state
    return "P"


def _state_from_markers(markers: Sequence[str]) -> str:
    if not markers:
        return "G"
    if "S" in markers:
        return "r"
    if "Y" in markers:
        return "g"
    return "G"


def _evaluate_conflicts(tokens: Sequence[str]) -> Dict[str, str]:
    if not tokens:
        return {}
    severity: Dict[str, List[str]] = defaultdict(list)
    unique_tokens: List[str] = []
    for token in tokens:
        if token not in unique_tokens:
            unique_tokens.append(token)
    for idx, token_a in enumerate(unique_tokens):
        for token_b in unique_tokens[idx + 1 :]:
            state_a, state_b = GLOBAL_CONFLICT_MATRIX.relation(token_a, token_b)
            severity[token_a].append(_normalize_conflict_state(token_a, token_b, state_a))
            severity[token_b].append(_normalize_conflict_state(token_b, token_a, state_b))
    return {token: _state_from_markers(severity.get(token, [])) for token in unique_tokens}


def _reduce_states(states: Sequence[str]) -> str:
    if not states:
        return "r"
    if "r" in states:
        return "r"
    if "g" in states:
        return "g"
    if "G" in states:
        return "G"
    return "r"


def _midblock_ped_allowances(tokens: Iterable[str]) -> Set[str]:
    allowed: Set[str] = set()
    for token in tokens:
        cleaned = token.strip()
        if not cleaned:
            continue
        lower = cleaned.lower()
        if lower == "pedx":
            allowed.update({"N", "S"})
            continue
        if lower.startswith("pedx_"):
            descriptor = cleaned.split("_", 1)[1]
            orientations, _ = _parse_ped_descriptor(descriptor)
            for orient in orientations:
                if orient in {"N", "S"}:
                    allowed.add(orient)
    return allowed


def _vehicle_group_active(
    phase_state: Mapping[str, str],
    movements: Sequence[str],
    prefix: str,
) -> bool:
    for movement in movements:
        if movement.startswith(prefix) and phase_state.get(movement, "r") in {"G", "g"}:
            return True
    return False


def _midblock_orientation_for_movement(movement: str) -> str | None:
    if not movement.startswith(PEDESTRIAN_PREFIX):
        return None
    lowered = movement.lower()
    if "north" in lowered:
        return "N"
    if "south" in lowered:
        return "S"
    return None


def _apply_midblock_overrides(
    phase_state: Dict[str, str],
    allowed_orients: Set[str],
    *,
    eb_active: bool,
    wb_active: bool,
    two_stage: bool,
) -> None:
    ped_groups: Dict[str, List[str]] = {"N": [], "S": []}
    for movement in list(phase_state.keys()):
        orientation = _midblock_orientation_for_movement(movement)
        if orientation in ped_groups:
            ped_groups[orientation].append(movement)

    if not ped_groups["N"] and not ped_groups["S"]:
        return

    allowed = set(allowed_orients)
    if not allowed:
        allowed = set()

    enforce_all_red = False
    if allowed and not two_stage and allowed != {"N", "S"}:
        enforce_all_red = True

    for orient, names in ped_groups.items():
        for name in names:
            if enforce_all_red or orient not in allowed:
                phase_state[name] = "r"
                continue
            if orient == "N" and eb_active:
                phase_state[name] = "r"
                continue
            if orient == "S" and wb_active:
                phase_state[name] = "r"
                continue
            if phase_state.get(name) == "r":
                phase_state[name] = "G"


def _apply_tail_substitution(
    states: MutableMapping[str, List[str]],
    *,
    yellow: int,
    ped_cutoff: int,
) -> None:
    if yellow <= 0 and ped_cutoff <= 0:
        return

    cycle = len(next(iter(states.values()))) if states else 0
    if cycle == 0:
        return

    vehicle_green: List[bool] = [False] * cycle
    if ped_cutoff > 0:
        for movement, timeline in states.items():
            if movement.startswith(PEDESTRIAN_PREFIX):
                continue
            for idx, state in enumerate(timeline):
                if state in ("G", "g"):
                    vehicle_green[idx] = True

    for movement, timeline in states.items():
        if movement.startswith(PEDESTRIAN_PREFIX):
            if ped_cutoff <= 0:
                continue
            # Only trim pedestrian phases that actually overlap a vehicle green; purely
            # pedestrian windows (common for two-stage halves) should retain their full duration.
            overlaps_vehicle = any(vehicle_green[idx] and timeline[idx] == "G" for idx in range(cycle))
            if not overlaps_vehicle:
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
    movement_halves: Dict[str, Set[str]] = {
        movement: _ped_half_tokens_for_movement(movement) for movement in movements
    }
    movements_by_base: Dict[str, List[str]] = defaultdict(list)
    halves_by_base: Dict[str, Set[str]] = defaultdict(set)
    for movement in movements:
        base = _ped_base(movement)
        if base == movement:
            continue
        movements_by_base[base].append(movement)
        halves_by_base[base].update(movement_halves.get(movement, set()))
    movement_states: Dict[str, List[str]] = {
        movement: ["r"] * cycle for movement in movements
    }

    catalog = MovementCatalog(movements)

    available_movements = list(movements)
    time_cursor = 0

    for phase in profile.phases:
        phase_tokens = _expand_phase_tokens(phase.allow_movements, catalog)
        midblock_ped_allow = (
            _midblock_ped_allowances(phase.allow_movements) if program.is_midblock else set()
        )
        canonical_order = [token.canonical for token in phase_tokens]
        token_states = _evaluate_conflicts(canonical_order)

        movement_phase_states: Dict[str, List[str]] = defaultdict(list)
        movement_granted_halves: Dict[str, Set[str]] = defaultdict(set)
        base_granted_halves: Dict[str, Set[str]] = defaultdict(set)
        for token in phase_tokens:
            state = token_states.get(token.canonical, "r")
            for movement in token.movements:
                movement_phase_states[movement].append(state)
                halves = movement_halves.get(movement)
                if halves and token.canonical in halves:
                    movement_granted_halves[movement].add(token.canonical)
                base = _ped_base(movement)
                if base != movement:
                    base_granted_halves[base].add(token.canonical)

        phase_state: Dict[str, str] = {}
        for movement in movements:
            phase_state[movement] = _reduce_states(movement_phase_states.get(movement, []))

        if program.is_midblock:
            eb_active = _vehicle_group_active(phase_state, movements, "main_EB")
            wb_active = _vehicle_group_active(phase_state, movements, "main_WB")
            _apply_midblock_overrides(
                phase_state,
                midblock_ped_allow,
                eb_active=eb_active,
                wb_active=wb_active,
                two_stage=bool(
                    program.two_stage_tll_control and program.refuge_island_on_main
                ),
            )

        for movement, required_halves in movement_halves.items():
            if not required_halves:
                continue
            granted = movement_granted_halves.get(movement, set())
            if not required_halves.issubset(granted):
                phase_state[movement] = "r"

        single_stage_crossing = (
            (not program.two_stage_tll_control or not program.refuge_island_on_main)
            and not program.is_midblock
        )
        if single_stage_crossing:
            for base, names in movements_by_base.items():
                required = halves_by_base.get(base, set())
                if not required:
                    continue
                granted = base_granted_halves.get(base, set())
                if not required.issubset(granted):
                    for name in names:
                        phase_state[name] = "r"

            for base, names in movements_by_base.items():
                if len(names) < 2:
                    continue
                if any(phase_state.get(name) == "r" for name in names):
                    for name in names:
                        phase_state[name] = "r"
                else:
                    for name in names:
                        if phase_state.get(name) != "r":
                            phase_state[name] = "G"

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
