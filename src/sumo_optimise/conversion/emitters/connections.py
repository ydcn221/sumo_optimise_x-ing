"""PlainXML connection and crossing emission."""
from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from ..builder.ids import (
    cluster_id,
    crossing_id_main,
    crossing_id_main_split,
    crossing_id_midblock,
    crossing_id_midblock_split,
    crossing_id_minor,
    main_edge_id,
    minor_edge_id,
)
from ..domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    MainRoadConfig,
    SideMinor,
    SnapRule,
)
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.lanes import find_neighbor_segments, pick_lanes_for_segment
from ..utils.errors import SemanticValidationError
from ..utils.logging import get_logger

LOG = get_logger()


def _band_partition(n_sources: int, m_targets: int) -> List[Tuple[int, int]]:
    if n_sources <= 0 or m_targets <= 0:
        return []
    bands: List[Tuple[int, int]] = []
    for k in range(1, n_sources + 1):
        low = int((k - 1) * m_targets / n_sources) + 1
        high = int(k * m_targets / n_sources)
        bands.append((low, high))
    return bands


_ORDER = ("L", "T", "R", "U")


def _canon(label: str) -> str:
    """Normalize lane labels to the canonical L→T→R→U order."""

    s = set(label)
    return "".join(ch for ch in _ORDER if ch in s)


def _add(label: str, move: str) -> str:
    """Append ``move`` to ``label`` while preserving canonical ordering."""

    return _canon(label + move)


def allocate_lanes(s: int, l: int, t: int, r: int, u: int) -> List[str]:
    """Allocate lane permissions for an approach.

    The algorithm mirrors the reference implementation from the product
    specification.  It assigns left/through/right/U-turn permissions for
    ``s`` inbound lanes given the dedicated demand counts ``l``, ``t``,
    ``r`` and ``u``.  See the specification docstring for the exhaustive
    set of rules (empty-lane placeholders, U-turn sharing, alternating
    left/right removal, etc.).
    """

    for name, value in {"s": s, "l": l, "t": t, "r": r, "u": u}.items():
        if not isinstance(value, int):
            raise ValueError(f"{name} must be int")
        if value < 0:
            raise ValueError(f"{name} must be non-negative")

    total = l + t + r + u

    if s >= total:
        return (
            ["L"] * l
            + ["T"] * t
            + [""] * (s - total)
            + ["R"] * r
            + ["U"] * u
        )

    if l + t + r < s < l + t + r + u:
        keep_u = s - (l + t + r)
        return ["L"] * l + ["T"] * t + ["R"] * r + ["U"] * keep_u

    if s == l + t + r:
        lanes = ["L"] * l + ["T"] * t + ["R"] * r
        if u > 0 and lanes:
            lanes[-1] = _add(lanes[-1], "U")
        return lanes

    if s < t + 2:
        raise ValueError("Case s < t+2 is not defined in the current spec.")

    lanes: List[str] = ["L"] * l + ["T"] * t + ["R"] * r
    if u > 0 and lanes:
        lanes[-1] = _add(lanes[-1], "U")

    min_L = 1 if l > 0 else 0
    min_R = 1 if r > 0 else 0

    def count_only(sym: str) -> int:
        return sum(1 for x in lanes if x == sym)

    def pop_R_only() -> bool:
        if count_only("R") <= min_R:
            return False
        for i in range(len(lanes) - 1, -1, -1):
            if lanes[i] == "R":
                lanes.pop(i)
                return True
        return False

    def pop_L_only() -> bool:
        if count_only("L") <= min_L:
            return False
        for i, val in enumerate(lanes):
            if val == "L":
                lanes.pop(i)
                return True
        return False

    turn = "L"
    while len(lanes) > s:
        if turn == "L":
            removed = pop_L_only() or pop_R_only()
        else:
            removed = pop_R_only() or pop_L_only()

        if not removed:
            raise ValueError("Cannot reduce lanes to s under the given constraints.")

        if u > 0 and lanes:
            for i, val in enumerate(lanes):
                if "U" in val and val != "U":
                    lanes[i] = val.replace("U", "")
            lanes[-1] = _add(lanes[-1], "U")

        turn = "R" if turn == "L" else "L"

    return lanes


def _emit_vehicle_connections_for_approach(
    lines: List[str],
    pos: int,
    in_edge_id: str,
    s_count: int,
    L_target: Optional[Tuple[str, int]],
    T_target: Optional[Tuple[str, int]],
    R_target: Optional[Tuple[str, int]],
    U_target: Optional[Tuple[str, int]],
) -> int:
    l_target = L_target[1] if L_target else 0
    t_target = T_target[1] if T_target else 0
    r_target = R_target[1] if R_target else 0
    u_target = U_target[1] if U_target else 0

    l = min(l_target, s_count)
    t = min(t_target, s_count)
    r = min(r_target, s_count)
    u = min(u_target, s_count)

    if s_count > 0 and (l + t + r + u) == 0:
        LOG.error(
            "[VAL] E401 no available movements: pos=%s in_edge=%s s=%d l=%d t=%d r=%d u=%d",
            pos,
            in_edge_id,
            s_count,
            l,
            t,
            r,
            u,
        )
        raise SemanticValidationError("no available movements for approach")

    try:
        lane_labels = allocate_lanes(s_count, l, t, r, u)
    except ValueError as exc:  # pragma: no cover - defensive, should be validated upstream
        LOG.error(
            "[VAL] E403 invalid lane allocation inputs: pos=%s in_edge=%s s=%d l=%d t=%d r=%d u=%d msg=%s",
            pos,
            in_edge_id,
            s_count,
            l,
            t,
            r,
            u,
            exc,
        )
        raise SemanticValidationError("invalid lane allocation") from exc

    idx_l = [i for i, label in enumerate(lane_labels, start=1) if "L" in label]
    idx_t = [i for i, label in enumerate(lane_labels, start=1) if "T" in label]
    idx_r = [i for i, label in enumerate(lane_labels, start=1) if "R" in label]
    idx_u = [i for i, label in enumerate(lane_labels, start=1) if "U" in label]

    emitted = 0

    def emit_for(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        nonlocal emitted
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        bands = _band_partition(len(idx), lane_count)
        for k, (low, high) in enumerate(bands, start=1):
            if low > high:
                continue
            from_lane = idx[k - 1]
            for to_lane in range(low, high + 1):
                lines.append(
                    f'  <connection from="{in_edge_id}" to="{edge_id}" '
                    f'fromLane="{from_lane}" toLane="{to_lane}"/>'
                )
                emitted += 1

    if l > 0:
        emit_for(idx_l, L_target)
    if t > 0:
        emit_for(idx_t, T_target)
    if r > 0:
        emit_for(idx_r, R_target)
    if u > 0:
        emit_for(idx_u, U_target)

    if (l + t + r + u) > 0 and emitted == 0 and s_count > 0:
        LOG.error(
            "[VAL] E402 zero vehicle connections emitted: pos=%s in_edge=%s s=%d l=%d t=%d r=%d u=%d",
            pos,
            in_edge_id,
            s_count,
            l,
            t,
            r,
            u,
        )
        raise SemanticValidationError("no vehicle connections emitted")
    return emitted


def get_main_edges_west_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    west, _ = find_neighbor_segments(breakpoints, pos)
    if west is None:
        return None, None
    return (main_edge_id("EB", west, pos), main_edge_id("WB", west, pos))


def get_main_edges_east_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    _, east = find_neighbor_segments(breakpoints, pos)
    if east is None:
        return None, None
    return (main_edge_id("EB", pos, east), main_edge_id("WB", pos, east))


def render_connections_xml(
    defaults: Defaults,
    clusters: List[Cluster],
    breakpoints: List[int],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    lane_overrides: Dict[str, List[LaneOverride]],
) -> str:
    width = defaults.ped_crossing_width_m
    lines: List[str] = []
    lines.append("<connections>")

    absorbed_pos: Set[int] = set()

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        node = cluster_id(pos)

        place_west = False
        place_east = False
        split_main = False
        for ev in junction_events:
            if ev.main_ped_crossing_placement:
                place_west = place_west or bool(ev.main_ped_crossing_placement.get("west", False))
                place_east = place_east or bool(ev.main_ped_crossing_placement.get("east", False))
            if ev.template_id and ev.template_id in junction_template_by_id:
                split_main = split_main or bool(junction_template_by_id[ev.template_id].split_ped_crossing_on_main)

        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if mid_events:
            absorbed_pos.add(pos)
            for mev in mid_events:
                side = decide_midblock_side_for_collision(mev.pos_m_raw, pos, snap_rule.tie_break)
                if side == "west":
                    place_west = True
                else:
                    place_east = True

        if any(ev.type == EventKind.CROSS for ev in junction_events):
            branches = ["north", "south"]
        else:
            branch = junction_events[0].branch.value if junction_events[0].branch else None
            branches = [branch] if branch in ("north", "south") else []
        for b in branches:
            ns = "N" if b == "north" else "S"
            e_to = f"Edge.Minor.{pos}.to.{ns}"
            e_from = f"Edge.Minor.{pos}.from.{ns}"
            cid = crossing_id_minor(pos, ns)
            lines.append(f'  <crossing id="{cid}" node="{node}" edges="{e_to} {e_from}" width="{width:.3f}"/>')

        if place_west:
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    lines.append(
                        f'  <crossing id="{crossing_id_main_split(pos, "West", "EB")}" '
                        f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    lines.append(
                        f'  <crossing id="{crossing_id_main_split(pos, "West", "WB")}" '
                        f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                else:
                    lines.append(
                        f'  <crossing id="{crossing_id_main(pos, "West")}" '
                        f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )

        if place_east:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    lines.append(
                        f'  <crossing id="{crossing_id_main_split(pos, "East", "EB")}" '
                        f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    lines.append(
                        f'  <crossing id="{crossing_id_main_split(pos, "East", "WB")}" '
                        f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                else:
                    lines.append(
                        f'  <crossing id="{crossing_id_main(pos, "East")}" '
                        f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )

    for cluster in clusters:
        pos = cluster.pos_m
        if pos in absorbed_pos:
            continue
        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if not mid_events:
            continue
        node = cluster_id(pos)

        if snap_rule.tie_break == "toward_west":
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
        else:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)

        if not (eb_edge and wb_edge):
            LOG.warning("midblock at %s: adjacent main edges not found; crossing omitted", pos)
            continue

        split_midblock = any(bool(ev.split_ped_crossing_on_main) for ev in mid_events)
        if split_midblock:
            lines.append(
                f'  <crossing id="{crossing_id_midblock_split(pos, "EB")}" '
                f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
            )
            lines.append(
                f'  <crossing id="{crossing_id_midblock_split(pos, "WB")}" '
                f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
            )
        else:
            lines.append(
                f'  <crossing id="{crossing_id_midblock(pos)}" '
                f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
            )

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        ev = junction_events[0]
        tpl = junction_template_by_id.get(ev.template_id) if ev.template_id else None
        if not tpl:
            LOG.warning("junction template not found: id=%s (pos=%s)", getattr(ev, "template_id", None), pos)
            continue

        if ev.type == EventKind.CROSS:
            exist_north = True
            exist_south = True
        else:
            exist_north = ev.branch == SideMinor.NORTH
            exist_south = ev.branch == SideMinor.SOUTH

        west, east = find_neighbor_segments(breakpoints, pos)

        def pick_main(direction: str, a: Optional[int], b: Optional[int]) -> int:
            if a is None or b is None:
                return 0
            return pick_lanes_for_segment(direction, a, b, main_road.lanes, lane_overrides)

        if west is not None:
            in_edge = main_edge_id("EB", west, pos)
            s_count = pick_main("EB", west, pos)
            L_target = (
                minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main
            ) if exist_north else None
            T_target = (
                main_edge_id("EB", pos, east), pick_main("EB", pos, east)
            ) if east is not None else None
            R_target = (
                minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main
            ) if exist_south else None
            u_lanes = pick_main("WB", west, pos)
            U_target = (
                main_edge_id("WB", west, pos), u_lanes
            ) if u_lanes > 0 else None
            if tpl.median_continuous:
                R_target = None
                U_target = None
            _emit_vehicle_connections_for_approach(
                lines, pos, in_edge, s_count, L_target, T_target, R_target, U_target
            )

        if east is not None:
            in_edge = main_edge_id("WB", pos, east)
            s_count = pick_main("WB", pos, east)
            L_target = (
                minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main
            ) if exist_south else None
            T_target = (
                main_edge_id("WB", west, pos), pick_main("WB", west, pos)
            ) if west is not None else None
            R_target = (
                minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main
            ) if exist_north else None
            u_lanes = pick_main("EB", pos, east)
            U_target = (
                main_edge_id("EB", pos, east), u_lanes
            ) if u_lanes > 0 else None
            if tpl.median_continuous:
                R_target = None
                U_target = None
            _emit_vehicle_connections_for_approach(
                lines, pos, in_edge, s_count, L_target, T_target, R_target, U_target
            )

        if exist_north:
            in_edge = minor_edge_id(pos, "to", "N")
            s_count = tpl.minor_lanes_to_main
            L_target = (
                main_edge_id("EB", pos, east), pick_main("EB", pos, east)
            ) if east is not None else None
            _emit_vehicle_connections_for_approach(
                lines, pos, in_edge, s_count, L_target, None, None, None
            )

        if exist_south:
            in_edge = minor_edge_id(pos, "to", "S")
            s_count = tpl.minor_lanes_to_main
            L_target = (
                main_edge_id("WB", west, pos), pick_main("WB", west, pos)
            ) if west is not None else None
            _emit_vehicle_connections_for_approach(
                lines, pos, in_edge, s_count, L_target, None, None, None
            )

    unique_lines: List[str] = []
    seen: Set[str] = set()
    for ln in lines:
        if ln.startswith("  <connection "):
            key = ln.strip()
            if key in seen:
                LOG.warning("[VAL] E405 duplicated <connection> suppressed: %s", key)
                continue
            seen.add(key)
        unique_lines.append(ln)

    unique_lines.append("</connections>")
    xml = "\n".join(unique_lines) + "\n"
    LOG.info("rendered connections (%d lines)", len(unique_lines))
    return xml
