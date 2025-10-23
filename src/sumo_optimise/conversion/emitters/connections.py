"""PlainXML connection and crossing emission."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

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
    ConnectionsEmission,
    CrossingSpec,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    MainRoadConfig,
    Movement,
    SideMinor,
    SnapRule,
    VehicleConnectionSpec,
)
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.lanes import find_neighbor_segments, pick_lanes_for_segment
from ..utils.errors import SemanticValidationError
from ..utils.logging import get_logger

LOG = get_logger()


_ORDER = ("L", "T", "R", "U")


def _canon(label: str) -> str:
    """Normalize lane labels to the canonical L→T→R→U order."""

    s = set(label)
    return "".join(ch for ch in _ORDER if ch in s)


def _add(label: str, move: str) -> str:
    """Append ``move`` to ``label`` while preserving canonical ordering."""

    return _canon(label + move)


def _ensure_u_on_rightmost(lanes: List[str], u: int) -> None:
    """Ensure that the rightmost non-U lane carries the shared U-turn when needed."""

    if u <= 0 or not lanes:
        return

    for idx, value in enumerate(lanes):
        if "U" in value and value != "U":
            lanes[idx] = value.replace("U", "")

    lanes[-1] = _add(lanes[-1], "U")


def _drop_exclusive(lanes: List[str], sym: str, from_left: bool) -> bool:
    """Remove a lane that is *exactly* ``sym``; return ``True`` on success."""

    indices = range(len(lanes)) if from_left else range(len(lanes) - 1, -1, -1)
    for idx in indices:
        if lanes[idx] == sym:
            lanes.pop(idx)
            return True
    return False


def _share_to_side(lanes: List[str], sym: str, side: str) -> None:
    """Attach ``sym`` as a shared movement to the requested side lane."""

    if not lanes:
        return

    if side == "L":
        lanes[0] = _add(lanes[0], sym)
    else:
        lanes[-1] = _add(lanes[-1], sym)


def _count_effective(lanes: List[str], sym: str) -> int:
    """Count lanes that effectively behave as ``sym`` while ignoring shared ``U``."""

    return sum(1 for lane in lanes if lane.replace("U", "") == sym)


def allocate_lanes(s: int, l: int, t: int, r: int, u: int) -> List[str]:
    """Allocate lane permissions according to the detailed specification."""

    for name, value in {"s": s, "l": l, "t": t, "r": r, "u": u}.items():
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative int")

    if s == 1:
        label = ""
        if l > 0:
            label = _add(label, "L")
        if t > 0:
            label = _add(label, "T")
        if r > 0:
            label = _add(label, "R")
        if u > 0:
            label = _add(label, "U")
        return [label]

    total = l + t + r + u
    base_lb = t + (1 if l > 0 else 0) + (1 if r > 0 else 0)
    side_min = (1 if l > 0 else 0) + (1 if r > 0 else 0)

    if s >= total:
        return (
            ["L"] * l
            + ["T"] * t
            + [""] * (s - total)
            + ["R"] * r
            + ["U"] * u
        )

    if l + t + r < s < total:
        keep_u = s - (l + t + r)
        return ["L"] * l + ["T"] * t + ["R"] * r + ["U"] * keep_u

    def _after_c() -> List[str]:
        lanes0 = ["L"] * l + ["T"] * t + ["R"] * r
        _ensure_u_on_rightmost(lanes0, u)
        return lanes0

    if s == l + t + r:
        return _after_c()

    lanes = _after_c()

    if s in {t, t + 1} and not (base_lb <= s < l + t + r):
        e_turn = "L"
        while len(lanes) > s:
            if e_turn == "L":
                if _drop_exclusive(lanes, "L", from_left=True):
                    _share_to_side(lanes, "L", "L")
                    _ensure_u_on_rightmost(lanes, u)
                    e_turn = "R"
                    continue
                if _drop_exclusive(lanes, "R", from_left=False):
                    _share_to_side(lanes, "R", "R")
                    _ensure_u_on_rightmost(lanes, u)
                    e_turn = "L"
                    continue
            else:
                if _drop_exclusive(lanes, "R", from_left=False):
                    _share_to_side(lanes, "R", "R")
                    _ensure_u_on_rightmost(lanes, u)
                    e_turn = "L"
                    continue
                if _drop_exclusive(lanes, "L", from_left=True):
                    _share_to_side(lanes, "L", "L")
                    _ensure_u_on_rightmost(lanes, u)
                    e_turn = "R"
                    continue

            raise ValueError("Cannot reach s by L/R drop-share at s in {t, t+1}.")

        _ensure_u_on_rightmost(lanes, u)
        return lanes

    if side_min <= s and s < t:
        target = max(s, base_lb)
        while len(lanes) > target:
            progressed = False
            if _drop_exclusive(lanes, "L", from_left=True):
                _share_to_side(lanes, "L", "L")
                _ensure_u_on_rightmost(lanes, u)
                progressed = True
            if len(lanes) > target and _drop_exclusive(lanes, "R", from_left=False):
                _share_to_side(lanes, "R", "R")
                _ensure_u_on_rightmost(lanes, u)
                progressed = True
            if not progressed:
                break

        next_side = "L"
        while len(lanes) > s:
            if not _drop_exclusive(lanes, "T", from_left=True):
                raise ValueError("No 'T' to drop while s < t requires removing T.")
            _share_to_side(lanes, "T", next_side)
            next_side = "R" if next_side == "L" else "L"
            _ensure_u_on_rightmost(lanes, u)
        return lanes

    if base_lb <= s < l + t + r:
        min_l = 1 if l > 0 else 0
        min_r = 1 if r > 0 else 0

        def _pop_l_only() -> bool:
            if _count_effective(lanes, "L") <= min_l:
                return False
            return _drop_exclusive(lanes, "L", from_left=True)

        def _pop_r_only() -> bool:
            if _count_effective(lanes, "R") <= min_r:
                return False
            return _drop_exclusive(lanes, "R", from_left=False)

        turn = "L"
        while len(lanes) > s:
            removed = _pop_l_only() if turn == "L" else _pop_r_only()
            if not removed:
                removed = _pop_r_only() if turn == "L" else _pop_l_only()
            if not removed:
                raise ValueError("Cannot reduce lanes to s under D-case constraints.")
            _ensure_u_on_rightmost(lanes, u)
            turn = "R" if turn == "L" else "L"
        return lanes

    raise ValueError("Configuration not supported by current specification.")


def _emit_vehicle_connections_for_approach(
    queue: List[Tuple[str, Dict[str, Any]]],
    pos: int,
    controller_id: str,
    approach: str,
    approach_side: Optional[str],
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

    def append_connection(
        from_lane: int,
        to_lane: int,
        edge_id: str,
        movement: Movement,
        is_uturn: bool = False,
    ) -> None:
        nonlocal emitted
        line = (
            f'  <connection from="{in_edge_id}" to="{edge_id}" '
            f'fromLane="{from_lane}" toLane="{to_lane}"/>'
        )
        metadata = {
            "controller_id": controller_id,
            "pos_m": pos,
            "approach": approach,
            "approach_side": approach_side,
            "from_edge": in_edge_id,
            "from_lane": from_lane,
            "to_edge": edge_id,
            "to_lane": to_lane,
            "movement": movement,
            "is_uturn": is_uturn,
        }
        queue.append((line, metadata))
        emitted += 1

    def emit_left(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        """Map left turns from the inside out, sharing the last target lane if needed."""
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        for offset, from_lane in enumerate(idx):
            to_lane = min(offset + 1, lane_count)
            append_connection(from_lane, to_lane, edge_id, Movement.LEFT)

    def emit_straight(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        """Assign straight lanes left-to-left, fanning the rightmost lane when required."""
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        count = len(idx)
        matched = min(count, lane_count)
        for offset in range(matched):
            append_connection(idx[offset], offset + 1, edge_id, Movement.THROUGH)
        if count > lane_count:
            for from_lane in idx[lane_count:]:
                append_connection(from_lane, lane_count, edge_id, Movement.THROUGH)
        elif lane_count > count:
            edge_from = idx[-1]
            for to_lane in range(count + 1, lane_count + 1):
                append_connection(edge_from, to_lane, edge_id, Movement.THROUGH)

    def emit_right(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        """Attach right (and U) turns from the outside in, sharing the edge lane as needed."""
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        for offset, from_lane in enumerate(reversed(idx)):
            if offset < lane_count:
                to_lane = lane_count - offset
            else:
                to_lane = lane_count
            append_connection(from_lane, to_lane, edge_id, Movement.RIGHT)

    def emit_uturn(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        for offset, from_lane in enumerate(reversed(idx)):
            if offset < lane_count:
                to_lane = lane_count - offset
            else:
                to_lane = lane_count
            append_connection(from_lane, to_lane, edge_id, Movement.UTURN, is_uturn=True)

    if l > 0:
        emit_left(idx_l, L_target)
    if t > 0:
        emit_straight(idx_t, T_target)
    if r > 0:
        emit_right(idx_r, R_target)
    if u > 0:
        emit_uturn(idx_u, U_target)

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
) -> ConnectionsEmission:
    width = defaults.ped_crossing_width_m
    crossing_lines: List[str] = []
    crossing_metadata: List[Dict[str, Any]] = []
    vehicle_entries: List[Tuple[str, Dict[str, Any]]] = []

    def record_crossing(
        line: str,
        *,
        controller_id: str,
        pos_m: int,
        crossing_id: str,
        node_id: str,
        edges: List[str],
        placement: str,
    ) -> None:
        crossing_lines.append(line)
        crossing_metadata.append(
            {
                "controller_id": controller_id,
                "pos_m": pos_m,
                "crossing_id": crossing_id,
                "node_id": node_id,
                "edges": edges,
                "placement": placement,
            }
        )

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
            split_main = split_main or bool(ev.refuge_island_on_main)

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
            line = (
                f'  <crossing id="{cid}" node="{node}" '
                f'edges="{e_to} {e_from}" width="{width:.3f}"/>'
            )
            record_crossing(
                line,
                controller_id=node,
                pos_m=pos,
                crossing_id=cid,
                node_id=node,
                edges=[e_to, e_from],
                placement=f"minor_{b}",
            )

        if place_west:
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    cid_eb = crossing_id_main_split(pos, "West", "EB")
                    line_eb = (
                        f'  <crossing id="{cid_eb}" node="{node}" '
                        f'edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line_eb,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid_eb,
                        node_id=node,
                        edges=[eb_edge],
                        placement="main_west_EB",
                    )
                    cid_wb = crossing_id_main_split(pos, "West", "WB")
                    line_wb = (
                        f'  <crossing id="{cid_wb}" node="{node}" '
                        f'edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line_wb,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid_wb,
                        node_id=node,
                        edges=[wb_edge],
                        placement="main_west_WB",
                    )
                else:
                    cid = crossing_id_main(pos, "West")
                    line = (
                        f'  <crossing id="{cid}" node="{node}" '
                        f'edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid,
                        node_id=node,
                        edges=[eb_edge, wb_edge],
                        placement="main_west",
                    )

        if place_east:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    cid_eb = crossing_id_main_split(pos, "East", "EB")
                    line_eb = (
                        f'  <crossing id="{cid_eb}" node="{node}" '
                        f'edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line_eb,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid_eb,
                        node_id=node,
                        edges=[eb_edge],
                        placement="main_east_EB",
                    )
                    cid_wb = crossing_id_main_split(pos, "East", "WB")
                    line_wb = (
                        f'  <crossing id="{cid_wb}" node="{node}" '
                        f'edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line_wb,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid_wb,
                        node_id=node,
                        edges=[wb_edge],
                        placement="main_east_WB",
                    )
                else:
                    cid = crossing_id_main(pos, "East")
                    line = (
                        f'  <crossing id="{cid}" node="{node}" '
                        f'edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )
                    record_crossing(
                        line,
                        controller_id=node,
                        pos_m=pos,
                        crossing_id=cid,
                        node_id=node,
                        edges=[eb_edge, wb_edge],
                        placement="main_east",
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

        split_midblock = any(bool(ev.refuge_island_on_main) for ev in mid_events)
        if split_midblock:
            cid_eb = crossing_id_midblock_split(pos, "EB")
            line_eb = (
                f'  <crossing id="{cid_eb}" node="{node}" '
                f'edges="{eb_edge}" width="{width:.3f}"/>'
            )
            record_crossing(
                line_eb,
                controller_id=node,
                pos_m=pos,
                crossing_id=cid_eb,
                node_id=node,
                edges=[eb_edge],
                placement="midblock_EB",
            )
            cid_wb = crossing_id_midblock_split(pos, "WB")
            line_wb = (
                f'  <crossing id="{cid_wb}" node="{node}" '
                f'edges="{wb_edge}" width="{width:.3f}"/>'
            )
            record_crossing(
                line_wb,
                controller_id=node,
                pos_m=pos,
                crossing_id=cid_wb,
                node_id=node,
                edges=[wb_edge],
                placement="midblock_WB",
            )
        else:
            cid = crossing_id_midblock(pos)
            line = (
                f'  <crossing id="{cid}" node="{node}" '
                f'edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
            )
            record_crossing(
                line,
                controller_id=node,
                pos_m=pos,
                crossing_id=cid,
                node_id=node,
                edges=[eb_edge, wb_edge],
                placement="midblock",
            )

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        node = cluster_id(pos)
        ev = junction_events[0]
        tpl = junction_template_by_id.get(ev.template_id) if ev.template_id else None
        if not tpl:
            LOG.warning("junction template not found: id=%s (pos=%s)", getattr(ev, "template_id", None), pos)
            continue

        allow_main_uturn = True
        for j_ev in junction_events:
            if j_ev.main_u_turn_allowed is False:
                allow_main_uturn = False
                break

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
            if not allow_main_uturn:
                u_lanes = 0
            U_target = (
                main_edge_id("WB", west, pos), u_lanes
            ) if u_lanes > 0 else None
            if tpl.median_continuous:
                R_target = None
                U_target = None
            _emit_vehicle_connections_for_approach(
                vehicle_entries,
                pos,
                node,
                "main",
                "EB",
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
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
            if not allow_main_uturn:
                u_lanes = 0
            U_target = (
                main_edge_id("EB", pos, east), u_lanes
            ) if u_lanes > 0 else None
            if tpl.median_continuous:
                R_target = None
                U_target = None
            _emit_vehicle_connections_for_approach(
                vehicle_entries,
                pos,
                node,
                "main",
                "WB",
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
            )

        if exist_north:
            in_edge = minor_edge_id(pos, "to", "N")
            s_count = tpl.minor_lanes_to_main
            east_lanes = pick_main("EB", pos, east) if east is not None else 0
            west_lanes = pick_main("WB", west, pos) if west is not None else 0
            L_target = (
                main_edge_id("EB", pos, east), east_lanes
            ) if east is not None and east_lanes > 0 else None
            T_target = (
                minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main
            ) if exist_south and tpl.minor_lanes_from_main > 0 else None
            R_target = (
                main_edge_id("WB", west, pos), west_lanes
            ) if west is not None and west_lanes > 0 else None
            U_target = None
            if tpl.median_continuous:
                T_target = None
                R_target = None
            if not (L_target or T_target or R_target or U_target):
                LOG.error(
                    "[VAL] E401 no main movements available for minor approach: pos=%s branch=%s west=%s east=%s west_lanes=%d east_lanes=%d",
                    pos,
                    "north",
                    west,
                    east,
                    west_lanes,
                    east_lanes,
                )
                raise SemanticValidationError("no available movements for approach")
            _emit_vehicle_connections_for_approach(
                vehicle_entries,
                pos,
                node,
                "minor",
                "north",
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
            )

        if exist_south:
            in_edge = minor_edge_id(pos, "to", "S")
            s_count = tpl.minor_lanes_to_main
            west_lanes = pick_main("WB", west, pos) if west is not None else 0
            east_lanes = pick_main("EB", pos, east) if east is not None else 0
            L_target = (
                main_edge_id("WB", west, pos), west_lanes
            ) if west is not None and west_lanes > 0 else None
            T_target = (
                minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main
            ) if exist_north and tpl.minor_lanes_from_main > 0 else None
            R_target = (
                main_edge_id("EB", pos, east), east_lanes
            ) if east is not None and east_lanes > 0 else None
            U_target = None
            if tpl.median_continuous:
                T_target = None
                R_target = None
            if not (L_target or T_target or R_target or U_target):
                LOG.error(
                    "[VAL] E401 no main movements available for minor approach: pos=%s branch=%s west=%s east=%s west_lanes=%d east_lanes=%d",
                    pos,
                    "south",
                    west,
                    east,
                    west_lanes,
                    east_lanes,
                )
                raise SemanticValidationError("no available movements for approach")
            _emit_vehicle_connections_for_approach(
                vehicle_entries,
                pos,
                node,
                "minor",
                "south",
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
            )

    xml_lines: List[str] = ["<connections>"]
    crossing_specs: List[CrossingSpec] = []
    for idx, (line, meta) in enumerate(zip(crossing_lines, crossing_metadata)):
        xml_lines.append(line)
        crossing_specs.append(
            CrossingSpec(
                index=idx,
                controller_id=meta["controller_id"],
                pos_m=meta["pos_m"],
                crossing_id=meta["crossing_id"],
                node_id=meta["node_id"],
                edges=meta["edges"],
                placement=meta["placement"],
            )
        )

    vehicle_specs: List[VehicleConnectionSpec] = []
    seen: Set[str] = set()
    for line, data in vehicle_entries:
        key = line.strip()
        if key in seen:
            LOG.warning("[VAL] E405 duplicated <connection> suppressed: %s", key)
            continue
        seen.add(key)
        xml_lines.append(line)
        vehicle_specs.append(
            VehicleConnectionSpec(
                index=len(vehicle_specs),
                controller_id=data["controller_id"],
                pos_m=data["pos_m"],
                approach=data["approach"],
                approach_side=data["approach_side"],
                from_edge=data["from_edge"],
                from_lane=data["from_lane"],
                to_edge=data["to_edge"],
                to_lane=data["to_lane"],
                movement=data["movement"],
                is_uturn=data["is_uturn"],
            )
        )

    xml_lines.append("</connections>")
    xml = "\n".join(xml_lines) + "\n"
    LOG.info("rendered connections (%d lines)", len(xml_lines))
    return ConnectionsEmission(
        xml=xml,
        vehicle_connections=vehicle_specs,
        crossings=crossing_specs,
    )
