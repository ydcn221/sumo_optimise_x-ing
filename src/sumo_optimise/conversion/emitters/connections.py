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


def _alloc_excess_by_ratio(excess: int, wL: int, wT: int, wR: int) -> Tuple[int, int, int]:
    weights = [max(0, wL), max(0, wT), max(0, wR)]
    total = sum(weights)
    if excess <= 0 or total == 0:
        return (0, 0, 0)
    quotas = [excess * (w / total) for w in weights]
    floors = [int(q) for q in quotas]
    remain = excess - sum(floors)
    remainders = [(i, quotas[i] - floors[i]) for i in range(3)]
    remainders.sort(key=lambda item: (-item[1], item[0]))
    for k in range(remain):
        floors[remainders[k][0]] += 1
    return tuple(floors)


def _band_partition(n_sources: int, m_targets: int) -> List[Tuple[int, int]]:
    if n_sources <= 0 or m_targets <= 0:
        return []
    bands: List[Tuple[int, int]] = []
    for k in range(1, n_sources + 1):
        low = int((k - 1) * m_targets / n_sources) + 1
        high = int(k * m_targets / n_sources)
        bands.append((low, high))
    return bands


def _build_lane_permissions(s: int, l: int, t: int, r: int) -> List[Tuple[bool, bool, bool]]:
    has_l, has_t, has_r = (l > 0), (t > 0), (r > 0)
    movements = int(has_l) + int(has_t) + int(has_r)
    if s == 0:
        return []
    if s >= movements and movements > 0:
        base_l, base_t, base_r = int(has_l), int(has_t), int(has_r)
        excess = s - (base_l + base_t + base_r)
        add_l, add_t, add_r = _alloc_excess_by_ratio(excess, l, t, r)
        count_l = base_l + add_l
        count_t = base_t + add_t
        count_r = base_r + add_r
        perms: List[Tuple[bool, bool, bool]] = []
        perms += [(True, False, False)] * count_l
        perms += [(False, True, False)] * count_t
        perms += [(False, False, True)] * count_r
        return perms
    if s == 2 and movements == 3:
        return [(True, True, False), (False, True, True)]
    if s == 1:
        return [(has_l, has_t, has_r)]
    if movements == 0:
        return []
    perms: List[Tuple[bool, bool, bool]] = []
    perms.extend([(True, True, False)] * min(s, l))
    perms.extend([(False, True, True)] * min(max(0, s - len(perms)), r))
    while len(perms) < s:
        perms.append((has_l, has_t, has_r))
    return perms[:s]


def _emit_vehicle_connections_for_approach(
    lines: List[str],
    pos: int,
    in_edge_id: str,
    s_count: int,
    L_target: Optional[Tuple[str, int]],
    T_target: Optional[Tuple[str, int]],
    R_target: Optional[Tuple[str, int]],
) -> int:
    l = L_target[1] if L_target else 0
    t = T_target[1] if T_target else 0
    r = R_target[1] if R_target else 0

    if s_count > 0 and (l + t + r) == 0:
        LOG.error(
            "[VAL] E401 no available movements: pos=%s in_edge=%s s=%d l=%d t=%d r=%d",
            pos,
            in_edge_id,
            s_count,
            l,
            t,
            r,
        )
        raise SemanticValidationError("no available movements for approach")

    perms = _build_lane_permissions(s_count, l, t, r)
    idx_l = [i for i, (pL, pT, _pR) in enumerate(perms, start=1) if pL]
    idx_t = [i for i, (_pL, pT, _pR) in enumerate(perms, start=1) if pT]
    idx_r = [i for i, (_pL, _pT, pR) in enumerate(perms, start=1) if pR]

    emitted = 0

    if l > 0 and idx_l:
        bands = _band_partition(len(idx_l), l)
        for k, (low, high) in enumerate(bands, start=1):
            if low <= high:
                from_lane = idx_l[k - 1]
                for to_lane in range(low, high + 1):
                    lines.append(
                        f'  <connection from="{in_edge_id}" to="{L_target[0]}" '
                        f'fromLane="{from_lane}" toLane="{to_lane}"/>'
                    )
                    emitted += 1

    if t > 0 and idx_t:
        bands = _band_partition(len(idx_t), t)
        for k, (low, high) in enumerate(bands, start=1):
            if low <= high:
                from_lane = idx_t[k - 1]
                for to_lane in range(low, high + 1):
                    lines.append(
                        f'  <connection from="{in_edge_id}" to="{T_target[0]}" '
                        f'fromLane="{from_lane}" toLane="{to_lane}"/>'
                    )
                    emitted += 1

    if r > 0 and idx_r:
        bands = _band_partition(len(idx_r), r)
        for k, (low, high) in enumerate(bands, start=1):
            if low <= high:
                from_lane = idx_r[k - 1]
                for to_lane in range(low, high + 1):
                    lines.append(
                        f'  <connection from="{in_edge_id}" to="{R_target[0]}" '
                        f'fromLane="{from_lane}" toLane="{to_lane}"/>'
                    )
                    emitted += 1

    if (l + t + r) > 0 and emitted == 0 and s_count > 0:
        LOG.error(
            "[VAL] E402 zero vehicle connections emitted: pos=%s in_edge=%s s=%d l=%d t=%d r=%d",
            pos,
            in_edge_id,
            s_count,
            l,
            t,
            r,
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
            if tpl.median_continuous:
                R_target = None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)

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
            if tpl.median_continuous:
                R_target = None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)

        if exist_north:
            in_edge = minor_edge_id(pos, "to", "N")
            s_count = tpl.minor_lanes_to_main
            L_target = (
                main_edge_id("EB", pos, east), pick_main("EB", pos, east)
            ) if east is not None else None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, None, None)

        if exist_south:
            in_edge = minor_edge_id(pos, "to", "S")
            s_count = tpl.minor_lanes_to_main
            L_target = (
                main_edge_id("WB", west, pos), pick_main("WB", west, pos)
            ) if west is not None else None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, None, None)

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
