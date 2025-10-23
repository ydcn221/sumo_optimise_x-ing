"""PlainXML connection and crossing emission."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
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
    ConnectionsRenderResult,
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    MainRoadConfig,
    SignalLink,
    SideMinor,
    SnapRule,
)
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.lanes import find_neighbor_segments, pick_lanes_for_segment
from ..utils.errors import SemanticValidationError
from ..utils.logging import get_logger

LOG = get_logger()


_ORDER = ("L", "T", "R", "U")


@dataclass
class ConnectionRecord:
    order: int
    from_edge: str
    to_edge: str
    from_lane: int
    to_lane: int
    movement: str
    tl_id: Optional[str]
    link_index: Optional[int] = None
    slot_index: Optional[int] = None

    def key(self) -> Tuple[str, str, int, int]:
        return (self.from_edge, self.to_edge, self.from_lane, self.to_lane)

    def raw_xml(self) -> str:
        return (
            f'  <connection from="{self.from_edge}" to="{self.to_edge}" '
            f'fromLane="{self.from_lane}" toLane="{self.to_lane}"/>'
        )

    def to_xml(self) -> str:
        attrs = [
            f'from="{self.from_edge}"',
            f'to="{self.to_edge}"',
            f'fromLane="{self.from_lane}"',
            f'toLane="{self.to_lane}"',
        ]
        return f'  <connection {" ".join(attrs)}/>'


@dataclass
class CrossingRecord:
    order: int
    crossing_id: str
    node_id: str
    edges: str
    width: float
    movement: str
    tl_id: Optional[str]
    link_index: Optional[int] = None
    slot_index: Optional[int] = None

    def to_xml(self) -> str:
        attrs = [
            f'id="{self.crossing_id}"',
            f'node="{self.node_id}"',
            f'edges="{self.edges}"',
            f'width="{self.width:.3f}"',
        ]
        if self.tl_id and self.link_index is not None:
            attrs.append(f'tl="{self.tl_id}"')
            attrs.append(f'linkIndex="{self.link_index}"')
        return f'  <crossing {" ".join(attrs)}/>'


class LinkEmissionCollector:
    def __init__(self) -> None:
        self._order = 0
        self.connections: List[ConnectionRecord] = []
        self.crossings: List[CrossingRecord] = []

    def _next_order(self) -> int:
        value = self._order
        self._order += 1
        return value

    def add_connection(
        self,
        *,
        from_edge: str,
        to_edge: str,
        from_lane: int,
        to_lane: int,
        movement: str,
        tl_id: Optional[str],
    ) -> None:
        self.connections.append(
            ConnectionRecord(
                order=self._next_order(),
                from_edge=from_edge,
                to_edge=to_edge,
                from_lane=from_lane,
                to_lane=to_lane,
                movement=movement,
                tl_id=tl_id,
            )
        )

    def add_crossing(
        self,
        *,
        crossing_id: str,
        node_id: str,
        edges: str,
        width: float,
        movement: str,
        tl_id: Optional[str],
    ) -> None:
        self.crossings.append(
            CrossingRecord(
                order=self._next_order(),
                crossing_id=crossing_id,
                node_id=node_id,
                edges=edges,
                width=width,
                movement=movement,
                tl_id=tl_id,
            )
        )

    def finalize(self) -> Tuple[List[str], List[SignalLink]]:
        ordered_connections = sorted(self.connections, key=lambda rec: rec.order)
        unique_connections: List[ConnectionRecord] = []
        seen_keys: Set[Tuple[str, str, int, int]] = set()
        for record in ordered_connections:
            key = record.key()
            if key in seen_keys:
                LOG.warning(
                    "[VAL] E405 duplicated <connection> suppressed: %s",
                    record.raw_xml().strip(),
                )
                continue
            seen_keys.add(key)
            unique_connections.append(record)

        ordered_crossings = sorted(self.crossings, key=lambda rec: rec.order)

        connections_by_tl: Dict[str, List[ConnectionRecord]] = defaultdict(list)
        for record in unique_connections:
            if record.tl_id:
                connections_by_tl[record.tl_id].append(record)

        crossings_by_tl: Dict[str, List[CrossingRecord]] = defaultdict(list)
        for record in ordered_crossings:
            if record.tl_id:
                crossings_by_tl[record.tl_id].append(record)

        metadata: List[SignalLink] = []

        for tl_id, records in connections_by_tl.items():
            for idx, record in enumerate(records):
                record.link_index = idx
                record.slot_index = idx
                metadata.append(
                    SignalLink(
                        tl_id=tl_id,
                        movement=record.movement,
                        slot_index=idx,
                        link_index=idx,
                        kind="connection",
                        element_id=f"{record.from_edge}->{record.to_edge}",
                    )
                )

        for tl_id, records in crossings_by_tl.items():
            base = len(connections_by_tl.get(tl_id, []))
            for idx, record in enumerate(records):
                record.link_index = base + idx
                record.slot_index = base + idx
                metadata.append(
                    SignalLink(
                        tl_id=tl_id,
                        movement=record.movement,
                        slot_index=record.slot_index,
                        link_index=record.link_index,
                        kind="crossing",
                        element_id=record.crossing_id,
                    )
                )

        metadata.sort(key=lambda item: (item.tl_id, item.slot_index))

        items: List[Tuple[int, str]] = []
        for record in unique_connections:
            items.append((record.order, record.to_xml()))
        for record in ordered_crossings:
            items.append((record.order, record.to_xml()))

        lines = [line for _, line in sorted(items, key=lambda pair: pair[0])]
        return lines, metadata


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
    collector: LinkEmissionCollector,
    pos: int,
    in_edge_id: str,
    s_count: int,
    L_target: Optional[Tuple[str, int]],
    T_target: Optional[Tuple[str, int]],
    R_target: Optional[Tuple[str, int]],
    U_target: Optional[Tuple[str, int]],
    *,
    tl_id: Optional[str],
    movement_prefix: str,
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

    def movement_token(suffix: str) -> str:
        if movement_prefix:
            return f"{movement_prefix}_{suffix}"
        return suffix

    def append_connection(from_lane: int, to_lane: int, edge_id: str, suffix: str) -> None:
        nonlocal emitted
        collector.add_connection(
            from_edge=in_edge_id,
            to_edge=edge_id,
            from_lane=from_lane,
            to_lane=to_lane,
            movement=movement_token(suffix),
            tl_id=tl_id,
        )
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
            append_connection(from_lane, to_lane, edge_id, "L")

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
            append_connection(idx[offset], offset + 1, edge_id, "T")
        if count > lane_count:
            for from_lane in idx[lane_count:]:
                append_connection(from_lane, lane_count, edge_id, "T")
        elif lane_count > count:
            edge_from = idx[-1]
            for to_lane in range(count + 1, lane_count + 1):
                append_connection(edge_from, to_lane, edge_id, "T")

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
            append_connection(from_lane, to_lane, edge_id, "R")

    if l > 0:
        emit_left(idx_l, L_target)
    if t > 0:
        emit_straight(idx_t, T_target)
    if r > 0:
        emit_right(idx_r, R_target)
    if u > 0:
        for offset, from_lane in enumerate(reversed(idx_u)):
            if not U_target:
                break
            edge_id, lane_count = U_target
            if lane_count <= 0:
                break
            if offset < lane_count:
                to_lane = lane_count - offset
            else:
                to_lane = lane_count
            append_connection(from_lane, to_lane, edge_id, "U")

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
) -> ConnectionsRenderResult:
    width = defaults.ped_crossing_width_m
    collector = LinkEmissionCollector()
    absorbed_pos: Set[int] = set()

    def cluster_tl_id(cluster: Cluster) -> Optional[str]:
        if any(bool(ev.signalized) for ev in cluster.events):
            return cluster_id(cluster.pos_m)
        return None

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        node = cluster_id(pos)
        tl_id = cluster_tl_id(cluster)

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
            collector.add_crossing(
                crossing_id=cid,
                node_id=node,
                edges=f"{e_to} {e_from}",
                width=width,
                movement=f"ped_minor_{b}",
                tl_id=tl_id,
            )

        if place_west:
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    collector.add_crossing(
                        crossing_id=crossing_id_main_split(pos, "West", "EB"),
                        node_id=node,
                        edges=eb_edge,
                        width=width,
                        movement="ped_main_west_EB",
                        tl_id=tl_id,
                    )
                    collector.add_crossing(
                        crossing_id=crossing_id_main_split(pos, "West", "WB"),
                        node_id=node,
                        edges=wb_edge,
                        width=width,
                        movement="ped_main_west_WB",
                        tl_id=tl_id,
                    )
                else:
                    collector.add_crossing(
                        crossing_id=crossing_id_main(pos, "West"),
                        node_id=node,
                        edges=f"{eb_edge} {wb_edge}",
                        width=width,
                        movement="ped_main_west",
                        tl_id=tl_id,
                    )

        if place_east:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    collector.add_crossing(
                        crossing_id=crossing_id_main_split(pos, "East", "EB"),
                        node_id=node,
                        edges=eb_edge,
                        width=width,
                        movement="ped_main_east_EB",
                        tl_id=tl_id,
                    )
                    collector.add_crossing(
                        crossing_id=crossing_id_main_split(pos, "East", "WB"),
                        node_id=node,
                        edges=wb_edge,
                        width=width,
                        movement="ped_main_east_WB",
                        tl_id=tl_id,
                    )
                else:
                    collector.add_crossing(
                        crossing_id=crossing_id_main(pos, "East"),
                        node_id=node,
                        edges=f"{eb_edge} {wb_edge}",
                        width=width,
                        movement="ped_main_east",
                        tl_id=tl_id,
                    )

    for cluster in clusters:
        pos = cluster.pos_m
        if pos in absorbed_pos:
            continue
        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if not mid_events:
            continue
        node = cluster_id(pos)
        tl_id = cluster_tl_id(cluster)

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
            collector.add_crossing(
                crossing_id=crossing_id_midblock_split(pos, "EB"),
                node_id=node,
                edges=eb_edge,
                width=width,
                movement="ped_mid_EB",
                tl_id=tl_id,
            )
            collector.add_crossing(
                crossing_id=crossing_id_midblock_split(pos, "WB"),
                node_id=node,
                edges=wb_edge,
                width=width,
                movement="ped_mid_WB",
                tl_id=tl_id,
            )
        else:
            collector.add_crossing(
                crossing_id=crossing_id_midblock(pos),
                node_id=node,
                edges=f"{eb_edge} {wb_edge}",
                width=width,
                movement="ped_mid",
                tl_id=tl_id,
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

        tl_id = cluster_tl_id(cluster)

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
                collector,
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                tl_id=tl_id,
                movement_prefix="main_EB",
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
                collector,
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                tl_id=tl_id,
                movement_prefix="main_WB",
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
                collector,
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                tl_id=tl_id,
                movement_prefix="minor_N",
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
                collector,
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                tl_id=tl_id,
                movement_prefix="minor_S",
            )

    inner_lines, metadata = collector.finalize()
    all_lines = ["<connections>", *inner_lines, "</connections>"]
    xml = "\n".join(all_lines) + "\n"
    LOG.info("rendered connections (%d lines)", len(all_lines))
    return ConnectionsRenderResult(xml=xml, links=metadata)
