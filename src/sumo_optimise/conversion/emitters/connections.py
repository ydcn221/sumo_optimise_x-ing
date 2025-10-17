"""PlainXML connection and crossing emission."""
from __future__ import annotations

from dataclasses import dataclass, field
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
from ..utils.signals import cluster_has_signal_reference

LOG = get_logger()


_ORDER = ("L", "T", "R", "U")


@dataclass(frozen=True)
class ApproachInfo:
    """Descriptor for a junction approach used when planning connections."""

    road: str  # ``main`` or ``minor``
    direction: str  # ``EB``/``WB``/``N``/``S``

    @property
    def key(self) -> str:
        return f"{self.road}_{self.direction}"


@dataclass(frozen=True)
class VehicleConnectionPlan:
    """Planned vehicle connection including movement metadata."""

    from_edge: str
    to_edge: str
    from_lane: int
    to_lane: int
    approach: ApproachInfo
    movement: str  # ``L``/``T``/``R``/``U``

    @property
    def normalized_movement(self) -> str:
        """Normalize movements so that U-turns reuse the right-turn bucket."""

        return "R" if self.movement == "U" else self.movement


@dataclass(frozen=True)
class CrossingPlan:
    """Planned pedestrian crossing controlled at the cluster."""

    crossing_id: str
    node: str
    edges: Tuple[str, ...]
    width: float
    category: str  # ``minor`` / ``main`` / ``midblock``
    main_side: Optional[str] = None  # ``west`` / ``east`` for main-road crossings
    lane_directions: Tuple[str, ...] = ()
    two_stage: bool = False


@dataclass
class ClusterLinkPlan:
    """Aggregated movements associated with a cluster position."""

    crossings: List[CrossingPlan] = field(default_factory=list)
    vehicle_connections: List[VehicleConnectionPlan] = field(default_factory=list)
    _vehicle_keys: Set[Tuple[str, str, int, int, str]] = field(
        default_factory=set, init=False, repr=False
    )

    def add_vehicle_connection(self, connection: VehicleConnectionPlan) -> bool:
        """Register ``connection`` if it has not been seen for the cluster."""

        key = (
            connection.from_edge,
            connection.to_edge,
            connection.from_lane,
            connection.to_lane,
            connection.movement,
        )
        if key in self._vehicle_keys:
            return False
        self._vehicle_keys.add(key)
        self.vehicle_connections.append(connection)
        return True


@dataclass(frozen=True)
class LinkIndexEntry:
    """Mapping of a link index to the physical connection/crossing."""

    link_index: int
    kind: str  # ``vehicle`` or ``pedestrian``
    tokens: Tuple[str, ...]
    movement: str  # ``L``/``T``/``R``/``P``
    connection: Optional[VehicleConnectionPlan] = None
    crossing: Optional[CrossingPlan] = None
    conflicts_with: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PedestrianSignalLink:
    """Lookup entry linking a pedestrian crossing to its signal metadata."""

    crossing_id: str
    tl_id: str
    link_index: int


@dataclass(frozen=True)
class ClusterLinkIndexing:
    """Per-cluster lookup for signalised movements."""

    tl_id: str
    links: Tuple[LinkIndexEntry, ...]
    token_to_indices: Dict[str, Tuple[int, ...]]
    pedestrian_links: Tuple[PedestrianSignalLink, ...] = ()

    def get_pedestrian_link(self, crossing_id: str) -> Optional[PedestrianSignalLink]:
        """Return the pedestrian link metadata for ``crossing_id`` if present."""

        for link in self.pedestrian_links:
            if link.crossing_id == crossing_id:
                return link
        return None



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


def _movement_tokens(approach: ApproachInfo, movement: str) -> Tuple[str, ...]:
    """Return movement tokens controlling ``movement`` for the given approach."""

    normalized = "R" if movement == "U" else movement
    base: Set[str] = {f"{approach.road}_{normalized}"}
    if approach.road == "main":
        base.add(f"{approach.direction}_{normalized}")
    return tuple(sorted(base))


def _pedestrian_conflict_tags(crossing: CrossingPlan) -> Tuple[str, ...]:
    """Return conflict tags (``left``/``right``) associated with a crossing plan."""

    if crossing.category == "midblock":
        return ()
    tags: List[str] = []
    if "WB" in crossing.lane_directions:
        tags.append("left")
    if "EB" in crossing.lane_directions:
        tags.append("right")
    return tuple(tags) if tags else ("left", "right")


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


def _plan_vehicle_connections_for_approach(
    pos: int,
    in_edge_id: str,
    s_count: int,
    L_target: Optional[Tuple[str, int]],
    T_target: Optional[Tuple[str, int]],
    R_target: Optional[Tuple[str, int]],
    U_target: Optional[Tuple[str, int]],
    approach: ApproachInfo,
) -> List[VehicleConnectionPlan]:
    """Derive the set of vehicle connections for a single approach."""

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

    plans: List[VehicleConnectionPlan] = []

    def append_plan(from_lane: int, to_lane: int, edge_id: str, movement: str) -> None:
        plans.append(
            VehicleConnectionPlan(
                from_edge=in_edge_id,
                to_edge=edge_id,
                from_lane=from_lane,
                to_lane=to_lane,
                approach=approach,
                movement=movement,
            )
        )

    def emit_left(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        for offset, from_lane in enumerate(idx):
            to_lane = min(offset + 1, lane_count)
            append_plan(from_lane, to_lane, edge_id, "L")

    def emit_straight(idx: List[int], target: Optional[Tuple[str, int]]) -> None:
        if not idx or not target:
            return
        edge_id, lane_count = target
        if lane_count <= 0:
            return
        count = len(idx)
        matched = min(count, lane_count)
        for offset in range(matched):
            append_plan(idx[offset], offset + 1, edge_id, "T")
        if count > lane_count:
            for from_lane in idx[lane_count:]:
                append_plan(from_lane, lane_count, edge_id, "T")
        elif lane_count > count:
            edge_from = idx[-1]
            for to_lane in range(count + 1, lane_count + 1):
                append_plan(edge_from, to_lane, edge_id, "T")

    def emit_right(idx: List[int], target: Optional[Tuple[str, int]], movement: str) -> None:
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
            append_plan(from_lane, to_lane, edge_id, movement)

    if l > 0:
        emit_left(idx_l, L_target)
    if t > 0:
        emit_straight(idx_t, T_target)
    if r > 0:
        emit_right(idx_r, R_target, "R")
    if u > 0:
        emit_right(idx_u, U_target or R_target, "U")

    if (l + t + r + u) > 0 and not plans and s_count > 0:
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
    return plans


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


def _build_cluster_link_indexing(
    plans: Dict[int, ClusterLinkPlan],
    signalized_positions: Set[int],
) -> Dict[int, ClusterLinkIndexing]:
    """Create link index mapping for signalised clusters."""

    mapping: Dict[int, ClusterLinkIndexing] = {}
    for pos in sorted(signalized_positions):
        plan = plans.get(pos)
        if not plan:
            continue
        links: List[LinkIndexEntry] = []
        token_to_indices: Dict[str, List[int]] = {}
        pedestrian_links: List[PedestrianSignalLink] = []
        index = 0
        for conn in plan.vehicle_connections:
            tokens = _movement_tokens(conn.approach, conn.movement)
            entry = LinkIndexEntry(
                link_index=index,
                kind="vehicle",
                tokens=tokens,
                movement=conn.normalized_movement,
                connection=conn,
            )
            links.append(entry)
            for token in tokens:
                token_to_indices.setdefault(token, []).append(index)
            index += 1
        for crossing in plan.crossings:
            tokens = ("pedestrian",)
            entry = LinkIndexEntry(
                link_index=index,
                kind="pedestrian",
                tokens=tokens,
                movement="P",
                crossing=crossing,
                conflicts_with=_pedestrian_conflict_tags(crossing),
            )
            links.append(entry)
            for token in tokens:
                token_to_indices.setdefault(token, []).append(index)
            pedestrian_links.append(
                PedestrianSignalLink(
                    crossing_id=crossing.crossing_id,
                    tl_id=cluster_id(pos),
                    link_index=index,
                )
            )
            index += 1
        mapping[pos] = ClusterLinkIndexing(
            tl_id=cluster_id(pos),
            links=tuple(links),
            token_to_indices={key: tuple(sorted(values)) for key, values in token_to_indices.items()},
            pedestrian_links=tuple(pedestrian_links),
        )
    return mapping


def render_connections_xml(
    defaults: Defaults,
    clusters: List[Cluster],
    breakpoints: List[int],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    lane_overrides: Dict[str, List[LaneOverride]],
) -> Tuple[str, Dict[int, ClusterLinkIndexing]]:
    width = defaults.ped_crossing_width_m
    lines: List[str] = ["<connections>"]
    plans: Dict[int, ClusterLinkPlan] = {}

    def get_plan(pos: int) -> ClusterLinkPlan:
        return plans.setdefault(pos, ClusterLinkPlan())

    def append_connection(plan: ClusterLinkPlan, connection: VehicleConnectionPlan) -> None:
        line = (
            f'  <connection from="{connection.from_edge}" to="{connection.to_edge}" '
            f'fromLane="{connection.from_lane}" toLane="{connection.to_lane}"/>'
        )
        if plan.add_vehicle_connection(connection):
            lines.append(line)
        else:
            LOG.warning("[VAL] E405 duplicated <connection> suppressed: %s", line.strip())

    absorbed_pos: Set[int] = set()

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        plan = get_plan(pos)
        node = cluster_id(pos)

        place_west = False
        place_east = False
        refuge_main_values: List[bool] = []
        two_stage_main_values: List[bool] = []
        for ev in junction_events:
            if ev.main_ped_crossing_placement:
                place_west = place_west or bool(ev.main_ped_crossing_placement.get("west", False))
                place_east = place_east or bool(ev.main_ped_crossing_placement.get("east", False))
            if ev.template_id and ev.template_id in junction_template_by_id:
                tpl = junction_template_by_id[ev.template_id]
                refuge_value = tpl.refuge_island_on_main
                if ev.refuge_island_on_main is not None:
                    refuge_value = bool(ev.refuge_island_on_main)
                two_stage_value = tpl.two_stage_ped_crossing_on_main
                if ev.two_stage_ped_crossing_on_main is not None:
                    two_stage_value = bool(ev.two_stage_ped_crossing_on_main)
                if two_stage_value and not refuge_value:
                    LOG.error(
                        "[VAL] E309 junction crossing two_stage requires refuge: node=%s tpl=%s",
                        node,
                        tpl.id,
                    )
                    raise SemanticValidationError("junction two_stage requires refuge")
                refuge_main_values.append(refuge_value)
                two_stage_main_values.append(two_stage_value)

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
            plan.crossings.append(
                CrossingPlan(
                    crossing_id=cid,
                    node=node,
                    edges=(e_to, e_from),
                    width=width,
                    category="minor",
                )
            )

        refuge_main = any(refuge_main_values)
        if refuge_main_values and len(set(refuge_main_values)) > 1:
            LOG.warning(
                "[BUILD] inconsistent refuge settings merged at node=%s (using any=True)",
                node,
            )
        two_stage_main = any(two_stage_main_values) and refuge_main

        if place_west:
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if refuge_main:
                    cid = crossing_id_main_split(pos, "West", "EB")
                    lines.append(
                        f'  <crossing id="{cid}" node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid,
                            node=node,
                            edges=(eb_edge,),
                            width=width,
                            category="main",
                            main_side="west",
                            lane_directions=("EB",),
                            two_stage=two_stage_main,
                        )
                    )
                    cid2 = crossing_id_main_split(pos, "West", "WB")
                    lines.append(
                        f'  <crossing id="{cid2}" node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid2,
                            node=node,
                            edges=(wb_edge,),
                            width=width,
                            category="main",
                            main_side="west",
                            lane_directions=("WB",),
                            two_stage=two_stage_main,
                        )
                    )
                else:
                    cid = crossing_id_main(pos, "West")
                    lines.append(
                        f'  <crossing id="{cid}" node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid,
                            node=node,
                            edges=(eb_edge, wb_edge),
                            width=width,
                            category="main",
                            main_side="west",
                            lane_directions=("EB", "WB"),
                            two_stage=False,
                        )
                    )

        if place_east:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if refuge_main:
                    cid = crossing_id_main_split(pos, "East", "EB")
                    lines.append(
                        f'  <crossing id="{cid}" node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid,
                            node=node,
                            edges=(eb_edge,),
                            width=width,
                            category="main",
                            main_side="east",
                            lane_directions=("EB",),
                            two_stage=two_stage_main,
                        )
                    )
                    cid2 = crossing_id_main_split(pos, "East", "WB")
                    lines.append(
                        f'  <crossing id="{cid2}" node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid2,
                            node=node,
                            edges=(wb_edge,),
                            width=width,
                            category="main",
                            main_side="east",
                            lane_directions=("WB",),
                            two_stage=two_stage_main,
                        )
                    )
                else:
                    cid = crossing_id_main(pos, "East")
                    lines.append(
                        f'  <crossing id="{cid}" node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
                    )
                    plan.crossings.append(
                        CrossingPlan(
                            crossing_id=cid,
                            node=node,
                            edges=(eb_edge, wb_edge),
                            width=width,
                            category="main",
                            main_side="east",
                            lane_directions=("EB", "WB"),
                            two_stage=False,
                        )
                    )

    for cluster in clusters:
        pos = cluster.pos_m
        if pos in absorbed_pos:
            continue
        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if not mid_events:
            continue
        plan = get_plan(pos)
        node = cluster_id(pos)

        if snap_rule.tie_break == "toward_west":
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            side = "west" if eb_edge and wb_edge else None
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
                side = "east" if eb_edge and wb_edge else side
        else:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            side = "east" if eb_edge and wb_edge else None
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
                side = "west" if eb_edge and wb_edge else side

        if not (eb_edge and wb_edge):
            LOG.warning("midblock at %s: adjacent main edges not found; crossing omitted", pos)
            continue

        refuge_midblock = any(bool(ev.refuge_island_on_main) for ev in mid_events)
        if mid_events and len({bool(ev.refuge_island_on_main) for ev in mid_events}) > 1:
            LOG.warning("[BUILD] inconsistent midblock refuge flags merged at node=%s", node)
        two_stage_midblock = any(bool(ev.two_stage_ped_crossing_on_main) for ev in mid_events) and refuge_midblock

        if refuge_midblock:
            cid = crossing_id_midblock_split(pos, "EB")
            lines.append(
                f'  <crossing id="{cid}" node="{node}" edges="{eb_edge}" width="{width:.3f}"/>'
            )
            plan.crossings.append(
                CrossingPlan(
                    crossing_id=cid,
                    node=node,
                    edges=(eb_edge,),
                    width=width,
                    category="midblock",
                    main_side=side,
                    lane_directions=("EB",),
                    two_stage=two_stage_midblock,
                )
            )
            cid2 = crossing_id_midblock_split(pos, "WB")
            lines.append(
                f'  <crossing id="{cid2}" node="{node}" edges="{wb_edge}" width="{width:.3f}"/>'
            )
            plan.crossings.append(
                CrossingPlan(
                    crossing_id=cid2,
                    node=node,
                    edges=(wb_edge,),
                    width=width,
                    category="midblock",
                    main_side=side,
                    lane_directions=("WB",),
                    two_stage=two_stage_midblock,
                )
            )
        else:
            cid = crossing_id_midblock(pos)
            lines.append(
                f'  <crossing id="{cid}" node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>'
            )
            plan.crossings.append(
                CrossingPlan(
                    crossing_id=cid,
                    node=node,
                    edges=(eb_edge, wb_edge),
                    width=width,
                    category="midblock",
                    main_side=side,
                    lane_directions=("EB", "WB"),
                    two_stage=False,
                )
            )

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue
        plan = get_plan(pos)
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
            plans_west = _plan_vehicle_connections_for_approach(
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                ApproachInfo(road="main", direction="EB"),
            )
            for conn in plans_west:
                append_connection(plan, conn)

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
            plans_east = _plan_vehicle_connections_for_approach(
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                ApproachInfo(road="main", direction="WB"),
            )
            for conn in plans_east:
                append_connection(plan, conn)

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
            plans_north = _plan_vehicle_connections_for_approach(
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                ApproachInfo(road="minor", direction="N"),
            )
            for conn in plans_north:
                append_connection(plan, conn)

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
            plans_south = _plan_vehicle_connections_for_approach(
                pos,
                in_edge,
                s_count,
                L_target,
                T_target,
                R_target,
                U_target,
                ApproachInfo(road="minor", direction="S"),
            )
            for conn in plans_south:
                append_connection(plan, conn)

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

    signalized_positions: Set[int] = {
        cluster.pos_m
        for cluster in clusters
        if cluster_has_signal_reference(cluster)
    }
    indexing = _build_cluster_link_indexing(plans, signalized_positions)
    return xml, indexing
