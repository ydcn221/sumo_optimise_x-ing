"""Lane override planning and breakpoint collection."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from ..domain.models import BreakpointInfo, Cluster, JunctionTemplate, LaneOverride, MainRoadConfig, SnapRule
from ..planner.snap import grid_upper_bound, snap_distance_to_step
from ..utils.logging import get_logger

LOG = get_logger()


def compute_lane_overrides(
    main_road: MainRoadConfig,
    clusters: List[Cluster],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
) -> Dict[str, List[LaneOverride]]:
    grid_max = grid_upper_bound(main_road.length_m, snap_rule.step_m)
    eb_overrides: List[LaneOverride] = []
    wb_overrides: List[LaneOverride] = []

    for cluster in clusters:
        pos = cluster.pos_m
        jt_id = next(
            (
                ev.template_id
                for ev in cluster.events
                if ev.type.value in ("tee", "cross") and ev.template_id
            ),
            None,
        )
        if not jt_id:
            continue
        tpl = junction_template_by_id.get(jt_id)
        if not tpl or tpl.main_approach_lanes <= 0:
            continue

        d_raw = float(tpl.main_approach_begin_m)
        d = snap_distance_to_step(d_raw, snap_rule.step_m)
        LOG.info("lane-override: pos=%d, d_raw=%.3f -> d_snap=%d, lanes=%d", pos, d_raw, d, tpl.main_approach_lanes)
        if d <= 0:
            continue

        start_eb = max(0, pos - d)
        end_eb = max(0, min(grid_max, pos))
        if start_eb < end_eb:
            eb_overrides.append(LaneOverride(start=start_eb, end=end_eb, lanes=tpl.main_approach_lanes))

        start_wb = max(0, min(grid_max, pos))
        end_wb = min(grid_max, pos + d)
        if start_wb < end_wb:
            wb_overrides.append(LaneOverride(start=start_wb, end=end_wb, lanes=tpl.main_approach_lanes))

    eb_overrides.sort(key=lambda r: (r.start, r.end))
    wb_overrides.sort(key=lambda r: (r.start, r.end))
    LOG.info("lane overrides EB: %s", [(r.start, r.end, r.lanes) for r in eb_overrides])
    LOG.info("lane overrides WB: %s", [(r.start, r.end, r.lanes) for r in wb_overrides])
    return {"EB": eb_overrides, "WB": wb_overrides}


def collect_breakpoints_and_reasons(
    main_road: MainRoadConfig,
    clusters: List[Cluster],
    lane_overrides: Dict[str, List[LaneOverride]],
    snap_rule: SnapRule,
) -> Tuple[List[int], Dict[int, BreakpointInfo]]:
    grid_max = grid_upper_bound(main_road.length_m, snap_rule.step_m)
    reason_by_pos: Dict[int, BreakpointInfo] = {}

    def add(pos: int, reason: str) -> None:
        if pos < 0 or pos > grid_max:
            return
        if pos not in reason_by_pos:
            reason_by_pos[pos] = BreakpointInfo(pos=pos, reasons=set())
        reason_by_pos[pos].reasons.add(reason)

    add(0, "endpoint")
    add(grid_max, "endpoint")

    for cluster in clusters:
        x = cluster.pos_m
        if any(ev.type.value in ("tee", "cross") for ev in cluster.events):
            add(x, "junction")
        if any(ev.type.value == "xwalk_midblock" for ev in cluster.events):
            add(x, "xwalk_midblock")

    for override in lane_overrides["EB"] + lane_overrides["WB"]:
        add(override.start, "lane_change")
        add(override.end, "lane_change")

    breakpoints = sorted(reason_by_pos.keys())
    LOG.info("breakpoints: %s", [(x, sorted(reason_by_pos[x].reasons)) for x in breakpoints])
    return breakpoints, reason_by_pos


def pick_lanes_for_segment(
    direction: str,
    west: int,
    east: int,
    base_lanes: int,
    lane_overrides: Dict[str, List[LaneOverride]],
) -> int:
    max_override = 0
    for override in lane_overrides[direction]:
        if not (east <= override.start or override.end <= west):
            max_override = max(max_override, override.lanes)
    return max(base_lanes, max_override)


def find_neighbor_segments(breakpoints: List[int], pos: int) -> Tuple[Optional[int], Optional[int]]:
    if pos not in breakpoints:
        return None, None
    idx = breakpoints.index(pos)
    west = breakpoints[idx - 1] if idx - 1 >= 0 else None
    east = breakpoints[idx + 1] if idx + 1 < len(breakpoints) else None
    return west, east
