"""PlainXML node emission."""
from __future__ import annotations

from typing import Dict, List

from ..builder.ids import cluster_id, main_node_id, minor_end_node_id
from ..domain.models import BreakpointInfo, Cluster, Defaults, EventKind, MainRoadConfig, SideMinor
from ..planner.geometry import build_main_carriageway_y
from ..utils.logging import get_logger

LOG = get_logger()


def render_nodes_xml(
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: List[Cluster],
    breakpoints: List[int],
    reason_by_pos: Dict[int, BreakpointInfo],
) -> str:
    y_north, y_south = build_main_carriageway_y(main_road)
    grid_max = breakpoints[-1] if breakpoints else 0

    lines: List[str] = []
    lines.append("<nodes>")

    signalised_positions = {
        cluster.pos_m
        for cluster in clusters
        if any(bool(event.signalized) for event in cluster.events)
    }
    unsignalised_positions = {
        cluster.pos_m
        for cluster in clusters
        if any(event.signalized is False for event in cluster.events if event.type.value in ("tee", "cross"))
    }
    lane_change_only_positions = {
        pos
        for pos, info in reason_by_pos.items()
        if info.reasons and info.reasons <= {"lane_change"}
    }

    def _attrs_for_main(half: str, pos: int, y: float) -> str:
        attrs = [
            ("id", main_node_id(pos, half)),
            ("x", pos),
            ("y", y),
        ]
        if breakpoints:
            if pos == breakpoints[0] or pos == breakpoints[-1]:
                attrs.append(("fringe", "outer"))
        if pos in signalised_positions:
            attrs.append(("type", "traffic_light"))
            attrs.append(("tl", cluster_id(pos)))
        return " ".join(f'{name}="{value}"' for name, value in attrs)

    for x in breakpoints:
        lines.append(f"  <node {_attrs_for_main('north', x, y_north)}/>")
        lines.append(f"  <node {_attrs_for_main('south', x, y_south)}/>")

    for pos in breakpoints:
        if pos in (0, grid_max):
            continue
        north_node = main_node_id(pos, "north")
        south_node = main_node_id(pos, "south")
        reasons_text = ",".join(sorted(reason_by_pos[pos].reasons))
        attrs = [
            ("id", cluster_id(pos)),
            ("x", pos),
            ("y", 0),
            ("nodes", f"{north_node} {south_node}"),
        ]
        if pos in signalised_positions:
            attrs.append(("type", "traffic_light"))
            attrs.append(("tl", cluster_id(pos)))
        keep_clear = True
        if pos in unsignalised_positions or pos in lane_change_only_positions:
            keep_clear = False
        if not keep_clear:
            attrs.append(("keepClear", "false"))
        attr_text = " ".join(f'{name}="{value}"' for name, value in attrs)
        lines.append(f"  <join {attr_text}/>  <!-- reasons: {reasons_text} -->")

    for cluster in clusters:
        pos = cluster.pos_m
        for layout_event in cluster.events:
            if layout_event.type.value not in ("tee", "cross"):
                continue
            if layout_event.type == EventKind.TEE:
                branches = [layout_event.branch] if layout_event.branch else []
            else:
                branches = [SideMinor.NORTH, SideMinor.SOUTH]
            for branch in branches:
                ns = "N" if branch == SideMinor.NORTH else "S"
                offset_m = defaults.minor_road_length_m
                y_end = +offset_m if ns == "N" else -offset_m
                end_id = minor_end_node_id(pos, ns)
                attrs = [
                    ("id", end_id),
                    ("x", pos),
                    ("y", y_end),
                    ("fringe", "outer"),
                ]
                attr_text = " ".join(f'{name}="{value}"' for name, value in attrs)
                lines.append(f"  <node {attr_text}/>  <!-- minor dead_end ({ns}), offset={offset_m} from y=0 -->")

    lines.append("</nodes>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered nodes (%d lines)", len(lines))
    return xml
