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
    y_eb, y_wb = build_main_carriageway_y(main_road)
    grid_max = breakpoints[-1] if breakpoints else 0

    lines: List[str] = []
    lines.append("<nodes>")

    signalised_positions = {
        cluster.pos_m
        for cluster in clusters
        if any(bool(event.signalized) for event in cluster.events)
    }

    def _attrs_for_main(direction: str, pos: int, y: float) -> str:
        attrs = [
            ("id", main_node_id(direction, pos)),
            ("x", pos),
            ("y", y),
        ]
        if pos in signalised_positions:
            attrs.append(("type", "traffic_light"))
            attrs.append(("tl", cluster_id(pos)))
        return " ".join(f'{name}="{value}"' for name, value in attrs)

    for x in breakpoints:
        lines.append(f"  <node {_attrs_for_main('EB', x, y_eb)}/>")
        lines.append(f"  <node {_attrs_for_main('WB', x, y_wb)}/>")

    for pos in breakpoints:
        if pos in (0, grid_max):
            continue
        eb = main_node_id("EB", pos)
        wb = main_node_id("WB", pos)
        reasons_text = ",".join(sorted(reason_by_pos[pos].reasons))
        attrs = [
            ("id", cluster_id(pos)),
            ("x", pos),
            ("y", 0),
            ("nodes", f"{eb} {wb}"),
        ]
        if pos in signalised_positions:
            attrs.append(("type", "traffic_light"))
            attrs.append(("tl", cluster_id(pos)))
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
                lines.append(
                    f'  <node id="{end_id}" x="{pos}" y="{y_end}"/>'
                    f'  <!-- minor dead_end ({ns}), offset={offset_m} from y=0 -->'
                )

    lines.append("</nodes>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered nodes (%d lines)", len(lines))
    return xml
