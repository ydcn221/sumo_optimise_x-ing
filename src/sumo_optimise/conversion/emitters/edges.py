"""PlainXML edge emission."""
from __future__ import annotations

from typing import Dict, List

from ..builder.ids import main_edge_id, main_node_id, minor_edge_id, minor_end_node_id
from ..domain.models import Cluster, Defaults, LaneOverride, MainRoadConfig
from ..planner.lanes import pick_lanes_for_segment
from ..planner.snap import kmh_to_mps
from ..utils.errors import InvalidConfigurationError
from ..utils.logging import get_logger

LOG = get_logger()


def attach_main_node_for_minor(ns: str, pos: int) -> str:
    if ns == "N":
        return main_node_id(pos, "north")
    if ns == "S":
        return main_node_id(pos, "south")
    raise InvalidConfigurationError(f"unknown ns={ns}")


def render_edges_xml(
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: List[Cluster],
    breakpoints: List[int],
    lane_overrides: Dict[str, List[LaneOverride]],
) -> str:
    speed_mps = kmh_to_mps(defaults.speed_kmh)
    sidewalk_width = defaults.sidewalk_width_m if defaults.sidewalk_width_m is not None else 3.0

    lines: List[str] = []
    lines.append("<edges>")

    def _emit_edge(edge_id: str, from_node: str, to_node: str, lanes: int) -> None:
        total_lanes = lanes + 1  # include sidewalk at index 0
        lines.append(
            f'  <edge id="{edge_id}" '
            f'from="{from_node}" to="{to_node}" '
            f'numLanes="{total_lanes}" speed="{speed_mps:.3f}">'
        )
        lines.append(f'    <lane index="0" allow="pedestrian" width="{sidewalk_width:.2f}"/>')
        for idx in range(lanes):
            lines.append(f'    <lane index="{idx + 1}" disallow="pedestrian"/>')
        lines.append("  </edge>")

    for west, east in zip(breakpoints[:-1], breakpoints[1:]):
        lanes_eb = pick_lanes_for_segment("EB", west, east, main_road.lanes, lane_overrides)
        _emit_edge(
            main_edge_id("EB", west, east),
            main_node_id(west, "north"),
            main_node_id(east, "north"),
            lanes_eb,
        )
        lanes_wb = pick_lanes_for_segment("WB", west, east, main_road.lanes, lane_overrides)
        _emit_edge(
            main_edge_id("WB", east, west),
            main_node_id(east, "south"),
            main_node_id(west, "south"),
            lanes_wb,
        )

    for cluster in clusters:
        pos = cluster.pos_m
        for layout_event in cluster.events:
            if layout_event.type.value not in ("tee", "cross"):
                continue
            if not layout_event.junction:
                LOG.warning("junction at %s has no geometry (skip)", pos)
                continue
            tpl = layout_event.junction

            if layout_event.type.value == "tee":
                branches = [layout_event.branch.value] if layout_event.branch else []
            else:
                branches = ["north", "south"]

            for b in branches:
                ns = "N" if b == "north" else "S"
                attach_node = attach_main_node_for_minor(ns, pos)
                _emit_edge(
                    minor_edge_id(pos, "to", ns),
                    minor_end_node_id(pos, ns),
                    attach_node,
                    tpl.minor_lanes_approach,
                )
                _emit_edge(
                    minor_edge_id(pos, "from", ns),
                    attach_node,
                    minor_end_node_id(pos, ns),
                    tpl.minor_lanes_departure,
                )

    lines.append("</edges>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered edges (%d lines)", len(lines))
    return xml
