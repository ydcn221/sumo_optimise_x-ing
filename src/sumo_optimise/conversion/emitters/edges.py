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

    lines: List[str] = []
    lines.append("<edges>")

    for west, east in zip(breakpoints[:-1], breakpoints[1:]):
        lanes_eb = pick_lanes_for_segment("EB", west, east, main_road.lanes, lane_overrides)
        lines.append(
            f'  <edge id="{main_edge_id("EB", west, east)}" '
            f'from="{main_node_id(west, "north")}" to="{main_node_id(east, "north")}" '
            f'numLanes="{lanes_eb}" speed="{speed_mps:.3f}"/>'
        )
        lanes_wb = pick_lanes_for_segment("WB", west, east, main_road.lanes, lane_overrides)
        lines.append(
            f'  <edge id="{main_edge_id("WB", east, west)}" '
            f'from="{main_node_id(east, "south")}" to="{main_node_id(west, "south")}" '
            f'numLanes="{lanes_wb}" speed="{speed_mps:.3f}"/>'
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
                lines.append(
                    f'  <edge id="{minor_edge_id(pos, "to", ns)}" '
                    f'from="{minor_end_node_id(pos, ns)}" to="{attach_node}" '
                    f'numLanes="{tpl.minor_lanes_approach}" speed="{speed_mps:.3f}"/>'
                )
                lines.append(
                    f'  <edge id="{minor_edge_id(pos, "from", ns)}" '
                    f'from="{attach_node}" to="{minor_end_node_id(pos, ns)}" '
                    f'numLanes="{tpl.minor_lanes_departure}" speed="{speed_mps:.3f}"/>'
                )

    lines.append("</edges>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered edges (%d lines)", len(lines))
    return xml
