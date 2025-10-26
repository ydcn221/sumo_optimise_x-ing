"""Helpers for constructing demand endpoint catalogues."""
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
    EndpointCatalog,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    MainRoadConfig,
    PedestrianEndpoint,
    SideMinor,
    SnapRule,
    VehicleEndpoint,
)
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.lanes import find_neighbor_segments, pick_lanes_for_segment
from ..utils.logging import get_logger

LOG = get_logger()


_VEHICLE_CATEGORY_ORDER = {
    "main_EB": 0,
    "main_WB": 1,
    "minor_N": 2,
    "minor_S": 3,
}

_PEDESTRIAN_MOVEMENT_ORDER = {
    "ped_minor_north": 0,
    "ped_minor_south": 1,
    "ped_main_west": 2,
    "ped_main_west_EB": 2,
    "ped_main_west_WB": 3,
    "ped_main_east": 4,
    "ped_main_east_EB": 4,
    "ped_main_east_WB": 5,
    "ped_mid": 6,
    "ped_mid_EB": 6,
    "ped_mid_WB": 7,
}


def _vehicle_endpoint_id(category: str, pos: int, role: str) -> str:
    return f"Endpoint.Vehicle.{category}.{pos}.{role}"


def _cluster_tl_id(cluster: Cluster) -> Optional[str]:
    if any(bool(ev.signalized) for ev in cluster.events):
        return cluster_id(cluster.pos_m)
    return None


def _resolve_template(
    cluster: Cluster,
    junction_template_by_id: Dict[str, JunctionTemplate],
) -> Optional[JunctionTemplate]:
    for ev in cluster.events:
        if ev.type not in (EventKind.TEE, EventKind.CROSS):
            continue
        if not ev.template_id:
            continue
        tpl = junction_template_by_id.get(ev.template_id)
        if tpl:
            return tpl
    return None


def _get_main_edges_west_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    west, _ = find_neighbor_segments(breakpoints, pos)
    if west is None:
        return None, None
    return main_edge_id("EB", west, pos), main_edge_id("WB", west, pos)


def _get_main_edges_east_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    _, east = find_neighbor_segments(breakpoints, pos)
    if east is None:
        return None, None
    return main_edge_id("EB", pos, east), main_edge_id("WB", pos, east)


def build_endpoint_catalog(
    *,
    defaults: Defaults,
    main_road: MainRoadConfig,
    clusters: List[Cluster],
    breakpoints: List[int],
    junction_template_by_id: Dict[str, JunctionTemplate],
    lane_overrides: Dict[str, List[LaneOverride]],
    snap_rule: SnapRule,
) -> EndpointCatalog:
    """Enumerate vehicle and pedestrian endpoints for demand planning."""

    vehicle_endpoints: List[VehicleEndpoint] = []
    pedestrian_endpoints: List[PedestrianEndpoint] = []

    absorbed_midblock_positions: Set[int] = set()

    def add_vehicle_endpoint(
        *,
        pos: int,
        category: str,
        edge_id: Optional[str],
        lane_count: int,
        is_inbound: bool,
        tl_id: Optional[str],
    ) -> None:
        if not edge_id or lane_count <= 0:
            return
        endpoint_id = _vehicle_endpoint_id(category, pos, "in" if is_inbound else "out")
        vehicle_endpoints.append(
            VehicleEndpoint(
                id=endpoint_id,
                pos=pos,
                category=category,
                edge_id=edge_id,
                lane_count=lane_count,
                is_inbound=is_inbound,
                tl_id=tl_id,
            )
        )

    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
        if not junction_events:
            continue

        tpl = _resolve_template(cluster, junction_template_by_id)
        if not tpl:
            LOG.warning("junction template missing for cluster at %s", pos)
            continue

        tl_id = _cluster_tl_id(cluster)
        west, east = find_neighbor_segments(breakpoints, pos)

        if west is not None:
            lane_in = pick_lanes_for_segment("EB", west, pos, main_road.lanes, lane_overrides)
            add_vehicle_endpoint(
                pos=pos,
                category="main_EB",
                edge_id=main_edge_id("EB", west, pos),
                lane_count=lane_in,
                is_inbound=True,
                tl_id=tl_id,
            )
        if east is not None:
            lane_out = pick_lanes_for_segment("EB", pos, east, main_road.lanes, lane_overrides)
            add_vehicle_endpoint(
                pos=pos,
                category="main_EB",
                edge_id=main_edge_id("EB", pos, east),
                lane_count=lane_out,
                is_inbound=False,
                tl_id=tl_id,
            )

        if east is not None:
            lane_in = pick_lanes_for_segment("WB", pos, east, main_road.lanes, lane_overrides)
            add_vehicle_endpoint(
                pos=pos,
                category="main_WB",
                edge_id=main_edge_id("WB", pos, east),
                lane_count=lane_in,
                is_inbound=True,
                tl_id=tl_id,
            )
        if west is not None:
            lane_out = pick_lanes_for_segment("WB", west, pos, main_road.lanes, lane_overrides)
            add_vehicle_endpoint(
                pos=pos,
                category="main_WB",
                edge_id=main_edge_id("WB", west, pos),
                lane_count=lane_out,
                is_inbound=False,
                tl_id=tl_id,
            )

        exist_north = False
        exist_south = False
        if any(ev.type == EventKind.CROSS for ev in junction_events):
            exist_north = True
            exist_south = True
        else:
            branch = junction_events[0].branch
            if branch == SideMinor.NORTH:
                exist_north = True
            if branch == SideMinor.SOUTH:
                exist_south = True

        if exist_north:
            add_vehicle_endpoint(
                pos=pos,
                category="minor_N",
                edge_id=minor_edge_id(pos, "to", "N"),
                lane_count=tpl.minor_lanes_to_main,
                is_inbound=True,
                tl_id=tl_id,
            )
            add_vehicle_endpoint(
                pos=pos,
                category="minor_N",
                edge_id=minor_edge_id(pos, "from", "N"),
                lane_count=tpl.minor_lanes_from_main,
                is_inbound=False,
                tl_id=tl_id,
            )

        if exist_south:
            add_vehicle_endpoint(
                pos=pos,
                category="minor_S",
                edge_id=minor_edge_id(pos, "to", "S"),
                lane_count=tpl.minor_lanes_to_main,
                is_inbound=True,
                tl_id=tl_id,
            )
            add_vehicle_endpoint(
                pos=pos,
                category="minor_S",
                edge_id=minor_edge_id(pos, "from", "S"),
                lane_count=tpl.minor_lanes_from_main,
                is_inbound=False,
                tl_id=tl_id,
            )

        place_west = False
        place_east = False
        split_main = False
        for ev in junction_events:
            if ev.main_ped_crossing_placement:
                place_west = place_west or bool(ev.main_ped_crossing_placement.get("west"))
                place_east = place_east or bool(ev.main_ped_crossing_placement.get("east"))
            split_main = split_main or bool(ev.refuge_island_on_main)

        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if mid_events:
            absorbed_midblock_positions.add(pos)
            for mid_event in mid_events:
                side = decide_midblock_side_for_collision(mid_event.pos_m_raw, pos, snap_rule.tie_break)
                if side == "west":
                    place_west = True
                else:
                    place_east = True

        node = cluster_id(pos)
        tl_id_crossing = tl_id

        if any(ev.type == EventKind.CROSS for ev in junction_events):
            branches = ["north", "south"]
        else:
            branch_val = junction_events[0].branch.value if junction_events[0].branch else None
            branches = [branch_val] if branch_val in ("north", "south") else []

        for branch_name in branches:
            ns = "N" if branch_name == "north" else "S"
            cid = crossing_id_minor(pos, ns)
            pedestrian_endpoints.append(
                PedestrianEndpoint(
                    id=cid,
                    pos=pos,
                    movement=f"ped_minor_{branch_name}",
                    node_id=node,
                    edges=(
                        f"{minor_edge_id(pos, 'to', ns)}",
                        f"{minor_edge_id(pos, 'from', ns)}",
                    ),
                    width=defaults.ped_crossing_width_m,
                    tl_id=tl_id_crossing,
                )
            )

        if place_west:
            eb_edge, wb_edge = _get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main_split(pos, "West", "EB"),
                            pos=pos,
                            movement="ped_main_west_EB",
                            node_id=node,
                            edges=(eb_edge,),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main_split(pos, "West", "WB"),
                            pos=pos,
                            movement="ped_main_west_WB",
                            node_id=node,
                            edges=(wb_edge,),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )
                else:
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main(pos, "West"),
                            pos=pos,
                            movement="ped_main_west",
                            node_id=node,
                            edges=(eb_edge, wb_edge),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )

        if place_east:
            eb_edge, wb_edge = _get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main_split(pos, "East", "EB"),
                            pos=pos,
                            movement="ped_main_east_EB",
                            node_id=node,
                            edges=(eb_edge,),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main_split(pos, "East", "WB"),
                            pos=pos,
                            movement="ped_main_east_WB",
                            node_id=node,
                            edges=(wb_edge,),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )
                else:
                    pedestrian_endpoints.append(
                        PedestrianEndpoint(
                            id=crossing_id_main(pos, "East"),
                            pos=pos,
                            movement="ped_main_east",
                            node_id=node,
                            edges=(eb_edge, wb_edge),
                            width=defaults.ped_crossing_width_m,
                            tl_id=tl_id_crossing,
                        )
                    )

    for cluster in clusters:
        pos = cluster.pos_m
        if pos in absorbed_midblock_positions:
            continue
        mid_events = [ev for ev in cluster.events if ev.type == EventKind.XWALK_MIDBLOCK]
        if not mid_events:
            continue

        node = cluster_id(pos)
        tl_id = _cluster_tl_id(cluster)

        west, east = find_neighbor_segments(breakpoints, pos)
        if west is None or east is None:
            LOG.warning("midblock at %s lacks adjacent main edges; skipping demand endpoint", pos)
            continue

        eb_in_edge = main_edge_id("EB", west, pos)
        eb_out_edge = main_edge_id("EB", pos, east)
        wb_in_edge = main_edge_id("WB", pos, east)
        wb_out_edge = main_edge_id("WB", west, pos)

        if snap_rule.tie_break == "toward_west":
            eb_edge, wb_edge = eb_in_edge, wb_out_edge
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = eb_out_edge, wb_in_edge
        else:
            eb_edge, wb_edge = eb_out_edge, wb_in_edge
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = eb_in_edge, wb_out_edge

        if not (eb_edge and wb_edge):
            LOG.warning("midblock at %s missing edge pairing for crossing", pos)
            continue

        split_midblock = any(bool(ev.refuge_island_on_main) for ev in mid_events)
        if split_midblock:
            pedestrian_endpoints.append(
                PedestrianEndpoint(
                    id=crossing_id_midblock_split(pos, "EB"),
                    pos=pos,
                    movement="ped_mid_EB",
                    node_id=node,
                    edges=(eb_edge,),
                    width=defaults.ped_crossing_width_m,
                    tl_id=tl_id,
                )
            )
            pedestrian_endpoints.append(
                PedestrianEndpoint(
                    id=crossing_id_midblock_split(pos, "WB"),
                    pos=pos,
                    movement="ped_mid_WB",
                    node_id=node,
                    edges=(wb_edge,),
                    width=defaults.ped_crossing_width_m,
                    tl_id=tl_id,
                )
            )
        else:
            pedestrian_endpoints.append(
                PedestrianEndpoint(
                    id=crossing_id_midblock(pos),
                    pos=pos,
                    movement="ped_mid",
                    node_id=node,
                    edges=(eb_edge, wb_edge),
                    width=defaults.ped_crossing_width_m,
                    tl_id=tl_id,
                )
            )

    vehicle_endpoints.sort(
        key=lambda ep: (
            ep.pos,
            _VEHICLE_CATEGORY_ORDER.get(ep.category, 99),
            0 if ep.is_inbound else 1,
            ep.edge_id,
        )
    )
    pedestrian_endpoints.sort(
        key=lambda ep: (
            ep.pos,
            _PEDESTRIAN_MOVEMENT_ORDER.get(ep.movement, 99),
            ep.id,
        )
    )

    return EndpointCatalog(
        vehicle_endpoints=vehicle_endpoints,
        pedestrian_endpoints=pedestrian_endpoints,
    )
