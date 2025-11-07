"""Construct a pedestrian graph for demand propagation."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import networkx as nx

from ...builder.ids import cluster_id, main_node_id
from ...domain.models import EndpointCatalog, PedestrianSide, Cluster, Defaults, MainRoadConfig
from ...planner.geometry import build_main_carriageway_y
from .identifier import minor_endpoint_id

GraphType = nx.MultiGraph


def _add_main_nodes(
    graph: GraphType,
    *,
    breakpoints: Sequence[int],
    y_north: float,
    y_south: float,
) -> None:
    if not breakpoints:
        return
    start = breakpoints[0]
    end = breakpoints[-1]
    for pos in breakpoints:
        node_n = main_node_id(pos, "north")
        node_s = main_node_id(pos, "south")
        graph.add_node(
            node_n,
            coord=(float(pos), float(y_north)),
            pos=pos,
            node_type="main_north",
            cluster_id=cluster_id(pos),
            is_endpoint=pos in {start, end},
        )
        graph.add_node(
            node_s,
            coord=(float(pos), float(y_south)),
            pos=pos,
            node_type="main_south",
            cluster_id=cluster_id(pos),
            is_endpoint=pos in {start, end},
        )


def _collect_minor_branches(clusters: Sequence[Cluster]) -> Dict[int, Set[str]]:
    branches: Dict[int, Set[str]] = defaultdict(set)
    for cluster in clusters:
        pos = cluster.pos_m
        for event in cluster.events:
            if event.type.value not in ("tee", "cross"):
                continue
            if event.type.value == "cross":
                branches[pos].update({"north", "south"})
            else:
                if event.branch:
                    branches[pos].add(event.branch.value)
    return branches


def _add_minor_nodes(
    graph: GraphType,
    *,
    branches: Dict[int, Set[str]],
    defaults: Defaults,
    y_base: float = 0.0,
) -> None:
    offset = float(defaults.minor_road_length_m)
    for pos, sides in branches.items():
        if "north" in sides:
            for side in (PedestrianSide.EAST_SIDE, PedestrianSide.WEST_SIDE):
                node_id = minor_endpoint_id(pos, "N", side)
                graph.add_node(
                    node_id,
                    coord=(float(pos), y_base + offset),
                    pos=pos,
                    node_type="minor_north_end",
                    cluster_id=cluster_id(pos),
                    is_endpoint=True,
                    side=side,
                )
        if "south" in sides:
            for side in (PedestrianSide.EAST_SIDE, PedestrianSide.WEST_SIDE):
                node_id = minor_endpoint_id(pos, "S", side)
                graph.add_node(
                    node_id,
                    coord=(float(pos), y_base - offset),
                    pos=pos,
                    node_type="minor_south_end",
                    cluster_id=cluster_id(pos),
                    is_endpoint=True,
                    side=side,
                )


def _ensure_node(graph: GraphType, node_id: str) -> None:
    if node_id not in graph:
        graph.add_node(
            node_id,
            coord=(0.0, 0.0),
            pos=None,
            node_type="unknown",
            cluster_id=None,
            is_endpoint=True,
        )


def _add_main_correspondence_edges(
    graph: GraphType,
    *,
    breakpoints: Sequence[int],
) -> None:
    if len(breakpoints) < 2:
        return
    for west, east in zip(breakpoints[:-1], breakpoints[1:]):
        length = float(east - west)
        node_w_n = main_node_id(west, "north")
        node_e_n = main_node_id(east, "north")
        node_w_s = main_node_id(west, "south")
        node_e_s = main_node_id(east, "south")

        graph.add_edge(
            node_w_n,
            node_e_n,
            orientation="EW",
            side=PedestrianSide.NORTH_SIDE,
            length=length,
        )
        graph.add_edge(
            node_w_s,
            node_e_s,
            orientation="EW",
            side=PedestrianSide.SOUTH_SIDE,
            length=length,
        )


def _add_minor_connectors(
    graph: GraphType,
    *,
    branches: Dict[int, Set[str]],
    defaults: Defaults,
) -> None:
    if not branches:
        return
    length = float(defaults.minor_road_length_m)
    for pos, sides in branches.items():
        node_main_n = main_node_id(pos, "north")
        node_main_s = main_node_id(pos, "south")
        if "north" in sides:
            for side in (PedestrianSide.EAST_SIDE, PedestrianSide.WEST_SIDE):
                node_minor = minor_endpoint_id(pos, "N", side)
                graph.add_edge(
                    node_main_n,
                    node_minor,
                    orientation="NS",
                    side=side,
                    length=length,
                )
        if "south" in sides:
            for side in (PedestrianSide.EAST_SIDE, PedestrianSide.WEST_SIDE):
                node_minor = minor_endpoint_id(pos, "S", side)
                graph.add_edge(
                    node_main_s,
                    node_minor,
                    orientation="NS",
                    side=side,
                    length=length,
                )


def _movement_to_side(movement: str) -> PedestrianSide | None:
    if movement.startswith("ped_main_west"):
        return PedestrianSide.WEST_SIDE
    if movement.startswith("ped_main_east"):
        return PedestrianSide.EAST_SIDE
    if movement.startswith("ped_mid"):
        # Treat mid-block crossings as operating on both sides (west preferred).
        return PedestrianSide.WEST_SIDE
    return None


def _add_crosswalks_from_catalog(
    graph: GraphType,
    *,
    catalog: EndpointCatalog,
    y_north: float,
    y_south: float,
) -> None:
    if not catalog.pedestrian_endpoints:
        return
    seen: Set[Tuple[int, PedestrianSide]] = set()
    crossing_length = abs(float(y_north) - float(y_south))

    for endpoint in catalog.pedestrian_endpoints:
        side = _movement_to_side(endpoint.movement)
        if side is None:
            continue
        pos = endpoint.pos
        key = (pos, side)
        if key in seen:
            continue
        node_n = main_node_id(pos, "north")
        node_s = main_node_id(pos, "south")
        if node_n not in graph or node_s not in graph:
            continue
        graph.add_edge(
            node_n,
            node_s,
            orientation="NS",
            side=side,
            length=crossing_length,
        )
        seen.add(key)


def build_pedestrian_graph(
    *,
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    catalog: EndpointCatalog,
) -> GraphType:
    """Build a MultiGraph capturing pedestrian endpoints and connectors."""

    graph: GraphType = nx.MultiGraph()
    y_north, y_south = build_main_carriageway_y(main_road)

    _add_main_nodes(graph, breakpoints=breakpoints, y_north=y_north, y_south=y_south)
    branches = _collect_minor_branches(clusters)
    _add_minor_nodes(graph, branches=branches, defaults=defaults)
    _add_main_correspondence_edges(graph, breakpoints=breakpoints)
    _add_minor_connectors(graph, branches=branches, defaults=defaults)
    _add_crosswalks_from_catalog(graph, catalog=catalog, y_north=y_north, y_south=y_south)

    # Mark nodes with degree <= 1 as endpoints if not already flagged.
    for node, degree in graph.degree():
        if degree <= 1:
            graph.nodes[node]["is_endpoint"] = True

    return graph


__all__ = ["build_pedestrian_graph", "GraphType"]
