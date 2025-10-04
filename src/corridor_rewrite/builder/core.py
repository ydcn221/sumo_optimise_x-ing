"""Transform corridor plans into PlainXML intermediate representations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from ..domain import models
from ..planner.layout import CorridorPlan


@dataclass(slots=True)
class CorridorBuilder:
    plan: CorridorPlan

    def build(self) -> models.CorridorIR:
        nodes = _build_nodes(self.plan)
        edges = _build_edges(self.plan, nodes)
        connections = _build_connections(edges)
        return models.CorridorIR(nodes=nodes, edges=edges, connections=connections)


def _build_nodes(plan: CorridorPlan) -> List[models.NodeIR]:
    nodes: Dict[float, models.NodeIR] = {}
    for breakpoint in plan.breakpoints:
        node_id = _node_id(breakpoint.pos_m)
        nodes[breakpoint.pos_m] = models.NodeIR(id=node_id, x=breakpoint.pos_m, y=0.0, type="priority")
    for event in plan.snapped_events:
        node_id = _junction_node_id(event.event.pos_m)
        nodes[event.event.pos_m] = models.NodeIR(id=node_id, x=event.event.pos_m, y=0.0, type="traffic_light" if event.event.signalized else "priority")
    ordered = [nodes[pos] for pos in sorted(nodes.keys())]
    return ordered


def _build_edges(plan: CorridorPlan, nodes: Sequence[models.NodeIR]) -> List[models.EdgeIR]:
    edges: List[models.EdgeIR] = []
    lanes_default = plan.spec.main_road.lanes
    overlays = plan.overlays
    overlay_idx = 0
    current_overlay = overlays[overlay_idx] if overlays else None

    sorted_nodes = sorted(nodes, key=lambda node: node.x)
    for from_node, to_node in zip(sorted_nodes[:-1], sorted_nodes[1:]):
        start = from_node.x
        end = to_node.x
        segment_lanes = lanes_default
        while current_overlay and current_overlay.end_m <= start and overlay_idx < len(overlays) - 1:
            overlay_idx += 1
            current_overlay = overlays[overlay_idx]
        if current_overlay and current_overlay.start_m <= start < current_overlay.end_m:
            segment_lanes = max(segment_lanes, current_overlay.lanes)
        length = max(end - start, 0.1)
        edges.append(
            models.EdgeIR(
                id=_edge_id("main_EB", start, end),
                from_node=from_node.id,
                to_node=to_node.id,
                num_lanes=segment_lanes,
                speed_mps=_kmh_to_mps(plan.spec.defaults.speed_kmh),
                length_m=length,
                priority=3,
            )
        )
        edges.append(
            models.EdgeIR(
                id=_edge_id("main_WB", start, end),
                from_node=to_node.id,
                to_node=from_node.id,
                num_lanes=segment_lanes,
                speed_mps=_kmh_to_mps(plan.spec.defaults.speed_kmh),
                length_m=length,
                priority=3,
            )
        )
    return edges


def _build_connections(edges: Sequence[models.EdgeIR]) -> List[models.ConnectionIR]:
    connections: List[models.ConnectionIR] = []
    by_from: Dict[str, List[models.EdgeIR]] = {}
    for edge in edges:
        by_from.setdefault(edge.from_node, []).append(edge)
    for edges_from_node in by_from.values():
        for idx, edge in enumerate(edges_from_node):
            for other in edges_from_node:
                if edge.id == other.id:
                    continue
                connections.append(
                    models.ConnectionIR(
                        from_edge=edge.id,
                        to_edge=other.id,
                        from_lane=0,
                        to_lane=0,
                    )
                )
    return connections


def _kmh_to_mps(value: float) -> float:
    return value / 3.6


def _node_id(pos: float) -> str:
    return f"main_node_{int(round(pos))}"


def _junction_node_id(pos: float) -> str:
    return f"junction_{int(round(pos))}"


def _edge_id(prefix: str, start: float, end: float) -> str:
    return f"{prefix}_{int(round(start))}_{int(round(end))}"
