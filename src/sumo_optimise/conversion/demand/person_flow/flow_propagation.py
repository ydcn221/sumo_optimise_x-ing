"""Flow propagation across the pedestrian graph."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from ...domain.models import (
    CardinalDirection,
    EndpointDemandRow,
    JunctionDirectionRatios,
    PedestrianSide,
)
from ...utils.errors import DemandValidationError
from .graph_builder import GraphType

EPS = 1e-6


EdgeRef = Tuple[str, str, int]
StateKey = Tuple[str, Optional[EdgeRef]]

RatioMap = Mapping[str, JunctionDirectionRatios]


def _main_endpoint_signature(node_id: str) -> Optional[Tuple[str, str]]:
    parts = node_id.split(".")
    if len(parts) < 3:
        return None
    pos_token = parts[1]
    suffix = parts[2]
    if suffix not in {"MainN", "MainS"}:
        return None
    return pos_token, suffix


def _is_main_u_turn(origin: str, destination: str) -> bool:
    origin_sig = _main_endpoint_signature(origin)
    dest_sig = _main_endpoint_signature(destination)
    if not origin_sig or not dest_sig:
        return False
    origin_pos, origin_half = origin_sig
    dest_pos, dest_half = dest_sig
    return origin_pos == dest_pos and origin_half != dest_half


def _edge_direction(
    graph: GraphType,
    from_node: str,
    to_node: str,
    edge_data: Mapping[str, object],
) -> CardinalDirection:
    orientation = edge_data.get("orientation")
    fx, fy = graph.nodes[from_node].get("coord", (0.0, 0.0))
    tx, ty = graph.nodes[to_node].get("coord", (0.0, 0.0))
    if orientation == "EW":
        return CardinalDirection.EAST if tx >= fx else CardinalDirection.WEST
    if orientation == "NS":
        return CardinalDirection.NORTH if ty >= fy else CardinalDirection.SOUTH
    raise DemandValidationError(f"unknown edge orientation {orientation!r}")


def _state_key(node: str, incoming: Optional[EdgeRef]) -> StateKey:
    return (node, incoming)


def _incoming_direction(
    graph: GraphType,
    incoming: Optional[EdgeRef],
) -> Optional[CardinalDirection]:
    if incoming is None:
        return None
    prev_node, node, key = incoming
    edge_data = graph[prev_node][node][key]
    return _edge_direction(graph, prev_node, node, edge_data)


def _distribute_with_ratios(
    graph: GraphType,
    *,
    node: str,
    neighbors: List[Tuple[str, int, Mapping[str, object]]],
    incoming_direction: Optional[CardinalDirection],
    ratios: JunctionDirectionRatios,
) -> List[Tuple[str, int, float]]:
    weighted: List[Tuple[str, int, float]] = []
    total_weight = 0.0

    for neighbor, key, edge_data in neighbors:
        direction = _edge_direction(graph, node, neighbor, edge_data)
        side = edge_data.get("side")
        if not isinstance(side, PedestrianSide):
            raise DemandValidationError(f"edge {node}->{neighbor} missing pedestrian side metadata")
        weight = ratios.weight(direction, side)
        if incoming_direction is not None and direction == incoming_direction:
            weight = 0.0
        weighted.append((neighbor, key, weight))
        total_weight += weight

    if total_weight <= 0.0:
        raise DemandValidationError(
            f"junction {ratios.junction_id} cannot normalise ratios (sum=0 after U-turn suppression)"
        )

    return [(neighbor, key, weight / total_weight) for neighbor, key, weight in weighted if weight > 0.0]


def _collect_neighbors(graph: GraphType, node: str) -> List[Tuple[str, int, Mapping[str, object]]]:
    neighbors: List[Tuple[str, int, Mapping[str, object]]] = []
    for neighbor in graph.neighbors(node):
        edge_dict = graph[node][neighbor]
        for key, data in edge_dict.items():
            neighbors.append((neighbor, key, data))
    return neighbors


def _propagate_single(
    graph: GraphType,
    ratios: RatioMap,
    *,
    source: str,
    amount: float,
) -> Dict[str, float]:
    queue: Deque[StateKey] = deque()
    pending: MutableMapping[StateKey, float] = defaultdict(float)
    results: Dict[str, float] = defaultdict(float)

    start_key = _state_key(source, None)
    pending[start_key] = amount
    queue.append(start_key)

    while queue:
        key = queue.popleft()
        node, incoming = key
        flow = pending.pop(key, 0.0)
        if flow <= EPS:
            continue

        node_data = graph.nodes[node]
        cluster_id = node_data.get("cluster_id")
        is_endpoint = bool(node_data.get("is_endpoint"))

        if (node != source and is_endpoint) or (node == source and incoming is not None and is_endpoint):
            results[node] += flow
            continue

        neighbors = _collect_neighbors(graph, node)
        if not neighbors:
            # Dead-end: treat node as endpoint to absorb residual flow.
            results[node] += flow
            continue

        incoming_direction = _incoming_direction(graph, incoming)

        if cluster_id and cluster_id in ratios:
            ratio = ratios[cluster_id]
            distributions = _distribute_with_ratios(
                graph,
                node=node,
                neighbors=neighbors,
                incoming_direction=incoming_direction,
                ratios=ratio,
            )
        else:
            # Default behaviour: forward to all neighbors except the edge used to arrive.
            filtered: List[Tuple[str, int, Mapping[str, object]]] = []
            for neighbor, key_edge, edge_data in neighbors:
                if incoming and neighbor == incoming[0] and key_edge == incoming[2] and node == incoming[1]:
                    continue
                filtered.append((neighbor, key_edge, edge_data))
            if not filtered:
                results[node] += flow
                continue
            share = 1.0 / len(filtered)
            distributions = [(neighbor, key_edge, share) for neighbor, key_edge, _ in filtered]

        for neighbor, key_edge, share in distributions:
            next_flow = flow * share
            if next_flow <= EPS:
                continue
            edge_ref: EdgeRef = (node, neighbor, key_edge)
            next_key = _state_key(neighbor, edge_ref)
            was_empty = next_key not in pending
            pending[next_key] += next_flow
            if was_empty:
                queue.append(next_key)

    return results


def compute_od_flows(
    graph: GraphType,
    ratios: RatioMap,
    demands: Iterable[EndpointDemandRow],
) -> List[Tuple[str, str, float, EndpointDemandRow]]:
    """Compute OD flows for each demand row.

    Returns a list of tuples (origin, destination, flow_per_hour, demand_row).
    """

    results: List[Tuple[str, str, float, EndpointDemandRow]] = []

    for row in demands:
        endpoint_id = row.endpoint_id
        if endpoint_id not in graph:
            raise DemandValidationError(f"endpoint {endpoint_id!r} not present in pedestrian graph")
        flow_mag = abs(row.flow_per_hour)
        if flow_mag <= EPS:
            continue
        raw_map = _propagate_single(graph, ratios, source=endpoint_id, amount=flow_mag)
        if row.flow_per_hour >= 0:
            origin = endpoint_id
            for destination, value in raw_map.items():
                if destination == origin or value <= EPS:
                    continue
                if _is_main_u_turn(origin, destination):
                    continue
                results.append((origin, destination, value, row))
        else:
            sink = endpoint_id
            for intermediate, value in raw_map.items():
                if intermediate == sink or value <= EPS:
                    continue
                # Reverse for sinks: actual origin is intermediate.
                if _is_main_u_turn(intermediate, sink):
                    continue
                results.append((intermediate, sink, value, row))

    return results


__all__ = ["compute_od_flows"]
