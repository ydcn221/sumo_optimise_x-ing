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
Directive = Tuple[CardinalDirection, PedestrianSide]
StateKey = Tuple[str, Optional[EdgeRef], Optional[Directive]]

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


def _state_key(
    node: str,
    incoming: Optional[EdgeRef],
    directive: Optional[Directive] = None,
) -> StateKey:
    return (node, incoming, directive)


def _opposite_cardinal(cardinal: CardinalDirection) -> CardinalDirection:
    mapping = {
        CardinalDirection.NORTH: CardinalDirection.SOUTH,
        CardinalDirection.SOUTH: CardinalDirection.NORTH,
        CardinalDirection.EAST: CardinalDirection.WEST,
        CardinalDirection.WEST: CardinalDirection.EAST,
    }
    return mapping[cardinal]


def _incoming_entry_direction(
    graph: GraphType,
    incoming: Optional[EdgeRef],
) -> Optional[CardinalDirection]:
    if incoming is None:
        return None
    prev_node, node, key = incoming
    edge_data = graph[prev_node][node][key]
    travel_dir = _edge_direction(graph, prev_node, node, edge_data)
    return _opposite_cardinal(travel_dir)


def _half_for_side(side: PedestrianSide) -> Optional[str]:
    if side == PedestrianSide.NORTH_SIDE:
        return "MainN"
    if side == PedestrianSide.SOUTH_SIDE:
        return "MainS"
    return None


def _is_main_crosswalk_edge(node: str, neighbor: str) -> bool:
    node_sig = _main_endpoint_signature(node)
    neighbor_sig = _main_endpoint_signature(neighbor)
    if not node_sig or not neighbor_sig:
        return False
    node_pos, node_half = node_sig
    neighbor_pos, neighbor_half = neighbor_sig
    return node_pos == neighbor_pos and node_half != neighbor_half


def _candidate_edges_for_ratio(
    graph: GraphType,
    *,
    node: str,
    neighbors: List[Tuple[str, int, Mapping[str, object]]],
    direction: CardinalDirection,
    side: PedestrianSide,
) -> List[Tuple[str, int, Mapping[str, object], bool]]:
    direct: List[Tuple[str, int, Mapping[str, object], bool]] = []
    for neighbor, key, edge_data in neighbors:
        current_direction = _edge_direction(graph, node, neighbor, edge_data)
        current_side = edge_data.get("side")
        if not isinstance(current_side, PedestrianSide):
            raise DemandValidationError(f"edge {node}->{neighbor} missing pedestrian side metadata")
        if current_direction == direction and current_side == side:
            direct.append((neighbor, key, edge_data, False))
    if direct:
        return direct

    if direction in {CardinalDirection.EAST, CardinalDirection.WEST}:
        target_half = _half_for_side(side)
        node_sig = _main_endpoint_signature(node)
        if not target_half or not node_sig:
            return []
        node_pos, node_half = node_sig
        if node_half == target_half:
            return []
        for neighbor, key, edge_data in neighbors:
            current_direction = _edge_direction(graph, node, neighbor, edge_data)
            if current_direction not in {CardinalDirection.NORTH, CardinalDirection.SOUTH}:
                continue
            if not _is_main_crosswalk_edge(node, neighbor):
                continue
            cross_side = edge_data.get("side")
            if not isinstance(cross_side, PedestrianSide):
                raise DemandValidationError(f"edge {node}->{neighbor} missing pedestrian side metadata")
            neighbor_sig = _main_endpoint_signature(neighbor)
            if not neighbor_sig:
                continue
            neighbor_pos, neighbor_half = neighbor_sig
            if neighbor_pos == node_pos and neighbor_half == target_half:
                neighbor_neighbors = _collect_neighbors(graph, neighbor)
                has_forward = any(
                    _edge_direction(graph, neighbor, next_neighbor, next_data) == direction
                    and next_data.get("side") == side
                    for next_neighbor, next_key, next_data in neighbor_neighbors
                )
                return [(neighbor, key, edge_data, has_forward)]
    return []


def _distribute_with_ratios(
    graph: GraphType,
    *,
    node: str,
    neighbors: List[Tuple[str, int, Mapping[str, object]]],
    incoming_direction: Optional[CardinalDirection],
    ratios: JunctionDirectionRatios,
) -> List[Tuple[str, int, float, Optional[Directive]]]:
    weighted: List[Tuple[str, int, float, Optional[Directive]]] = []
    total_weight = 0.0

    for (direction, side), weight in ratios.weights.items():
        if weight <= 0.0:
            continue
        if incoming_direction is not None and direction == incoming_direction:
            continue
        candidates = _candidate_edges_for_ratio(
            graph,
            node=node,
            neighbors=neighbors,
            direction=direction,
            side=side,
        )
        if not candidates:
            continue
        share = weight / len(candidates)
        for neighbor, key, _, needs_continuation in candidates:
            directive = (direction, side) if needs_continuation else None
            weighted.append((neighbor, key, share, directive))
            total_weight += share

    if total_weight <= 0.0:
        return []

    return [
        (neighbor, key, weight / total_weight, directive)
        for neighbor, key, weight, directive in weighted
        if weight > 0.0
    ]


def _default_distributions(
    graph: GraphType,
    *,
    node: str,
    neighbors: List[Tuple[str, int, Mapping[str, object]]],
    incoming: Optional[EdgeRef],
) -> List[Tuple[str, int, float, Optional[Directive]]]:
    filtered: List[Tuple[str, int, Mapping[str, object]]] = []
    for neighbor, key_edge, edge_data in neighbors:
        if incoming and neighbor == incoming[0] and key_edge == incoming[2] and node == incoming[1]:
            continue
        filtered.append((neighbor, key_edge, edge_data))
    if not filtered:
        return []
    if incoming:
        prev_node, current_node, incoming_key = incoming
        incoming_data = graph[prev_node][current_node][incoming_key]
        incoming_orientation = incoming_data.get("orientation")
        incoming_side = incoming_data.get("side")
        if incoming_orientation is not None:
            oriented = [
                (neighbor, key_edge, edge_data)
                for neighbor, key_edge, edge_data in filtered
                if edge_data.get("orientation") == incoming_orientation
            ]
            if oriented:
                filtered = oriented
                if incoming_side is not None:
                    same_side = [
                        (neighbor, key_edge, edge_data)
                        for neighbor, key_edge, edge_data in filtered
                        if edge_data.get("side") == incoming_side
                    ]
                    if same_side:
                        filtered = same_side
    if not filtered:
        return []
    share = 1.0 / len(filtered)
    return [(neighbor, key_edge, share, None) for neighbor, key_edge, _ in filtered]


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

    start_key = _state_key(source, None, None)
    pending[start_key] = amount
    queue.append(start_key)

    while queue:
        key = queue.popleft()
        node, incoming, directive = key
        flow = pending.pop(key, 0.0)
        if flow <= EPS:
            continue

        node_data = graph.nodes[node]
        cluster_id = node_data.get("cluster_id")
        is_endpoint = bool(node_data.get("is_endpoint"))

        if directive is not None:
            direction, side = directive
            neighbors = _collect_neighbors(graph, node)
            directed_edges = _candidate_edges_for_ratio(
                graph,
                node=node,
                neighbors=neighbors,
                direction=direction,
                side=side,
            )
            if not directed_edges:
                results[node] += flow
                continue
            share = 1.0 / len(directed_edges)
            for neighbor, key_edge, _, needs_more in directed_edges:
                if needs_more:
                    next_directive: Optional[Directive] = (direction, side)
                else:
                    next_directive = None
                next_flow = flow * share
                if next_flow <= EPS:
                    continue
                edge_ref: EdgeRef = (node, neighbor, key_edge)
                next_key = _state_key(neighbor, edge_ref, next_directive)
                was_empty = next_key not in pending
                pending[next_key] = pending.get(next_key, 0.0) + next_flow
                if was_empty:
                    queue.append(next_key)
            continue

        if (node != source and is_endpoint) or (node == source and incoming is not None and is_endpoint):
            results[node] += flow
            continue

        neighbors = _collect_neighbors(graph, node)
        if not neighbors:
            # Dead-end: treat node as endpoint to absorb residual flow.
            results[node] += flow
            continue

        incoming_direction = _incoming_entry_direction(graph, incoming)

        if cluster_id and cluster_id in ratios and not is_endpoint:
            ratio = ratios[cluster_id]
            distributions = _distribute_with_ratios(
                graph,
                node=node,
                neighbors=neighbors,
                incoming_direction=incoming_direction,
                ratios=ratio,
            )
            if not distributions:
                distributions = _default_distributions(
                    graph,
                    node=node,
                    neighbors=neighbors,
                    incoming=incoming,
                )
        else:
            distributions = _default_distributions(
                graph,
                node=node,
                neighbors=neighbors,
                incoming=incoming,
            )

        if not distributions:
            results[node] += flow
            continue

        for neighbor, key_edge, share, continuation in distributions:
            next_flow = flow * share
            if next_flow <= EPS:
                continue
            edge_ref: EdgeRef = (node, neighbor, key_edge)
            next_key = _state_key(neighbor, edge_ref, continuation)
            was_empty = next_key not in pending
            pending[next_key] = pending.get(next_key, 0.0) + next_flow
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
