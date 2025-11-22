"""Flow propagation across the pedestrian graph."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from ...domain.models import (
    CardinalDirection,
    EndpointDemandRow,
    JunctionTurnWeights,
    PedestrianSide,
    TurnMovement,
)
from ...utils.errors import DemandValidationError
from .graph_builder import GraphType
from .identifier import parse_main_ped_endpoint_id

EPS = 1e-6


EdgeRef = Tuple[str, str, int]
Directive = Tuple[CardinalDirection, PedestrianSide]
StateKey = Tuple[str, Optional[EdgeRef], Optional[Directive]]

TurnWeightMap = Mapping[str, JunctionTurnWeights]


def _main_endpoint_signature(node_id: str) -> Optional[Tuple[str, str]]:
    parts = node_id.split(".")
    if len(parts) != 4:
        return None
    if parts[0] != "Node" or parts[1] != "Main":
        return None
    pos_token = parts[2]
    suffix = parts[3]
    if suffix not in {"N", "S"}:
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


def _incoming_travel_direction(
    graph: GraphType,
    incoming: Optional[EdgeRef],
) -> Optional[CardinalDirection]:
    if incoming is None:
        return None
    prev_node, node, key = incoming
    edge_data = graph[prev_node][node][key]
    return _edge_direction(graph, prev_node, node, edge_data)


def _main_position_bounds(graph: GraphType) -> Optional[Tuple[int, int]]:
    positions: List[int] = []
    for node in graph.nodes:
        parts = node.split(".")
        if len(parts) == 4 and parts[0] == "Node" and parts[1] == "Main":
            try:
                positions.append(int(parts[2]))
            except ValueError:
                continue
    if not positions:
        return None
    positions.sort()
    return positions[0], positions[-1]


def _graph_node_for_endpoint(
    graph: GraphType,
    endpoint_id: str,
    main_bounds: Optional[Tuple[int, int]],
) -> str:
    if endpoint_id in graph:
        return endpoint_id
    parsed = parse_main_ped_endpoint_id(endpoint_id)
    if not parsed:
        return endpoint_id
    pos_token, half = parsed
    try:
        pos = int(pos_token)
    except ValueError:
        if not main_bounds:
            raise DemandValidationError(
                f"cannot resolve pedestrian endpoint {endpoint_id!r} without main breakpoints"
            )
        min_pos, max_pos = main_bounds
        label = pos_token.strip().upper()
        if label in {"W_END", "WEST_END"}:
            pos = min_pos
        elif label in {"E_END", "EAST_END"}:
            pos = max_pos
        else:
            raise DemandValidationError(f"unsupported main pedestrian endpoint token {pos_token!r}")
    candidate = f"Node.Main.{pos}.{half}"
    if candidate not in graph:
        raise DemandValidationError(
            f"pedestrian endpoint {endpoint_id!r} resolves to missing graph node {candidate!r}"
        )
    return candidate


def _distribute_with_turn_weights(
    graph: GraphType,
    *,
    node: str,
    neighbors: List[Tuple[str, int, Mapping[str, object]]],
    incoming: Optional[EdgeRef],
    turn_weights: Optional[JunctionTurnWeights],
) -> List[Tuple[str, int, float, Optional[Directive]]]:
    if incoming is None:
        return _default_distributions(graph, node=node, neighbors=neighbors, incoming=None)

    incoming_dir = _incoming_travel_direction(graph, incoming)
    if incoming_dir is None:
        return _default_distributions(graph, node=node, neighbors=neighbors, incoming=incoming)

    approach = _approach_for_pedestrian(graph, incoming)
    if turn_weights is None:
        movement_weights = _default_pedestrian_movement_weights(approach)
    else:
        movement_weights = _movement_weights_for_pedestrian(
            approach=approach,
            turn_weights=turn_weights,
        )

    direction_candidates: Dict[CardinalDirection, List[Tuple[str, int, Mapping[str, object]]]] = defaultdict(list)
    for neighbor, key_edge, edge_data in neighbors:
        if incoming and neighbor == incoming[0] and key_edge == incoming[2] and node == incoming[1]:
            continue  # block U-turns
        direction = _edge_direction(graph, node, neighbor, edge_data)
        direction_candidates[direction].append((neighbor, key_edge, edge_data))

    direction_weights = _direction_weights_from_movements(
        direction_candidates=direction_candidates,
        movement_weights=movement_weights,
        approach=approach,
        incoming_dir=incoming_dir,
    )

    total_weight = sum(value for value in direction_weights.values() if value > 0.0)
    if total_weight <= 0.0:
        fallback_direction_weights = _direction_weights_from_movements(
            direction_candidates=direction_candidates,
            movement_weights=_default_pedestrian_movement_weights(approach),
            approach=approach,
            incoming_dir=incoming_dir,
        )
        total_fallback = sum(value for value in fallback_direction_weights.values() if value > 0.0)
        if total_fallback <= 0.0:
            return _default_distributions(graph, node=node, neighbors=neighbors, incoming=incoming)
        return [
            (neighbor, key_edge, value / total_fallback, None)
            for (neighbor, key_edge), value in fallback_direction_weights.items()
            if value > 0.0
        ]

    return [
        (neighbor, key_edge, value / total_weight, None)
        for (neighbor, key_edge), value in direction_weights.items()
        if value > 0.0
    ]


def _movement_weights_for_pedestrian(
    *,
    approach: str,
    turn_weights: JunctionTurnWeights,
) -> Dict[TurnMovement, float]:
    base = turn_weights.main if approach == "main" else turn_weights.minor
    weights = dict(base)
    if approach == "minor":
        weights[TurnMovement.THROUGH] = 0.0
    return weights


def _movement_direction_for_pedestrian(
    incoming: CardinalDirection,
    movement: TurnMovement,
) -> CardinalDirection:
    mapping = {
        CardinalDirection.EAST: {
            TurnMovement.LEFT: CardinalDirection.NORTH,
            TurnMovement.THROUGH: CardinalDirection.EAST,
            TurnMovement.RIGHT: CardinalDirection.SOUTH,
        },
        CardinalDirection.WEST: {
            TurnMovement.LEFT: CardinalDirection.SOUTH,
            TurnMovement.THROUGH: CardinalDirection.WEST,
            TurnMovement.RIGHT: CardinalDirection.NORTH,
        },
        CardinalDirection.NORTH: {
            TurnMovement.LEFT: CardinalDirection.WEST,
            TurnMovement.THROUGH: CardinalDirection.NORTH,
            TurnMovement.RIGHT: CardinalDirection.EAST,
        },
        CardinalDirection.SOUTH: {
            TurnMovement.LEFT: CardinalDirection.EAST,
            TurnMovement.THROUGH: CardinalDirection.SOUTH,
            TurnMovement.RIGHT: CardinalDirection.WEST,
        },
    }
    try:
        return mapping[incoming][movement]
    except KeyError as exc:
        raise DemandValidationError(
            f"unsupported pedestrian movement mapping for incoming={incoming}, movement={movement}"
        ) from exc


def _default_pedestrian_movement_weights(approach: str) -> Dict[TurnMovement, float]:
    return {
        TurnMovement.LEFT: 1.0,
        TurnMovement.THROUGH: 1.0 if approach == "main" else 0.0,
        TurnMovement.RIGHT: 1.0,
    }


def _direction_weights_from_movements(
    *,
    direction_candidates: Dict[CardinalDirection, List[Tuple[str, int, Mapping[str, object]]]],
    movement_weights: Dict[TurnMovement, float],
    approach: str,
    incoming_dir: CardinalDirection,
) -> Dict[Tuple[str, int], float]:
    direction_weights: Dict[Tuple[str, int], float] = defaultdict(float)
    through_candidates = direction_candidates.get(
        _movement_direction_for_pedestrian(incoming_dir, TurnMovement.THROUGH), []
    )
    through_bonus = 0.0

    for movement, weight in movement_weights.items():
        if approach == "minor" and movement is TurnMovement.THROUGH:
            weight = 0.0
        target_dir = _movement_direction_for_pedestrian(incoming_dir, movement)
        candidates = direction_candidates.get(target_dir, [])
        if not candidates:
            if approach == "main" and movement in (TurnMovement.LEFT, TurnMovement.RIGHT):
                through_bonus += weight
            continue
        if weight <= 0.0:
            continue
        share = weight / len(candidates)
        for neighbor, key_edge, _ in candidates:
            direction_weights[(neighbor, key_edge)] += share

    if through_bonus > 0.0 and through_candidates:
        share = through_bonus / len(through_candidates)
        for neighbor, key_edge, _ in through_candidates:
            direction_weights[(neighbor, key_edge)] += share

    return direction_weights


def _approach_for_pedestrian(graph: GraphType, incoming: EdgeRef) -> str:
    prev_node, _, _ = incoming
    prev_data = graph.nodes.get(prev_node, {})
    node_type = str(prev_data.get("node_type", "")).lower()
    if prev_node.startswith("PedEnd.Minor") or "minor" in node_type:
        return "minor"
    return "main"


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
    turn_weight_map: TurnWeightMap,
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

        if (node != source and is_endpoint) or (node == source and incoming is not None and is_endpoint):
            results[node] += flow
            continue

        neighbors = _collect_neighbors(graph, node)
        if not neighbors:
            # Dead-end: treat node as endpoint to absorb residual flow.
            results[node] += flow
            continue

        if cluster_id and not is_endpoint:
            junction_turn_weights = turn_weight_map.get(cluster_id)
            distributions = _distribute_with_turn_weights(
                graph,
                node=node,
                neighbors=neighbors,
                incoming=incoming,
                turn_weights=junction_turn_weights,
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
    turn_weight_map: TurnWeightMap,
    demands: Iterable[EndpointDemandRow],
) -> List[Tuple[str, str, float, EndpointDemandRow]]:
    """Compute OD flows for each demand row.

    Returns a list of tuples (origin, destination, flow_per_hour, demand_row).
    """

    results: List[Tuple[str, str, float, EndpointDemandRow]] = []
    main_bounds = _main_position_bounds(graph)

    for row in demands:
        endpoint_id = row.endpoint_id
        graph_node = _graph_node_for_endpoint(graph, endpoint_id, main_bounds)
        if graph_node not in graph:
            raise DemandValidationError(f"endpoint {endpoint_id!r} not present in pedestrian graph")
        flow_mag = abs(row.flow_per_hour)
        if flow_mag <= EPS:
            continue
        raw_map = _propagate_single(graph, turn_weight_map, source=graph_node, amount=flow_mag)
        if row.flow_per_hour >= 0:
            origin = endpoint_id
            for destination, value in raw_map.items():
                if destination == graph_node or value <= EPS:
                    continue
                if _is_main_u_turn(graph_node, destination):
                    continue
                results.append((origin, destination, value, row))
        else:
            sink = endpoint_id
            for intermediate, value in raw_map.items():
                if intermediate == graph_node or value <= EPS:
                    continue
                # Reverse for sinks: actual origin is intermediate.
                if _is_main_u_turn(intermediate, graph_node):
                    continue
                results.append((intermediate, sink, value, row))

    return results


__all__ = ["compute_od_flows"]
