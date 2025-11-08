"""Propagate vehicle endpoint demand across the linear corridor."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Tuple

from ...domain.models import CardinalDirection, EndpointDemandRow
from ...utils.errors import DemandValidationError
from .demand_input import VehicleTurnWeights
from .topology import VehicleNetwork, canonicalize_vehicle_endpoint, vehicle_cluster_id

VehicleTurnMap = Dict[str, VehicleTurnWeights]


@dataclass(frozen=True)
class VehicleState:
    pos: int
    incoming: CardinalDirection  # direction vehicles used to enter this cluster


def compute_vehicle_od_flows(
    rows: Iterable[EndpointDemandRow],
    *,
    network: VehicleNetwork,
    turn_weights: VehicleTurnMap,
) -> List[Tuple[str, str, float, EndpointDemandRow]]:
    """Return OD rows as (origin, destination, value, source_row)."""

    results: List[Tuple[str, str, float, EndpointDemandRow]] = []
    for row in rows:
        canonical_id = canonicalize_vehicle_endpoint(
            row.endpoint_id,
            network=network,
            prefer_departing_half=row.flow_per_hour >= 0,
        )
        amount = abs(row.flow_per_hour)
        if amount <= 0.0:
            continue
        initial_states = _initial_states(canonical_id, network)
        if not initial_states:
            raise DemandValidationError(f"unsupported vehicle endpoint identifier: {row.endpoint_id}")

        od_map = _propagate(initial_states, amount, network=network, turn_weights=turn_weights)
        if row.flow_per_hour >= 0:
            for destination, value in od_map.items():
                if value <= 0.0 or destination == canonical_id:
                    continue
                results.append((canonical_id, destination, value, row))
        else:
            for origin, value in od_map.items():
                if value <= 0.0 or origin == canonical_id:
                    continue
                results.append((origin, canonical_id, value, row))
    return results


def _initial_states(endpoint_id: str, network: VehicleNetwork) -> List[VehicleState]:
    tokens = endpoint_id.split(".")
    if tokens[0] == "Node" and tokens[1] == "Minor":
        pos = int(tokens[2])
        suffix = tokens[3]
        if suffix == "N_end":
            return [VehicleState(pos=pos, incoming=CardinalDirection.NORTH)]
        if suffix == "S_end":
            return [VehicleState(pos=pos, incoming=CardinalDirection.SOUTH)]
        raise DemandValidationError(f"unsupported minor endpoint suffix: {suffix}")
    if tokens[0] == "Node" and tokens[1] == "Main":
        pos = int(tokens[2])
        half = tokens[3]
        if pos == network.min_pos and half == "N":
            return [VehicleState(pos=pos, incoming=CardinalDirection.WEST)]
        if pos == network.min_pos and half == "S":
            return [VehicleState(pos=pos, incoming=CardinalDirection.WEST)]
        if pos == network.max_pos and half == "N":
            return [VehicleState(pos=pos, incoming=CardinalDirection.EAST)]
        if pos == network.max_pos and half == "S":
            return [VehicleState(pos=pos, incoming=CardinalDirection.EAST)]
        raise DemandValidationError("main endpoints must lie at the corridor ends")
    return []


def _propagate(
    initial_states: List[VehicleState],
    amount: float,
    *,
    network: VehicleNetwork,
    turn_weights: VehicleTurnMap,
) -> Dict[str, float]:
    queue: Deque[Tuple[VehicleState, float]] = deque((state, amount) for state in initial_states)
    od_map: Dict[str, float] = defaultdict(float)

    while queue:
        state, flow = queue.popleft()
        if flow <= 0.0:
            continue

        shares = _compute_shares(state, network=network, turn_weights=turn_weights)
        if not shares:
            continue

        for direction, share in shares.items():
            portion = flow * share
            if portion <= 0.0:
                continue
            destination = _destination_for(state.pos, direction, network)
            if destination is not None:
                od_map[destination] += portion
                continue
            next_state = _advance_state(state, direction, network)
            if next_state is not None:
                queue.append((next_state, portion))

    return od_map


def _compute_shares(
    state: VehicleState,
    *,
    network: VehicleNetwork,
    turn_weights: VehicleTurnMap,
) -> Dict[CardinalDirection, float]:
    available = _available_directions(state.pos, network)
    available[state.incoming] = False  # block U-turns

    weights = turn_weights.get(vehicle_cluster_id(state.pos))
    raw: Dict[CardinalDirection, float] = {}
    if weights:
        for direction, weight in weights.weights.items():
            if available.get(direction, False):
                raw[direction] = weight
    else:
        for direction, is_available in available.items():
            if is_available:
                raw[direction] = 1.0

    total = sum(value for value in raw.values() if value > 0.0)
    if total <= 0.0:
        return {}

    return {direction: value / total for direction, value in raw.items() if value > 0.0}


def _available_directions(pos: int, network: VehicleNetwork) -> Dict[CardinalDirection, bool]:
    meta = network.meta(pos)
    return {
        CardinalDirection.NORTH: meta.has_north_minor,
        CardinalDirection.SOUTH: meta.has_south_minor,
        CardinalDirection.EAST: pos < network.max_pos or pos == network.max_pos,
        CardinalDirection.WEST: pos > network.min_pos or pos == network.min_pos,
    }


def _destination_for(pos: int, direction: CardinalDirection, network: VehicleNetwork) -> str | None:
    if direction == CardinalDirection.NORTH:
        return f"Node.Minor.{pos}.N_end"
    if direction == CardinalDirection.SOUTH:
        return f"Node.Minor.{pos}.S_end"
    if direction == CardinalDirection.EAST and pos == network.max_pos:
        return f"Node.Main.{network.max_pos}.N"
    if direction == CardinalDirection.WEST and pos == network.min_pos:
        return f"Node.Main.{network.min_pos}.S"
    return None


def _advance_state(state: VehicleState, direction: CardinalDirection, network: VehicleNetwork) -> VehicleState | None:
    if direction in {CardinalDirection.NORTH, CardinalDirection.SOUTH}:
        return None  # already handled as destination
    next_pos = network.next_position(state.pos, direction)
    if next_pos is None:
        return None
    return VehicleState(pos=next_pos, incoming=_opposite(direction))


def _opposite(direction: CardinalDirection) -> CardinalDirection:
    mapping = {
        CardinalDirection.NORTH: CardinalDirection.SOUTH,
        CardinalDirection.SOUTH: CardinalDirection.NORTH,
        CardinalDirection.EAST: CardinalDirection.WEST,
        CardinalDirection.WEST: CardinalDirection.EAST,
    }
    return mapping[direction]


__all__ = ["compute_vehicle_od_flows"]
