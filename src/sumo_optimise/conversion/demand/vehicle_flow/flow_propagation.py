"""Propagate vehicle endpoint demand across the linear corridor."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Tuple

from ...domain.models import CardinalDirection, EndpointDemandRow, JunctionTurnWeights, TurnMovement
from ...utils.errors import DemandValidationError
from .topology import VehicleNetwork, canonicalize_vehicle_endpoint, vehicle_cluster_id

VehicleTurnMap = Dict[str, JunctionTurnWeights]


@dataclass(frozen=True)
class VehicleState:
    pos: int
    incoming: CardinalDirection  # direction vehicles used to enter this cluster


def _requires_turn_weights(pos: int, network: VehicleNetwork) -> bool:
    meta = network.meta(pos)
    return meta.has_north_minor or meta.has_south_minor


def compute_vehicle_od_flows(
    rows: Iterable[EndpointDemandRow],
    *,
    network: VehicleNetwork,
    turn_weights: VehicleTurnMap,
) -> List[Tuple[str, str, float, EndpointDemandRow]]:
    """Return OD rows as (origin, destination, value, source_row)."""

    missing_turns = {
        vehicle_cluster_id(meta.pos)
        for meta in network.cluster_meta.values()
        if _requires_turn_weights(meta.pos, network)
    } - set(turn_weights.keys())
    if missing_turns:
        missing_list = ", ".join(sorted(missing_turns))
        raise DemandValidationError(
            f"missing vehicle turn-weight rows for: {missing_list}"
        )

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
                if _is_main_u_turn(canonical_id, destination):
                    continue
                results.append((canonical_id, destination, value, row))
        else:
            for origin, value in od_map.items():
                if value <= 0.0 or origin == canonical_id:
                    continue
                if _is_main_u_turn(origin, canonical_id):
                    continue
                results.append((origin, canonical_id, value, row))
    return results


def _initial_states(endpoint_id: str, network: VehicleNetwork) -> List[VehicleState]:
    tokens = endpoint_id.split(".")
    if tokens[0] == "Node" and tokens[1] == "Minor":
        pos = int(tokens[2])
        suffix = tokens[3]
        if suffix == "N_end":
            return [VehicleState(pos=pos, incoming=CardinalDirection.SOUTH)]
        if suffix == "S_end":
            return [VehicleState(pos=pos, incoming=CardinalDirection.NORTH)]
        raise DemandValidationError(f"unsupported minor endpoint suffix: {suffix}")
    if tokens[0] == "Node" and tokens[1] == "Main":
        pos = int(tokens[2])
        half = tokens[3]
        if pos not in {network.min_pos, network.max_pos}:
            raise DemandValidationError("main endpoints must lie at the corridor ends")
        if half == "N":
            return [VehicleState(pos=pos, incoming=CardinalDirection.EAST)]
        if half == "S":
            return [VehicleState(pos=pos, incoming=CardinalDirection.WEST)]
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
    # Prevent immediate U-turns while keeping the forward direction available.
    # The incoming direction captures the travel heading used to enter the cluster,
    # so the true U-turn would send vehicles toward the opposite heading.
    available[_opposite(state.incoming)] = False
    approach = _approach_for(state.incoming)
    weights_for_cluster = turn_weights.get(vehicle_cluster_id(state.pos))
    if weights_for_cluster is None:
        if _requires_turn_weights(state.pos, network):
            raise DemandValidationError(
                f"missing vehicle turn-weight row for {vehicle_cluster_id(state.pos)}"
            )
    movement_weights = _movement_weights(approach=approach, turn_weights=weights_for_cluster)

    direction_weights: Dict[CardinalDirection, float] = defaultdict(float)
    through_bonus = 0.0
    movement_targets: Dict[TurnMovement, Tuple[CardinalDirection, float]] = {}
    for movement, weight in movement_weights.items():
        direction = _movement_direction(state.incoming, movement)
        if not available.get(direction, False):
            if approach == "main" and movement in (TurnMovement.LEFT, TurnMovement.RIGHT):
                through_bonus += weight
            weight = 0.0
        # When a main approach turn is blocked (e.g., missing branch), we explicitly
        # fold its weight into the through movement to keep vehicles advancing.
        if approach == "main" and movement is TurnMovement.THROUGH and not available.get(direction, False):
            weight = 0.0
        movement_targets[movement] = (direction, weight)

    for movement, (direction, weight) in movement_targets.items():
        if movement is TurnMovement.THROUGH and approach == "main":
            weight += through_bonus
        direction_weights[direction] += weight

    total = sum(value for value in direction_weights.values() if value > 0.0)
    if total <= 0.0:
        return _default_direction_shares(available, approach, incoming=state.incoming)

    return {direction: value / total for direction, value in direction_weights.items() if value > 0.0}


def _movement_weights(
    *,
    approach: str,
    turn_weights: JunctionTurnWeights | None,
) -> Dict[TurnMovement, float]:
    if turn_weights:
        return dict(turn_weights.main if approach == "main" else turn_weights.minor)
    # Fallback only for internal defaults (e.g., after all weights zero out)
    return {
        TurnMovement.LEFT: 1.0,
        TurnMovement.THROUGH: 98.0,
        TurnMovement.RIGHT: 1.0,
    }


def _movement_direction(incoming: CardinalDirection, movement: TurnMovement) -> CardinalDirection:
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
        raise DemandValidationError(f"unsupported movement mapping for incoming={incoming}, movement={movement}") from exc


def _approach_for(incoming: CardinalDirection) -> str:
    return "main" if incoming in {CardinalDirection.EAST, CardinalDirection.WEST} else "minor"


def _default_direction_shares(
    available: Dict[CardinalDirection, bool],
    approach: str,
    *,
    incoming: CardinalDirection,
) -> Dict[CardinalDirection, float]:
    movement_weights = _movement_weights(approach=approach, turn_weights=None)
    direction_weights: Dict[CardinalDirection, float] = defaultdict(float)
    through_bonus = 0.0
    movement_targets: Dict[TurnMovement, Tuple[CardinalDirection, float]] = {}

    for movement, weight in movement_weights.items():
        direction = _movement_direction(incoming, movement)
        if not available.get(direction, False):
            if approach == "main" and movement in (TurnMovement.LEFT, TurnMovement.RIGHT):
                through_bonus += weight
            weight = 0.0
        if approach == "main" and movement is TurnMovement.THROUGH and not available.get(direction, False):
            weight = 0.0
        movement_targets[movement] = (direction, weight)

    for movement, (direction, weight) in movement_targets.items():
        if movement is TurnMovement.THROUGH and approach == "main":
            weight += through_bonus
        if weight <= 0.0:
            continue
        direction_weights[direction] += weight

    total = sum(value for value in direction_weights.values() if value > 0.0)
    if total <= 0.0:
        return {}
    return {direction: value / total for direction, value in direction_weights.items() if value > 0.0}


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
    return VehicleState(pos=next_pos, incoming=direction)


def _opposite(direction: CardinalDirection) -> CardinalDirection:
    mapping = {
        CardinalDirection.NORTH: CardinalDirection.SOUTH,
        CardinalDirection.SOUTH: CardinalDirection.NORTH,
        CardinalDirection.EAST: CardinalDirection.WEST,
        CardinalDirection.WEST: CardinalDirection.EAST,
    }
    return mapping[direction]


def _is_main_u_turn(origin: str, destination: str) -> bool:
    o = origin.split(".")
    d = destination.split(".")
    if len(o) == 4 and len(d) == 4 and o[0] == "Node" and d[0] == "Node" and o[1] == "Main" and d[1] == "Main":
        return o[2] == d[2] and o[3] != d[3]
    return False


__all__ = ["compute_vehicle_od_flows"]
