from __future__ import annotations

from sumo_optimise.conversion.demand.vehicle_flow.flow_propagation import compute_vehicle_od_flows
from sumo_optimise.conversion.demand.vehicle_flow.topology import (
    VehicleClusterMeta,
    VehicleNetwork,
    vehicle_cluster_id,
)
from sumo_optimise.conversion.domain.models import CardinalDirection, EndpointDemandRow, JunctionTurnWeights, TurnMovement


def test_minor_origin_reaches_main_end():
    """Flows from a minor north arm should be able to continue east to the far main endpoint."""

    network = VehicleNetwork(
        positions=[0, 350, 650, 1500],
        index_by_pos={0: 0, 350: 1, 650: 2, 1500: 3},
        cluster_meta={
            350: VehicleClusterMeta(pos=350, has_north_minor=True, has_south_minor=False),
            650: VehicleClusterMeta(pos=650, has_north_minor=True, has_south_minor=True),
        },
        min_pos=0,
        max_pos=1500,
    )
    turn_weights = {
        vehicle_cluster_id(350): JunctionTurnWeights(
            junction_id=vehicle_cluster_id(350),
            main={TurnMovement.LEFT: 1.0, TurnMovement.THROUGH: 1.0, TurnMovement.RIGHT: 1.0},
            minor={TurnMovement.LEFT: 1.0, TurnMovement.THROUGH: 0.0, TurnMovement.RIGHT: 1.0},
        ),
        vehicle_cluster_id(650): JunctionTurnWeights(
            junction_id=vehicle_cluster_id(650),
            main={TurnMovement.LEFT: 1.0, TurnMovement.THROUGH: 1.0, TurnMovement.RIGHT: 1.0},
            minor={TurnMovement.LEFT: 1.0, TurnMovement.THROUGH: 0.0, TurnMovement.RIGHT: 1.0},
        ),
    }
    rows = [EndpointDemandRow(endpoint_id="Node.Minor.350.N_end", flow_per_hour=500.0)]

    od_flows = compute_vehicle_od_flows(rows, network=network, turn_weights=turn_weights)
    destinations = {destination for _, destination, _, _ in od_flows}

    assert "Node.Main.1500.N" in destinations
