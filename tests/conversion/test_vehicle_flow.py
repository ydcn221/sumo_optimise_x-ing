from math import isclose
from pathlib import Path
from typing import Dict

from sumo_optimise.conversion.demand.routes import render_routes_document
from sumo_optimise.conversion.demand.vehicle_flow.demand_input import VehicleTurnWeights
from sumo_optimise.conversion.demand.vehicle_flow.flow_propagation import compute_vehicle_od_flows
from sumo_optimise.conversion.demand.vehicle_flow.reachability import evaluate_vehicle_od_reachability
from sumo_optimise.conversion.demand.vehicle_flow.route_output import build_vehicle_flow_entries
from sumo_optimise.conversion.demand.vehicle_flow.topology import build_vehicle_network
from sumo_optimise.conversion.domain.models import (
    CardinalDirection,
    Cluster,
    EndpointDemandRow,
    EventKind,
    LayoutEvent,
    PersonFlowPattern,
)


def _make_cluster(pos: int) -> Cluster:
    event = LayoutEvent(type=EventKind.CROSS, pos_m_raw=float(pos), pos_m=pos)
    return Cluster(pos_m=pos, events=[event])


def test_vehicle_flow_propagation_and_rendering() -> None:
    breakpoints = [0, 100, 200]
    clusters = [_make_cluster(100)]
    network = build_vehicle_network(breakpoints, clusters)
    turn_weights: Dict[str, VehicleTurnWeights] = {
        "Cluster.100": VehicleTurnWeights(
            junction_id="Cluster.100",
            weights={
                CardinalDirection.NORTH: 0.0,
                CardinalDirection.WEST: 2.0,
                CardinalDirection.SOUTH: 4.0,
                CardinalDirection.EAST: 8.0,
            },
        )
    }

    rows = [
        EndpointDemandRow(endpoint_id="Node.Minor.100.N_end", flow_per_hour=600.0),
        EndpointDemandRow(endpoint_id="Node.Minor.100.S_end", flow_per_hour=-300.0),
    ]

    od_flows = compute_vehicle_od_flows(rows, network=network, turn_weights=turn_weights)
    assert od_flows, "expected at least one OD flow"

    # source row splits 600 veh/h into 2/14 (west), 4/14 (south endpoint), 8/14 (east end)
    source_totals = {
        dest: value
        for origin, dest, value, source_row in od_flows
        if source_row.endpoint_id == "Node.Minor.100.N_end"
    }
    assert isclose(source_totals["Node.Minor.100.S_end"], 600 * (4 / 14), rel_tol=1e-6)
    assert source_totals["Node.Main.200.N"] > source_totals["Node.Main.0.S"]

    sink_totals = {
        origin: value
        for origin, dest, value, source_row in od_flows
        if source_row.endpoint_id == "Node.Minor.100.S_end"
    }
    assert isclose(sink_totals["Node.Main.0.S"] + sink_totals["Node.Main.200.N"], 300.0)

    entries = build_vehicle_flow_entries(
        od_flows,
        vehicle_pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=3600.0,
    )
    assert any("vehsPerHour" in entry for entry in entries)

    combined = render_routes_document(person_entries=None, vehicle_entries=entries)
    assert combined is not None
    assert "<flow" in combined


def test_vehicle_od_reachability_filters_unreachable_pairs() -> None:
    edges_xml = """
<edges>
  <edge id="edge_main" from="Node.Main.0.N" to="Node.Main.100.N"/>
  <edge id="edge_minor_s" from="Node.Main.100.N" to="Node.Minor.100.S_end"/>
  <edge id="edge_minor_blocked" from="Node.Main.100.N" to="Node.Minor.200.S_end"/>
</edges>
""".strip()
    connections_xml = """
<connections>
  <connection from="edge_main" to="edge_minor_s"/>
</connections>
""".strip()

    flows = [
        ("Node.Main.0.N", "Node.Minor.100.S_end", 50.0, EndpointDemandRow(endpoint_id="Node.Main.0.N", flow_per_hour=50.0, row_index=0)),
        ("Node.Main.0.N", "Node.Minor.200.S_end", 25.0, EndpointDemandRow(endpoint_id="Node.Main.0.N", flow_per_hour=25.0, row_index=1)),
    ]

    report = evaluate_vehicle_od_reachability(flows, edges_xml=edges_xml, connections_xml=connections_xml)
    assert len(report.reachable) == 1
    assert len(report.unreachable) == 1
    assert report.reachable[0].destination == "Node.Minor.100.S_end"
    assert report.unreachable[0].destination == "Node.Minor.200.S_end"
    assert report.unreachable[0].row.row_index == 1


def test_vehicle_od_reachability_uses_default_adjacency_when_no_connections() -> None:
    edges_xml = """
<edges>
  <edge id="edge_main_ab" from="Node.Main.0.N" to="Node.Main.100.N"/>
  <edge id="edge_main_bc" from="Node.Main.100.N" to="Node.Minor.200.N_end"/>
</edges>
""".strip()
    connections_xml = "<connections/>"

    flows = [
        (
            "Node.Main.0.N",
            "Node.Minor.200.N_end",
            10.0,
            EndpointDemandRow(endpoint_id="Node.Main.0.N", flow_per_hour=10.0),
        ),
    ]

    report = evaluate_vehicle_od_reachability(flows, edges_xml=edges_xml, connections_xml=connections_xml)
    assert len(report.reachable) == 1
    assert not report.unreachable
