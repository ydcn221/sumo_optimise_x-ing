import re
from math import isclose
from pathlib import Path
from typing import Dict

from sumo_optimise.conversion.demand.routes import render_routes_document
from sumo_optimise.conversion.demand.vehicle_flow.flow_propagation import compute_vehicle_od_flows
from sumo_optimise.conversion.demand.vehicle_flow.reachability import evaluate_vehicle_od_reachability
from sumo_optimise.conversion.demand.vehicle_flow.route_output import build_vehicle_flow_entries
from sumo_optimise.conversion.demand.vehicle_flow.topology import (
    build_vehicle_network,
    canonicalize_vehicle_endpoint,
)
from sumo_optimise.conversion.domain.models import (
    CardinalDirection,
    Cluster,
    EndpointDemandRow,
    EventKind,
    LayoutEvent,
    PersonFlowPattern,
    JunctionTurnWeights,
    TurnMovement,
)


def _make_cluster(pos: int) -> Cluster:
    event = LayoutEvent(type=EventKind.CROSS, pos_m_raw=float(pos), pos_m=pos)
    return Cluster(pos_m=pos, events=[event])


def test_vehicle_flow_propagation_and_rendering() -> None:
    breakpoints = [0, 100, 200]
    clusters = [_make_cluster(100)]
    network = build_vehicle_network(breakpoints, clusters)
    turn_weights: Dict[str, JunctionTurnWeights] = {
        "Cluster.100": JunctionTurnWeights(
            junction_id="Cluster.100",
            main={
                TurnMovement.LEFT: 1.0,
                TurnMovement.THROUGH: 1.0,
                TurnMovement.RIGHT: 1.0,
            },
            minor={
                TurnMovement.LEFT: 8.0,
                TurnMovement.THROUGH: 0.0,
                TurnMovement.RIGHT: 2.0,
            },
        )
    }

    rows = [
        EndpointDemandRow(endpoint_id="VehEnd_Minor_100_N_end", flow_per_hour=600.0),
    ]

    od_flows = compute_vehicle_od_flows(rows, network=network, turn_weights=turn_weights)
    assert od_flows, "expected at least one OD flow"

    source_totals = {
        dest: value
        for origin, dest, value, source_row in od_flows
        if source_row.endpoint_id == "VehEnd_Minor_100_N_end"
    }
    assert isclose(source_totals["Node.Main.0.S"] + source_totals["Node.Main.200.N"], 600.0, rel_tol=1e-6)
    # weights 8 (left to WB/main west) vs 2 (right to EB/main east) -> 80% / 20% split
    assert isclose(source_totals["Node.Main.0.S"], 600 * 0.8, rel_tol=1e-6)
    assert isclose(source_totals["Node.Main.200.N"], 600 * 0.2, rel_tol=1e-6)

    entries = build_vehicle_flow_entries(
        od_flows,
        vehicle_pattern=PersonFlowPattern.STEADY,
        simulation_end_time=3600.0,
    )
    assert all('departLane="best_prob"' in entry for entry in entries)
    assert all('departSpeed="desired"' in entry for entry in entries)
    assert any("vehsPerHour" in entry for entry in entries)

    combined = render_routes_document(person_entries=None, vehicle_entries=entries)
    assert combined is not None
    assert "<flow" in combined


def test_vehicle_flow_ids_remain_unique_for_duplicate_pairs() -> None:
    row1 = EndpointDemandRow(endpoint_id="VehEnd_Minor_100_N_end", flow_per_hour=500.0, row_index=5)
    row2 = EndpointDemandRow(endpoint_id="VehEnd_Minor_100_N_end", flow_per_hour=250.0, row_index=6)
    flows = [
        ("Node.Main.0.N", "Node.Main.200.S", 500.0, row1),
        ("Node.Main.0.N", "Node.Main.200.S", 250.0, row2),
    ]

    entries = build_vehicle_flow_entries(
        flows,
        vehicle_pattern=PersonFlowPattern.STEADY,
        simulation_end_time=1800.0,
    )
    ids = [re.search(r'id="([^"]+)"', entry).group(1) for entry in entries]
    assert ids == [
        "vf_Node.Main.0.N__Node.Main.200.S__0",
        "vf_Node.Main.0.N__Node.Main.200.S__1",
    ]


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


def test_canonicalize_vehicle_endpoint_accepts_template_ids() -> None:
    breakpoints = [0, 100]
    clusters = [_make_cluster(100)]
    network = build_vehicle_network(breakpoints, clusters)

    assert canonicalize_vehicle_endpoint(
        "VehEnd_Main_W_end", network=network, prefer_departing_half=True
    ) == "Node.Main.0.S"
    assert canonicalize_vehicle_endpoint(
        "VehEnd_Main_W_end", network=network, prefer_departing_half=False
    ) == "Node.Main.0.N"
    assert canonicalize_vehicle_endpoint(
        "VehEnd_Main_E_end", network=network, prefer_departing_half=True
    ) == "Node.Main.100.N"
    assert canonicalize_vehicle_endpoint(
        "VehEnd_Main_E_end", network=network, prefer_departing_half=False
    ) == "Node.Main.100.S"
    assert canonicalize_vehicle_endpoint(
        "VehEnd_Minor_100_N_end", network=network, prefer_departing_half=True
    ) == "Node.Minor.100.N_end"
    assert canonicalize_vehicle_endpoint(
        "VehEnd_Minor_100_S_end", network=network, prefer_departing_half=True
    ) == "Node.Minor.100.S_end"


def test_vehicle_no_main_u_turn_pairs() -> None:
    breakpoints = [0, 100, 200]
    clusters = [_make_cluster(100)]
    network = build_vehicle_network(breakpoints, clusters)
    turn_weights: Dict[str, JunctionTurnWeights] = {
        "Cluster.100": JunctionTurnWeights(
            junction_id="Cluster.100",
            main={
                TurnMovement.LEFT: 1.0,
                TurnMovement.THROUGH: 1.0,
                TurnMovement.RIGHT: 1.0,
            },
            minor={
                TurnMovement.LEFT: 1.0,
                TurnMovement.THROUGH: 0.0,
                TurnMovement.RIGHT: 1.0,
            },
        )
    }

    rows = [
        EndpointDemandRow(endpoint_id="VehEnd_Minor_100_N_end", flow_per_hour=500.0),
        EndpointDemandRow(endpoint_id="VehEnd_Minor_100_S_end", flow_per_hour=-200.0),
    ]
    od_flows = compute_vehicle_od_flows(rows, network=network, turn_weights=turn_weights)
    for origin, dest, _, _ in od_flows:
        if origin.startswith("Node.Main") and dest.startswith("Node.Main"):
            o_parts = origin.split(".")
            d_parts = dest.split(".")
            if len(o_parts) == 4 and len(d_parts) == 4 and o_parts[2] == d_parts[2] and o_parts[3] != d_parts[3]:
                raise AssertionError(f"vehicle U-turn flow detected: {origin} -> {dest}")


def test_vehicle_no_main_u_turn_pairs() -> None:
    breakpoints = [0, 1500]
    clusters = [_make_cluster(1500)]
    network = build_vehicle_network(breakpoints, clusters)
    turn_weights: Dict[str, JunctionTurnWeights] = {
        "Cluster.1500": JunctionTurnWeights(
            junction_id="Cluster.1500",
            main={
                TurnMovement.LEFT: 1.0,
                TurnMovement.THROUGH: 1.0,
                TurnMovement.RIGHT: 1.0,
            },
            minor={
                TurnMovement.LEFT: 1.0,
                TurnMovement.THROUGH: 0.0,
                TurnMovement.RIGHT: 1.0,
            },
        )
    }

    rows = [
        EndpointDemandRow(endpoint_id="VehEnd_Main_E_end", flow_per_hour=100.0),
        EndpointDemandRow(endpoint_id="VehEnd_Main_W_end", flow_per_hour=-50.0),
    ]
    od_flows = compute_vehicle_od_flows(rows, network=network, turn_weights=turn_weights)
    for origin, dest, _, _ in od_flows:
        if origin.startswith("Node.Main") and dest.startswith("Node.Main"):
            o_parts = origin.split(".")
            d_parts = dest.split(".")
            if len(o_parts) == 4 and len(d_parts) == 4 and o_parts[2] == d_parts[2] and o_parts[3] != d_parts[3]:
                raise AssertionError(f"vehicle U-turn flow detected: {origin} -> {dest}")
