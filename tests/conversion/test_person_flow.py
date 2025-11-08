from __future__ import annotations

from io import StringIO
from pathlib import Path

import networkx as nx
import pytest

from sumo_optimise.conversion.cli.main import (
    _build_options,
    _resolve_output_files,
    _resolve_output_template,
    parse_args,
)
from sumo_optimise.conversion.domain.models import (
    CardinalDirection,
    Defaults,
    EndpointDemandRow,
    JunctionTurnWeights,
    OutputDirectoryTemplate,
    PedestrianSide,
    PersonFlowPattern,
)
from sumo_optimise.conversion.demand.person_flow.demand_input import (
    load_endpoint_demands,
    load_junction_turn_weights,
)
from sumo_optimise.conversion.demand.person_flow.flow_propagation import compute_od_flows
from sumo_optimise.conversion.demand.person_flow.identifier import minor_endpoint_id
from sumo_optimise.conversion.demand.person_flow.route_output import render_person_flows


def _simple_graph() -> nx.MultiGraph:
    graph = nx.MultiGraph()
    graph.add_node(
        "Node.Main.0.N",
        coord=(0.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.0",
    )
    graph.add_node(
        "Node.Main.0.S",
        coord=(0.0, -2.0),
        is_endpoint=True,
        cluster_id="Cluster.0",
    )
    graph.add_node(
        "Node.Main.100.N",
        coord=(100.0, 2.0),
        is_endpoint=False,
        cluster_id="Cluster.100",
    )
    graph.add_node(
        "Node.Main.100.S",
        coord=(100.0, -2.0),
        is_endpoint=True,
        cluster_id="Cluster.100",
    )
    graph.add_edge(
        "Node.Main.0.N",
        "Node.Main.100.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.Main.0.N",
        "Node.Main.0.S",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.0.N",
        "Node.Main.0.S",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.100.N",
        "Node.Main.100.S",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.100.N",
        "Node.Main.100.S",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    return graph


def _junction_weights() -> JunctionTurnWeights:
    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = 0.0
    weights[(CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE)] = 1.0
    weights[(CardinalDirection.NORTH, PedestrianSide.WEST_SIDE)] = 1.0
    weights[(CardinalDirection.WEST, PedestrianSide.NORTH_SIDE)] = 1.0
    return JunctionTurnWeights(junction_id="Cluster.100", weights=weights)


def test_compute_od_flows_positive_and_negative() -> None:
    graph = _simple_graph()
    turn_weights = {"Cluster.100": _junction_weights()}

    positive_row = EndpointDemandRow(
        endpoint_id="PedEnd.Main.W_end.N_sidewalk", flow_per_hour=1200.0, row_index=2
    )
    flows = compute_od_flows(graph, turn_weights, [positive_row])

    assert len(flows) == 1
    origin, destination, value, row_ref = flows[0]
    assert origin == "PedEnd.Main.W_end.N_sidewalk"
    assert destination == "Node.Main.100.S"
    assert value > 0
    assert row_ref is positive_row

    negative_row = EndpointDemandRow(
        endpoint_id="PedEnd.Main.E_end.S_sidewalk", flow_per_hour=-600.0, row_index=3
    )
    flows_negative = compute_od_flows(graph, turn_weights, [negative_row])

    assert len(flows_negative) == 1
    origin, destination, value, row_ref = flows_negative[0]
    assert origin == "Node.Main.0.N"
    assert destination == "PedEnd.Main.E_end.S_sidewalk"
    assert value > 0
    assert row_ref is negative_row


def test_render_person_flows_emits_expected_xml() -> None:
    row = EndpointDemandRow(endpoint_id="PedEnd.Main.W_end.N_sidewalk", flow_per_hour=1200.0, row_index=2)
    flows = [("PedEnd.Main.W_end.N_sidewalk", "Node.Main.100.S", 1200.0, row)]
    defaults = Defaults(
        minor_road_length_m=120,
        ped_crossing_width_m=3.5,
        speed_kmh=50,
        ped_endpoint_offset_m=0.10,
        sidewalk_width_m=2.0,
    )
    xml = render_person_flows(
        flows,
        ped_pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=7200.0,
        endpoint_offset_m=defaults.ped_endpoint_offset_m,
        breakpoints=[0, 100],
        defaults=defaults,
    )

    assert '<personFlow id="pf_PedEnd.Main.W_end.N_sidewalk__Node.Main.100.S__0"' in xml
    assert 'personsPerHour="1200.000000"' in xml
    assert 'departPos="0.10"' in xml
    assert '<personTrip from="Edge.Main.EB.0-100" to="Edge.Main.WB.100-0"' in xml


def test_render_person_flows_maps_minor_side_edges() -> None:
    flows = [
        (
            "PedEnd.Main.W_end.N_sidewalk",
            "PedEnd.Minor.1000.S_end.W_sidewalk",
            300.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.1000.S_end.W_sidewalk", flow_per_hour=-300.0, row_index=1
            ),
        ),
        (
            "PedEnd.Main.W_end.N_sidewalk",
            "PedEnd.Minor.1000.S_end.E_sidewalk",
            300.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.1000.S_end.E_sidewalk", flow_per_hour=-300.0, row_index=2
            ),
        ),
    ]
    defaults = Defaults(
        minor_road_length_m=120,
        ped_crossing_width_m=3.5,
        speed_kmh=50,
        ped_endpoint_offset_m=0.10,
        sidewalk_width_m=2.0,
    )

    xml = render_person_flows(
        flows,
        ped_pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=7200.0,
        endpoint_offset_m=defaults.ped_endpoint_offset_m,
        breakpoints=[0, 200],
        defaults=defaults,
    )

    assert '<personTrip from="Edge.Main.EB.0-200" to="Edge.Minor.S_arm.NB.1000" arrivalPos="0.10"/>' in xml
    assert '<personTrip from="Edge.Main.EB.0-200" to="Edge.Minor.S_arm.SB.1000" arrivalPos="119.90"/>' in xml


def test_render_person_flows_minor_endpoint_offsets() -> None:
    flows = [
        (
            "PedEnd.Minor.350.N_end.W_sidewalk",
            "Node.Main.0.N",
            150.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.350.N_end.W_sidewalk", flow_per_hour=150.0, row_index=1
            ),
        ),
        (
            "PedEnd.Minor.350.N_end.E_sidewalk",
            "Node.Main.0.N",
            150.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.350.N_end.E_sidewalk", flow_per_hour=150.0, row_index=2
            ),
        ),
        (
            "Node.Main.0.N",
            "PedEnd.Minor.350.N_end.W_sidewalk",
            150.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.350.N_end.W_sidewalk", flow_per_hour=-150.0, row_index=3
            ),
        ),
        (
            "Node.Main.0.N",
            "PedEnd.Minor.350.N_end.E_sidewalk",
            150.0,
            EndpointDemandRow(
                endpoint_id="PedEnd.Minor.350.N_end.E_sidewalk", flow_per_hour=-150.0, row_index=4
            ),
        ),
    ]

    defaults = Defaults(
        minor_road_length_m=120,
        ped_crossing_width_m=3.5,
        speed_kmh=50,
        ped_endpoint_offset_m=0.10,
        sidewalk_width_m=2.0,
    )

    xml = render_person_flows(
        flows,
        ped_pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=7200.0,
        endpoint_offset_m=defaults.ped_endpoint_offset_m,
        breakpoints=[0, 500],
        defaults=defaults,
    )

    assert (
        '<personFlow id="pf_PedEnd.Minor.350.N_end.W_sidewalk__Node.Main.0.N__0" '
        'begin="0.00" end="7200.00" departPos="119.90" personsPerHour="150.000000">'
        in xml
    )
    assert (
        '<personFlow id="pf_PedEnd.Minor.350.N_end.E_sidewalk__Node.Main.0.N__0" '
        'begin="0.00" end="7200.00" departPos="0.10" personsPerHour="150.000000">'
        in xml
    )
    assert '<personTrip from="Edge.Main.EB.0-500" to="Edge.Minor.N_arm.NB.350" arrivalPos="119.90"/>' in xml
    assert '<personTrip from="Edge.Main.EB.0-500" to="Edge.Minor.N_arm.SB.350" arrivalPos="0.10"/>' in xml


def test_load_endpoint_demands_and_weights() -> None:
    endpoint_csv = StringIO(
        "Pattern,persons_per_hour\n"
        "SidewalkEndID,PedFlow,Label\n"
        "PedEnd.Main.W_end.N_sidewalk,100,Peak\n"
        "PedEnd.Main.E_end.S_sidewalk,-50,\n"
    )
    pattern, rows = load_endpoint_demands(endpoint_csv)
    assert pattern is PersonFlowPattern.PERSONS_PER_HOUR
    assert [row.endpoint_id for row in rows] == [
        "PedEnd.Main.W_end.N_sidewalk",
        "PedEnd.Main.E_end.S_sidewalk",
    ]
    assert rows[0].flow_per_hour == 100
    assert rows[1].flow_per_hour == -50

    turn_weight_csv = StringIO(
        ",".join(
            [
                "JunctionID",
                "ToNorth_EastSide",
                "ToNorth_WestSide",
                "ToWest_NorthSide",
                "ToWest_SouthSide",
                "ToSouth_WestSide",
                "ToSouth_EastSide",
                "ToEast_SouthSide",
                "ToEast_NorthSide",
            ]
        )
        + "\n"
        + "Cluster.100,0,0,0,0,1,0,0,0\n"
    )
    turn_weight_map = load_junction_turn_weights(turn_weight_csv)
    assert set(turn_weight_map) == {"Cluster.100"}
    weights = turn_weight_map["Cluster.100"]
    assert weights.weight(CardinalDirection.SOUTH, PedestrianSide.WEST_SIDE) == 1.0


def test_cli_accepts_demand_options() -> None:
    args = parse_args(
        [
            "spec.json",
            "--ped-endpoint-demand",
            "PedDemand.csv",
            "--ped-junction-turn-weight",
            "PedRatio.csv",
            "--veh-endpoint-demand",
            "VehDemand.csv",
            "--veh-junction-turn-weight",
            "VehRatio.csv",
            "--demand-sim-end",
            "5400",
        ]
    )
    template = _resolve_output_template(args)
    file_templates = _resolve_output_files(args)
    options = _build_options(args, template, file_templates)
    assert options.demand is not None
    assert options.demand.ped_endpoint_csv == Path("PedDemand.csv")
    assert options.demand.ped_junction_turn_weight_csv == Path("PedRatio.csv")
    assert options.demand.veh_endpoint_csv == Path("VehDemand.csv")
    assert options.demand.veh_junction_turn_weight_csv == Path("VehRatio.csv")
    assert options.demand.simulation_end_time == 5400


def test_cli_requires_both_demand_csvs() -> None:
    args = parse_args(["spec.json"])
    template = _resolve_output_template(args)
    file_templates = _resolve_output_files(args)
    options = _build_options(args, template, file_templates)
    assert options.demand is None

    partial_args = parse_args(["spec.json", "--ped-endpoint-demand", "Demand.csv"])
    with pytest.raises(SystemExit):
        _build_options(partial_args, OutputDirectoryTemplate(), _resolve_output_files(partial_args))

    vehicle_partial = parse_args(["spec.json", "--veh-endpoint-demand", "Veh.csv"])
    with pytest.raises(SystemExit):
        _build_options(vehicle_partial, OutputDirectoryTemplate(), _resolve_output_files(vehicle_partial))


def test_turn_weight_zeroing_keeps_forward_direction() -> None:
    graph = nx.MultiGraph()
    graph.add_node(
        "Node.Main.0.N",
        coord=(0.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.0",
    )
    graph.add_node(
        "Node.Main.100.N",
        coord=(100.0, 2.0),
        is_endpoint=False,
        cluster_id="Cluster.100",
    )
    graph.add_node(
        "Node.Main.200.N",
        coord=(200.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.200",
    )
    graph.add_edge(
        "Node.Main.0.N",
        "Node.Main.100.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.Main.100.N",
        "Node.Main.200.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = 0.0
    weights[(CardinalDirection.EAST, PedestrianSide.NORTH_SIDE)] = 1.0
    turn_weights = {"Cluster.100": JunctionTurnWeights("Cluster.100", weights)}

    row = EndpointDemandRow(endpoint_id="PedEnd.Main.W_end.N_sidewalk", flow_per_hour=500.0)
    flows = compute_od_flows(graph, turn_weights, [row])

    assert flows == [("PedEnd.Main.W_end.N_sidewalk", "Node.Main.200.N", 500.0, row)]


def _make_weights(overrides: dict[tuple[CardinalDirection, PedestrianSide], float], junction: str) -> JunctionTurnWeights:
    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = overrides.get((direction, side), 0.0)
    return JunctionTurnWeights(junction_id=junction, weights=weights)


def test_mainline_flow_directed_to_east_end() -> None:
    graph = nx.MultiGraph()
    graph.add_node("Node.Main.0.N", coord=(0.0, 2.0), is_endpoint=True, cluster_id="Cluster.0")
    graph.add_node("Node.Main.100.N", coord=(100.0, 2.0), is_endpoint=False, cluster_id="Cluster.100")
    graph.add_node("Node.Main.200.N", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200")

    graph.add_edge(
        "Node.Main.0.N",
        "Node.Main.100.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.Main.100.N",
        "Node.Main.200.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    turn_weights = {
        "Cluster.100": _make_weights(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
            },
            "Cluster.100",
        )
    }

    row = EndpointDemandRow(endpoint_id="PedEnd.Main.W_end.N_sidewalk", flow_per_hour=300.0)
    flows = compute_od_flows(graph, turn_weights, [row])

    assert flows == [("PedEnd.Main.W_end.N_sidewalk", "Node.Main.200.N", 300.0, row)]


def test_minor_side_flow_directed_to_east_main_endpoint() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.100"
    origin = minor_endpoint_id(100, "N", PedestrianSide.EAST_SIDE)

    graph.add_node(origin, coord=(100.0, 10.0), is_endpoint=True, cluster_id=junction, side=PedestrianSide.EAST_SIDE)
    graph.add_node("Node.Main.100.N", coord=(100.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.200.N", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200")

    graph.add_edge(
        origin,
        "Node.Main.100.N",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=8.0,
    )
    graph.add_edge(
        "Node.Main.100.N",
        "Node.Main.200.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    turn_weights = {
        junction: _make_weights(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 2.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id=origin, flow_per_hour=180.0)
    flows = compute_od_flows(graph, turn_weights, [row])

    assert flows == [(origin, "Node.Main.200.N", 180.0, row)]


def test_crosswalk_follows_east_side_turn_weights() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.150"

    graph.add_node("Node.Main.150.N", coord=(150.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.150.S", coord=(150.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.200.N", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200")
    graph.add_node("Node.Main.200.S", coord=(200.0, -2.0), is_endpoint=True, cluster_id="Cluster.200")

    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.200.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.Main.150.S",
        "Node.Main.200.S",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.150.S",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.150.S",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )

    turn_weights = {
        junction: _make_weights(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
                (CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE): 2.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="PedEnd.Main.150.N_sidewalk", flow_per_hour=300.0)
    flows = compute_od_flows(graph, turn_weights, [row])

    assert sorted((origin, dest, round(value, 6)) for origin, dest, value, _ in flows) == [
        ("PedEnd.Main.150.N_sidewalk", "Node.Main.200.N", 100.0),
        ("PedEnd.Main.150.N_sidewalk", "Node.Main.200.S", 200.0),
    ]


def test_crosswalk_follows_west_side_for_north_half() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.150"

    graph.add_node("Node.Main.150.N", coord=(150.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.150.S", coord=(150.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.200.N", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200")
    graph.add_node("Node.Main.200.S", coord=(200.0, -2.0), is_endpoint=True, cluster_id="Cluster.200")

    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.200.N",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.Main.150.S",
        "Node.Main.200.S",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.150.S",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.150.N",
        "Node.Main.150.S",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )

    turn_weights = {
        junction: _make_weights(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="PedEnd.Main.150.S_sidewalk", flow_per_hour=180.0)
    flows = compute_od_flows(graph, turn_weights, [row])

    assert flows == [("PedEnd.Main.150.S_sidewalk", "Node.Main.200.N", 180.0, row)]


def test_crosswalk_handles_multiple_directions_without_conflict() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.200"

    graph.add_node("Node.Main.200.N", coord=(200.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.200.S", coord=(200.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.Main.100.S", coord=(100.0, -2.0), is_endpoint=True, cluster_id="Cluster.100")
    graph.add_node("Node.Main.300.S", coord=(300.0, -2.0), is_endpoint=True, cluster_id="Cluster.300")

    graph.add_edge(
        "Node.Main.200.N",
        "Node.Main.200.S",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.Main.200.S",
        "Node.Main.300.S",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.Main.200.S",
        "Node.Main.100.S",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=100.0,
    )

    turn_weights = {
        junction: _make_weights(
            {
                (CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE): 1.0,
                (CardinalDirection.WEST, PedestrianSide.SOUTH_SIDE): 1.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="PedEnd.Main.200.N_sidewalk", flow_per_hour=300.0)
    flows = compute_od_flows(graph, turn_weights, [row])
    summary = {(origin, dest): value for origin, dest, value, _ in flows}

    assert summary[("PedEnd.Main.200.N_sidewalk", "Node.Main.300.S")] == pytest.approx(150.0)
    assert summary[("PedEnd.Main.200.N_sidewalk", "Node.Main.100.S")] == pytest.approx(150.0)
