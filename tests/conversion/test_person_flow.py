from __future__ import annotations

from io import StringIO
from pathlib import Path

import networkx as nx
import pytest

from sumo_optimise.conversion.cli.main import _build_options, _resolve_demand_options, _resolve_output_template, parse_args
from sumo_optimise.conversion.domain.models import (
    CardinalDirection,
    DemandOptions,
    Defaults,
    EndpointDemandRow,
    JunctionDirectionRatios,
    OutputDirectoryTemplate,
    PedestrianSide,
    PersonFlowPattern,
)
from sumo_optimise.conversion.demand.person_flow.demand_input import load_endpoint_demands, load_junction_ratios
from sumo_optimise.conversion.demand.person_flow.flow_propagation import compute_od_flows
from sumo_optimise.conversion.demand.person_flow.identifier import minor_endpoint_id
from sumo_optimise.conversion.demand.person_flow.route_output import render_person_flows


def _simple_graph() -> nx.MultiGraph:
    graph = nx.MultiGraph()
    graph.add_node(
        "Node.0.MainN",
        coord=(0.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.0.Main",
    )
    graph.add_node(
        "Node.0.MainS",
        coord=(0.0, -2.0),
        is_endpoint=True,
        cluster_id="Cluster.0.Main",
    )
    graph.add_node(
        "Node.100.MainN",
        coord=(100.0, 2.0),
        is_endpoint=False,
        cluster_id="Cluster.100.Main",
    )
    graph.add_node(
        "Node.100.MainS",
        coord=(100.0, -2.0),
        is_endpoint=True,
        cluster_id="Cluster.100.Main",
    )
    graph.add_edge(
        "Node.0.MainN",
        "Node.100.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.0.MainN",
        "Node.0.MainS",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.0.MainN",
        "Node.0.MainS",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.100.MainN",
        "Node.100.MainS",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.100.MainN",
        "Node.100.MainS",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    return graph


def _junction_ratio() -> JunctionDirectionRatios:
    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = 0.0
    weights[(CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE)] = 1.0
    weights[(CardinalDirection.NORTH, PedestrianSide.WEST_SIDE)] = 1.0
    weights[(CardinalDirection.WEST, PedestrianSide.NORTH_SIDE)] = 1.0
    return JunctionDirectionRatios(junction_id="Cluster.100.Main", weights=weights)


def test_compute_od_flows_positive_and_negative() -> None:
    graph = _simple_graph()
    ratios = {"Cluster.100.Main": _junction_ratio()}

    positive_row = EndpointDemandRow(endpoint_id="Node.0.MainN", flow_per_hour=1200.0, row_index=2)
    flows = compute_od_flows(graph, ratios, [positive_row])

    assert len(flows) == 1
    origin, destination, value, row_ref = flows[0]
    assert origin == "Node.0.MainN"
    assert destination == "Node.100.MainS"
    assert value > 0
    assert row_ref is positive_row

    negative_row = EndpointDemandRow(endpoint_id="Node.100.MainS", flow_per_hour=-600.0, row_index=3)
    flows_negative = compute_od_flows(graph, ratios, [negative_row])

    assert len(flows_negative) == 1
    origin, destination, value, row_ref = flows_negative[0]
    assert origin == "Node.0.MainN"
    assert destination == "Node.100.MainS"
    assert value > 0
    assert row_ref is negative_row


def test_render_person_flows_emits_expected_xml() -> None:
    row = EndpointDemandRow(endpoint_id="Node.0.MainN", flow_per_hour=1200.0, row_index=2)
    flows = [("Node.0.MainN", "Node.100.MainS", 1200.0, row)]
    options = DemandOptions(
        endpoint_csv=Path("DemandPerEndpoint.csv"),
        junction_csv=Path("JunctionDirectionRatio.csv"),
        pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=7200.0,
        endpoint_offset_m=0.10,
    )
    defaults = Defaults(
        minor_road_length_m=120,
        ped_crossing_width_m=3.5,
        speed_kmh=50,
        sidewalk_width_m=2.0,
    )
    xml = render_person_flows(flows, options=options, breakpoints=[0, 100], defaults=defaults)

    assert '<personFlow id="pf_Node.0.MainN__Node.100.MainS__0"' in xml
    assert 'personsPerHour="1200.000000"' in xml
    assert 'departPos="0.10"' in xml
    assert '<personTrip from="Edge.Main.EB.0-100" to="Edge.Main.WB.100-0"' in xml


def test_render_person_flows_maps_minor_side_edges() -> None:
    flows = [
        (
            "Node.0.MainN",
            "Node.1000.MinorSEndpoint.WestSide",
            300.0,
            EndpointDemandRow(
                endpoint_id="Node.1000.MinorSEndpoint.WestSide", flow_per_hour=-300.0, row_index=1
            ),
        ),
        (
            "Node.0.MainN",
            "Node.1000.MinorSEndpoint.EastSide",
            300.0,
            EndpointDemandRow(
                endpoint_id="Node.1000.MinorSEndpoint.EastSide", flow_per_hour=-300.0, row_index=2
            ),
        ),
    ]
    options = DemandOptions(
        endpoint_csv=Path("DemandPerEndpoint.csv"),
        junction_csv=Path("JunctionDirectionRatio.csv"),
        pattern=PersonFlowPattern.PERSONS_PER_HOUR,
        simulation_end_time=7200.0,
        endpoint_offset_m=0.10,
    )
    defaults = Defaults(
        minor_road_length_m=120,
        ped_crossing_width_m=3.5,
        speed_kmh=50,
        sidewalk_width_m=2.0,
    )

    xml = render_person_flows(flows, options=options, breakpoints=[0, 200], defaults=defaults)

    assert '<personTrip from="Edge.Main.EB.0-200" to="Edge.MinorS.NB.1000" arrivalPos="0.10"/>' in xml
    assert '<personTrip from="Edge.Main.EB.0-200" to="Edge.MinorS.SB.1000" arrivalPos="119.90"/>' in xml


def test_load_endpoint_demands_and_ratios() -> None:
    endpoint_csv = StringIO("EndpointID,PedFlow,Label\nNode.0.MainN,100,Peak\nNode.100.MainS,-50,\n")
    rows = load_endpoint_demands(endpoint_csv)
    assert [row.endpoint_id for row in rows] == ["Node.0.MainN", "Node.100.MainS"]
    assert rows[0].flow_per_hour == 100
    assert rows[1].flow_per_hour == -50

    ratio_csv = StringIO(
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
        + "Cluster.100.Main,0,0,0,0,1,0,0,0\n"
    )
    ratio_map = load_junction_ratios(ratio_csv)
    assert set(ratio_map) == {"Cluster.100.Main"}
    ratio = ratio_map["Cluster.100.Main"]
    assert ratio.weight(CardinalDirection.SOUTH, PedestrianSide.WEST_SIDE) == 1.0


def test_cli_accepts_demand_options() -> None:
    args = parse_args(
        [
            "spec.json",
            "--demand-endpoints",
            "Demand.csv",
            "--demand-junctions",
            "Ratio.csv",
            "--demand-pattern",
            "period",
            "--demand-sim-end",
            "5400",
            "--demand-endpoint-offset",
            "0.25",
        ]
    )
    template = _resolve_output_template(args)
    options = _build_options(args, template)
    assert options.demand is not None
    assert options.demand.pattern is PersonFlowPattern.PERIOD
    assert options.demand.simulation_end_time == 5400
    assert options.demand.endpoint_offset_m == 0.25


def test_cli_requires_both_demand_csvs() -> None:
    args = parse_args(["spec.json"])
    template = _resolve_output_template(args)
    options = _build_options(args, template)
    assert options.demand is None

    partial_args = parse_args(["spec.json", "--demand-endpoints", "Demand.csv"])
    with pytest.raises(SystemExit):
        _build_options(partial_args, OutputDirectoryTemplate())


def test_ratio_zeroing_keeps_forward_direction() -> None:
    graph = nx.MultiGraph()
    graph.add_node(
        "Node.0.MainN",
        coord=(0.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.0.Main",
    )
    graph.add_node(
        "Node.100.MainN",
        coord=(100.0, 2.0),
        is_endpoint=False,
        cluster_id="Cluster.100.Main",
    )
    graph.add_node(
        "Node.200.MainN",
        coord=(200.0, 2.0),
        is_endpoint=True,
        cluster_id="Cluster.200.Main",
    )
    graph.add_edge(
        "Node.0.MainN",
        "Node.100.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.100.MainN",
        "Node.200.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = 0.0
    weights[(CardinalDirection.EAST, PedestrianSide.NORTH_SIDE)] = 1.0
    ratios = {"Cluster.100.Main": JunctionDirectionRatios("Cluster.100.Main", weights)}

    row = EndpointDemandRow(endpoint_id="Node.0.MainN", flow_per_hour=500.0)
    flows = compute_od_flows(graph, ratios, [row])

    assert flows == [("Node.0.MainN", "Node.200.MainN", 500.0, row)]


def _make_ratio(overrides: dict[tuple[CardinalDirection, PedestrianSide], float], junction: str) -> JunctionDirectionRatios:
    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = overrides.get((direction, side), 0.0)
    return JunctionDirectionRatios(junction_id=junction, weights=weights)


def test_mainline_flow_directed_to_east_end() -> None:
    graph = nx.MultiGraph()
    graph.add_node("Node.0.MainN", coord=(0.0, 2.0), is_endpoint=True, cluster_id="Cluster.0.Main")
    graph.add_node("Node.100.MainN", coord=(100.0, 2.0), is_endpoint=False, cluster_id="Cluster.100.Main")
    graph.add_node("Node.200.MainN", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200.Main")

    graph.add_edge(
        "Node.0.MainN",
        "Node.100.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.100.MainN",
        "Node.200.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    ratios = {
        "Cluster.100.Main": _make_ratio(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
            },
            "Cluster.100.Main",
        )
    }

    row = EndpointDemandRow(endpoint_id="Node.0.MainN", flow_per_hour=300.0)
    flows = compute_od_flows(graph, ratios, [row])

    assert flows == [("Node.0.MainN", "Node.200.MainN", 300.0, row)]


def test_minor_side_flow_directed_to_east_main_endpoint() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.100.Main"
    origin = minor_endpoint_id(100, "N", PedestrianSide.EAST_SIDE)

    graph.add_node(origin, coord=(100.0, 10.0), is_endpoint=True, cluster_id=junction, side=PedestrianSide.EAST_SIDE)
    graph.add_node("Node.100.MainN", coord=(100.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.200.MainN", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200.Main")

    graph.add_edge(
        origin,
        "Node.100.MainN",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=8.0,
    )
    graph.add_edge(
        "Node.100.MainN",
        "Node.200.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=100.0,
    )

    ratios = {
        junction: _make_ratio(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 2.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id=origin, flow_per_hour=180.0)
    flows = compute_od_flows(graph, ratios, [row])

    assert flows == [(origin, "Node.200.MainN", 180.0, row)]


def test_crosswalk_follows_east_side_ratios() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.150.Main"

    graph.add_node("Node.150.MainN", coord=(150.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.150.MainS", coord=(150.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.200.MainN", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200.Main")
    graph.add_node("Node.200.MainS", coord=(200.0, -2.0), is_endpoint=True, cluster_id="Cluster.200.Main")

    graph.add_edge(
        "Node.150.MainN",
        "Node.200.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.150.MainS",
        "Node.200.MainS",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.150.MainN",
        "Node.150.MainS",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.150.MainN",
        "Node.150.MainS",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )

    ratios = {
        junction: _make_ratio(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
                (CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE): 2.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="Node.150.MainN", flow_per_hour=300.0)
    flows = compute_od_flows(graph, ratios, [row])

    assert sorted((origin, dest, round(value, 6)) for origin, dest, value, _ in flows) == [
        ("Node.150.MainN", "Node.200.MainN", 100.0),
        ("Node.150.MainN", "Node.200.MainS", 200.0),
    ]


def test_crosswalk_follows_west_side_for_north_half() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.150.Main"

    graph.add_node("Node.150.MainN", coord=(150.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.150.MainS", coord=(150.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.200.MainN", coord=(200.0, 2.0), is_endpoint=True, cluster_id="Cluster.200.Main")
    graph.add_node("Node.200.MainS", coord=(200.0, -2.0), is_endpoint=True, cluster_id="Cluster.200.Main")

    graph.add_edge(
        "Node.150.MainN",
        "Node.200.MainN",
        orientation="EW",
        side=PedestrianSide.NORTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.150.MainS",
        "Node.200.MainS",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=50.0,
    )
    graph.add_edge(
        "Node.150.MainN",
        "Node.150.MainS",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.150.MainN",
        "Node.150.MainS",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )

    ratios = {
        junction: _make_ratio(
            {
                (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE): 1.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="Node.150.MainS", flow_per_hour=180.0)
    flows = compute_od_flows(graph, ratios, [row])

    assert flows == [("Node.150.MainS", "Node.200.MainN", 180.0, row)]


def test_crosswalk_handles_multiple_directions_without_conflict() -> None:
    graph = nx.MultiGraph()
    junction = "Cluster.200.Main"

    graph.add_node("Node.200.MainN", coord=(200.0, 2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.200.MainS", coord=(200.0, -2.0), is_endpoint=False, cluster_id=junction)
    graph.add_node("Node.100.MainS", coord=(100.0, -2.0), is_endpoint=True, cluster_id="Cluster.100.Main")
    graph.add_node("Node.300.MainS", coord=(300.0, -2.0), is_endpoint=True, cluster_id="Cluster.300.Main")

    graph.add_edge(
        "Node.200.MainN",
        "Node.200.MainS",
        orientation="NS",
        side=PedestrianSide.EAST_SIDE,
        length=4.0,
    )
    graph.add_edge(
        "Node.200.MainS",
        "Node.300.MainS",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=100.0,
    )
    graph.add_edge(
        "Node.200.MainS",
        "Node.100.MainS",
        orientation="EW",
        side=PedestrianSide.SOUTH_SIDE,
        length=100.0,
    )

    ratios = {
        junction: _make_ratio(
            {
                (CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE): 1.0,
                (CardinalDirection.WEST, PedestrianSide.SOUTH_SIDE): 1.0,
            },
            junction,
        )
    }

    row = EndpointDemandRow(endpoint_id="Node.200.MainN", flow_per_hour=300.0)
    flows = compute_od_flows(graph, ratios, [row])
    summary = {(origin, dest): value for origin, dest, value, _ in flows}

    assert summary[("Node.200.MainN", "Node.300.MainS")] == pytest.approx(150.0)
    assert summary[("Node.200.MainN", "Node.100.MainS")] == pytest.approx(150.0)
