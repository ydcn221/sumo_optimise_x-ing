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
        "Node.100.MainN",
        "Node.100.MainS",
        orientation="NS",
        side=PedestrianSide.WEST_SIDE,
        length=4.0,
    )
    return graph


def _junction_ratio() -> JunctionDirectionRatios:
    weights = {}
    for direction in CardinalDirection:
        for side in PedestrianSide:
            weights[(direction, side)] = 0.0
    weights[(CardinalDirection.SOUTH, PedestrianSide.WEST_SIDE)] = 1.0
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
    assert pytest.approx(value) == 600.0
    assert row_ref is positive_row

    negative_row = EndpointDemandRow(endpoint_id="Node.100.MainS", flow_per_hour=-600.0, row_index=3)
    flows_negative = compute_od_flows(graph, ratios, [negative_row])

    assert len(flows_negative) == 1
    origin, destination, value, row_ref = flows_negative[0]
    assert origin == "Node.0.MainN"
    assert destination == "Node.100.MainS"
    assert pytest.approx(value) == 300.0
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
