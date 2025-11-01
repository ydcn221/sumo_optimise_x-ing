from io import StringIO

import pytest

from sumo_optimise.conversion.demand.parser import (
    parse_demand,
    parse_pedestrian_demand,
    parse_vehicle_demand,
)
from sumo_optimise.conversion.domain.models import (
    DemandInput,
    DirectionMain,
    EndpointCatalog,
    PedestrianEndpoint,
    PedestrianRateKind,
    PedestrianSegmentKind,
    VehicleEndpoint,
)
from sumo_optimise.conversion.utils.errors import DemandValidationError


def _build_catalog() -> EndpointCatalog:
    vehicle_endpoints = [
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.100.in",
            pos=100,
            category="main_EB",
            edge_id="Edge.Main.EB.50-100",
            lane_count=2,
            is_inbound=True,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.100.out",
            pos=100,
            category="main_WB",
            edge_id="Edge.Main.WB.150-100",
            lane_count=2,
            is_inbound=False,
        ),
    ]

    pedestrian_endpoints = [
        PedestrianEndpoint(
            id="Cross.0.W.N",
            pos=0,
            movement="ped_main_west_EB",
            node_id="Cluster.0.Main",
            edges=("Edge.Main.EB.-50-0",),
            width=3.5,
        ),
        PedestrianEndpoint(
            id="Cross.0.W.S",
            pos=0,
            movement="ped_main_west_WB",
            node_id="Cluster.0.Main",
            edges=("Edge.Main.WB.0--50",),
            width=3.5,
        ),
        PedestrianEndpoint(
            id="Cross.50.W.N",
            pos=50,
            movement="ped_main_west_EB",
            node_id="Cluster.50.Main",
            edges=("Edge.Main.EB.0-50",),
            width=3.5,
        ),
        PedestrianEndpoint(
            id="Cross.50.W.S",
            pos=50,
            movement="ped_main_west_WB",
            node_id="Cluster.50.Main",
            edges=("Edge.Main.WB.50-0",),
            width=3.5,
        ),
        PedestrianEndpoint(
            id="Cross.100.W.N",
            pos=100,
            movement="ped_main_west_EB",
            node_id="Cluster.100.Main",
            edges=("Edge.Main.EB.50-100",),
            width=3.5,
        ),
        PedestrianEndpoint(
            id="Cross.100.W.S",
            pos=100,
            movement="ped_main_west_WB",
            node_id="Cluster.100.Main",
            edges=("Edge.Main.WB.100-50",),
            width=3.5,
        ),
    ]

    return EndpointCatalog(
        vehicle_endpoints=vehicle_endpoints,
        pedestrian_endpoints=pedestrian_endpoints,
    )


def test_parse_demand_happy_path() -> None:
    catalog = _build_catalog()

    vehicle_csv = StringIO(
        """endpoint_id,generated_veh_per_h,attracted_veh_per_h\n"
        "Endpoint.Vehicle.main_EB.100.in,120,15\n"
        "Endpoint.Vehicle.main_WB.100.out,0,80\n"
        """
    )

    header = (
        "endpoint_id,location_id,position_m,start_m,end_m,"
        "generated_peds_per_h,attracted_peds_per_h,"
        "generated_peds_per_h_per_m,attracted_peds_per_h_per_m\n"
    )
    rows = [
        ",".join(
            [
                "Cross.50.W.N",
                "",
                "",
                "",
                "",
                "20.5",
                "10",
                "",
                "",
            ]
        ),
        ",".join(
            [
                "",
                "Walk.Main.EB.P050",
                "50",
                "",
                "",
                "40",
                "5",
                "",
                "",
            ]
        ),
        ",".join(
            [
                "",
                "Walk.Main.WB.R000-100",
                "",
                "0",
                "100",
                "",
                "",
                "1.5",
                "0.5",
            ]
        ),
    ]
    pedestrian_csv = StringIO(header + "\n".join(rows) + "\n")

    demand = parse_demand(
        vehicle_source=vehicle_csv,
        pedestrian_source=pedestrian_csv,
        catalog=catalog,
    )

    assert isinstance(demand, DemandInput)
    assert [segment.endpoint_id for segment in demand.vehicles] == [
        "Endpoint.Vehicle.main_EB.100.in",
        "Endpoint.Vehicle.main_WB.100.out",
    ]
    assert demand.vehicles[0].departures_per_hour == 120
    assert demand.vehicles[1].arrivals_per_hour == 80

    assert len(demand.pedestrians) == 3
    first, second, third = demand.pedestrians

    assert first.kind is PedestrianSegmentKind.ENDPOINT
    assert first.endpoint_id == "Cross.50.W.N"
    assert first.rate_kind is PedestrianRateKind.ABSOLUTE

    assert second.kind is PedestrianSegmentKind.POSITION
    assert second.endpoint_id == "Cross.50.W.N"
    assert second.side is DirectionMain.EB
    assert second.departures == 40

    assert third.kind is PedestrianSegmentKind.RANGE
    assert third.rate_kind is PedestrianRateKind.PER_METER
    assert third.side is DirectionMain.WB
    assert third.start_m == 0
    assert third.end_m == 100


def test_vehicle_unknown_endpoint_reports_error() -> None:
    catalog = _build_catalog()

    vehicle_csv = StringIO(
        """endpoint_id,generated_veh_per_h,attracted_veh_per_h\n"
        "Unknown,10,10\n"
        """
    )

    with pytest.raises(DemandValidationError) as excinfo:
        parse_vehicle_demand(vehicle_csv, catalog)

    message = str(excinfo.value)
    assert "vehicle demand CSV errors were found" in message
    assert "vehicle endpoint 'Unknown'" in message


def test_pedestrian_point_without_match_reports_error() -> None:
    catalog = _build_catalog()

    pedestrian_csv = StringIO(
        """location_id,generated_peds_per_h,attracted_peds_per_h\n"
        "Walk.Main.EB.P075,10,10\n"
        """
    )

    with pytest.raises(DemandValidationError) as excinfo:
        parse_pedestrian_demand(pedestrian_csv, catalog)

    message = str(excinfo.value)
    assert "pedestrian demand CSV errors were found" in message
    assert "no pedestrian endpoint on side EB at position 75" in message


def test_pedestrian_range_out_of_bounds_reports_error() -> None:
    catalog = _build_catalog()

    pedestrian_csv = StringIO(
        """location_id,start_m,end_m,generated_peds_per_h_per_m,attracted_peds_per_h_per_m\n"
        "Walk.Main.EB.R000-200,0,200,1,1\n"
        """
    )

    with pytest.raises(DemandValidationError) as excinfo:
        parse_pedestrian_demand(pedestrian_csv, catalog)

    message = str(excinfo.value)
    assert "pedestrian demand CSV errors were found" in message
    assert "exceeds known endpoints on side EB" in message


def test_pedestrian_row_must_choose_single_layout() -> None:
    catalog = _build_catalog()

    pedestrian_csv = StringIO(
        """endpoint_id,location_id,generated_peds_per_h,attracted_peds_per_h\n"
        "Cross.50.W.N,Walk.Main.EB.P050,10,10\n"
        """
    )

    with pytest.raises(DemandValidationError) as excinfo:
        parse_pedestrian_demand(pedestrian_csv, catalog)

    message = str(excinfo.value)
    assert "specify exactly one" in message
