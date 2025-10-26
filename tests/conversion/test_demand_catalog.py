from pathlib import Path

from sumo_optimise.conversion.demand.catalog import build_endpoint_catalog
from sumo_optimise.conversion.domain.models import PedestrianEndpoint, VehicleEndpoint
from sumo_optimise.conversion.parser.spec_loader import (
    build_clusters,
    load_json_file,
    load_schema_file,
    parse_defaults,
    parse_junction_templates,
    parse_layout_events,
    parse_main_road,
    parse_snap_rule,
    validate_json_schema,
)
from sumo_optimise.conversion.planner.lanes import (
    collect_breakpoints_and_reasons,
    compute_lane_overrides,
)


def _prepare_inputs():
    spec_path = Path("data/reference/SUMO_OPTX_v1.3_sample.json")
    schema_path = Path("src/sumo_optimise/conversion/data/schema.json")

    spec_json = load_json_file(spec_path)
    schema_json = load_schema_file(schema_path)
    validate_json_schema(spec_json, schema_json)

    snap_rule = parse_snap_rule(spec_json)
    defaults = parse_defaults(spec_json)
    main_road = parse_main_road(spec_json)
    junction_template_by_id = parse_junction_templates(spec_json)
    layout_events = parse_layout_events(spec_json, snap_rule, main_road)
    clusters = build_clusters(layout_events)
    lane_overrides = compute_lane_overrides(
        main_road, clusters, junction_template_by_id, snap_rule
    )
    breakpoints, _ = collect_breakpoints_and_reasons(
        main_road, clusters, lane_overrides, snap_rule
    )
    return (
        defaults,
        main_road,
        clusters,
        breakpoints,
        junction_template_by_id,
        lane_overrides,
        snap_rule,
    )


def test_endpoint_catalog_is_deterministic_and_matches_expected():
    (
        defaults,
        main_road,
        clusters,
        breakpoints,
        junction_template_by_id,
        lane_overrides,
        snap_rule,
    ) = _prepare_inputs()

    catalog_first = build_endpoint_catalog(
        defaults=defaults,
        main_road=main_road,
        clusters=clusters,
        breakpoints=breakpoints,
        junction_template_by_id=junction_template_by_id,
        lane_overrides=lane_overrides,
        snap_rule=snap_rule,
    )
    catalog_second = build_endpoint_catalog(
        defaults=defaults,
        main_road=main_road,
        clusters=clusters,
        breakpoints=breakpoints,
        junction_template_by_id=junction_template_by_id,
        lane_overrides=lane_overrides,
        snap_rule=snap_rule,
    )

    assert catalog_first.vehicle_endpoints == catalog_second.vehicle_endpoints
    assert catalog_first.pedestrian_endpoints == catalog_second.pedestrian_endpoints

    expected_vehicle_endpoints = [
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.350.in",
            pos=350,
            category="main_EB",
            edge_id="Edge.Main.EB.250-350",
            lane_count=4,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.350.out",
            pos=350,
            category="main_EB",
            edge_id="Edge.Main.EB.350-450",
            lane_count=3,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.350.in",
            pos=350,
            category="main_WB",
            edge_id="Edge.Main.WB.350-450",
            lane_count=4,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.350.out",
            pos=350,
            category="main_WB",
            edge_id="Edge.Main.WB.250-350",
            lane_count=3,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.350.in",
            pos=350,
            category="minor_N",
            edge_id="Edge.Minor.350.to.N",
            lane_count=2,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.350.out",
            pos=350,
            category="minor_N",
            edge_id="Edge.Minor.350.from.N",
            lane_count=1,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.650.in",
            pos=650,
            category="main_EB",
            edge_id="Edge.Main.EB.500-650",
            lane_count=5,
            is_inbound=True,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.650.out",
            pos=650,
            category="main_EB",
            edge_id="Edge.Main.EB.650-780",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.650.in",
            pos=650,
            category="main_WB",
            edge_id="Edge.Main.WB.650-780",
            lane_count=5,
            is_inbound=True,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.650.out",
            pos=650,
            category="main_WB",
            edge_id="Edge.Main.WB.500-650",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.650.in",
            pos=650,
            category="minor_N",
            edge_id="Edge.Minor.650.to.N",
            lane_count=3,
            is_inbound=True,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.650.out",
            pos=650,
            category="minor_N",
            edge_id="Edge.Minor.650.from.N",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.650.in",
            pos=650,
            category="minor_S",
            edge_id="Edge.Minor.650.to.S",
            lane_count=3,
            is_inbound=True,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.650.out",
            pos=650,
            category="minor_S",
            edge_id="Edge.Minor.650.from.S",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.650",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.900.in",
            pos=900,
            category="main_EB",
            edge_id="Edge.Main.EB.800-900",
            lane_count=4,
            is_inbound=True,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.900.out",
            pos=900,
            category="main_EB",
            edge_id="Edge.Main.EB.900-920",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.900.in",
            pos=900,
            category="main_WB",
            edge_id="Edge.Main.WB.900-920",
            lane_count=4,
            is_inbound=True,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.900.out",
            pos=900,
            category="main_WB",
            edge_id="Edge.Main.WB.800-900",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.900.in",
            pos=900,
            category="minor_S",
            edge_id="Edge.Minor.900.to.S",
            lane_count=3,
            is_inbound=True,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.900.out",
            pos=900,
            category="minor_S",
            edge_id="Edge.Minor.900.from.S",
            lane_count=2,
            is_inbound=False,
            tl_id="Cluster.Main.900",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.1000.in",
            pos=1000,
            category="main_EB",
            edge_id="Edge.Main.EB.920-1000",
            lane_count=4,
            is_inbound=True,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.1000.out",
            pos=1000,
            category="main_EB",
            edge_id="Edge.Main.EB.1000-1020",
            lane_count=3,
            is_inbound=False,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.1000.in",
            pos=1000,
            category="main_WB",
            edge_id="Edge.Main.WB.1000-1020",
            lane_count=4,
            is_inbound=True,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.1000.out",
            pos=1000,
            category="main_WB",
            edge_id="Edge.Main.WB.920-1000",
            lane_count=4,
            is_inbound=False,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.1000.in",
            pos=1000,
            category="minor_N",
            edge_id="Edge.Minor.1000.to.N",
            lane_count=2,
            is_inbound=True,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.1000.out",
            pos=1000,
            category="minor_N",
            edge_id="Edge.Minor.1000.from.N",
            lane_count=2,
            is_inbound=False,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.1000.in",
            pos=1000,
            category="minor_S",
            edge_id="Edge.Minor.1000.to.S",
            lane_count=2,
            is_inbound=True,
            tl_id="Cluster.Main.1000",
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.1000.out",
            pos=1000,
            category="minor_S",
            edge_id="Edge.Minor.1000.from.S",
            lane_count=2,
            is_inbound=False,
            tl_id="Cluster.Main.1000",
        ),
    ]

    expected_pedestrian_endpoints = [
        PedestrianEndpoint(
            id="Cross.Mid.200.EB",
            pos=200,
            movement="ped_mid_EB",
            node_id="Cluster.Main.200",
            edges=("Edge.Main.EB.0-200",),
            width=3.5,
            tl_id="Cluster.Main.200",
        ),
        PedestrianEndpoint(
            id="Cross.Mid.200.WB",
            pos=200,
            movement="ped_mid_WB",
            node_id="Cluster.Main.200",
            edges=("Edge.Main.WB.0-200",),
            width=3.5,
            tl_id="Cluster.Main.200",
        ),
        PedestrianEndpoint(
            id="Cross.Minor.350.N",
            pos=350,
            movement="ped_minor_north",
            node_id="Cluster.Main.350",
            edges=("Edge.Minor.350.to.N", "Edge.Minor.350.from.N"),
            width=3.5,
            tl_id=None,
        ),
        PedestrianEndpoint(
            id="Cross.Main.350.West",
            pos=350,
            movement="ped_main_west",
            node_id="Cluster.Main.350",
            edges=("Edge.Main.EB.250-350", "Edge.Main.WB.250-350"),
            width=3.5,
            tl_id=None,
        ),
        PedestrianEndpoint(
            id="Cross.Minor.650.N",
            pos=650,
            movement="ped_minor_north",
            node_id="Cluster.Main.650",
            edges=("Edge.Minor.650.to.N", "Edge.Minor.650.from.N"),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Minor.650.S",
            pos=650,
            movement="ped_minor_south",
            node_id="Cluster.Main.650",
            edges=("Edge.Minor.650.to.S", "Edge.Minor.650.from.S"),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Main.650.West.EB",
            pos=650,
            movement="ped_main_west_EB",
            node_id="Cluster.Main.650",
            edges=("Edge.Main.EB.500-650",),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Main.650.West.WB",
            pos=650,
            movement="ped_main_west_WB",
            node_id="Cluster.Main.650",
            edges=("Edge.Main.WB.500-650",),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Main.650.East.EB",
            pos=650,
            movement="ped_main_east_EB",
            node_id="Cluster.Main.650",
            edges=("Edge.Main.EB.650-780",),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Main.650.East.WB",
            pos=650,
            movement="ped_main_east_WB",
            node_id="Cluster.Main.650",
            edges=("Edge.Main.WB.650-780",),
            width=3.5,
            tl_id="Cluster.Main.650",
        ),
        PedestrianEndpoint(
            id="Cross.Minor.900.S",
            pos=900,
            movement="ped_minor_south",
            node_id="Cluster.Main.900",
            edges=("Edge.Minor.900.to.S", "Edge.Minor.900.from.S"),
            width=3.5,
            tl_id="Cluster.Main.900",
        ),
        PedestrianEndpoint(
            id="Cross.Main.900.East.EB",
            pos=900,
            movement="ped_main_east_EB",
            node_id="Cluster.Main.900",
            edges=("Edge.Main.EB.900-920",),
            width=3.5,
            tl_id="Cluster.Main.900",
        ),
        PedestrianEndpoint(
            id="Cross.Main.900.East.WB",
            pos=900,
            movement="ped_main_east_WB",
            node_id="Cluster.Main.900",
            edges=("Edge.Main.WB.900-920",),
            width=3.5,
            tl_id="Cluster.Main.900",
        ),
        PedestrianEndpoint(
            id="Cross.Minor.1000.N",
            pos=1000,
            movement="ped_minor_north",
            node_id="Cluster.Main.1000",
            edges=("Edge.Minor.1000.to.N", "Edge.Minor.1000.from.N"),
            width=3.5,
            tl_id="Cluster.Main.1000",
        ),
        PedestrianEndpoint(
            id="Cross.Minor.1000.S",
            pos=1000,
            movement="ped_minor_south",
            node_id="Cluster.Main.1000",
            edges=("Edge.Minor.1000.to.S", "Edge.Minor.1000.from.S"),
            width=3.5,
            tl_id="Cluster.Main.1000",
        ),
        PedestrianEndpoint(
            id="Cross.Main.1000.East",
            pos=1000,
            movement="ped_main_east",
            node_id="Cluster.Main.1000",
            edges=("Edge.Main.EB.1000-1020", "Edge.Main.WB.1000-1020"),
            width=3.5,
            tl_id="Cluster.Main.1000",
        ),
        PedestrianEndpoint(
            id="Cross.Mid.1050",
            pos=1050,
            movement="ped_mid",
            node_id="Cluster.Main.1050",
            edges=("Edge.Main.EB.1020-1050", "Edge.Main.WB.1020-1050"),
            width=3.5,
            tl_id=None,
        ),
    ]

    assert catalog_first.vehicle_endpoints == expected_vehicle_endpoints
    assert catalog_first.pedestrian_endpoints == expected_pedestrian_endpoints
