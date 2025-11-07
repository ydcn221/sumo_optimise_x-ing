from pathlib import Path

from sumo_optimise.conversion.builder.ids import (
    cluster_id,
    crossing_id_main,
    crossing_id_main_split,
    crossing_id_midblock,
    crossing_id_midblock_split,
    main_edge_id,
    minor_edge_id,
)
from sumo_optimise.conversion.demand.catalog import build_endpoint_catalog
from sumo_optimise.conversion.domain.models import PedestrianEndpoint, VehicleEndpoint, PedestrianSide
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
from sumo_optimise.conversion.demand.person_flow.identifier import minor_endpoint_id


def _prepare_inputs():
    spec_path = Path("data/reference/SUMO_OPTX_demo(connection_build)") / "SUMO_OPTX_v1.3_sample.json"
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

    cluster_650 = cluster_id(650)
    cluster_900 = cluster_id(900)
    cluster_1000 = cluster_id(1000)
    expected_vehicle_endpoints = [
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.350.in",
            pos=350,
            category="main_EB",
            edge_id=main_edge_id("EB", 250, 350),
            lane_count=4,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.350.out",
            pos=350,
            category="main_EB",
            edge_id=main_edge_id("EB", 350, 450),
            lane_count=3,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.350.in",
            pos=350,
            category="main_WB",
            edge_id=main_edge_id("WB", 450, 350),
            lane_count=4,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.350.out",
            pos=350,
            category="main_WB",
            edge_id=main_edge_id("WB", 350, 250),
            lane_count=3,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.350.in",
            pos=350,
            category="minor_N",
            edge_id=minor_edge_id(350, "to", "N"),
            lane_count=2,
            is_inbound=True,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.350.out",
            pos=350,
            category="minor_N",
            edge_id=minor_edge_id(350, "from", "N"),
            lane_count=1,
            is_inbound=False,
            tl_id=None,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.650.in",
            pos=650,
            category="main_EB",
            edge_id=main_edge_id("EB", 500, 650),
            lane_count=5,
            is_inbound=True,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.650.out",
            pos=650,
            category="main_EB",
            edge_id=main_edge_id("EB", 650, 780),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.650.in",
            pos=650,
            category="main_WB",
            edge_id=main_edge_id("WB", 780, 650),
            lane_count=5,
            is_inbound=True,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.650.out",
            pos=650,
            category="main_WB",
            edge_id=main_edge_id("WB", 650, 500),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.650.in",
            pos=650,
            category="minor_N",
            edge_id=minor_edge_id(650, "to", "N"),
            lane_count=3,
            is_inbound=True,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.650.out",
            pos=650,
            category="minor_N",
            edge_id=minor_edge_id(650, "from", "N"),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.650.in",
            pos=650,
            category="minor_S",
            edge_id=minor_edge_id(650, "to", "S"),
            lane_count=3,
            is_inbound=True,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.650.out",
            pos=650,
            category="minor_S",
            edge_id=minor_edge_id(650, "from", "S"),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_650,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.900.in",
            pos=900,
            category="main_EB",
            edge_id=main_edge_id("EB", 800, 900),
            lane_count=4,
            is_inbound=True,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.900.out",
            pos=900,
            category="main_EB",
            edge_id=main_edge_id("EB", 900, 920),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.900.in",
            pos=900,
            category="main_WB",
            edge_id=main_edge_id("WB", 920, 900),
            lane_count=4,
            is_inbound=True,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.900.out",
            pos=900,
            category="main_WB",
            edge_id=main_edge_id("WB", 900, 800),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.900.in",
            pos=900,
            category="minor_S",
            edge_id=minor_edge_id(900, "to", "S"),
            lane_count=3,
            is_inbound=True,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.900.out",
            pos=900,
            category="minor_S",
            edge_id=minor_edge_id(900, "from", "S"),
            lane_count=2,
            is_inbound=False,
            tl_id=cluster_900,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.1000.in",
            pos=1000,
            category="main_EB",
            edge_id=main_edge_id("EB", 920, 1000),
            lane_count=4,
            is_inbound=True,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_EB.1000.out",
            pos=1000,
            category="main_EB",
            edge_id=main_edge_id("EB", 1000, 1020),
            lane_count=3,
            is_inbound=False,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.1000.in",
            pos=1000,
            category="main_WB",
            edge_id=main_edge_id("WB", 1020, 1000),
            lane_count=4,
            is_inbound=True,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.main_WB.1000.out",
            pos=1000,
            category="main_WB",
            edge_id=main_edge_id("WB", 1000, 920),
            lane_count=4,
            is_inbound=False,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.1000.in",
            pos=1000,
            category="minor_N",
            edge_id=minor_edge_id(1000, "to", "N"),
            lane_count=2,
            is_inbound=True,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_N.1000.out",
            pos=1000,
            category="minor_N",
            edge_id=minor_edge_id(1000, "from", "N"),
            lane_count=2,
            is_inbound=False,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.1000.in",
            pos=1000,
            category="minor_S",
            edge_id=minor_edge_id(1000, "to", "S"),
            lane_count=2,
            is_inbound=True,
            tl_id=cluster_1000,
        ),
        VehicleEndpoint(
            id="Endpoint.Vehicle.minor_S.1000.out",
            pos=1000,
            category="minor_S",
            edge_id=minor_edge_id(1000, "from", "S"),
            lane_count=2,
            is_inbound=False,
            tl_id=cluster_1000,
        ),
    ]
    cluster_200 = cluster_id(200)
    cluster_350 = cluster_id(350)
    cluster_650 = cluster_id(650)
    cluster_1050 = cluster_id(1050)
    mid_200_n = crossing_id_midblock_split(200, "north")
    mid_200_s = crossing_id_midblock_split(200, "south")
    cross_350_w = crossing_id_main(350, "West")
    cross_650_w_n = crossing_id_main_split(650, "West", "north")
    cross_650_w_s = crossing_id_main_split(650, "West", "south")
    cross_650_e_n = crossing_id_main_split(650, "East", "north")
    cross_650_e_s = crossing_id_main_split(650, "East", "south")
    cross_900_e_n = crossing_id_main_split(900, "East", "north")
    cross_900_e_s = crossing_id_main_split(900, "East", "south")
    cross_1000_e = crossing_id_main(1000, "East")
    mid_1050 = crossing_id_midblock(1050)
    minor_350_n_e = minor_endpoint_id(350, "N", PedestrianSide.EAST_SIDE)
    minor_350_n_w = minor_endpoint_id(350, "N", PedestrianSide.WEST_SIDE)
    minor_650_n_e = minor_endpoint_id(650, "N", PedestrianSide.EAST_SIDE)
    minor_650_n_w = minor_endpoint_id(650, "N", PedestrianSide.WEST_SIDE)
    minor_650_s_e = minor_endpoint_id(650, "S", PedestrianSide.EAST_SIDE)
    minor_650_s_w = minor_endpoint_id(650, "S", PedestrianSide.WEST_SIDE)
    minor_900_s_e = minor_endpoint_id(900, "S", PedestrianSide.EAST_SIDE)
    minor_900_s_w = minor_endpoint_id(900, "S", PedestrianSide.WEST_SIDE)
    minor_1000_n_e = minor_endpoint_id(1000, "N", PedestrianSide.EAST_SIDE)
    minor_1000_n_w = minor_endpoint_id(1000, "N", PedestrianSide.WEST_SIDE)
    minor_1000_s_e = minor_endpoint_id(1000, "S", PedestrianSide.EAST_SIDE)
    minor_1000_s_w = minor_endpoint_id(1000, "S", PedestrianSide.WEST_SIDE)
    expected_pedestrian_endpoints = [
        PedestrianEndpoint(
            id=mid_200_n,
            pos=200,
            movement="ped_mid_north",
            node_id=cluster_200,
            edges=(main_edge_id("EB", 0, 200),),
            width=3.5,
            tl_id=cluster_200,
        ),
        PedestrianEndpoint(
            id=mid_200_s,
            pos=200,
            movement="ped_mid_south",
            node_id=cluster_200,
            edges=(main_edge_id("WB", 200, 0),),
            width=3.5,
            tl_id=cluster_200,
        ),
        PedestrianEndpoint(
            id=minor_350_n_e,
            pos=350,
            movement="ped_minor_north",
            node_id=cluster_350,
            edges=(minor_edge_id(350, "to", "N"), minor_edge_id(350, "from", "N")),
            width=3.5,
            tl_id=None,
        ),
        PedestrianEndpoint(
            id=minor_350_n_w,
            pos=350,
            movement="ped_minor_north",
            node_id=cluster_350,
            edges=(minor_edge_id(350, "to", "N"), minor_edge_id(350, "from", "N")),
            width=3.5,
            tl_id=None,
        ),
        PedestrianEndpoint(
            id=cross_350_w,
            pos=350,
            movement="ped_main_west",
            node_id=cluster_350,
            edges=(main_edge_id("EB", 250, 350), main_edge_id("WB", 350, 250)),
            width=3.5,
            tl_id=None,
        ),
        PedestrianEndpoint(
            id=minor_650_n_e,
            pos=650,
            movement="ped_minor_north",
            node_id=cluster_650,
            edges=(minor_edge_id(650, "to", "N"), minor_edge_id(650, "from", "N")),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=minor_650_n_w,
            pos=650,
            movement="ped_minor_north",
            node_id=cluster_650,
            edges=(minor_edge_id(650, "to", "N"), minor_edge_id(650, "from", "N")),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=minor_650_s_e,
            pos=650,
            movement="ped_minor_south",
            node_id=cluster_650,
            edges=(minor_edge_id(650, "to", "S"), minor_edge_id(650, "from", "S")),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=minor_650_s_w,
            pos=650,
            movement="ped_minor_south",
            node_id=cluster_650,
            edges=(minor_edge_id(650, "to", "S"), minor_edge_id(650, "from", "S")),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=cross_650_w_n,
            pos=650,
            movement="ped_main_west_north",
            node_id=cluster_650,
            edges=(main_edge_id("EB", 500, 650),),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=cross_650_w_s,
            pos=650,
            movement="ped_main_west_south",
            node_id=cluster_650,
            edges=(main_edge_id("WB", 650, 500),),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=cross_650_e_n,
            pos=650,
            movement="ped_main_east_north",
            node_id=cluster_650,
            edges=(main_edge_id("EB", 650, 780),),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=cross_650_e_s,
            pos=650,
            movement="ped_main_east_south",
            node_id=cluster_650,
            edges=(main_edge_id("WB", 780, 650),),
            width=3.5,
            tl_id=cluster_650,
        ),
        PedestrianEndpoint(
            id=minor_900_s_e,
            pos=900,
            movement="ped_minor_south",
            node_id=cluster_900,
            edges=(minor_edge_id(900, "to", "S"), minor_edge_id(900, "from", "S")),
            width=3.5,
            tl_id=cluster_900,
        ),
        PedestrianEndpoint(
            id=minor_900_s_w,
            pos=900,
            movement="ped_minor_south",
            node_id=cluster_900,
            edges=(minor_edge_id(900, "to", "S"), minor_edge_id(900, "from", "S")),
            width=3.5,
            tl_id=cluster_900,
        ),
        PedestrianEndpoint(
            id=cross_900_e_n,
            pos=900,
            movement="ped_main_east_north",
            node_id=cluster_900,
            edges=(main_edge_id("EB", 900, 920),),
            width=3.5,
            tl_id=cluster_900,
        ),
        PedestrianEndpoint(
            id=cross_900_e_s,
            pos=900,
            movement="ped_main_east_south",
            node_id=cluster_900,
            edges=(main_edge_id("WB", 920, 900),),
            width=3.5,
            tl_id=cluster_900,
        ),
        PedestrianEndpoint(
            id=minor_1000_n_e,
            pos=1000,
            movement="ped_minor_north",
            node_id=cluster_1000,
            edges=(minor_edge_id(1000, "to", "N"), minor_edge_id(1000, "from", "N")),
            width=3.5,
            tl_id=cluster_1000,
        ),
        PedestrianEndpoint(
            id=minor_1000_n_w,
            pos=1000,
            movement="ped_minor_north",
            node_id=cluster_1000,
            edges=(minor_edge_id(1000, "to", "N"), minor_edge_id(1000, "from", "N")),
            width=3.5,
            tl_id=cluster_1000,
        ),
        PedestrianEndpoint(
            id=minor_1000_s_e,
            pos=1000,
            movement="ped_minor_south",
            node_id=cluster_1000,
            edges=(minor_edge_id(1000, "to", "S"), minor_edge_id(1000, "from", "S")),
            width=3.5,
            tl_id=cluster_1000,
        ),
        PedestrianEndpoint(
            id=minor_1000_s_w,
            pos=1000,
            movement="ped_minor_south",
            node_id=cluster_1000,
            edges=(minor_edge_id(1000, "to", "S"), minor_edge_id(1000, "from", "S")),
            width=3.5,
            tl_id=cluster_1000,
        ),
        PedestrianEndpoint(
            id=cross_1000_e,
            pos=1000,
            movement="ped_main_east",
            node_id=cluster_1000,
            edges=(main_edge_id("EB", 1000, 1020), main_edge_id("WB", 1020, 1000)),
            width=3.5,
            tl_id=cluster_1000,
        ),
        PedestrianEndpoint(
            id=mid_1050,
            pos=1050,
            movement="ped_mid",
            node_id=cluster_1050,
            edges=(main_edge_id("EB", 1020, 1050), main_edge_id("WB", 1050, 1020)),
            width=3.5,
            tl_id=None,
        ),
    ]
    assert catalog_first.vehicle_endpoints == expected_vehicle_endpoints
    assert catalog_first.pedestrian_endpoints == expected_pedestrian_endpoints
