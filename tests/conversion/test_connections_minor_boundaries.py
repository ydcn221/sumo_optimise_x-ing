from sumo_optimise.conversion.builder.ids import main_edge_id, minor_edge_id
from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LayoutEvent,
    MainRoadConfig,
    SnapRule,
)
from sumo_optimise.conversion.emitters.connections import render_connections_xml


def _build_args(pos: int, *, length: int = 50):
    template_id = "Boundary"
    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=length, center_gap_m=6.0, lanes=2)
    template = JunctionTemplate(
        id=template_id,
        main_approach_begin_m=0,
        main_approach_lanes=0,
        minor_lanes_to_main=1,
        minor_lanes_from_main=1,
        split_ped_crossing_on_main=False,
        median_continuous=False,
        kind=EventKind.CROSS,
    )
    cluster = Cluster(
        pos_m=pos,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=float(pos),
                pos_m=pos,
                template_id=template_id,
            )
        ],
    )
    lane_overrides = {"EB": [], "WB": []}
    breakpoints = [0, length]
    return (
        defaults,
        [cluster],
        breakpoints,
        {template_id: template},
        snap_rule,
        main_road,
        lane_overrides,
    )


def test_minor_south_branch_uses_available_east_segment_at_zero():
    args = _build_args(pos=0, length=60)
    xml = render_connections_xml(*args)

    in_edge = minor_edge_id(0, "to", "S")
    east_edge = main_edge_id("EB", 0, 60)

    assert f'from="{in_edge}" to="{east_edge}"' in xml


def test_minor_north_branch_uses_available_west_segment_at_grid_max():
    length = 80
    args = _build_args(pos=length, length=length)
    xml = render_connections_xml(*args)

    in_edge = minor_edge_id(length, "to", "N")
    west_edge = main_edge_id("WB", 0, length)

    assert f'from="{in_edge}" to="{west_edge}"' in xml
