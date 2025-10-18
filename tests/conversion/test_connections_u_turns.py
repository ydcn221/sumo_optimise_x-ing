from sumo_optimise.conversion.builder.ids import main_edge_id
from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    LayoutEvent,
    MainRoadConfig,
    SnapRule,
)
from sumo_optimise.conversion.emitters.connections import render_connections_xml


def _build_args(median: bool = False):
    template_id = "X_UTurn_Median" if median else "X_UTurn_Base"
    defaults = Defaults(minor_road_length_m=80, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=200, center_gap_m=6.0, lanes=2)
    lane_overrides = {
        "EB": [LaneOverride(start=0, end=100, lanes=4)],
        "WB": [LaneOverride(start=100, end=200, lanes=4)],
    }
    template = JunctionTemplate(
        id=template_id,
        main_approach_begin_m=0,
        main_approach_lanes=0,
        minor_lanes_to_main=1,
        minor_lanes_from_main=1,
        median_continuous=median,
        kind=EventKind.CROSS,
    )
    cluster = Cluster(
        pos_m=100,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=100.0,
                pos_m=100,
                template_id=template_id,
            )
        ],
    )
    return (
        defaults,
        [cluster],
        [0, 100, 200],
        {template_id: template},
        snap_rule,
        main_road,
        lane_overrides,
    )


def test_render_connections_emits_u_turn_links_for_main_approach():
    args = _build_args(median=False)
    xml = render_connections_xml(*args)

    eb_in = main_edge_id("EB", 0, 100)
    wb_back = main_edge_id("WB", 0, 100)
    wb_in = main_edge_id("WB", 100, 200)
    eb_back = main_edge_id("EB", 100, 200)

    assert (
        f'  <connection from="{eb_in}" to="{wb_back}" fromLane="4" toLane="2"/>'
        in xml
    )
    assert (
        f'  <connection from="{wb_in}" to="{eb_back}" fromLane="4" toLane="2"/>'
        in xml
    )


def test_render_connections_suppresses_u_turn_when_median_continuous():
    args = _build_args(median=True)
    xml = render_connections_xml(*args)

    assert 'from="Edge.Main.EB.0-100" to="Edge.Main.WB.0-100"' not in xml
    assert 'from="Edge.Main.WB.100-200" to="Edge.Main.EB.100-200"' not in xml
