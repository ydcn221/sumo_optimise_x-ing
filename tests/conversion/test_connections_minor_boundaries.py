from typing import Optional

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


def _build_args(
    pos: int,
    *,
    length: int = 50,
    median: bool = False,
    lanes: int = 1,
    breakpoints: Optional[list[int]] = None,
):
    template_id = "Boundary"
    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=length, center_gap_m=6.0, lanes=lanes)
    template = JunctionTemplate(
        id=template_id,
        main_approach_begin_m=0,
        main_approach_lanes=0,
        minor_lanes_to_main=1,
        minor_lanes_from_main=1,
        split_ped_crossing_on_main=False,
        median_continuous=median,
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
    if breakpoints is None:
        bp = [0, length]
        if 0 < pos < length:
            bp.insert(1, pos)
    else:
        bp = breakpoints
    return (
        defaults,
        [cluster],
        bp,
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


def test_minor_straight_between_branches_is_emitted():
    args = _build_args(pos=30, length=60)
    xml = render_connections_xml(*args)

    in_edge = minor_edge_id(30, "to", "N")
    straight_edge = minor_edge_id(30, "from", "S")

    assert f'from="{in_edge}" to="{straight_edge}"' in xml


def test_minor_straight_removed_when_median_continuous():
    args = _build_args(pos=30, length=60, median=True)
    xml = render_connections_xml(*args)

    in_edge = minor_edge_id(30, "to", "N")
    straight_edge = minor_edge_id(30, "from", "S")
    left_edge = main_edge_id("EB", 30, 60)

    assert f'from="{in_edge}" to="{straight_edge}"' not in xml
    assert f'from="{in_edge}" to="{left_edge}"' in xml
