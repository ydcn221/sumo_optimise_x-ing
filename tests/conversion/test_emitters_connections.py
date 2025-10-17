from __future__ import annotations
from sumo_optimise.conversion.builder.ids import cluster_id
from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LayoutEvent,
    MainRoadConfig,
    SignalRef,
    SnapRule,
)
from sumo_optimise.conversion.emitters.connections import render_connections_xml


def _make_template() -> JunctionTemplate:
    return JunctionTemplate(
        id="X",
        main_approach_begin_m=0,
        main_approach_lanes=2,
        minor_lanes_to_main=1,
        minor_lanes_from_main=1,
        refuge_island_on_main=False,
        two_stage_ped_crossing_on_main=False,
        median_continuous=False,
        kind=EventKind.CROSS,
    )


def test_render_connections_includes_pedestrian_signal_metadata() -> None:
    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=120, center_gap_m=6.0, lanes=1)
    template = _make_template()
    cluster = Cluster(
        pos_m=60,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=60.0,
                pos_m=60,
                template_id=template.id,
                signalized=True,
                signal=SignalRef(profile_id="profile", offset_s=0),
                main_ped_crossing_placement={"west": True, "east": True},
            )
        ],
    )

    connections_xml, link_indexing = render_connections_xml(
        defaults,
        [cluster],
        [0, 60, 120],
        {template.id: template},
        snap_rule,
        main_road,
        {"EB": [], "WB": []},
    )

    assert "<crossing" in connections_xml
    assert 60 in link_indexing
    index_info = link_indexing[60]
    tl_id = cluster_id(60)
    ped_links = index_info.pedestrian_links
    assert ped_links, "expected pedestrian signal metadata"
    for ped_link in ped_links:
        assert ped_link.tl_id == tl_id
        resolved = index_info.get_pedestrian_link(ped_link.crossing_id)
        assert resolved is not None
        assert resolved == ped_link
        assert any(
            entry.link_index == ped_link.link_index and entry.kind == "pedestrian"
            for entry in index_info.links
        )

    connection_lines = [
        line.strip()
        for line in connections_xml.splitlines()
        if line.strip().startswith("<connection ")
    ]
    assert connection_lines, "expected vehicle connections"
    assert all(" tl=\"" in line and " linkIndex=\"" in line for line in connection_lines)
    for entry in index_info.links:
        if entry.kind != "vehicle":
            continue
        expected_token = f'linkIndex="{entry.link_index}"'
        assert any(expected_token in line for line in connection_lines)

