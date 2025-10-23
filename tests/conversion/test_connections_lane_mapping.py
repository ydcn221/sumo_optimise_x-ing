from xml.etree import ElementTree as ET

import sumo_optimise.conversion.emitters.connections as mod
from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    LayoutEvent,
    MainRoadConfig,
    SignalRef,
    SnapRule,
)


def extract_connections(lines: list[str], to_edge: str) -> set[str]:
    return {
        line.strip()
        for line in lines
        if f'to="{to_edge}"' in line
    }


def test_straight_fans_out_to_rightmost_targets():
    collector = mod.LinkEmissionCollector()
    emitted = mod._emit_vehicle_connections_for_approach(
        collector,
        pos=0,
        in_edge_id="Edge.In",
        s_count=2,
        L_target=None,
        T_target=("Edge.Straight", 4),
        R_target=None,
        U_target=None,
        tl_id="TestTL",
        movement_prefix="approach",
    )

    lines, metadata = collector.finalize()
    assert emitted == 4
    assert extract_connections(lines, "Edge.Straight") == {
        '<connection from="Edge.In" to="Edge.Straight" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="3"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="4"/>',
    }
    assert [link.link_index for link in metadata] == [0, 1, 2, 3]
    assert {link.tl_id for link in metadata} == {"TestTL"}


def test_left_turns_share_last_target_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["L", "L", "L", "L"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    collector = mod.LinkEmissionCollector()
    emitted = mod._emit_vehicle_connections_for_approach(
        collector,
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=("Edge.Left", 2),
        T_target=None,
        R_target=None,
        U_target=None,
        tl_id="TestTL",
        movement_prefix="approach",
    )

    lines, metadata = collector.finalize()
    assert emitted == 4
    assert extract_connections(lines, "Edge.Left") == {
        '<connection from="Edge.In" to="Edge.Left" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="3" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="4" toLane="2"/>',
    }
    assert [link.link_index for link in metadata] == [0, 1, 2, 3]
    assert {link.tl_id for link in metadata} == {"TestTL"}


def test_right_turns_share_outer_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["R", "R", "R", "R"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    collector = mod.LinkEmissionCollector()
    emitted = mod._emit_vehicle_connections_for_approach(
        collector,
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=None,
        T_target=None,
        R_target=("Edge.Right", 2),
        U_target=None,
        tl_id="TestTL",
        movement_prefix="approach",
    )

    lines, metadata = collector.finalize()
    assert emitted == 4
    assert extract_connections(lines, "Edge.Right") == {
        '<connection from="Edge.In" to="Edge.Right" fromLane="4" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="3" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="1" toLane="2"/>',
    }
    assert [link.link_index for link in metadata] == [0, 1, 2, 3]
    assert {link.tl_id for link in metadata} == {"TestTL"}


def test_crossing_link_index_offsets_after_vehicle_links():
    collector = mod.LinkEmissionCollector()
    collector.add_connection(
        from_edge="Edge.In",
        to_edge="Edge.Out",
        from_lane=1,
        to_lane=1,
        movement="veh_0",
        tl_id="TestTL",
    )
    collector.add_connection(
        from_edge="Edge.In",
        to_edge="Edge.Diverge",
        from_lane=2,
        to_lane=1,
        movement="veh_1",
        tl_id="TestTL",
    )
    collector.add_crossing(
        crossing_id="Cross.0",
        node_id="Cluster.Main.0",
        edges="Edge.A Edge.B",
        width=4.0,
        movement="ped_minor",
        tl_id="TestTL",
    )

    lines, metadata = collector.finalize()

    ped_line = next(line for line in lines if line.strip().startswith("<crossing"))
    assert 'linkIndex="2"' in ped_line

    tl_links = [link for link in metadata if link.tl_id == "TestTL"]
    assert [link.kind for link in tl_links] == ["connection", "connection", "crossing"]
    assert [link.link_index for link in tl_links] == [0, 1, 2]
    assert [link.slot_index for link in tl_links] == [0, 1, 2]


def test_midblock_crossings_align_after_mainline_connections():
    defaults = Defaults(minor_road_length_m=25, ped_crossing_width_m=3.5, speed_kmh=40)
    main_road = MainRoadConfig(length_m=400.0, center_gap_m=0.0, lanes=3)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    cluster = Cluster(
        pos_m=200,
        events=[
            LayoutEvent(
                type=EventKind.XWALK_MIDBLOCK,
                pos_m_raw=200.0,
                pos_m=200,
                signalized=True,
                signal=SignalRef(profile_id="mid", offset_s=0),
                refuge_island_on_main=True,
            )
        ],
    )

    result = mod.render_connections_xml(
        defaults=defaults,
        clusters=[cluster],
        breakpoints=[0, 200, 400],
        junction_template_by_id={},
        snap_rule=snap_rule,
        main_road=main_road,
        lane_overrides={"EB": [], "WB": []},
    )

    root = ET.fromstring(result.xml)
    tl_id = "Cluster.Main.200"

    crossing_indexes = sorted(
        int(elem.get("linkIndex"))
        for elem in root.findall("crossing")
        if elem.get("tl") == tl_id
    )
    assert crossing_indexes == [6, 7]

    connections = [elem for elem in root.findall("connection")]
    assert len(connections) == 6

    tl_links = [link for link in result.links if link.tl_id == tl_id]
    assert [link.kind for link in tl_links] == ["connection"] * 6 + ["crossing"] * 2
    assert [link.link_index for link in tl_links] == list(range(8))

