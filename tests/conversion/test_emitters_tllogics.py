from __future__ import annotations

from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
    LayoutEvent,
    MainRoadConfig,
    PedestrianConflictConfig,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
    SnapRule,
)
from sumo_optimise.conversion.emitters.connections import render_connections_xml
from sumo_optimise.conversion.emitters.tllogics import render_tllogics_xml


def _build_signal_cluster(pos: int, profile_id: str, offset: int) -> Cluster:
    return Cluster(
        pos_m=pos,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=float(pos),
                pos_m=pos,
                template_id="X",
                signalized=True,
                signal=SignalRef(profile_id=profile_id, offset_s=offset),
                main_ped_crossing_placement={"west": True, "east": True},
            )
        ],
    )


def test_render_tllogics_emits_state_strings_and_link_indices(monkeypatch) -> None:
    def fake_allocate(s: int, l: int, t: int, r: int, u: int) -> list[str]:
        labels: list[str] = []
        labels.extend(["L"] * min(l, s))
        while len(labels) < s and t > 0:
            labels.append("T")
            t -= 1
        while len(labels) < s and r > 0:
            labels.append("R")
            r -= 1
        while len(labels) < s:
            labels.append("")
        if u > 0 and labels:
            labels[-1] = labels[-1] + "U"
        return labels

    monkeypatch.setattr(
        "sumo_optimise.conversion.emitters.connections.allocate_lanes",
        fake_allocate,
    )
    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=120, center_gap_m=6.0, lanes=2)
    template = JunctionTemplate(
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
    clusters = [_build_signal_cluster(60, "cross_profile", 15)]
    breakpoints = [0, 60, 120]
    lane_overrides = {
        "EB": [LaneOverride(start=0, end=60, lanes=2)],
        "WB": [LaneOverride(start=60, end=120, lanes=2)],
    }

    connections_xml, link_indexing = render_connections_xml(
        defaults,
        clusters,
        breakpoints,
        {"X": template},
        snap_rule,
        main_road,
        lane_overrides,
    )

    assert "<connections>" in connections_xml
    assert 60 in link_indexing
    index_info = link_indexing[60]
    ped_indices = [entry.link_index for entry in index_info.links if entry.kind == "pedestrian"]
    assert ped_indices, "expected at least one pedestrian link"

    profile = SignalProfileDef(
        id="cross_profile",
        cycle_s=60,
        ped_red_offset_s=5,
        yellow_duration_s=3,
        phases=[
            SignalPhaseDef(duration_s=15, allow_movements=["pedestrian"]),
            SignalPhaseDef(duration_s=15, allow_movements=["main_L"]),
            SignalPhaseDef(duration_s=15, allow_movements=["main_R", "pedestrian"]),
            SignalPhaseDef(duration_s=15, allow_movements=["minor_T"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=False),
    )

    xml = render_tllogics_xml(
        clusters,
        {EventKind.CROSS.value: {profile.id: profile}},
        link_indexing,
    )

    states = [
        line.split('state="')[1].split('"')[0]
        for line in xml.splitlines()
        if line.strip().startswith("<phase ")
    ]
    assert states, "expected phases to be emitted"
    state_length = len(states[0])
    assert state_length == len(index_info.links)

    ped_index = ped_indices[0]
    # Pedestrian only phase should be green at the pedestrian index.
    assert states[0][ped_index] == "g"
    # Phase with main right turn should force pedestrian red due to conflict setting.
    assert states[2][ped_index] == "r"

    connection_lines = [line for line in xml.splitlines() if line.strip().startswith("<connection ")]
    assert connection_lines, "expected connection entries in net.tll.xml"
    assert all("linkIndex" in line and "tl=" in line for line in connection_lines)


def test_two_stage_crossing_allows_opposite_half_green(monkeypatch) -> None:
    def fake_allocate(s: int, l: int, t: int, r: int, u: int) -> list[str]:
        labels: list[str] = []
        labels.extend(["L"] * min(l, s))
        while len(labels) < s and t > 0:
            labels.append("T")
            t -= 1
        while len(labels) < s and r > 0:
            labels.append("R")
            r -= 1
        while len(labels) < s:
            labels.append("")
        if u > 0 and labels:
            labels[-1] = labels[-1] + "U"
        return labels

    monkeypatch.setattr(
        "sumo_optimise.conversion.emitters.connections.allocate_lanes",
        fake_allocate,
    )

    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap_rule = SnapRule(step_m=10, tie_break="toward_west")
    main_road = MainRoadConfig(length_m=120, center_gap_m=6.0, lanes=2)
    template = JunctionTemplate(
        id="X2",
        main_approach_begin_m=0,
        main_approach_lanes=2,
        minor_lanes_to_main=1,
        minor_lanes_from_main=1,
        refuge_island_on_main=True,
        two_stage_ped_crossing_on_main=True,
        median_continuous=False,
        kind=EventKind.CROSS,
    )
    cluster = Cluster(
        pos_m=60,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=60.0,
                pos_m=60,
                template_id="X2",
                signalized=True,
                signal=SignalRef(profile_id="two_stage_profile", offset_s=0),
                main_ped_crossing_placement={"west": True, "east": False},
            )
        ],
    )
    breakpoints = [0, 60, 120]
    lane_overrides = {
        "EB": [LaneOverride(start=0, end=60, lanes=2)],
        "WB": [LaneOverride(start=60, end=120, lanes=2)],
    }

    _, link_indexing = render_connections_xml(
        defaults,
        [cluster],
        breakpoints,
        {"X2": template},
        snap_rule,
        main_road,
        lane_overrides,
    )

    index_info = link_indexing[60]
    ped_entries = [
        entry
        for entry in index_info.links
        if entry.kind == "pedestrian" and entry.crossing and entry.crossing.category == "main"
    ]
    assert len(ped_entries) == 2
    ped_index_by_dir = {
        entry.crossing.lane_directions[0]: entry.link_index for entry in ped_entries
    }
    eb_index = ped_index_by_dir["EB"]
    wb_index = ped_index_by_dir["WB"]

    profile = SignalProfileDef(
        id="two_stage_profile",
        cycle_s=30,
        ped_red_offset_s=0,
        yellow_duration_s=3,
        phases=[
            SignalPhaseDef(duration_s=10, allow_movements=["pedestrian"]),
            SignalPhaseDef(duration_s=10, allow_movements=["pedestrian", "WB_R"]),
            SignalPhaseDef(duration_s=10, allow_movements=["pedestrian", "EB_R"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=False, right=False),
    )

    xml = render_tllogics_xml(
        [cluster],
        {EventKind.CROSS.value: {profile.id: profile}},
        link_indexing,
    )

    states = [
        line.split('state="')[1].split('"')[0]
        for line in xml.splitlines()
        if line.strip().startswith("<phase ")
    ]
    assert len(states) == 3

    assert states[1][wb_index] == "r"
    assert states[1][eb_index] == "g"
    assert states[2][eb_index] == "r"
    assert states[2][wb_index] == "g"
def test_render_tllogics_without_signals_returns_empty() -> None:
    cluster = Cluster(
        pos_m=50,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=50.0,
                pos_m=50,
                signalized=False,
                signal=None,
            )
        ],
    )

    xml = render_tllogics_xml([cluster], {EventKind.CROSS.value: {}}, {})

    assert xml == "<tlLogics>\n</tlLogics>\n"
