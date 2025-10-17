from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET

from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    BuildOptions,
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
from sumo_optimise.conversion.pipeline import build_corridor_artifacts


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "src/sumo_optimise/conversion/data/schema.json"
REFERENCE_DIR = REPO_ROOT / "data/reference/tll reference (plainXML sample)"


def _extract_phases(xml: str) -> list[tuple[int, str]]:
    phases: list[tuple[int, str]] = []
    for line in xml.splitlines():
        stripped = line.strip()
        if not stripped.startswith("<phase "):
            continue
        duration_part = stripped.split('duration="', 1)[1]
        duration = int(duration_part.split('"', 1)[0])
        state_part = stripped.split('state="', 1)[1]
        state = state_part.split('"', 1)[0]
        phases.append((duration, state))
    return phases


def _states_by_tl(root: ET.Element) -> dict[str, list[tuple[int, str]]]:
    mapping: dict[str, list[tuple[int, str]]] = {}
    for logic in root.findall("tlLogic"):
        mapping[logic.attrib["id"]] = [
            (int(phase.attrib["duration"]), phase.attrib["state"]) for phase in logic.findall("phase")
        ]
    return mapping


def _states_by_tl_from_string(xml: str) -> dict[str, list[tuple[int, str]]]:
    root = ET.fromstring(xml)
    return _states_by_tl(root)


EXPECTED_STATES_1015: dict[str, list[tuple[int, str]]] = {
    "Cluster.Main.100": [
        (25, "gGGGrrgGGGrrrrrrrrrrGGrr"),
        (4, "gGGGrrgGGGrrrrrrrrrrrrrr"),
        (3, "yyyyrryyyyrrrrrrrrrrrrrr"),
        (12, "rrrrggrrrrggrrrrrrrrrrrr"),
        (3, "rrrryyrrrryyrrrrrrrrrrrr"),
        (3, "rrrrrrrrrrrrrrrrrrrrrrrr"),
        (20, "rrrrrrrrrrrrgGGrgGGrGGGG"),
        (4, "rrrrrrrrrrrrgGGrgGGrrrGG"),
        (3, "rrrrrrrrrrrryyyryyyrrrGG"),
        (3, "rrrrrrrrrrrrrrrgrrrgrrGG"),
        (4, "rrrrrrrrrrrrrrrgrrrgrrrr"),
        (3, "rrrrrrrrrrrrrrryrrryrrrr"),
        (3, "rrrrrrrrrrrrrrrrrrrrrrrr"),
    ],
    "Cluster.Main.150": [(60, "rr"), (30, "GG")],
    "Cluster.Main.200": [
        (21, "rGGGrrrGGGrrrrrrrrrrrrGGr"),
        (5, "rGGGrrrGGGrrrrrrrrrrrrrrr"),
        (4, "ryyyrrryyyrrrrrrrrrrrrrrr"),
        (11, "rrrrggrrrrgggrrrrgrrrrrrr"),
        (4, "rrrryyrrrryyyrrrryrrrrrrr"),
        (21, "rrrrrrrrrrrrrGGGrrGGGrGGG"),
        (5, "rrrrrrrrrrrrrGGGrrGGGrrrr"),
        (4, "rrrrrrrrrrrrryyyrryyyrrrr"),
        (11, "grrrrrgrrrrrrrrrgrrrrgrrr"),
        (4, "yrrrrryrrrrrrrrryrrrryrrr"),
    ],
}


EXPECTED_STATES_1016: dict[str, list[tuple[int, str]]] = {
    "Cluster.Main.100": [
        (29, "gGGGrGGGrrrrrrrrrrr"),
        (3, "yyyyryyyrrrrrrrrrrr"),
        (12, "rrrrgrrrggrrrrrrrrr"),
        (3, "rrrryrrryyrrrrrrrrr"),
        (3, "rrrrrrrrrrrrrrrrrrr"),
        (24, "rrrrrrrrrrggrrrGGGG"),
        (3, "rrrrrrrrrryyrrrGGGG"),
        (3, "rrrrrrrrrrrrggrGGGG"),
        (4, "rrrrrrrrrrrrggrrrrr"),
        (3, "rrrrrrrrrrrryyrrrrr"),
        (3, "rrrrrrrrrrrrrrrrrrr"),
    ],
    "Cluster.Main.200": [
        (21, "GGGrrrGGGrrrrrGrrrr"),
        (5, "GGGrrrGGGrrrrrrrrrr"),
        (4, "yyyrrryyyrrrrrrrrrr"),
        (11, "rrrggrrrrgggrrrrrrr"),
        (4, "rrryyrrrryyyrrrrrrr"),
        (21, "rrrrrrrrrrrrrrGGGGG"),
        (9, "rrrrrrrrrrrrrrrrrrr"),
        (11, "rrrrrgrrrrrrggrrrrr"),
        (4, "rrrrryrrrrrryyrrrrr"),
    ],
}


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
    ped_signal_links = index_info.pedestrian_links
    assert ped_signal_links, "expected stored pedestrian signal references"
    assert all(link.tl_id == index_info.tl_id for link in ped_signal_links)
    ped_crossing_ids = {link.crossing_id for link in ped_signal_links}
    planned_crossing_ids = {
        entry.crossing.crossing_id
        for entry in index_info.links
        if entry.kind == "pedestrian" and entry.crossing is not None
    }
    assert ped_crossing_ids == planned_crossing_ids

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

    assert "<crossing" not in xml
    phases = _extract_phases(xml)
    assert phases, "expected phases to be emitted"
    state_length = len(phases[0][1])
    assert state_length == len(index_info.links)

    ped_index = ped_indices[0]
    first_phase_duration = profile.phases[0].duration_s
    consumed = 0
    first_phase_segments: list[tuple[int, str]] = []
    for duration, state in phases:
        if consumed >= first_phase_duration:
            break
        remaining = first_phase_duration - consumed
        take = min(duration, remaining)
        first_phase_segments.append((take, state))
        consumed += duration
    assert sum(duration for duration, _ in first_phase_segments) == first_phase_duration
    assert first_phase_segments[0][1][ped_index] == "G"
    assert first_phase_segments[-1][1][ped_index] == "r"

    yellow_segments = [(duration, state) for duration, state in phases if "y" in state]
    assert yellow_segments, "expected yellow segments"
    assert all(duration == profile.yellow_duration_s for duration, _ in yellow_segments)

    assert any(state[ped_index] == "G" for _, state in phases)
    assert any(
        state[ped_index] == "r" and any(ch in {"g", "G"} for ch in state[:ped_index])
        for _, state in phases
    )

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

    phases = _extract_phases(xml)
    assert phases
    assert [duration for duration, _ in phases] == [10, 7, 3, 7, 3]

    wb_states = [state[wb_index] for _, state in phases]
    eb_states = [state[eb_index] for _, state in phases]

    assert wb_states == ["G", "r", "r", "G", "G"]
    assert eb_states == ["G", "G", "G", "r", "r"]

    yellow_segments = [(duration, state) for duration, state in phases if "y" in state]
    assert yellow_segments and all(duration == profile.yellow_duration_s for duration, _ in yellow_segments)


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


def test_reference_profile_1015_matches_expected_states() -> None:
    spec_path = REFERENCE_DIR / "1015.2.json"
    options = BuildOptions(schema_path=SCHEMA_PATH)
    result = build_corridor_artifacts(spec_path, options)

    generated = _states_by_tl_from_string(result.tllogics_xml)
    assert generated == EXPECTED_STATES_1015


def test_reference_profile_1016_matches_expected_states() -> None:
    spec_path = REFERENCE_DIR / "1016_doubleT.json"
    options = BuildOptions(schema_path=SCHEMA_PATH)
    result = build_corridor_artifacts(spec_path, options)

    generated = _states_by_tl_from_string(result.tllogics_xml)
    assert generated == EXPECTED_STATES_1016
