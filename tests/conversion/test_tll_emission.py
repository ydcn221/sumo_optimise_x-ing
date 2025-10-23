"""Unit tests for tlLogic emission."""
from __future__ import annotations

import xml.etree.ElementTree as ET

from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    LayoutEvent,
    MainRoadConfig,
    PedestrianConflictConfig,
    SignalLink,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
    SnapRule,
)
from sumo_optimise.conversion.emitters.tll import render_tll_xml


def _dummy_defaults() -> Defaults:
    return Defaults(minor_road_length_m=0, ped_crossing_width_m=3.0, speed_kmh=30)


def _dummy_main() -> MainRoadConfig:
    return MainRoadConfig(length_m=500, center_gap_m=2.0, lanes=2)


def _dummy_snap() -> SnapRule:
    return SnapRule(step_m=10, tie_break="toward_west")


def _cluster(pos: int, *events: LayoutEvent) -> Cluster:
    return Cluster(pos_m=pos, events=list(events))


def _signal_event(
    *,
    pos: int,
    kind: EventKind,
    profile_id: str,
    offset: int = 0,
    signalized: bool = True,
    refuge_island_on_main: bool = False,
    two_stage: bool = False,
) -> LayoutEvent:
    return LayoutEvent(
        type=kind,
        pos_m_raw=float(pos),
        pos_m=pos,
        signalized=signalized,
        signal=SignalRef(profile_id=profile_id, offset_s=offset),
        refuge_island_on_main=refuge_island_on_main,
        two_stage_tll_control=two_stage,
    )


def _profile(
    *,
    pid: str,
    cycle: int,
    yellow: int,
    ped_cutoff: int,
    phases: list[SignalPhaseDef],
    conflicts: PedestrianConflictConfig | None = None,
    kind: EventKind = EventKind.CROSS,
) -> SignalProfileDef:
    return SignalProfileDef(
        id=pid,
        cycle_s=cycle,
        ped_early_cutoff_s=ped_cutoff,
        yellow_duration_s=yellow,
        phases=phases,
        kind=kind,
        pedestrian_conflicts=conflicts or PedestrianConflictConfig(left=False, right=False),
    )


def _profiles_map(profile: SignalProfileDef) -> dict[str, dict[str, SignalProfileDef]]:
    return {profile.kind.value: {profile.id: profile}}


def _render(
    *,
    clusters: list[Cluster],
    profiles: dict[str, dict[str, SignalProfileDef]],
    links: list[SignalLink],
) -> ET.Element:
    xml = render_tll_xml(
        defaults=_dummy_defaults(),
        clusters=clusters,
        breakpoints=[0, 100, 200],
        junction_template_by_id={},
        snap_rule=_dummy_snap(),
        main_road=_dummy_main(),
        lane_overrides=[],
        signal_profiles_by_kind=profiles,
        connection_links=links,
    )
    return ET.fromstring(xml)


def test_vehicle_yellow_and_multi_phase_green():
    profile = _profile(
        pid="cross_A",
        cycle=20,
        yellow=3,
        ped_cutoff=0,
        phases=[
            SignalPhaseDef(duration_s=10, allow_movements=["EB_T"]),
            SignalPhaseDef(duration_s=5, allow_movements=["EB_T", "WB_T"]),
            SignalPhaseDef(duration_s=5, allow_movements=[]),
        ],
    )
    event = _signal_event(pos=100, kind=EventKind.CROSS, profile_id="cross_A")
    cluster = _cluster(100, event)
    links = [
        SignalLink(
            tl_id="Cluster.Main.100",
            movement="main_EB_T",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="EdgeA->EdgeB",
        ),
        SignalLink(
            tl_id="Cluster.Main.100",
            movement="main_WB_T",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="EdgeC->EdgeD",
        ),
    ]

    root = _render(clusters=[cluster], profiles=_profiles_map(profile), links=links)
    tl = root.find("./tlLogic[@id='Cluster.Main.100']")
    assert tl is not None

    phases = [(int(node.attrib["duration"]), node.attrib["state"]) for node in tl.findall("phase")]
    assert phases == [(10, "Gr"), (2, "GG"), (3, "yy"), (5, "rr")]

    conn_refs = [(node.attrib["movement"], node.attrib["linkIndex"]) for node in tl.findall("connection")]
    assert conn_refs == [("main_EB_T", "0"), ("main_WB_T", "1")]


def test_pedestrian_early_cutoff():
    profile = _profile(
        pid="cross_ped",
        cycle=10,
        yellow=0,
        ped_cutoff=2,
        phases=[
            SignalPhaseDef(duration_s=8, allow_movements=["pedestrian"]),
            SignalPhaseDef(duration_s=2, allow_movements=[]),
        ],
    )
    event = _signal_event(pos=150, kind=EventKind.CROSS, profile_id="cross_ped")
    cluster = _cluster(150, event)
    links = [
        SignalLink(
            tl_id="Cluster.Main.150",
            movement="ped_main_west",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="Cross.Main.150.West",
        )
    ]

    root = _render(clusters=[cluster], profiles=_profiles_map(profile), links=links)
    tl = root.find("./tlLogic[@id='Cluster.Main.150']")
    assert tl is not None

    phases = [(int(node.attrib["duration"]), node.attrib["state"]) for node in tl.findall("phase")]
    assert phases == [(6, "G"), (4, "r")]


def test_two_stage_split_behaviour():
    profile = _profile(
        pid="split_main",
        cycle=6,
        yellow=0,
        ped_cutoff=0,
        phases=[
            SignalPhaseDef(duration_s=3, allow_movements=["pedestrian", "EB_T"]),
            SignalPhaseDef(duration_s=3, allow_movements=[]),
        ],
    )
    links_split = [
        SignalLink(
            tl_id="Cluster.Main.200",
            movement="ped_main_west_EB",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="Cross.Main.200.West.EB",
        ),
        SignalLink(
            tl_id="Cluster.Main.200",
            movement="ped_main_west_WB",
            slot_index=1,
            link_index=1,
            kind="crossing",
            element_id="Cross.Main.200.West.WB",
        ),
        SignalLink(
            tl_id="Cluster.Main.200",
            movement="main_EB_T",
            slot_index=2,
            link_index=2,
            kind="connection",
            element_id="EdgeE->EdgeF",
        ),
    ]
    links_grouped = [
        SignalLink(
            tl_id="Cluster.Main.300",
            movement="ped_main_west_EB",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="Cross.Main.300.West.EB",
        ),
        SignalLink(
            tl_id="Cluster.Main.300",
            movement="ped_main_west_WB",
            slot_index=1,
            link_index=1,
            kind="crossing",
            element_id="Cross.Main.300.West.WB",
        ),
        SignalLink(
            tl_id="Cluster.Main.300",
            movement="main_EB_T",
            slot_index=2,
            link_index=2,
            kind="connection",
            element_id="EdgeG->EdgeH",
        ),
    ]

    cluster_split = _cluster(200, _signal_event(pos=200, kind=EventKind.CROSS, profile_id="split_main", refuge_island_on_main=True, two_stage=True))
    cluster_grouped = _cluster(300, _signal_event(pos=300, kind=EventKind.CROSS, profile_id="split_main", refuge_island_on_main=True, two_stage=False))

    profiles = _profiles_map(profile)
    root = _render(
        clusters=[cluster_split, cluster_grouped],
        profiles=profiles,
        links=links_split + links_grouped,
    )

    tl_split = root.find("./tlLogic[@id='Cluster.Main.200']")
    assert tl_split is not None
    states_split = [node.attrib["state"] for node in tl_split.findall("phase")]
    assert states_split[0] == "rGG"

    tl_grouped = root.find("./tlLogic[@id='Cluster.Main.300']")
    assert tl_grouped is not None
    states_grouped = [node.attrib["state"] for node in tl_grouped.findall("phase")]
    assert states_grouped[0] == "rrG"


def test_pedestrian_conflicts_allow_yield():
    profile_block = _profile(
        pid="minor_block",
        cycle=6,
        yellow=0,
        ped_cutoff=0,
        phases=[
            SignalPhaseDef(duration_s=3, allow_movements=["pedestrian", "main_EB_L"]),
            SignalPhaseDef(duration_s=3, allow_movements=[]),
        ],
        conflicts=PedestrianConflictConfig(left=False, right=False),
    )
    profile_allow = _profile(
        pid="minor_allow",
        cycle=6,
        yellow=0,
        ped_cutoff=0,
        phases=profile_block.phases,
        conflicts=PedestrianConflictConfig(left=True, right=False),
    )

    cluster_block = _cluster(400, _signal_event(pos=400, kind=EventKind.CROSS, profile_id="minor_block"))
    cluster_allow = _cluster(500, _signal_event(pos=500, kind=EventKind.CROSS, profile_id="minor_allow"))

    links_block = [
        SignalLink(
            tl_id="Cluster.Main.400",
            movement="ped_minor_north",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="Cross.Minor.400.N",
        ),
        SignalLink(
            tl_id="Cluster.Main.400",
            movement="main_EB_L",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="EdgeI->EdgeJ",
        ),
    ]
    links_allow = [
        SignalLink(
            tl_id="Cluster.Main.500",
            movement="ped_minor_north",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="Cross.Minor.500.N",
        ),
        SignalLink(
            tl_id="Cluster.Main.500",
            movement="main_EB_L",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="EdgeK->EdgeL",
        ),
    ]

    profiles = {
        EventKind.CROSS.value: {
            profile_block.id: profile_block,
            profile_allow.id: profile_allow,
        }
    }

    root = _render(
        clusters=[cluster_block, cluster_allow],
        profiles=profiles,
        links=links_block + links_allow,
    )

    tl_block = root.find("./tlLogic[@id='Cluster.Main.400']")
    assert tl_block is not None
    block_states = [node.attrib["state"] for node in tl_block.findall("phase")]
    assert block_states[0] == "rG"

    tl_allow = root.find("./tlLogic[@id='Cluster.Main.500']")
    assert tl_allow is not None
    allow_states = [node.attrib["state"] for node in tl_allow.findall("phase")]
    assert allow_states[0] == "Gg"

