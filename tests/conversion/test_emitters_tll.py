"""Unit tests covering tlLogic emission scenarios."""
from __future__ import annotations

from typing import Dict, List

from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionTemplate,
    LaneOverride,
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


def _make_defaults() -> Defaults:
    return Defaults(minor_road_length_m=0, ped_crossing_width_m=4.0, speed_kmh=30)


def _make_main_road() -> MainRoadConfig:
    return MainRoadConfig(length_m=300.0, center_gap_m=0.0, lanes=2)


def _empty_templates() -> Dict[str, JunctionTemplate]:
    return {}


def _empty_lane_overrides() -> List[LaneOverride]:
    return []


def _snap_rule() -> SnapRule:
    return SnapRule(step_m=10, tie_break="toward_west")


def _cluster_with_signal(
    *,
    pos: int,
    profile_id: str,
    kind: EventKind,
    offset: int = 0,
    refuge: bool = False,
    two_stage: bool = False,
) -> Cluster:
    event = LayoutEvent(
        type=kind,
        pos_m_raw=float(pos),
        pos_m=pos,
        signalized=True,
        signal=SignalRef(profile_id=profile_id, offset_s=offset),
        refuge_island_on_main=refuge,
        two_stage_tll_control=two_stage,
    )
    return Cluster(pos_m=pos, events=[event])


def _render(
    *,
    clusters: List[Cluster],
    profiles: Dict[str, Dict[str, SignalProfileDef]],
    links: List[SignalLink],
) -> str:
    return render_tll_xml(
        defaults=_make_defaults(),
        clusters=clusters,
        breakpoints=[],
        junction_template_by_id=_empty_templates(),
        snap_rule=_snap_rule(),
        main_road=_make_main_road(),
        lane_overrides=_empty_lane_overrides(),
        signal_profiles_by_kind=profiles,
        connection_links=links,
    )


def _extract_states(xml: str) -> List[str]:
    states: List[str] = []
    parts = xml.split('state="')
    for part in parts[1:]:
        states.append(part.split('"', 1)[0])
    return states


def test_vehicle_yellow_and_ped_cutoff_applied():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=10,
        ped_early_cutoff_s=1,
        yellow_duration_s=2,
        phases=[
            SignalPhaseDef(duration_s=6, allow_movements=["EB_R", "pedestrian"]),
            SignalPhaseDef(duration_s=4, allow_movements=[]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(pos=100, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id="Cluster.Main.100",
            movement="main_EB_R",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="veh",
        ),
        SignalLink(
            tl_id="Cluster.Main.100",
            movement="ped_minor_north",
            slot_index=1,
            link_index=1,
            kind="crossing",
            element_id="ped",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["gG", "yG", "yr", "rr"]


def test_two_stage_ped_split_distinguishes_halves():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=1,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB_L", "pedestrian"]),
            SignalPhaseDef(duration_s=4, allow_movements=["minor_N_R", "pedestrian"]),
            SignalPhaseDef(duration_s=2, allow_movements=[]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=False),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(
        pos=120,
        profile_id="profile",
        kind=EventKind.CROSS,
        two_stage=True,
        refuge=True,
    )
    links = [
        SignalLink(
            tl_id="Cluster.Main.120",
            movement="main_EB_L",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="vehL",
        ),
        SignalLink(
            tl_id="Cluster.Main.120",
            movement="minor_N_R",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="vehR",
        ),
        SignalLink(
            tl_id="Cluster.Main.120",
            movement="ped_main_west_EB",
            slot_index=2,
            link_index=2,
            kind="crossing",
            element_id="ped_w_eb",
        ),
        SignalLink(
            tl_id="Cluster.Main.120",
            movement="ped_main_west_WB",
            slot_index=3,
            link_index=3,
            kind="crossing",
            element_id="ped_w_wb",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["GrrG", "yrrG", "rgGr", "ryGr", "rrrr"]


def test_movement_stays_green_across_phases():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=2,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB_T"]),
            SignalPhaseDef(duration_s=4, allow_movements=["EB_T"]),
            SignalPhaseDef(duration_s=2, allow_movements=[]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(pos=150, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id="Cluster.Main.150",
            movement="main_EB_T",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="vehT",
        )
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["G", "y", "r"]
