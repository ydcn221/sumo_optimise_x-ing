"""Unit tests covering tlLogic emission scenarios."""
from __future__ import annotations

import csv
from itertools import product
from pathlib import Path
from typing import Dict, List

from sumo_optimise.conversion.builder.ids import cluster_id
from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    LaneOverride,
    LayoutEvent,
    MainRoadConfig,
    PedestrianConflictConfig,
    ControlledConnection,
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
    controlled: List[ControlledConnection] | None = None,
) -> str:
    if controlled is None:
        controlled = []
    return render_tll_xml(
        defaults=_make_defaults(),
        clusters=clusters,
        breakpoints=[],
        snap_rule=_snap_rule(),
        main_road=_make_main_road(),
        lane_overrides=_empty_lane_overrides(),
        signal_profiles_by_kind=profiles,
        connection_links=links,
        controlled_connections=controlled,
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
            SignalPhaseDef(duration_s=6, allow_movements=["EB_R_pg", "PedX_NS"]),
            SignalPhaseDef(duration_s=4, allow_movements=[]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(pos=100, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id=cluster_id(100),
            movement="main_EB_R",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="veh",
        ),
        SignalLink(
            tl_id=cluster_id(100),
            movement="ped_minor_north",
            slot_index=1,
            link_index=1,
            kind="crossing",
            element_id="ped",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["GG", "yG", "yr", "rr"]


def test_main_right_phase_enables_u_turn_when_available():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=4,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB_R_pg", "EB_U_pg"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(pos=100, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id=cluster_id(100),
            movement="main_EB_R",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="veh_r",
        ),
        SignalLink(
            tl_id=cluster_id(100),
            movement="main_EB_U",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="veh_u",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["GG"]


def test_two_stage_ped_split_distinguishes_halves():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=1,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB_L_pg", "PedX_W"]),
            SignalPhaseDef(duration_s=4, allow_movements=["SB_R_pg", "PedX_W"]),
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
            tl_id=cluster_id(120),
            movement="main_EB_L",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="vehL",
        ),
        SignalLink(
            tl_id=cluster_id(120),
            movement="minor_N_R",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="vehR",
        ),
        SignalLink(
            tl_id=cluster_id(120),
            movement="ped_main_west_north",
            slot_index=2,
            link_index=2,
            kind="crossing",
            element_id="ped_w_eb",
        ),
        SignalLink(
            tl_id=cluster_id(120),
            movement="ped_main_west_south",
            slot_index=3,
            link_index=3,
            kind="crossing",
            element_id="ped_w_wb",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["GrrG", "yrrG", "rgGG", "ryGG", "rrrr"]


def test_half_tokens_require_pairs_without_two_stage():
    profile = SignalProfileDef(
        id="half_profile",
        cycle_s=6,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[
            SignalPhaseDef(duration_s=3, allow_movements=["PedX_W_N-half"]),
            SignalPhaseDef(duration_s=3, allow_movements=["PedX_W_S-half"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"half_profile": profile}}
    cluster = _cluster_with_signal(
        pos=140,
        profile_id="half_profile",
        kind=EventKind.CROSS,
        two_stage=False,
        refuge=True,
    )
    links = [
        SignalLink(
            tl_id=cluster_id(140),
            movement="ped_main_west_north",
            slot_index=0,
            link_index=0,
            kind="crossing",
            element_id="ped_w_n",
        ),
        SignalLink(
            tl_id=cluster_id(140),
            movement="ped_main_west_south",
            slot_index=1,
            link_index=1,
            kind="crossing",
            element_id="ped_w_s",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["rr"]


def test_movement_stays_green_across_phases():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=2,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB_T_pg"]),
            SignalPhaseDef(duration_s=4, allow_movements=["EB_T_pg"]),
            SignalPhaseDef(duration_s=2, allow_movements=[]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(pos=150, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id=cluster_id(150),
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


def test_eastbound_flow_blocks_west_crosswalk_via_aggregate_conflict():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=6,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[
            SignalPhaseDef(duration_s=3, allow_movements=["EB_LTR", "WB_LTR", "PedX_W"]),
            SignalPhaseDef(duration_s=3, allow_movements=["PedX_W"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    cluster = _cluster_with_signal(
        pos=165,
        profile_id="profile",
        kind=EventKind.CROSS,
        refuge=True,
        two_stage=True,
    )
    links = [
        SignalLink(
            tl_id=cluster_id(165),
            movement="main_EB_T",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="veh_eb",
        ),
        SignalLink(
            tl_id=cluster_id(165),
            movement="main_WB_T",
            slot_index=1,
            link_index=1,
            kind="connection",
            element_id="veh_wb",
        ),
        SignalLink(
            tl_id=cluster_id(165),
            movement="ped_main_west_north",
            slot_index=2,
            link_index=2,
            kind="crossing",
            element_id="ped_w_n",
        ),
        SignalLink(
            tl_id=cluster_id(165),
            movement="ped_main_west_south",
            slot_index=3,
            link_index=3,
            kind="crossing",
            element_id="ped_w_s",
        ),
    ]

    xml = _render(clusters=[cluster], profiles=profiles, links=links)
    states = _extract_states(xml)

    assert states == ["GGrr", "rrGG"]


def test_controlled_connections_emitted():
    profile = SignalProfileDef(
        id="profile",
        cycle_s=6,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[SignalPhaseDef(duration_s=6, allow_movements=["EB_T_pg", "WB_T_pg"])],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=True),
    )
    profiles = {EventKind.CROSS.value: {"profile": profile}}
    tl_id = cluster_id(150)
    cluster = _cluster_with_signal(pos=150, profile_id="profile", kind=EventKind.CROSS)
    links = [
        SignalLink(
            tl_id=tl_id,
            movement="main_EB_T",
            slot_index=0,
            link_index=0,
            kind="connection",
            element_id="Edge.A->Edge.B",
        )
    ]
    controlled = [
        ControlledConnection(
            tl_id=tl_id,
            from_edge="Edge.A",
            to_edge="Edge.B",
            from_lane=0,
            to_lane=1,
            link_index=0,
        )
    ]

    xml = _render(
        clusters=[cluster],
        profiles=profiles,
        links=links,
        controlled=controlled,
    )
    assert (
        '<connection from="Edge.A" to="Edge.B" fromLane="0" toLane="1" '
        f'tl="{tl_id}" linkIndex="0"/>'
    ) in xml


def _midblock_links(pos: int, include_vehicle: bool = False) -> List[SignalLink]:
    tl = cluster_id(pos)
    links: List[SignalLink] = []
    slot = 0
    if include_vehicle:
        links.append(
            SignalLink(
                tl_id=tl,
                movement="main_EB_T",
                slot_index=slot,
                link_index=slot,
                kind="connection",
                element_id="veh_EB",
            )
        )
        slot += 1
    links.append(
        SignalLink(
            tl_id=tl,
            movement="ped_mid_north",
            slot_index=slot,
            link_index=slot,
            kind="crossing",
            element_id="ped_mid_n",
        )
    )
    slot += 1
    links.append(
        SignalLink(
            tl_id=tl,
            movement="ped_mid_south",
            slot_index=slot,
            link_index=slot,
            kind="crossing",
            element_id="ped_mid_s",
        )
    )
    return links


def test_midblock_single_side_without_two_stage_forces_red():
    profile = SignalProfileDef(
        id="mid_profile",
        cycle_s=4,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[SignalPhaseDef(duration_s=4, allow_movements=["PedX_N"])],
        kind=EventKind.XWALK_MIDBLOCK,
        pedestrian_conflicts=PedestrianConflictConfig(left=False, right=False),
    )
    profiles = {EventKind.XWALK_MIDBLOCK.value: {"mid_profile": profile}}
    cluster = _cluster_with_signal(
        pos=300,
        profile_id="mid_profile",
        kind=EventKind.XWALK_MIDBLOCK,
        two_stage=False,
        refuge=False,
    )
    xml = _render(clusters=[cluster], profiles=profiles, links=_midblock_links(300))
    states = _extract_states(xml)
    assert states == ["rr"]


def test_midblock_flow_blocks_corresponding_crosswalk():
    profile = SignalProfileDef(
        id="mid_profile",
        cycle_s=6,
        ped_early_cutoff_s=0,
        yellow_duration_s=0,
        phases=[
            SignalPhaseDef(duration_s=4, allow_movements=["EB", "PedX"]),
            SignalPhaseDef(duration_s=2, allow_movements=["PedX"]),
        ],
        kind=EventKind.XWALK_MIDBLOCK,
        pedestrian_conflicts=PedestrianConflictConfig(left=False, right=False),
    )
    profiles = {EventKind.XWALK_MIDBLOCK.value: {"mid_profile": profile}}
    cluster = _cluster_with_signal(
        pos=320,
        profile_id="mid_profile",
        kind=EventKind.XWALK_MIDBLOCK,
        two_stage=False,
        refuge=False,
    )
    xml = _render(
        clusters=[cluster],
        profiles=profiles,
        links=_midblock_links(320, include_vehicle=True),
    )
    states = _extract_states(xml)
    assert states == ["GrG", "rGG"]
