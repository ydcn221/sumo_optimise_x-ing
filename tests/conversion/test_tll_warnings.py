from __future__ import annotations

import logging

from sumo_optimise.conversion.domain.models import (
    Cluster,
    Defaults,
    EventKind,
    JunctionConfig,
    LaneOverride,
    LayoutEvent,
    MainRoadConfig,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
    SnapRule,
)
from sumo_optimise.conversion.emitters.connections import render_connections_xml
from sumo_optimise.conversion.emitters.tll import render_tll_xml


def test_unsupported_allow_movement_logs_context(caplog):
    """Unsupported allow_movement tokens should include profile, phase, and tl in the warning."""

    profile = SignalProfileDef(
        id="warn_profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=3,
        phases=[SignalPhaseDef(duration_s=10, allow_movements=["BAD_TOKEN"])],
        kind=EventKind.CROSS,
    )

    cluster = Cluster(
        pos_m=50,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=50.0,
                pos_m=50,
                junction=JunctionConfig(
                    main_approach_begin_m=0,
                    main_approach_lanes=0,
                    minor_lanes_approach=1,
                    minor_lanes_departure=1,
                    median_continuous=True,
                ),
                signalized=True,
                signal=SignalRef(profile_id=profile.id, offset_s=0),
            )
        ],
    )

    defaults = Defaults(minor_road_length_m=80, ped_crossing_width_m=3.0, speed_kmh=40)
    snap = SnapRule(step_m=10, tie_break="toward_west")
    main = MainRoadConfig(length_m=100, center_gap_m=6.0, lanes=2)
    lane_overrides = {"EB": [LaneOverride(start=0, end=50, lanes=1)], "WB": []}

    connections = render_connections_xml(
        defaults,
        [cluster],
        [0, 50, 100],
        snap,
        main,
        lane_overrides,
    )

    signal_profiles_by_kind = {"cross": {profile.id: profile}}

    with caplog.at_level(logging.WARNING):
        render_tll_xml(
            defaults=defaults,
            clusters=[cluster],
            breakpoints=[0, 50, 100],
            snap_rule=snap,
            main_road=main,
            lane_overrides=tuple(),
            signal_profiles_by_kind=signal_profiles_by_kind,
            connection_links=connections.links,
            controlled_connections=connections.controlled_connections,
        )

    messages = [rec.message for rec in caplog.records if "unsupported allow_movement token" in rec.message]
    assert any("profile=warn_profile" in msg and "phase_index=0" in msg and "tl_id=Cluster.50" in msg for msg in messages)


def test_valid_but_absent_vehicle_token_is_silently_ignored(caplog):
    """Movement tokens that match schema but have no actual links (e.g., blocked by median) are ignored without warnings."""

    profile = SignalProfileDef(
        id="median_profile",
        cycle_s=10,
        ped_early_cutoff_s=0,
        yellow_duration_s=3,
        phases=[SignalPhaseDef(duration_s=10, allow_movements=["NB_R_pg"])],
        kind=EventKind.CROSS,
    )

    cluster = Cluster(
        pos_m=30,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=30.0,
                pos_m=30,
                junction=JunctionConfig(
                    main_approach_begin_m=0,
                    main_approach_lanes=0,
                    minor_lanes_approach=1,
                    minor_lanes_departure=1,
                    median_continuous=True,
                ),
                signalized=True,
                signal=SignalRef(profile_id=profile.id, offset_s=0),
            )
        ],
    )

    defaults = Defaults(minor_road_length_m=40, ped_crossing_width_m=3.0, speed_kmh=40)
    snap = SnapRule(step_m=10, tie_break="toward_west")
    main = MainRoadConfig(length_m=60, center_gap_m=6.0, lanes=2)
    lane_overrides = {"EB": [], "WB": []}

    connections = render_connections_xml(
        defaults,
        [cluster],
        [0, 30, 60],
        snap,
        main,
        lane_overrides,
    )

    signal_profiles_by_kind = {"cross": {profile.id: profile}}

    with caplog.at_level(logging.WARNING):
        render_tll_xml(
            defaults=defaults,
            clusters=[cluster],
            breakpoints=[0, 30, 60],
            snap_rule=snap,
            main_road=main,
            lane_overrides=tuple(),
            signal_profiles_by_kind=signal_profiles_by_kind,
            connection_links=connections.links,
            controlled_connections=connections.controlled_connections,
        )

    messages = [rec.message for rec in caplog.records if "unsupported allow_movement token" in rec.message]
    assert messages == []
