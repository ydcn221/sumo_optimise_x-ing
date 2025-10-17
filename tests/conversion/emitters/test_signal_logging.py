from __future__ import annotations

import logging

from sumo_optimise.conversion.domain.models import (
    Cluster,
    EventKind,
    LayoutEvent,
    PedestrianConflictConfig,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
)
from sumo_optimise.conversion.emitters.connections import (
    ApproachInfo,
    ClusterLinkIndexing,
    ClusterLinkPlan,
    CrossingPlan,
    LinkIndexEntry,
    VehicleConnectionPlan,
    _build_cluster_link_indexing,
)
from sumo_optimise.conversion.emitters.tllogics import render_tllogics_xml


def _build_plan_with_signal_links() -> dict[int, ClusterLinkPlan]:
    approach = ApproachInfo(road="main", direction="EB")
    vehicle_plan = VehicleConnectionPlan(
        from_edge="Edge.Main.EB.0-42",
        to_edge="Edge.Main.EB.42-84",
        from_lane=0,
        to_lane=0,
        approach=approach,
        movement="T",
    )
    crossing_plan = CrossingPlan(
        crossing_id="Crossing.Main.42.west",
        node="Cluster.Main.42",
        edges=("Edge.Main.EB.0-42", "Edge.Main.WB.84-42"),
        width=3.0,
        category="main",
    )
    return {42: ClusterLinkPlan(crossings=[crossing_plan], vehicle_connections=[vehicle_plan])}


def test_build_cluster_link_indexing_logs_summary(caplog):
    plans = _build_plan_with_signal_links()

    with caplog.at_level(logging.INFO):
        result = _build_cluster_link_indexing(plans, {42})

    assert 42 in result
    messages = [record.message for record in caplog.records if "cluster link index summary" in record.message]
    assert messages
    summary = messages[0]
    assert "[BUILD]" in summary
    assert "tl=Cluster.Main.42" in summary
    assert "vehicle_links=1" in summary
    assert "pedestrian_links=1" in summary
    assert "max_link_index=1" in summary


def test_render_tllogics_logs_connection_mapping(caplog):
    approach = ApproachInfo(road="main", direction="EB")
    connection = VehicleConnectionPlan(
        from_edge="Edge.Main.EB.0-42",
        to_edge="Edge.Main.EB.42-84",
        from_lane=0,
        to_lane=0,
        approach=approach,
        movement="T",
    )
    link_entry = LinkIndexEntry(
        link_index=0,
        kind="vehicle",
        tokens=("main_T",),
        movement="T",
        connection=connection,
    )
    indexing = {
        42: ClusterLinkIndexing(
            tl_id="Cluster.Main.42",
            links=(link_entry,),
            token_to_indices={"main_T": (0,)},
        )
    }
    cluster = Cluster(
        pos_m=42,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=42.0,
                pos_m=42,
                signalized=True,
                signal=SignalRef(profile_id="profile", offset_s=0),
            )
        ],
    )
    profile = SignalProfileDef(
        id="profile",
        cycle_s=60,
        ped_red_offset_s=0,
        yellow_duration_s=3,
        phases=[SignalPhaseDef(duration_s=30, allow_movements=["main_T"])],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=False, right=False),
    )

    with caplog.at_level(logging.INFO):
        xml = render_tllogics_xml(
            [cluster],
            {EventKind.CROSS.value: {"profile": profile}},
            indexing,
        )

    assert "<connection" in xml
    messages = [record.message for record in caplog.records if "tl connection mapping" in record.message]
    assert messages
    mapping_message = messages[0]
    assert "[BUILD]" in mapping_message
    assert "tl=Cluster.Main.42" in mapping_message
    assert "tokens=main_T" in mapping_message
    assert "movement=T" in mapping_message
    assert "linkIndex=0" in mapping_message
