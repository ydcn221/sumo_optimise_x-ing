from __future__ import annotations

from sumo_optimise.conversion.domain.models import (
    Cluster,
    EventKind,
    LayoutEvent,
    PedestrianConflictConfig,
    SignalPhaseDef,
    SignalProfileDef,
    SignalRef,
)
from sumo_optimise.conversion.emitters.tllogics import render_tllogics_xml


def _layout_event(
    pos: int,
    profile_id: str,
    offset: int,
    kind: EventKind,
    *,
    signalized: bool = True,
) -> LayoutEvent:
    return LayoutEvent(
        type=kind,
        pos_m_raw=float(pos),
        pos_m=pos,
        signalized=signalized,
        signal=SignalRef(profile_id=profile_id, offset_s=offset) if signalized else None,
    )


def test_render_tllogics_xml_includes_profile_data() -> None:
    cluster = Cluster(pos_m=120, events=[_layout_event(120, "cross_profile", 15, EventKind.CROSS)])
    profile = SignalProfileDef(
        id="cross_profile",
        cycle_s=60,
        ped_red_offset_s=5,
        yellow_duration_s=3,
        phases=[
            SignalPhaseDef(duration_s=30, allow_movements=["main_T", "pedestrian"]),
            SignalPhaseDef(duration_s=30, allow_movements=["minor_T"]),
        ],
        kind=EventKind.CROSS,
        pedestrian_conflicts=PedestrianConflictConfig(left=True, right=False),
    )
    xml = render_tllogics_xml(
        [cluster],
        {EventKind.CROSS.value: {profile.id: profile}},
    )

    assert '<tlLogic id="Cluster.Main.120" type="static" programID="cross_profile" offset="15">' in xml
    assert '<param key="pedestrian_conflicts.left" value="true"/>' in xml
    assert '<param key="pedestrian_conflicts.right" value="false"/>' in xml
    assert '<phase duration="30" state="main_T pedestrian"/>' in xml
    assert '<phase duration="30" state="minor_T"/>' in xml


def test_render_tllogics_xml_without_signals() -> None:
    cluster = Cluster(
        pos_m=50,
        events=[_layout_event(50, "unused", 0, EventKind.CROSS, signalized=False)],
    )

    xml = render_tllogics_xml([cluster], {EventKind.CROSS.value: {}})

    assert xml == "<tlLogics>\n</tlLogics>\n"
