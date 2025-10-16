"""Tests for PlainXML node emission."""
from sumo_optimise.conversion.domain.models import (
    BreakpointInfo,
    Cluster,
    Defaults,
    EventKind,
    LayoutEvent,
    MainRoadConfig,
    SignalRef,
)
from sumo_optimise.conversion.emitters.nodes import render_nodes_xml


def _make_defaults() -> Defaults:
    return Defaults(minor_road_length_m=25, ped_crossing_width_m=4.0, speed_kmh=50)


def _make_main() -> MainRoadConfig:
    return MainRoadConfig(length_m=120, center_gap_m=3, lanes=2)


def test_render_nodes_xml_marks_signalised_clusters_with_traffic_light_attributes() -> None:
    signal_cluster = Cluster(
        pos_m=50,
        events=[
            LayoutEvent(
                type=EventKind.TEE,
                pos_m_raw=50.0,
                pos_m=50,
                signalized=True,
                signal=SignalRef(profile_id="sig-tee", offset_s=0),
            )
        ],
    )
    unsignalised_cluster = Cluster(
        pos_m=70,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=70.0,
                pos_m=70,
                signalized=False,
            )
        ],
    )
    breakpoints = [0, 50, 70, 120]
    reason_by_pos = {
        50: BreakpointInfo(pos=50, reasons={"cluster"}),
        70: BreakpointInfo(pos=70, reasons={"cluster"}),
    }

    xml = render_nodes_xml(
        main_road=_make_main(),
        defaults=_make_defaults(),
        clusters=[signal_cluster, unsignalised_cluster],
        breakpoints=breakpoints,
        reason_by_pos=reason_by_pos,
    )

    join_lines = [line.strip() for line in xml.splitlines() if line.strip().startswith("<join ")]
    assert any(
        "id=\"Cluster.Main.50\"" in line
        and "type=\"traffic_light\"" in line
        and "tl=\"Cluster.Main.50\"" in line
        and "tlType=\"static\"" in line
        for line in join_lines
    )
    assert any(
        "id=\"Cluster.Main.70\"" in line
        and "type=\"traffic_light\"" not in line
        and "tl=\"" not in line
        and "tlType=\"" not in line
        for line in join_lines
    )
