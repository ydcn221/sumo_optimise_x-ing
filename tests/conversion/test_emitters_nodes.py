"""Tests for PlainXML node emission."""
from __future__ import annotations

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
    return Defaults(minor_road_length_m=25, ped_crossing_width_m=4.0, speed_kmh=40)


def _make_main_road(length: float = 200.0) -> MainRoadConfig:
    return MainRoadConfig(length_m=length, center_gap_m=0.0, lanes=2)


def _breakpoints() -> list[int]:
    return [0, 100, 200]


def _reasons() -> dict[int, BreakpointInfo]:
    return {100: BreakpointInfo(pos=100, reasons={"cluster"})}


def _extract_join_line(xml: str) -> str:
    for line in xml.splitlines():
        if line.strip().startswith("<join"):
            return line.strip()
    raise AssertionError("join line not found in XML")


def _extract_node_lines(xml: str) -> list[str]:
    return [line.strip() for line in xml.splitlines() if line.strip().startswith("<node ")]


def test_signalised_cluster_sets_traffic_light_attributes() -> None:
    cluster = Cluster(
        pos_m=100,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=100.0,
                pos_m=100,
                signalized=True,
                signal=SignalRef(profile_id="profile", offset_s=0),
            )
        ],
    )

    xml = render_nodes_xml(
        main_road=_make_main_road(),
        defaults=_make_defaults(),
        clusters=[cluster],
        breakpoints=_breakpoints(),
        reason_by_pos=_reasons(),
    )

    node_lines = _extract_node_lines(xml)
    assert any(
        'id="Node.Main.EB.100"' in line and 'type="traffic_light"' in line and 'tl="Cluster.Main.100"' in line
        for line in node_lines
    )
    assert any(
        'id="Node.Main.WB.100"' in line and 'type="traffic_light"' in line and 'tl="Cluster.Main.100"' in line
        for line in node_lines
    )

    join_line = _extract_join_line(xml)
    assert 'type="traffic_light"' in join_line
    assert 'tl="Cluster.Main.100"' in join_line


def test_unsignalised_cluster_has_no_traffic_light_attributes() -> None:
    cluster = Cluster(
        pos_m=100,
        events=[
            LayoutEvent(
                type=EventKind.CROSS,
                pos_m_raw=100.0,
                pos_m=100,
                signalized=False,
                signal=None,
            )
        ],
    )

    xml = render_nodes_xml(
        main_road=_make_main_road(),
        defaults=_make_defaults(),
        clusters=[cluster],
        breakpoints=_breakpoints(),
        reason_by_pos=_reasons(),
    )

    node_lines = _extract_node_lines(xml)
    assert all(
        'type="traffic_light"' not in line and ' tl="' not in line
        for line in node_lines
        if 'Node.Main.EB.100' in line or 'Node.Main.WB.100' in line
    )

    join_line = _extract_join_line(xml)
    assert 'type="traffic_light"' not in join_line
    assert ' tl="' not in join_line
