from __future__ import annotations

from sumo_optimise.conversion.emitters.connections import (
    _emit_vehicle_connections_for_approach,
)


def test_left_turn_sources_share_rightmost_target_lane() -> None:
    lines: list[str] = []

    emitted = _emit_vehicle_connections_for_approach(
        lines=lines,
        pos=100,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=("Edge.Left", 2),
        T_target=None,
        R_target=None,
    )

    assert emitted == 4
    assert lines == [
        '  <connection from="Edge.In" to="Edge.Left" fromLane="1" toLane="1"/>',
        '  <connection from="Edge.In" to="Edge.Left" fromLane="2" toLane="2"/>',
        '  <connection from="Edge.In" to="Edge.Left" fromLane="3" toLane="2"/>',
        '  <connection from="Edge.In" to="Edge.Left" fromLane="4" toLane="2"/>',
    ]


def test_straight_rightmost_source_fans_out_to_extra_targets() -> None:
    lines: list[str] = []

    emitted = _emit_vehicle_connections_for_approach(
        lines=lines,
        pos=200,
        in_edge_id="Edge.In",
        s_count=2,
        L_target=None,
        T_target=("Edge.Straight", 4),
        R_target=None,
    )

    assert emitted == 4
    assert lines == [
        '  <connection from="Edge.In" to="Edge.Straight" fromLane="1" toLane="1"/>',
        '  <connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="2"/>',
        '  <connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="3"/>',
        '  <connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="4"/>',
    ]


def test_right_turn_sources_anchor_on_rightmost_lane() -> None:
    lines: list[str] = []

    emitted = _emit_vehicle_connections_for_approach(
        lines=lines,
        pos=300,
        in_edge_id="Edge.In",
        s_count=3,
        L_target=None,
        T_target=None,
        R_target=("Edge.Right", 2),
    )

    assert emitted == 3
    assert lines == [
        '  <connection from="Edge.In" to="Edge.Right" fromLane="1" toLane="2"/>',
        '  <connection from="Edge.In" to="Edge.Right" fromLane="2" toLane="1"/>',
        '  <connection from="Edge.In" to="Edge.Right" fromLane="3" toLane="2"/>',
    ]
