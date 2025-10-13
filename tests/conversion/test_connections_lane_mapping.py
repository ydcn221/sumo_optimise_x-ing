from sumo_optimise.conversion.emitters import connections as mod


def extract_connections(lines: list[str], to_edge: str) -> set[str]:
    return {
        line.strip()
        for line in lines
        if f'to="{to_edge}"' in line
    }


def test_straight_fans_out_to_rightmost_targets():
    lines: list[str] = []
    emitted = mod._emit_vehicle_connections_for_approach(
        lines,
        pos=0,
        in_edge_id="Edge.In",
        s_count=2,
        L_target=None,
        T_target=("Edge.Straight", 4),
        R_target=None,
        U_target=None,
    )

    assert emitted == 4
    assert extract_connections(lines, "Edge.Straight") == {
        '<connection from="Edge.In" to="Edge.Straight" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="3"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="4"/>',
    }


def test_left_turns_share_last_target_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["L", "L", "L", "L"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    lines: list[str] = []
    emitted = mod._emit_vehicle_connections_for_approach(
        lines,
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=("Edge.Left", 2),
        T_target=None,
        R_target=None,
        U_target=None,
    )

    assert emitted == 4
    assert extract_connections(lines, "Edge.Left") == {
        '<connection from="Edge.In" to="Edge.Left" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="3" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="4" toLane="2"/>',
    }


def test_right_turns_share_outer_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["R", "R", "R", "R"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    lines: list[str] = []
    emitted = mod._emit_vehicle_connections_for_approach(
        lines,
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=None,
        T_target=None,
        R_target=("Edge.Right", 2),
        U_target=None,
    )

    assert emitted == 4
    assert extract_connections(lines, "Edge.Right") == {
        '<connection from="Edge.In" to="Edge.Right" fromLane="4" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="3" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="1" toLane="2"/>',
    }
