from sumo_optimise.conversion.emitters import connections as mod


def extract_connections(plans: list[mod.VehicleConnectionPlan], to_edge: str) -> set[str]:
    return {
        f'<connection from="{plan.from_edge}" to="{plan.to_edge}" '
        f'fromLane="{plan.from_lane}" toLane="{plan.to_lane}"/>'
        for plan in plans
        if plan.to_edge == to_edge
    }


def test_straight_fans_out_to_rightmost_targets():
    plans = mod._plan_vehicle_connections_for_approach(
        pos=0,
        in_edge_id="Edge.In",
        s_count=2,
        L_target=None,
        T_target=("Edge.Straight", 4),
        R_target=None,
        U_target=None,
        approach=mod.ApproachInfo(road="main", direction="EB"),
    )

    assert len(plans) == 4
    assert extract_connections(plans, "Edge.Straight") == {
        '<connection from="Edge.In" to="Edge.Straight" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="3"/>',
        '<connection from="Edge.In" to="Edge.Straight" fromLane="2" toLane="4"/>',
    }


def test_left_turns_share_last_target_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["L", "L", "L", "L"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    plans = mod._plan_vehicle_connections_for_approach(
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=("Edge.Left", 2),
        T_target=None,
        R_target=None,
        U_target=None,
        approach=mod.ApproachInfo(road="main", direction="EB"),
    )

    assert len(plans) == 4
    assert extract_connections(plans, "Edge.Left") == {
        '<connection from="Edge.In" to="Edge.Left" fromLane="1" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="3" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Left" fromLane="4" toLane="2"/>',
    }


def test_right_turns_share_outer_lane(monkeypatch):
    def fake_allocate(s, l, t, r, u):
        return ["R", "R", "R", "R"]

    monkeypatch.setattr(mod, "allocate_lanes", fake_allocate)

    plans = mod._plan_vehicle_connections_for_approach(
        pos=0,
        in_edge_id="Edge.In",
        s_count=4,
        L_target=None,
        T_target=None,
        R_target=("Edge.Right", 2),
        U_target=None,
        approach=mod.ApproachInfo(road="main", direction="EB"),
    )

    assert len(plans) == 4
    assert extract_connections(plans, "Edge.Right") == {
        '<connection from="Edge.In" to="Edge.Right" fromLane="4" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="3" toLane="1"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="2" toLane="2"/>',
        '<connection from="Edge.In" to="Edge.Right" fromLane="1" toLane="2"/>',
    }
