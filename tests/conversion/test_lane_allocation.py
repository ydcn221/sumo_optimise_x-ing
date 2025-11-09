import pytest

from sumo_optimise.conversion.emitters.connections import allocate_lanes


@pytest.mark.parametrize(
    ("s", "l", "t", "r", "u", "expected"),
    [
        (9, 2, 3, 1, 1, ["L", "L", "T", "T", "T", "", "", "R", "U"]),
        (8, 2, 3, 1, 3, ["L", "L", "T", "T", "T", "R", "U", "U"]),
        (6, 2, 3, 1, 2, ["L", "L", "T", "T", "T", "RU"]),
        (2, 2, 0, 0, 1, ["L", "LU"]),
        (6, 3, 2, 3, 1, ["L", "L", "T", "T", "R", "RU"]),
        (5, 2, 2, 2, 1, ["L", "T", "T", "R", "RU"]),
    ],
)
def test_allocate_lanes_matches_spec_examples(s, l, t, r, u, expected):
    assert allocate_lanes(s, l, t, r, u) == expected


def test_allocate_lanes_rejects_negative_inputs():
    with pytest.raises(ValueError):
        allocate_lanes(2, -1, 0, 0, 0)


def test_allocate_lanes_counts_shared_u_for_minimums():
    """When only an ``RU`` lane remains it should satisfy the R minimum."""

    assert allocate_lanes(3, 1, 1, 2, 1) == ["L", "T", "RU"]


def test_allocate_lanes_single_lane_combines_movements():
    assert allocate_lanes(1, 1, 1, 1, 1) == ["LTRU"]


def test_allocate_lanes_drop_share_case_e():
    assert allocate_lanes(3, 1, 3, 1, 0) == ["LT", "T", "TR"]


def test_allocate_lanes_drop_share_case_f():
    assert allocate_lanes(3, 1, 5, 1, 0) == ["LT", "T", "TR"]


def test_allocate_lanes_compact_shares_when_starved():
    assert allocate_lanes(3, 2, 3, 2, 3) == ["LT", "T", "TRU"]
