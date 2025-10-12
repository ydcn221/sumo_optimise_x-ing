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


def test_allocate_lanes_invalid_range():
    with pytest.raises(ValueError):
        allocate_lanes(1, 0, 2, 0, 0)
