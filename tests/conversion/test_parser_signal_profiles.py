import pytest

from sumo_optimise.conversion.domain.models import EventKind, PedestrianConflictConfig
from sumo_optimise.conversion.parser.spec_loader import parse_signal_profiles
from sumo_optimise.conversion.utils.errors import SemanticValidationError


def _phases(*durations):
    return [
        {"duration_s": dur, "allow_movements": []}
        for dur in durations
    ]


def _spec_with_profile(kind: EventKind, profile):
    return {
        "signal_profiles": {
            EventKind.TEE.value: profile if kind == EventKind.TEE else [],
            EventKind.CROSS.value: profile if kind == EventKind.CROSS else [],
            EventKind.XWALK_MIDBLOCK.value: profile if kind == EventKind.XWALK_MIDBLOCK else [],
        }
    }


def test_parse_signal_profiles_with_conflicts():
    profile = [
        {
            "id": "tee_profile",
            "cycle_s": 32,
            "ped_red_offset_s": 7,
            "yellow_duration_s": 2,
            "phases": _phases(10, 20),
            "pedestrian_conflicts": {"left": True, "right": False},
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.TEE.value]["tee_profile"]
    assert parsed.pedestrian_conflicts == PedestrianConflictConfig(left=True, right=False)
    assert parsed.ped_red_offset_s == 7
    assert parsed.yellow_duration_s == 2


def test_parse_signal_profiles_defaults_when_missing():
    profile = [
        {
            "id": "xwalk_profile",
            "cycle_s": 40,
            "phases": _phases(20, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.XWALK_MIDBLOCK, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.XWALK_MIDBLOCK.value]["xwalk_profile"]
    assert parsed.pedestrian_conflicts == PedestrianConflictConfig(left=False, right=False)
    assert parsed.ped_red_offset_s == 0
    assert parsed.yellow_duration_s == 0


def test_parse_signal_profiles_rejects_invalid_timings():
    profile = [
        {
            "id": "bad_profile",
            "cycle_s": 30,
            "ped_red_offset_s": -1,
            "yellow_duration_s": 5,
            "phases": _phases(10, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)
