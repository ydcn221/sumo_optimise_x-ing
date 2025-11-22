import pytest

from sumo_optimise.conversion.domain.models import EventKind
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


def test_parse_signal_profiles_parses_intersection_profile():
    profile = [
        {
            "id": "tee_profile",
            "cycle_s": 30,
            "ped_early_cutoff_s": 7,
            "yellow_duration_s": 2,
            "phases": _phases(10, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.TEE.value]["tee_profile"]
    assert parsed.ped_early_cutoff_s == 7
    assert parsed.yellow_duration_s == 2


def test_parse_signal_profiles_reject_cycle_mismatch():
    profile = [
        {
            "id": "cycle_mismatch",
            "cycle_s": 35,
            "ped_early_cutoff_s": 2,
            "yellow_duration_s": 3,
            "phases": _phases(10, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)


def test_parse_signal_profiles_midblock_without_conflicts():
    profile = [
        {
            "id": "xwalk_profile",
            "cycle_s": 40,
            "ped_early_cutoff_s": 4,
            "yellow_duration_s": 3,
            "phases": _phases(20, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.XWALK_MIDBLOCK, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.XWALK_MIDBLOCK.value]["xwalk_profile"]
    assert parsed.ped_early_cutoff_s == 4
    assert parsed.yellow_duration_s == 3


def test_parse_signal_profiles_rejects_invalid_timings():
    profile = [
        {
            "id": "bad_profile",
            "cycle_s": 30,
            "ped_early_cutoff_s": -1,
            "yellow_duration_s": 5,
            "phases": _phases(10, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)


def test_parse_signal_profiles_require_ped_cutoff_for_midblock():
    profile = [
        {
            "id": "midblock_missing_ped_cutoff",
            "cycle_s": 35,
            "yellow_duration_s": 5,
            "phases": _phases(20, 15),
        }
    ]
    spec = _spec_with_profile(EventKind.XWALK_MIDBLOCK, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)


def test_parse_signal_profiles_allow_new_token_patterns():
    profile = [
        {
            "id": "cross_tokens",
            "cycle_s": 20,
            "ped_early_cutoff_s": 2,
            "yellow_duration_s": 3,
            "phases": [
                {"duration_s": 10, "allow_movements": ["EB_LTR_pg", "PedX_NESW"]},
                {"duration_s": 10, "allow_movements": ["NB_R_pg", "PedX_E_N-half", "PedX_N_E-half"]},
            ],
        }
    ]
    spec = _spec_with_profile(EventKind.CROSS, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.CROSS.value]["cross_tokens"]
    assert parsed.phases[0].allow_movements[0] == "EB_LTR_pg"
    assert parsed.phases[1].allow_movements[1] == "PedX_E_N-half"
    assert parsed.phases[1].allow_movements[2] == "PedX_N_E-half"


def test_parse_signal_profiles_accepts_pedx_shortcut():
    profile = [
        {
            "id": "ped_shortcut",
            "cycle_s": 12,
            "ped_early_cutoff_s": 3,
            "yellow_duration_s": 2,
            "phases": [
                {"duration_s": 6, "allow_movements": ["PedX", "PedX_S_W-half"]},
                {"duration_s": 6, "allow_movements": []},
            ],
        }
    ]
    spec = _spec_with_profile(EventKind.CROSS, profile)
    result = parse_signal_profiles(spec)
    parsed = result[EventKind.CROSS.value]["ped_shortcut"]
    assert parsed.phases[0].allow_movements[0] == "PedX"
    assert parsed.phases[0].allow_movements[1] == "PedX_S_W-half"


def test_parse_signal_profiles_require_timings():
    profile = [
        {
            "id": "timing_missing",
            "cycle_s": 40,
            "phases": _phases(20, 20),
        }
    ]
    spec = _spec_with_profile(EventKind.TEE, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)


def test_parse_signal_profiles_reject_duplicate_vehicle_turns():
    profile = [
        {
            "id": "dup_turns",
            "cycle_s": 20,
            "ped_early_cutoff_s": 2,
            "yellow_duration_s": 3,
            "phases": [
                {"duration_s": 10, "allow_movements": ["EB_LL"]},
                {"duration_s": 10, "allow_movements": []},
            ],
        }
    ]
    spec = _spec_with_profile(EventKind.CROSS, profile)
    with pytest.raises(SemanticValidationError):
        parse_signal_profiles(spec)
