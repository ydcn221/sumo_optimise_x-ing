"""Semantic validation mirroring the legacy implementation."""
from __future__ import annotations

from typing import Dict, List, Tuple

from ..domain.models import MainRoadConfig, SignalProfileDef, SnapRule
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.snap import grid_upper_bound, round_position
from ..utils.errors import SemanticValidationError
from ..utils.movements import ALLOWED_LANE_SYMBOLS
from ..utils.logging import get_logger

LOG = get_logger()


def validate_semantics(
    spec_json: Dict,
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
) -> None:
    def norm_type(etype: str) -> str:
        return "junction" if etype in ("tee", "cross") else etype

    length = float(main_road.length_m)
    grid_max = grid_upper_bound(length, snap_rule.step_m)
    errors: List[str] = []
    warnings: List[str] = []

    def _validate_lane_movements(idx: int, etype: str, lane_movements) -> None:
        allowed_keys = {"main", "EB", "WB", "minor", "NB", "SB"}

        if not isinstance(lane_movements, dict):
            errors.append(f"[VAL] E109 lane_movements must be an object: index={idx} type={etype}")
            return

        keys = set(lane_movements.keys())
        invalid_keys = keys - allowed_keys
        if invalid_keys:
            errors.append(
                f"[VAL] E109 invalid lane_movements keys: index={idx} type={etype} keys={','.join(sorted(invalid_keys))}"
            )

        if "main" in keys and ({"EB", "WB"} & keys):
            errors.append(f"[VAL] E112 lane_movements main conflicts with EB/WB: index={idx} type={etype}")
        if "minor" in keys and ({"NB", "SB"} & keys):
            errors.append(f"[VAL] E112 lane_movements minor conflicts with NB/SB: index={idx} type={etype}")

        for approach, templates in lane_movements.items():
            if approach not in allowed_keys:
                continue
            if not isinstance(templates, list) or len(templates) == 0:
                errors.append(
                    f"[VAL] E109 lane_movements[{approach}] must be a non-empty array: index={idx} type={etype}"
                )
                continue
            seen_counts = set()
            for template in templates:
                if not isinstance(template, list) or len(template) == 0:
                    errors.append(
                        f"[VAL] E110 lane_movements[{approach}] entry must be a non-empty array: index={idx} type={etype}"
                    )
                    continue
                lane_count = len(template)
                if lane_count in seen_counts:
                    errors.append(
                        f"[VAL] E111 duplicate lane_movements for lane_count={lane_count}: index={idx} type={etype} key={approach}"
                    )
                seen_counts.add(lane_count)
                for label in template:
                    label_str = str(label)
                    upper = label_str.upper()
                    if not upper or any(ch not in ALLOWED_LANE_SYMBOLS for ch in upper):
                        errors.append(
                            f"[VAL] E110 invalid lane label in lane_movements[{approach}]: index={idx} label={label_str}"
                        )

    layout = spec_json.get("layout", []) or []
    seen: Dict[Tuple[str, int], int] = {}

    pos_to_junction: Dict[int, Dict] = {}
    pos_to_midblocks: Dict[int, List[Tuple[int, float, int]]] = {}
    all_midblocks: List[Tuple[int, float, int]] = []

    for idx, e in enumerate(layout):
        try:
            etype = str(e["type"])
            pos_raw = float(e["pos_m"])
        except Exception:
            errors.append(f"[VAL] E000 unknown layout item at index={idx}: {e!r}")
            continue

        if not (0.0 <= pos_raw <= length):
            errors.append(f"[VAL] E101 out-of-range (raw): index={idx} type={etype} pos_m={pos_raw} valid=[0,{length}]")
            continue

        pos_snap = round_position(pos_raw, snap_rule.step_m, snap_rule.tie_break)
        if not (0 <= pos_snap <= grid_max):
            errors.append(
                f"[VAL] E102 out-of-range (snapped): index={idx} type={etype} raw={pos_raw} snap={pos_snap} valid=[0,{grid_max}]"
            )

        if pos_snap in (0, grid_max):
            warnings.append(f"[VAL] W201 snapped position at endpoint: index={idx} type={etype} snap={pos_snap}")

        if etype in ("tee", "cross"):
            required_geometry_keys = [
                "main_approach_begin_m",
                "main_approach_lanes",
                "minor_lanes_approach",
                "minor_lanes_departure",
                "median_continuous",
            ]
            missing_geom = [key for key in required_geometry_keys if key not in e]
            if missing_geom:
                errors.append(
                    f"[VAL] E103 junction geometry missing fields: index={idx} type={etype} missing={','.join(sorted(missing_geom))}"
                )
            if etype == "tee":
                branch = e.get("branch")
                if branch not in ("north", "south"):
                    errors.append(f"[VAL] E104 tee.branch invalid: index={idx} branch={branch}")

            signalized = e.get("signalized")
            has_signal = bool(e.get("signal"))
            if signalized is True and not has_signal:
                errors.append(f"[VAL] E105 signal required but missing: index={idx} type={etype}")
            if signalized is False and has_signal:
                errors.append(f"[VAL] E105 signal must be absent when signalized=false: index={idx} type={etype}")

            refuge = bool(e.get("refuge_island_on_main"))
            two_stage_present = "two_stage_tll_control" in e
            if signalized is True and refuge:
                if not two_stage_present:
                    errors.append(
                        "[VAL] E305 two_stage_tll_control must be provided when signalized=true and refuge_island_on_main=true: "
                        f"index={idx} type={etype}"
                    )
            else:
                if two_stage_present:
                    errors.append(
                        "[VAL] E305 two_stage_tll_control is only allowed when signalized=true and refuge_island_on_main=true: "
                        f"index={idx} type={etype}"
                    )

            if signalized is True:
                sig = e.get("signal") or {}
                pid = sig.get("profile_id")
                kind = "tee" if etype == "tee" else "cross"
                exists = bool(pid) and (pid in signal_profiles_by_kind.get(kind, {}))
                if not exists:
                    errors.append(
                        f"[VAL] E107 unknown signal profile or kind mismatch: index={idx} type={etype} profile_id={pid} "
                        f"expected_kind={kind}"
                    )

            if "lane_movements" in e:
                _validate_lane_movements(idx, etype, e.get("lane_movements"))

            pos_to_junction[pos_snap] = {
                "index": idx,
                "placement": (e.get("main_ped_crossing_placement") or {"west": False, "east": False}),
            }

        elif etype == "xwalk_midblock":
            signalized = e.get("signalized")
            has_signal = bool(e.get("signal"))
            if signalized is True and not has_signal:
                errors.append(f"[VAL] E105 signal required but missing: index={idx} type={etype}")
            if signalized is False and has_signal:
                errors.append(f"[VAL] E105 signal must be absent when signalized=false: index={idx} type={etype}")

            refuge = bool(e.get("refuge_island_on_main"))
            two_stage_present = "two_stage_tll_control" in e
            if signalized is True and refuge:
                if not two_stage_present:
                    errors.append(
                        "[VAL] E305 two_stage_tll_control must be provided when signalized=true and refuge_island_on_main=true: "
                        f"index={idx} type={etype}"
                    )
            else:
                if two_stage_present:
                    errors.append(
                        "[VAL] E305 two_stage_tll_control is only allowed when signalized=true and refuge_island_on_main=true: "
                        f"index={idx} type={etype}"
                    )

            if signalized is True:
                sig = e.get("signal") or {}
                pid = sig.get("profile_id")
                kind = "xwalk_midblock"
                exists = bool(pid) and (pid in signal_profiles_by_kind.get(kind, {}))
                if not exists:
                    errors.append(
                        f"[VAL] E107 unknown signal profile or kind mismatch: index={idx} type={etype} profile_id={pid} "
                        f"expected_kind={kind}"
                    )

            pos_to_midblocks.setdefault(pos_snap, []).append((idx, pos_raw, pos_snap))
            all_midblocks.append((idx, pos_raw, pos_snap))

        else:
            errors.append(f"[VAL] E000 unknown event type: index={idx} type={etype}")

        key = (norm_type(etype), pos_snap)
        seen[key] = seen.get(key, 0) + 1

    for (ntype, pos_snap), cnt in seen.items():
        if cnt >= 2:
            if ntype == "xwalk_midblock" and pos_snap in pos_to_junction:
                continue
            errors.append(f"[VAL] E106 duplicated events at same snapped position: type={ntype} pos={pos_snap} count={cnt}")

    step = snap_rule.step_m
    for jpos, jinfo in pos_to_junction.items():
        colliders = pos_to_midblocks.get(jpos, [])
        if colliders:
            side_to_indices: Dict[str, List[int]] = {"west": [], "east": []}
            for (idx, raw, _snap) in colliders:
                side = decide_midblock_side_for_collision(raw, jpos, snap_rule.tie_break)
                side_to_indices[side].append(idx)

            for side, idxs in side_to_indices.items():
                if len(idxs) >= 2:
                    errors.append(
                        f"[VAL] E108 junction-midblock collision causes duplicate crossing: pos={jpos} side={side} "
                        f"midblock_indices={','.join(map(str, idxs))}"
                    )

            near_west = {jpos - step, jpos - 2 * step}
            near_east = {jpos + step, jpos + 2 * step}
            near_west_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_west and snap >= 0]
            near_east_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_east and snap <= grid_max]

            if len(side_to_indices["west"]) >= 1 and len(near_west_idxs) >= 1:
                errors.append(
                    f"[VAL] E108 additional nearby midblock on absorbed side (west): pos={jpos} "
                    f"colliders={','.join(map(str, side_to_indices['west']))} near={','.join(map(str, near_west_idxs))}"
                )
            if len(side_to_indices["east"]) >= 1 and len(near_east_idxs) >= 1:
                errors.append(
                    f"[VAL] E108 additional nearby midblock on absorbed side (east): pos={jpos} "
                    f"colliders={','.join(map(str, side_to_indices['east']))} near={','.join(map(str, near_east_idxs))}"
                )
        else:
            near_west = {jpos - step, jpos - 2 * step}
            near_east = {jpos + step, jpos + 2 * step}
            west_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_west and snap >= 0]
            east_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_east and snap <= grid_max]

            if len(west_idxs) >= 2:
                errors.append(
                    f"[VAL] E108 multiple nearby midblocks on west side around junction: pos={jpos} "
                    f"indices={','.join(map(str, west_idxs))}"
                )
            if len(east_idxs) >= 2:
                errors.append(
                    f"[VAL] E108 multiple nearby midblocks on east side around junction: pos={jpos} "
                    f"indices={','.join(map(str, east_idxs))}"
                )

    if warnings:
        for msg in warnings:
            LOG.warning(msg)
    if errors:
        for msg in errors:
            LOG.error(msg)
        raise SemanticValidationError(f"semantic validation failed with {len(errors)} error(s)")
