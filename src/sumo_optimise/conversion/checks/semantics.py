"""Semantic validation mirroring the legacy implementation."""
from __future__ import annotations

from typing import Dict, List, Tuple

from ..domain.models import JunctionTemplate, MainRoadConfig, SignalProfileDef, SnapRule
from ..planner.crossings import decide_midblock_side_for_collision
from ..planner.snap import grid_upper_bound, round_position
from ..utils.errors import SemanticValidationError
from ..utils.logging import get_logger

LOG = get_logger()


def validate_semantics(
    spec_json: Dict,
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    junction_template_by_id: Dict[str, JunctionTemplate],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
) -> None:
    def norm_type(etype: str) -> str:
        return "junction" if etype in ("tee", "cross") else etype

    length = float(main_road.length_m)
    grid_max = grid_upper_bound(length, snap_rule.step_m)
    errors: List[str] = []
    warnings: List[str] = []

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
            tpl_id = e.get("template")
            if not tpl_id or tpl_id not in junction_template_by_id:
                errors.append(f"[VAL] E103 template missing/unknown: index={idx} type={etype} template={tpl_id}")
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
                if not two_stage_present or not bool(e.get("two_stage_tll_control")):
                    errors.append(
                        "[VAL] E305 two_stage_tll_control must be true when signalized and refuge island are enabled: "
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
                if not two_stage_present or not bool(e.get("two_stage_tll_control")):
                    errors.append(
                        "[VAL] E305 two_stage_tll_control must be true when signalized and refuge island are enabled: "
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
