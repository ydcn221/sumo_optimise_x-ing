"""Planner that maps layout events to a deterministic corridor plan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from ..domain import models


@dataclass(slots=True)
class JunctionPlan:
    event: models.LayoutEvent
    template: models.JunctionTemplate | None


@dataclass(slots=True)
class CorridorPlan:
    spec: models.CorridorSpec
    snapped_events: Sequence[JunctionPlan]
    breakpoints: Sequence[models.BreakPoint]
    overlays: Sequence[models.LaneOverlay]


class PlanningError(RuntimeError):
    """Raised when semantic rules are violated."""


def build_corridor_plan(spec: models.CorridorSpec) -> CorridorPlan:
    _validate_layout(spec)
    snapped = _snap_events(spec)
    breakpoints = _collect_breakpoints(spec, snapped)
    overlays = _collect_overlays(snapped)
    return CorridorPlan(spec=spec, snapped_events=snapped, breakpoints=breakpoints, overlays=overlays)


def _validate_layout(spec: models.CorridorSpec) -> None:
    last = -float("inf")
    for event in spec.layout:
        if event.pos_m < 0 or event.pos_m > spec.main_road.length_m:
            raise PlanningError(f"layout event at {event.pos_m} outside main corridor")
        if event.pos_m < last:
            raise PlanningError("layout events must be sorted by pos_m")
        last = event.pos_m
        if event.template_id:
            templates = spec.junction_templates.get(event.kind)
            if not templates or event.template_id not in templates:
                raise PlanningError(f"Unknown template id {event.template_id} for {event.kind.value}")
        if event.signalized and event.signal:
            profiles = spec.signal_profiles.get(event.kind)
            if not profiles or event.signal.profile_id not in profiles:
                raise PlanningError(f"Unknown signal profile {event.signal.profile_id} for {event.kind.value}")


def _snap_events(spec: models.CorridorSpec) -> List[JunctionPlan]:
    step = spec.snap.step_m
    snapped: List[JunctionPlan] = []
    for event in spec.layout:
        snapped_pos = _snap_value(event.pos_m, step, spec.snap.tie_break, spec.main_road.length_m)
        template = None
        if event.template_id:
            template = spec.junction_templates[event.kind][event.template_id]
        snapped.append(
            JunctionPlan(
                event=models.LayoutEvent(
                    kind=event.kind,
                    pos_m=snapped_pos,
                    template_id=event.template_id,
                    branch=event.branch,
                    signalized=event.signalized,
                    signal=event.signal,
                    split_ped_crossing_on_main=event.split_ped_crossing_on_main,
                    main_ped_crossing_placement=event.main_ped_crossing_placement,
                ),
                template=template,
            )
        )
    snapped.sort(key=lambda plan: plan.event.pos_m)
    return snapped


def _collect_breakpoints(spec: models.CorridorSpec, plans: Sequence[JunctionPlan]) -> List[models.BreakPoint]:
    points: Dict[float, List[str]] = {0.0: ["origin"], spec.main_road.length_m: ["terminus"]}
    for plan in plans:
        pos = plan.event.pos_m
        reasons = points.setdefault(pos, [])
        reasons.append(plan.event.kind.value)
        template = plan.template
        if template and template.main_approach_begin_m > 0:
            start = max(0.0, pos - template.main_approach_begin_m)
            reasons = points.setdefault(start, [])
            reasons.append("main_overlay_start")
    return [
        models.BreakPoint(pos_m=pos, reasons=tuple(sorted(reasons)))
        for pos, reasons in sorted(points.items())
    ]


def _collect_overlays(plans: Sequence[JunctionPlan]) -> List[models.LaneOverlay]:
    overlays: List[models.LaneOverlay] = []
    for plan in plans:
        template = plan.template
        if not template or template.main_approach_lanes <= 0 or template.main_approach_begin_m <= 0:
            continue
        start = plan.event.pos_m - template.main_approach_begin_m
        overlays.append(
            models.LaneOverlay(
                start_m=max(0.0, start),
                end_m=plan.event.pos_m,
                lanes=template.main_approach_lanes,
            )
        )
    overlays.sort(key=lambda ov: (ov.start_m, ov.end_m))
    return overlays


def _snap_value(value: float, step: int, tie_break: models.TieBreak, max_value: float) -> float:
    ratio = value / step
    lower = int(ratio) * step
    upper = lower + step
    if value - lower < upper - value:
        snapped = lower
    elif value - lower > upper - value:
        snapped = upper
    else:
        snapped = lower if tie_break == models.TieBreak.TOWARD_WEST else upper
    snapped = max(0.0, min(max_value, float(snapped)))
    return snapped
