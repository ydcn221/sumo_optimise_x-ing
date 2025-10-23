"""Temporary tlLogic emitter stub."""
from __future__ import annotations

from typing import Dict, Sequence

from ..domain.models import Cluster, Defaults, JunctionTemplate, LaneOverride, MainRoadConfig, SnapRule


def render_tll_xml(
    *,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    lane_overrides: Sequence[LaneOverride],
    signal_profiles_by_kind: Dict[str, Dict[str, object]],
) -> str:
    """Return an empty tlLogic document.

    This stub keeps the pipeline wiring in place until the tlLogic
    generation is implemented.
    """

    _ = (
        defaults,
        clusters,
        breakpoints,
        junction_template_by_id,
        snap_rule,
        main_road,
        lane_overrides,
        signal_profiles_by_kind,
    )
    return ""
