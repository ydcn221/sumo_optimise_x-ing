"""PlainXML ``<tlLogics>`` emission derived from signal profiles."""
from __future__ import annotations

from typing import Dict, List

from ..builder.ids import cluster_id
from ..domain.models import Cluster, SignalProfileDef
from ..utils.logging import get_logger

LOG = get_logger()


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def render_tllogics_xml(
    clusters: List[Cluster],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
) -> str:
    """Render ``<tlLogics>`` based on signal references attached to clusters."""

    lines: List[str] = []
    lines.append("<tlLogics>")

    rendered = 0

    for cluster in clusters:
        tl_events = [
            ev
            for ev in cluster.events
            if bool(ev.signalized) and ev.signal is not None and ev.type.value in signal_profiles_by_kind
        ]
        if not tl_events:
            continue
        tl_id = cluster_id(cluster.pos_m)
        for event in tl_events:
            signal_ref = event.signal
            if signal_ref is None:
                continue
            profiles = signal_profiles_by_kind.get(event.type.value, {})
            profile = profiles.get(signal_ref.profile_id)
            if profile is None:
                LOG.warning(
                    "[BUILD] missing signal profile for tlLogics emission: cluster=%s profile_id=%s kind=%s",
                    tl_id,
                    signal_ref.profile_id,
                    event.type.value,
                )
                continue
            lines.append(
                f'  <tlLogic id="{tl_id}" type="static" programID="{profile.id}" offset="{signal_ref.offset_s}">'  # noqa: E501
            )
            lines.append(f"    <param key=\"event_kind\" value=\"{event.type.value}\"/>")
            lines.append(f'    <param key="cycle_s" value="{profile.cycle_s}"/>')
            lines.append(f'    <param key="ped_red_offset_s" value="{profile.ped_red_offset_s}"/>')
            lines.append(f'    <param key="yellow_duration_s" value="{profile.yellow_duration_s}"/>')
            conflicts = profile.pedestrian_conflicts
            lines.append(f'    <param key="pedestrian_conflicts.left" value="{_bool_str(conflicts.left)}"/>')
            lines.append(f'    <param key="pedestrian_conflicts.right" value="{_bool_str(conflicts.right)}"/>')
            for phase in profile.phases:
                state = " ".join(phase.allow_movements)
                lines.append(f'    <phase duration="{phase.duration_s}" state="{state}"/>')
            lines.append("  </tlLogic>")
            rendered += 1

    lines.append("</tlLogics>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered tlLogics (%d logic(s))", rendered)
    return xml


__all__ = ["render_tllogics_xml"]
