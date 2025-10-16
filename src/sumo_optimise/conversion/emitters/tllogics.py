"""PlainXML ``<tlLogics>`` emission derived from signal profiles."""
from __future__ import annotations

from typing import Dict, Iterable, List, Set

from ..domain.models import Cluster, SignalPhaseDef, SignalProfileDef
from ..utils.logging import get_logger
from ..utils.signals import cluster_has_signal_reference
from .connections import ClusterLinkIndexing, LinkIndexEntry

LOG = get_logger()


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


def _active_indices(allowed: Set[str], links: Iterable[LinkIndexEntry]) -> Set[int]:
    indices: Set[int] = set()
    for entry in links:
        if any(token in allowed for token in entry.tokens):
            indices.add(entry.link_index)
    return indices


def _phase_state(
    phase: SignalPhaseDef,
    indexing: ClusterLinkIndexing,
    profile: SignalProfileDef,
) -> str:
    allowed = set(phase.allow_movements)
    active = _active_indices(allowed, indexing.links)

    left_active = any(
        entry.link_index in active and entry.kind == "vehicle" and entry.movement == "L"
        for entry in indexing.links
    )
    right_active = any(
        entry.link_index in active and entry.kind == "vehicle" and entry.movement == "R"
        for entry in indexing.links
    )

    chars: List[str] = []
    for entry in indexing.links:
        is_active = entry.link_index in active
        if entry.kind == "vehicle":
            chars.append("G" if is_active else "r")
            continue

        if not is_active:
            chars.append("r")
            continue

        forced_red = False
        if "left" in entry.conflicts_with and not profile.pedestrian_conflicts.left and left_active:
            forced_red = True
        if "right" in entry.conflicts_with and not profile.pedestrian_conflicts.right and right_active:
            forced_red = True
        chars.append("r" if forced_red else "g")
    return "".join(chars)


def render_tllogics_xml(
    clusters: List[Cluster],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
    link_indexing: Dict[int, ClusterLinkIndexing],
) -> str:
    """Render ``<tlLogics>`` based on signal references attached to clusters."""

    lines: List[str] = ["<tlLogics>"]
    connection_lines: List[str] = []

    rendered = 0

    for cluster in clusters:
        if not cluster_has_signal_reference(cluster):
            continue
        tl_events = [
            ev
            for ev in cluster.events
            if ev.signalized is True and ev.signal is not None and ev.type.value in signal_profiles_by_kind
        ]
        if not tl_events:
            continue
        index_info = link_indexing.get(cluster.pos_m)
        if index_info is None:
            LOG.warning("[BUILD] signalised cluster lacks connection index mapping: pos=%s", cluster.pos_m)
            continue
        tl_id = index_info.tl_id
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
                state = _phase_state(phase, index_info, profile)
                lines.append(f'    <phase duration="{phase.duration_s}" state="{state}"/>')
            lines.append("  </tlLogic>")
            rendered += 1

    for pos in sorted(link_indexing):
        idx = link_indexing[pos]
        for entry in idx.links:
            if entry.kind == "vehicle" and entry.connection is not None:
                conn = entry.connection
                connection_lines.append(
                    f'  <connection from="{conn.from_edge}" to="{conn.to_edge}" '
                    f'fromLane="{conn.from_lane}" toLane="{conn.to_lane}" '
                    f'tl="{idx.tl_id}" linkIndex="{entry.link_index}"/>'
                )
            elif entry.kind == "pedestrian" and entry.crossing is not None:
                crossing = entry.crossing
                edges = " ".join(crossing.edges)
                connection_lines.append(
                    f'  <crossing id="{crossing.crossing_id}" node="{crossing.node}" edges="{edges}" '
                    f'width="{crossing.width:.3f}" tl="{idx.tl_id}" linkIndex="{entry.link_index}"/>'
                )

    lines.extend(connection_lines)
    lines.append("</tlLogics>")
    xml = "\n".join(lines) + "\n"
    LOG.info("rendered tlLogics (%d logic(s))", rendered)
    return xml


__all__ = ["render_tllogics_xml"]
