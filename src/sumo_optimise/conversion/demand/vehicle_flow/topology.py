"""Topology helpers for vehicle demand propagation."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from ...builder.ids import cluster_id
from ...domain.models import CardinalDirection, Cluster, EventKind, SideMinor
from ...utils.errors import DemandValidationError


@dataclass(frozen=True)
class VehicleClusterMeta:
    pos: int
    has_north_minor: bool
    has_south_minor: bool


@dataclass(frozen=True)
class VehicleNetwork:
    positions: List[int]
    index_by_pos: Dict[int, int]
    cluster_meta: Dict[int, VehicleClusterMeta]
    min_pos: int
    max_pos: int

    def next_position(self, pos: int, direction: CardinalDirection) -> int | None:
        idx = self.index_by_pos[pos]
        if direction == CardinalDirection.EAST:
            return self.positions[idx + 1] if idx + 1 < len(self.positions) else None
        if direction == CardinalDirection.WEST:
            return self.positions[idx - 1] if idx - 1 >= 0 else None
        raise ValueError(f"unsupported direction for mainline traversal: {direction}")

    def meta(self, pos: int) -> VehicleClusterMeta:
        return self.cluster_meta.get(pos, VehicleClusterMeta(pos=pos, has_north_minor=False, has_south_minor=False))


def build_vehicle_network(
    breakpoints: Sequence[int],
    clusters: Sequence[Cluster],
) -> VehicleNetwork:
    if not breakpoints:
        raise DemandValidationError("cannot build vehicle demand network without breakpoints")
    positions = sorted(dict.fromkeys(int(pos) for pos in breakpoints))
    cluster_meta = _build_cluster_meta(clusters)
    index_by_pos = {pos: idx for idx, pos in enumerate(positions)}
    return VehicleNetwork(
        positions=positions,
        index_by_pos=index_by_pos,
        cluster_meta=cluster_meta,
        min_pos=positions[0],
        max_pos=positions[-1],
    )


def _build_cluster_meta(clusters: Sequence[Cluster]) -> Dict[int, VehicleClusterMeta]:
    meta: Dict[int, VehicleClusterMeta] = {}
    for cluster in clusters:
        north, south = _branches_for_cluster(cluster)
        meta[cluster.pos_m] = VehicleClusterMeta(pos=cluster.pos_m, has_north_minor=north, has_south_minor=south)
    return meta


def _branches_for_cluster(cluster: Cluster) -> tuple[bool, bool]:
    junction_events = [ev for ev in cluster.events if ev.type in (EventKind.TEE, EventKind.CROSS)]
    if not junction_events:
        return False, False
    if any(ev.type == EventKind.CROSS for ev in junction_events):
        return True, True
    branch = junction_events[0].branch.value if junction_events[0].branch else None
    if branch == SideMinor.NORTH.value:
        return True, False
    if branch == SideMinor.SOUTH.value:
        return False, True
    return False, False


_MINOR_TEMPLATE_ALIAS = re.compile(r"^VEHEND_MINOR_(?P<pos>-?\d+)_(?P<branch>[NS])_END$")


def canonicalize_vehicle_endpoint(
    endpoint_id: str,
    *,
    network: VehicleNetwork,
    prefer_departing_half: bool,
) -> str:
    """Resolve aliases such as Node.Main.E_end to concrete node identifiers."""

    template_alias = _canonicalize_vehicle_template_alias(endpoint_id, network, prefer_departing_half)
    if template_alias:
        return template_alias

    tokens = endpoint_id.split(".")
    if len(tokens) >= 3 and tokens[0] == "Node" and tokens[1] == "Main":
        suffix = tokens[-1].upper()
        if suffix in {"N", "S"}:
            return endpoint_id
        if suffix in {"E_END", "W_END"}:
            half = _main_half_for_alias(suffix, prefer_departing_half)
            pos = network.max_pos if suffix.startswith("E") else network.min_pos
            return f"Node.Main.{pos}.{half}"
        if suffix in {"E_END_N", "E_END_S", "W_END_N", "W_END_S"}:
            side_token, half = suffix.split("_END_")
            pos = network.max_pos if side_token == "E" else network.min_pos
            return f"Node.Main.{pos}.{half}"
    if len(tokens) == 4 and tokens[0] == "Node" and tokens[1] == "Minor":
        return endpoint_id
    raise DemandValidationError(f"unsupported vehicle endpoint identifier: {endpoint_id}")


def _canonicalize_vehicle_template_alias(
    endpoint_id: str,
    network: VehicleNetwork,
    prefer_departing_half: bool,
) -> str | None:
    token = endpoint_id.strip()
    if not token:
        return None
    normalized = token.upper()
    if normalized == "VEHEND_MAIN_W_END":
        half = _main_half_for_alias("W_END", prefer_departing_half)
        return f"Node.Main.{network.min_pos}.{half}"
    if normalized == "VEHEND_MAIN_E_END":
        half = _main_half_for_alias("E_END", prefer_departing_half)
        return f"Node.Main.{network.max_pos}.{half}"

    match = _MINOR_TEMPLATE_ALIAS.match(normalized)
    if match:
        pos = int(match.group("pos"))
        branch = match.group("branch")
        suffix = "N_end" if branch == "N" else "S_end"
        return f"Node.Minor.{pos}.{suffix}"
    return None


def _main_half_for_alias(alias: str, prefer_departing_half: bool) -> str:
    if alias.startswith("E"):
        return "S" if prefer_departing_half else "N"
    if alias.startswith("W"):
        return "N" if prefer_departing_half else "S"
    raise DemandValidationError(f"unknown main endpoint alias: {alias}")


def vehicle_cluster_id(pos: int) -> str:
    return cluster_id(pos)


__all__ = [
    "VehicleClusterMeta",
    "VehicleNetwork",
    "build_vehicle_network",
    "canonicalize_vehicle_endpoint",
    "vehicle_cluster_id",
]
