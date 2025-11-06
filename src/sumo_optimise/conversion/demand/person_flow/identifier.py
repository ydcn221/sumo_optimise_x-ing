"""Shared helpers for demand-side pedestrian endpoint identifiers."""
from __future__ import annotations

from typing import Optional, Tuple

from ...domain.models import PedestrianSide

_SIDE_SUFFIX = {
    PedestrianSide.EAST_SIDE: "EastSide",
    PedestrianSide.WEST_SIDE: "WestSide",
}

_SIDE_LOOKUP = {value: key for key, value in _SIDE_SUFFIX.items()}


def minor_endpoint_id(pos: int, orientation: str, side: PedestrianSide) -> str:
    card = orientation.strip().upper()
    if card not in {"N", "S"}:
        raise ValueError(f"Unsupported minor orientation: {orientation!r}")
    suffix = _SIDE_SUFFIX.get(side)
    if suffix is None:
        raise ValueError(f"Unsupported minor side: {side!r}")
    prefix = "MinorN" if card == "N" else "MinorS"
    return f"Node.{pos}.{prefix}Edge.{suffix}"


def parse_minor_endpoint_id(endpoint_id: str) -> Optional[Tuple[int, str, PedestrianSide]]:
    tokens = endpoint_id.split(".")
    if len(tokens) != 4:
        return None
    if tokens[0] != "Node":
        return None
    try:
        pos = int(tokens[1])
    except ValueError:
        return None
    orientation_token = tokens[2]
    if orientation_token not in {"MinorNEdge", "MinorSEdge"}:
        return None
    suffix = tokens[3]
    side = _SIDE_LOOKUP.get(suffix)
    if side is None:
        return None
    orientation = "N" if orientation_token == "MinorNEdge" else "S"
    return pos, orientation, side


__all__ = ["minor_endpoint_id", "parse_minor_endpoint_id"]
