"""Shared helpers for demand-side pedestrian endpoint identifiers."""
from __future__ import annotations

from typing import Optional, Tuple

from ...domain.models import PedestrianSide

_SIDE_SUFFIX = {
    PedestrianSide.EAST_SIDE: "E_sidewalk",
    PedestrianSide.WEST_SIDE: "W_sidewalk",
}
_ORIENTATION_SUFFIX = {"N": "N_end", "S": "S_end"}
_SIDE_LOOKUP = {value: key for key, value in _SIDE_SUFFIX.items()}
_ORIENTATION_LOOKUP = {value: key for key, value in _ORIENTATION_SUFFIX.items()}
_MAIN_SIDEWALK_TOKENS = {"N_SIDEWALK": "N", "S_SIDEWALK": "S"}


def minor_endpoint_id(pos: int, orientation: str, side: PedestrianSide) -> str:
    card = orientation.strip().upper()
    if card not in {"N", "S"}:
        raise ValueError(f"Unsupported minor orientation: {orientation!r}")
    suffix = _SIDE_SUFFIX.get(side)
    if suffix is None:
        raise ValueError(f"Unsupported minor side: {side!r}")
    orientation_token = _ORIENTATION_SUFFIX[card]
    return f"PedEnd.Minor.{pos}.{orientation_token}.{suffix}"


def parse_minor_endpoint_id(endpoint_id: str) -> Optional[Tuple[int, str, PedestrianSide]]:
    tokens = endpoint_id.split(".")
    if len(tokens) != 5:
        return None
    if tokens[0] != "PedEnd" or tokens[1] != "Minor":
        return None
    try:
        pos = int(tokens[2])
    except ValueError:
        return None
    orientation_token = tokens[3]
    orientation = _ORIENTATION_LOOKUP.get(orientation_token)
    if orientation is None:
        return None
    suffix = tokens[4]
    side = _SIDE_LOOKUP.get(suffix)
    if side is None:
        return None
    return pos, orientation, side


def parse_main_ped_endpoint_id(endpoint_id: str) -> Optional[Tuple[str, str]]:
    tokens = endpoint_id.split(".")
    if len(tokens) != 4:
        return None
    if tokens[0] != "PedEnd" or tokens[1] != "Main":
        return None
    sidewalk = tokens[3].strip().upper()
    half = _MAIN_SIDEWALK_TOKENS.get(sidewalk)
    if half is None:
        return None
    pos_token = tokens[2]
    return pos_token, half


__all__ = ["minor_endpoint_id", "parse_minor_endpoint_id", "parse_main_ped_endpoint_id"]
