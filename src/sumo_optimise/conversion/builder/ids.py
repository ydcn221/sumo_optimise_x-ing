"""Identifier helpers for nodes, edges, and crossings."""
from __future__ import annotations

from typing import Literal, cast


MainDirection = Literal["EB", "WB"]
MinorOrientation = Literal["N", "S"]
MinorFlow = Literal["NB", "SB"]
MainSide = Literal["West", "East"]
Cardinal = Literal["N", "S", "E", "W"]
_MAIN_HALF_MAP = {
    "N": "MainN",
    "NORTH": "MainN",
    "MAINN": "MainN",
    "EB": "MainN",
    "S": "MainS",
    "SOUTH": "MainS",
    "MAINS": "MainS",
    "WB": "MainS",
}


def _cardinal(direction: str) -> Cardinal:
    """Normalise a free-form direction token to a single cardinal letter."""

    token = direction.strip().upper()
    mapping = {
        "NORTH": "N",
        "SOUTH": "S",
        "EAST": "E",
        "WEST": "W",
    }
    if token in mapping:
        return mapping[token]
    if token in {"N", "S", "E", "W"}:
        return token  # type: ignore[return-value]
    raise ValueError(f"Unsupported cardinal token: {direction!r}")


def _main_split_half(side: Cardinal, half: str) -> Cardinal:
    """Translate a split-half token for a main-road crossing."""

    token = half.strip().upper()
    if side in {"E", "W"}:  # halves align with north/south carriageways
        mapping = {
            "EB": "N",
            "WB": "S",
            "N": "N",
            "S": "S",
            "NORTH": "N",
            "SOUTH": "S",
        }
    else:  # halves align with east/west on minor approaches
        mapping = {
            "NB": "N",
            "SB": "S",
            "E": "E",
            "W": "W",
            "EAST": "E",
            "WEST": "W",
        }
    try:
        return mapping[token]  # type: ignore[return-value]
    except KeyError as exc:
        raise ValueError(f"Unsupported split token {half!r} for side {side!r}") from exc


def _minor_flow_token(flow: str, orientation: MinorOrientation) -> MinorFlow:
    """Normalise a minor-road flow descriptor."""

    token = flow.strip().upper()
    mapping = {
        "TO": {"N": "SB", "S": "NB"},
        "FROM": {"N": "NB", "S": "SB"},
        "NB": {"N": "NB", "S": "NB"},
        "SB": {"N": "SB", "S": "SB"},
    }
    if token not in mapping:
        raise ValueError(f"Unsupported minor flow token: {flow!r}")
    return mapping[token][orientation]  # type: ignore[return-value]


def _main_half_suffix(token: str) -> str:
    return _MAIN_HALF_MAP[token]


def main_node_id(pos: int, half: str) -> str:
    """Return the ID for a main-road breakpoint node."""

    token = half.strip().upper()
    if token not in _MAIN_HALF_MAP:
        raise ValueError(f"Unsupported main-half token: {half!r}")
    suffix = _main_half_suffix(token)
    return f"Node.{pos}.{suffix}"


def main_edge_id(direction: MainDirection, begin_pos: int, end_pos: int) -> str:
    """Return the ID for a main-road edge oriented along ``direction``.

    ``begin_pos``/``end_pos`` follow the travel direction. Eastbound edges must
    increase in position while westbound edges must decrease. Violations raise
    :class:`ValueError` to catch upstream ordering mistakes early.
    """

    if direction == "EB":
        if not begin_pos < end_pos:
            raise ValueError(
                f"Eastbound edges require begin_pos < end_pos (got {begin_pos} ≥ {end_pos})"
            )
    elif direction == "WB":
        if not begin_pos > end_pos:
            raise ValueError(
                f"Westbound edges require begin_pos > end_pos (got {begin_pos} ≤ {end_pos})"
            )
    else:
        raise ValueError(f"Unsupported main direction: {direction!r}")
    return f"Edge.Main.{direction}.{begin_pos}-{end_pos}"


def minor_end_node_id(pos: int, orientation: MinorOrientation) -> str:
    """Return the dead-end node ID for a minor approach."""

    cardinal = _cardinal(orientation)
    label = "MinorNEdge" if cardinal == "N" else "MinorSEdge"
    return f"Node.{pos}.{label}"


def minor_edge_id(pos: int, flow: str, orientation: MinorOrientation) -> str:
    """Return the edge ID for a minor approach in the requested flow direction."""

    cardinal = _cardinal(orientation)
    if cardinal not in {"N", "S"}:
        raise ValueError(f"Unsupported minor orientation: {orientation!r}")
    flow_token = _minor_flow_token(flow, cast(MinorOrientation, cardinal))
    prefix = "MinorN" if cardinal == "N" else "MinorS"
    return f"Edge.{prefix}.{flow_token}.{pos}"


def cluster_id(pos: int) -> str:
    """Return the ID for a cluster join node at position ``pos``."""

    return f"Cluster.{pos}.Main"


def crossing_id_minor(pos: int, orientation: MinorOrientation) -> str:
    """Return the crossing ID for a minor-road approach."""

    cardinal = _cardinal(orientation)
    return f"Cross.{pos}.{cardinal}"


def crossing_id_main(pos: int, side: MainSide) -> str:
    """Return the crossing ID for the main road on the given side."""

    cardinal = _cardinal(side)
    return f"Cross.{pos}.{cardinal}"


def crossing_id_main_split(pos: int, side: MainSide, half: str) -> str:
    """Return the split crossing ID for the main road (refuge island present)."""

    cardinal = _cardinal(side)
    half_cardinal = _main_split_half(cardinal, half)
    return f"Cross.{pos}.{cardinal}.{half_cardinal}"


def crossing_id_midblock(pos: int) -> str:
    """Return the crossing ID for an unsplit mid-block crossing."""

    return f"CrossMid.{pos}"


def crossing_id_midblock_split(pos: int, half: str) -> str:
    """Return the split crossing ID for a mid-block crossing."""

    half_cardinal = _main_split_half("E", half)  # treat halves as north/south
    return f"CrossMid.{pos}.{half_cardinal}"
