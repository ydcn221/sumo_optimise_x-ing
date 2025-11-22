"""Helpers for canonicalising lane movement labels."""
from __future__ import annotations

from typing import Iterable

LANE_MOVEMENT_ORDER = ("L", "T", "R", "U")
ALLOWED_LANE_SYMBOLS = set(LANE_MOVEMENT_ORDER)


def canonical_lane_label(label: str, *, allowed: Iterable[str] | None = None) -> str:
    """Return the lane label in canonical L→T→R→U order."""

    allowed_symbols = set(allowed) if allowed is not None else ALLOWED_LANE_SYMBOLS
    upper = str(label).upper()
    symbols = {ch for ch in upper if ch in allowed_symbols}
    return "".join(ch for ch in LANE_MOVEMENT_ORDER if ch in symbols)
