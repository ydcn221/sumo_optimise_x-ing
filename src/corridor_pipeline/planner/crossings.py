"""Crossing placement helpers."""
from __future__ import annotations


def decide_midblock_side_for_collision(raw_pos: float, snapped_pos: int, tie_break: str) -> str:
    if raw_pos < float(snapped_pos):
        return "west"
    if raw_pos > float(snapped_pos):
        return "east"
    return "west" if tie_break == "toward_west" else "east"
