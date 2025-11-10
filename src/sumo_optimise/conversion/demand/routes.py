"""Helpers for combining pedestrian and vehicle route fragments."""
from __future__ import annotations

from typing import Iterable, List, Sequence


def render_routes_document(
    *,
    person_entries: Sequence[str] | None,
    vehicle_entries: Sequence[str] | None,
) -> str | None:
    fragments: List[str] = []
    if vehicle_entries:
        fragments.extend(vehicle_entries)
    if person_entries:
        fragments.extend(person_entries)
    if not fragments:
        return None
    lines = [
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '        xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
        *fragments,
        "</routes>",
    ]
    return "\n".join(lines) + "\n"


__all__ = ["render_routes_document"]
