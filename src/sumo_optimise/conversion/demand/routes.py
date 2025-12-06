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

    def _begin_time(fragment: str) -> float:
        """Extract the begin= value so SUMO sees routes sorted by departure."""

        marker = ' begin="'
        start = fragment.find(marker)
        if start == -1:
            return 0.0
        start += len(marker)
        end = fragment.find('"', start)
        if end == -1:
            return 0.0
        try:
            return float(fragment[start:end])
        except ValueError:
            return 0.0

    fragments.sort(key=_begin_time)

    lines = [
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '        xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
        *fragments,
        "</routes>",
    ]
    return "\n".join(lines) + "\n"


__all__ = ["render_routes_document"]
