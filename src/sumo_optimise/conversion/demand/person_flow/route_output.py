"""Render SUMO personFlow XML from propagated pedestrian demand."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ...builder.ids import main_edge_id, minor_edge_id
from ...domain.models import DemandOptions, EndpointDemandRow, PersonFlowPattern, Defaults
from ...utils.errors import DemandValidationError


@dataclass(frozen=True)
class EndpointPlacement:
    edge_id: str
    is_start: bool
    length: float


class EndpointPlacementResolver:
    def __init__(
        self,
        *,
        breakpoints: Sequence[int],
        defaults: Defaults,
    ) -> None:
        self._defaults = defaults
        self._prev_by_pos: Dict[int, Optional[int]] = {}
        self._next_by_pos: Dict[int, Optional[int]] = {}
        ordered = list(breakpoints)
        for idx, pos in enumerate(ordered):
            self._prev_by_pos[pos] = ordered[idx - 1] if idx > 0 else None
            self._next_by_pos[pos] = ordered[idx + 1] if idx < len(ordered) - 1 else None

    def _parse_endpoint(self, endpoint_id: str) -> Tuple[int, str]:
        tokens = endpoint_id.split(".")
        if len(tokens) != 3 or tokens[0] != "Node":
            raise DemandValidationError(f"unsupported endpoint identifier: {endpoint_id}")
        try:
            pos = int(tokens[1])
        except ValueError as exc:
            raise DemandValidationError(f"endpoint {endpoint_id!r} has non-integer position token") from exc
        suffix = tokens[2]
        return pos, suffix

    def _main_north_depart(self, pos: int) -> EndpointPlacement:
        next_pos = self._next_by_pos.get(pos)
        if next_pos is not None:
            edge_id = main_edge_id("EB", pos, next_pos)
            length = float(next_pos - pos)
            return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)
        prev_pos = self._prev_by_pos.get(pos)
        if prev_pos is None:
            raise DemandValidationError(f"cannot resolve depart edge for Node.{pos}.MainN")
        edge_id = main_edge_id("EB", prev_pos, pos)
        length = float(pos - prev_pos)
        return EndpointPlacement(edge_id=edge_id, is_start=False, length=length)

    def _main_north_arrival(self, pos: int) -> EndpointPlacement:
        prev_pos = self._prev_by_pos.get(pos)
        if prev_pos is not None:
            edge_id = main_edge_id("EB", prev_pos, pos)
            length = float(pos - prev_pos)
            return EndpointPlacement(edge_id=edge_id, is_start=False, length=length)
        next_pos = self._next_by_pos.get(pos)
        if next_pos is None:
            raise DemandValidationError(f"cannot resolve arrival edge for Node.{pos}.MainN")
        edge_id = main_edge_id("EB", pos, next_pos)
        length = float(next_pos - pos)
        return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)

    def _main_south_depart(self, pos: int) -> EndpointPlacement:
        prev_pos = self._prev_by_pos.get(pos)
        if prev_pos is not None:
            edge_id = main_edge_id("WB", pos, prev_pos)
            length = float(pos - prev_pos)
            return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)
        next_pos = self._next_by_pos.get(pos)
        if next_pos is None:
            raise DemandValidationError(f"cannot resolve depart edge for Node.{pos}.MainS")
        edge_id = main_edge_id("WB", next_pos, pos)
        length = float(next_pos - pos)
        return EndpointPlacement(edge_id=edge_id, is_start=False, length=length)

    def _main_south_arrival(self, pos: int) -> EndpointPlacement:
        next_pos = self._next_by_pos.get(pos)
        if next_pos is not None:
            edge_id = main_edge_id("WB", next_pos, pos)
            length = float(next_pos - pos)
            return EndpointPlacement(edge_id=edge_id, is_start=False, length=length)
        prev_pos = self._prev_by_pos.get(pos)
        if prev_pos is None:
            raise DemandValidationError(f"cannot resolve arrival edge for Node.{pos}.MainS")
        edge_id = main_edge_id("WB", pos, prev_pos)
        length = float(pos - prev_pos)
        return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)

    def _minor_depart(self, pos: int, orientation: str) -> EndpointPlacement:
        edge_id = minor_edge_id(pos, "to", orientation)
        length = float(self._defaults.minor_road_length_m)
        return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)

    def _minor_arrival(self, pos: int, orientation: str) -> EndpointPlacement:
        edge_id = minor_edge_id(pos, "from", orientation)
        length = float(self._defaults.minor_road_length_m)
        return EndpointPlacement(edge_id=edge_id, is_start=False, length=length)

    def resolve_depart(self, endpoint_id: str) -> EndpointPlacement:
        pos, suffix = self._parse_endpoint(endpoint_id)
        if suffix == "MainN":
            return self._main_north_depart(pos)
        if suffix == "MainS":
            return self._main_south_depart(pos)
        if suffix == "MinorNEdge":
            return self._minor_depart(pos, "N")
        if suffix == "MinorSEdge":
            return self._minor_depart(pos, "S")
        raise DemandValidationError(f"unsupported endpoint for depart placement: {endpoint_id}")

    def resolve_arrival(self, endpoint_id: str) -> EndpointPlacement:
        pos, suffix = self._parse_endpoint(endpoint_id)
        if suffix == "MainN":
            return self._main_north_arrival(pos)
        if suffix == "MainS":
            return self._main_south_arrival(pos)
        if suffix == "MinorNEdge":
            return self._minor_arrival(pos, "N")
        if suffix == "MinorSEdge":
            return self._minor_arrival(pos, "S")
        raise DemandValidationError(f"unsupported endpoint for arrival placement: {endpoint_id}")


def _format_pattern_attribute(pattern: PersonFlowPattern, flow_per_hour: float) -> str:
    if flow_per_hour <= 0.0:
        raise DemandValidationError("cannot emit personFlow with non-positive demand")
    if pattern is PersonFlowPattern.PERSONS_PER_HOUR:
        return f'personsPerHour="{flow_per_hour:.6f}"'
    if pattern is PersonFlowPattern.PERIOD:
        period = 3600.0 / flow_per_hour
        return f'period="{period:.6f}"'
    if pattern is PersonFlowPattern.POISSON:
        lam = flow_per_hour / 3600.0
        return f'period="exp({lam:.6f})"'
    raise DemandValidationError(f"unsupported personFlow pattern {pattern}")


def _clamp_position(length: float, offset: float, *, at_start: bool) -> float:
    if length <= 0.0:
        return 0.0
    offset = max(min(offset, length / 2.0), 0.0)
    if at_start:
        return offset
    return max(length - offset, 0.0)


def render_person_flows(
    flows: Iterable[Tuple[str, str, float, EndpointDemandRow]],
    *,
    options: DemandOptions,
    breakpoints: Sequence[int],
    defaults: Defaults,
) -> str:
    resolver = EndpointPlacementResolver(
        breakpoints=breakpoints,
        defaults=defaults,
    )
    counter_by_row: Dict[int, int] = defaultdict(int)

    lines: List[str] = [
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '        xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
    ]

    for origin, destination, value, row in flows:
        row_key = row.row_index if row.row_index is not None else id(row)
        seq = counter_by_row[row_key]
        counter_by_row[row_key] += 1

        pf_id = f"pf_{origin}__{destination}__{seq}"
        depart = resolver.resolve_depart(origin)
        arrive = resolver.resolve_arrival(destination)

        depart_pos = _clamp_position(depart.length, options.endpoint_offset_m, at_start=depart.is_start)
        arrival_pos = _clamp_position(arrive.length, options.endpoint_offset_m, at_start=arrive.is_start)
        pattern_attr = _format_pattern_attribute(options.pattern, value)

        lines.append(
            f'  <personFlow id="{pf_id}" begin="0.00" end="{options.simulation_end_time:.2f}" '
            f'departPos="{depart_pos:.2f}" {pattern_attr}>'
        )
        lines.append(
            f'    <personTrip from="{depart.edge_id}" to="{arrive.edge_id}" arrivalPos="{arrival_pos:.2f}"/>'
        )
        lines.append("  </personFlow>")

    lines.append("</routes>")
    return "\n".join(lines) + "\n"


__all__ = ["render_person_flows"]
