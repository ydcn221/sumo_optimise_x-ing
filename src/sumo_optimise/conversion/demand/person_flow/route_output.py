"""Render SUMO personFlow XML from propagated pedestrian demand."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ...builder.ids import main_edge_id, minor_edge_id
from ...domain.models import Defaults, EndpointDemandRow, PedestrianSide, PersonFlowPattern
from ...utils.errors import DemandValidationError
from .identifier import parse_main_ped_endpoint_id, parse_minor_endpoint_id


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
        self._first_pos: Optional[int] = None
        self._last_pos: Optional[int] = None
        ordered = list(breakpoints)
        if ordered:
            self._first_pos = ordered[0]
            self._last_pos = ordered[-1]
        for idx, pos in enumerate(ordered):
            self._prev_by_pos[pos] = ordered[idx - 1] if idx > 0 else None
            self._next_by_pos[pos] = ordered[idx + 1] if idx < len(ordered) - 1 else None

    def _resolve_position_label(self, token: str) -> int:
        try:
            return int(token)
        except ValueError:
            pass
        label = token.strip().upper()
        if label in {"W_END", "WEST_END"}:
            if self._first_pos is None:
                raise DemandValidationError("cannot resolve west-end pedestrian endpoint without breakpoints")
            return self._first_pos
        if label in {"E_END", "EAST_END"}:
            if self._last_pos is None:
                raise DemandValidationError("cannot resolve east-end pedestrian endpoint without breakpoints")
            return self._last_pos
        raise DemandValidationError(f"unsupported pedestrian endpoint position token: {token!r}")

    def _parse_endpoint(self, endpoint_id: str) -> Tuple[int, str]:
        tokens = endpoint_id.split(".")
        if len(tokens) == 4 and tokens[0] == "Node" and tokens[1] == "Main":
            try:
                pos = int(tokens[2])
            except ValueError as exc:
                raise DemandValidationError(
                    f"endpoint {endpoint_id!r} has non-integer position token"
                ) from exc
            suffix = tokens[3]
            if suffix not in {"N", "S"}:
                raise DemandValidationError(f"unsupported main endpoint half: {endpoint_id}")
            return pos, suffix
        parsed_alias = parse_main_ped_endpoint_id(endpoint_id)
        if parsed_alias:
            pos_token, half = parsed_alias
            pos = self._resolve_position_label(pos_token)
            return pos, half
        raise DemandValidationError(f"unsupported endpoint identifier: {endpoint_id}")

    def _main_north_depart(self, pos: int) -> EndpointPlacement:
        next_pos = self._next_by_pos.get(pos)
        if next_pos is not None:
            edge_id = main_edge_id("EB", pos, next_pos)
            length = float(next_pos - pos)
            return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)
        prev_pos = self._prev_by_pos.get(pos)
        if prev_pos is None:
            raise DemandValidationError(f"cannot resolve depart edge for Node.Main.{pos}.N")
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
            raise DemandValidationError(f"cannot resolve arrival edge for Node.Main.{pos}.N")
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
            raise DemandValidationError(f"cannot resolve depart edge for Node.Main.{pos}.S")
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
            raise DemandValidationError(f"cannot resolve arrival edge for Node.Main.{pos}.S")
        edge_id = main_edge_id("WB", pos, prev_pos)
        length = float(pos - prev_pos)
        return EndpointPlacement(edge_id=edge_id, is_start=True, length=length)

    def _minor_edge_for_side(
        self,
        pos: int,
        orientation: str,
        side: PedestrianSide,
    ) -> Tuple[str, bool]:
        if side == PedestrianSide.WEST_SIDE:
            desired_token = "NB"
        elif side == PedestrianSide.EAST_SIDE:
            desired_token = "SB"
        else:
            raise DemandValidationError(f"unsupported minor side: {side}")
        if orientation not in {"N", "S"}:
            raise DemandValidationError(f"unsupported minor orientation: {orientation}")
        if desired_token == "NB":
            flow = "from" if orientation == "N" else "to"
        else:
            flow = "to" if orientation == "N" else "from"
        edge_id = minor_edge_id(pos, flow, orientation)
        remote_is_start = flow == "to"
        return edge_id, remote_is_start

    def _minor_local_is_start(self, orientation: str, side: PedestrianSide) -> bool:
        if orientation == "N":
            if side == PedestrianSide.WEST_SIDE:
                return False
            if side == PedestrianSide.EAST_SIDE:
                return True
        elif orientation == "S":
            if side == PedestrianSide.WEST_SIDE:
                return True
            if side == PedestrianSide.EAST_SIDE:
                return False
        raise DemandValidationError(
            f"unsupported minor endpoint placement for orientation={orientation!r}, side={side!r}"
        )

    def _minor_depart(self, pos: int, orientation: str, side: PedestrianSide) -> EndpointPlacement:
        edge_id, _ = self._minor_edge_for_side(pos, orientation, side)
        length = float(self._defaults.minor_road_length_m)
        local_is_start = self._minor_local_is_start(orientation, side)
        return EndpointPlacement(edge_id=edge_id, is_start=local_is_start, length=length)

    def _minor_arrival(self, pos: int, orientation: str, side: PedestrianSide) -> EndpointPlacement:
        edge_id, _ = self._minor_edge_for_side(pos, orientation, side)
        length = float(self._defaults.minor_road_length_m)
        local_is_start = self._minor_local_is_start(orientation, side)
        return EndpointPlacement(edge_id=edge_id, is_start=local_is_start, length=length)

    def resolve_depart(self, endpoint_id: str) -> EndpointPlacement:
        parsed_minor = parse_minor_endpoint_id(endpoint_id)
        if parsed_minor:
            pos, orientation, side = parsed_minor
            return self._minor_depart(pos, orientation, side)
        pos, suffix = self._parse_endpoint(endpoint_id)
        if suffix == "N":
            return self._main_north_depart(pos)
        if suffix == "S":
            return self._main_south_depart(pos)
        raise DemandValidationError(f"unsupported endpoint for depart placement: {endpoint_id}")

    def resolve_arrival(self, endpoint_id: str) -> EndpointPlacement:
        parsed_minor = parse_minor_endpoint_id(endpoint_id)
        if parsed_minor:
            pos, orientation, side = parsed_minor
            return self._minor_arrival(pos, orientation, side)
        pos, suffix = self._parse_endpoint(endpoint_id)
        if suffix == "N":
            return self._main_north_arrival(pos)
        if suffix == "S":
            return self._main_south_arrival(pos)
        raise DemandValidationError(f"unsupported endpoint for arrival placement: {endpoint_id}")


def _format_pattern_attribute(pattern: PersonFlowPattern, flow_per_hour: float) -> str:
    if flow_per_hour <= 0.0:
        raise DemandValidationError("cannot emit personFlow with non-positive demand")
    if pattern is PersonFlowPattern.STEADY:
        return f'personsPerHour="{flow_per_hour:.6f}"'
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


def build_person_flow_entries(
    flows: Iterable[Tuple[str, str, float, EndpointDemandRow]],
    *,
    ped_pattern: PersonFlowPattern,
    simulation_end_time: float,
    endpoint_offset_m: float,
    breakpoints: Sequence[int],
    defaults: Defaults,
) -> List[str]:
    """Return the list of <personFlow> XML fragments for the supplied OD flows."""

    resolver = EndpointPlacementResolver(
        breakpoints=breakpoints,
        defaults=defaults,
    )
    counter_by_pair: Dict[Tuple[str, str], int] = defaultdict(int)

    entries: List[str] = []
    for origin, destination, value, row in flows:
        pair_key = (origin, destination)
        seq = counter_by_pair[pair_key]
        counter_by_pair[pair_key] += 1

        pf_id = f"pf_{origin}__{destination}__{seq}"
        depart = resolver.resolve_depart(origin)
        arrive = resolver.resolve_arrival(destination)

        depart_pos = _clamp_position(depart.length, endpoint_offset_m, at_start=depart.is_start)
        arrival_pos = _clamp_position(arrive.length, endpoint_offset_m, at_start=arrive.is_start)
        pattern_attr = _format_pattern_attribute(ped_pattern, value)

        entry_lines = [
            f'  <personFlow id="{pf_id}" begin="0.00" end="{simulation_end_time:.2f}" '
            f'departPos="{depart_pos:.2f}" {pattern_attr}>',
            f'    <personTrip from="{depart.edge_id}" to="{arrive.edge_id}" arrivalPos="{arrival_pos:.2f}"/>',
            "  </personFlow>",
        ]
        entries.append("\n".join(entry_lines))

    return entries


def render_person_flows(
    flows: Iterable[Tuple[str, str, float, EndpointDemandRow]],
    *,
    ped_pattern: PersonFlowPattern,
    simulation_end_time: float,
    endpoint_offset_m: float,
    breakpoints: Sequence[int],
    defaults: Defaults,
) -> str:
    entries = build_person_flow_entries(
        flows,
        ped_pattern=ped_pattern,
        simulation_end_time=simulation_end_time,
        endpoint_offset_m=endpoint_offset_m,
        breakpoints=breakpoints,
        defaults=defaults,
    )
    lines: List[str] = [
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '        xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
        *entries,
        "</routes>",
    ]
    return "\n".join(lines) + "\n"


__all__ = ["build_person_flow_entries", "render_person_flows"]
