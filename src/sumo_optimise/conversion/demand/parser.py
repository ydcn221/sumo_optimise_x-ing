"""CSV parsers for demand inputs.

The conversion pipeline consumes demand definitions alongside the JSON
specification.  This module keeps the logic pure: callers are expected to pass
text streams (``io.TextIOBase`` or any iterable returning text rows).  The
parsers normalise different pedestrian layouts, validate entries against the
endpoint catalogue, and report aggregated errors once the full CSV has been
processed.

CSV schema (UTF-8, header required)
===================================

Vehicle demand file
-------------------

Columns: ``endpoint_id``, ``generated*``, ``attracted*``.

``endpoint_id``
    Identifier of a vehicle endpoint returned by the catalogue.
``generated*``
    Column whose name contains ``generated`` (e.g., ``generated_veh_per_h``).
    Represents vehicles leaving the corridor (veh/h).
``attracted*``
    Column whose name contains ``attracted`` (e.g., ``attracted_veh_per_h``).
    Represents vehicles entering the corridor (veh/h).

Pedestrian demand file
----------------------

Four logical column groups are recognised.  Every row must match exactly one of
the supported layouts below, using the required columns for that layout and
leaving the others blank.

``endpoint_id``
    Identifier of a pedestrian endpoint from the catalogue.  Side is always
    derived from the endpoint metadata, so the CSV must not attempt to specify
    it.
``location_id``
    Identifier of a main-road pedestrian frontage.  The schema is strictly
    enforced to communicate both the side (``EB``/``WB``) and the position or
    range:

    * Point frontage: ``Walk.Main.<side>.P<pos>`` where ``pos`` is an integer in
      metres (e.g., ``Walk.Main.EB.P050``).
    * Range frontage: ``Walk.Main.<side>.R<start>-<end>`` with integers in
      metres (e.g., ``Walk.Main.WB.R000-200``) and ``start < end``.

``generated*`` / ``attracted*``
    Absolute pedestrian volumes per hour.  The column names must contain the
    tokens ``generated`` / ``attracted`` and **must not** contain ``per_m``.
``generated*_per_m`` / ``attracted*_per_m``
    Distributed pedestrian rates per metre.  Column names must contain both the
    ``generated`` / ``attracted`` token and the suffix ``per_m``.

Supported row layouts
~~~~~~~~~~~~~~~~~~~~~

1. ``endpoint_id`` + ``generated*`` + ``attracted*``
    Demand tied directly to a known pedestrian endpoint.  Side is inferred from
    the endpoint identifier (both main-road EB/WB endpoints must be listed
    separately in the catalogue).
2. ``location_id`` (point) + ``generated*`` + ``attracted*``
    Point demand located along the main corridor on the specified side.  The
    ``location_id`` provides both the side and snapped position; the ID must
    match ``Walk.Main.<side>.P<pos>``.
3. ``location_id`` (range) + ``generated*_per_m`` + ``attracted*_per_m``
    Distributed demand covering the inclusive range ``[start, end]`` encoded in
    ``location_id``.  The ID must match ``Walk.Main.<side>.R<start>-<end>`` and
    the range must lie within the known endpoints for that side.

Cells may contain leading/trailing whitespace.  Empty rows are ignored.  The
parsers gather all structural and catalogue errors for each CSV and raise a
single :class:`~sumo_optimise.conversion.utils.errors.DemandValidationError`
with a grouped summary after the entire file has been inspected.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, TextIO, Tuple

from ..domain.models import (
    DemandInput,
    DirectionMain,
    EndpointCatalog,
    PedestrianDemandSegment,
    PedestrianEndpoint,
    PedestrianRateKind,
    PedestrianSegmentKind,
    VehicleDemandSegment,
)
from ..utils.errors import DemandValidationError

_POINT_ID_PATTERN = re.compile(r"^Walk\.Main\.(EB|WB)\.P(?P<pos>\d+)$")
_RANGE_ID_PATTERN = re.compile(r"^Walk\.Main\.(EB|WB)\.R(?P<start>\d+)-(?P<end>\d+)$")


@dataclass(frozen=True)
class _VehicleRow:
    row_index: int
    endpoint_id: str
    generated: float
    attracted: float


@dataclass(frozen=True)
class _PedestrianRow:
    row_index: int
    kind: PedestrianSegmentKind
    side: Optional[DirectionMain]
    start_m: Optional[int]
    end_m: Optional[int]
    endpoint_id: Optional[str]
    generated: float
    attracted: float
    rate_kind: PedestrianRateKind


@dataclass(frozen=True)
class _VehicleColumns:
    endpoint_id: Optional[str]
    generated: Optional[str]
    attracted: Optional[str]

    def is_complete(self) -> bool:
        return all((self.endpoint_id, self.generated, self.attracted))


@dataclass(frozen=True)
class _PedestrianColumns:
    generated: Optional[str]
    attracted: Optional[str]
    generated_per_m: Optional[str]
    attracted_per_m: Optional[str]

    def has_absolute(self) -> bool:
        return bool(self.generated and self.attracted)


class _ErrorCollector:
    def __init__(self, context: str):
        self._context = context
        self._messages: List[str] = []

    def add(self, message: str) -> None:
        self._messages.append(message)

    def extend(self, messages: Iterable[str]) -> None:
        for message in messages:
            self.add(message)

    def has_errors(self) -> bool:
        return bool(self._messages)

    def raise_if_any(self) -> None:
        if not self._messages:
            return
        detail = "\n".join(f"- {msg}" for msg in self._messages)
        raise DemandValidationError(f"{self._context} CSV errors were found:\n{detail}")


def parse_demand(
    *,
    vehicle_source: TextIO | Iterable[str],
    pedestrian_source: TextIO | Iterable[str],
    catalog: EndpointCatalog,
) -> DemandInput:
    """Parse vehicle and pedestrian CSVs into a structured demand payload."""

    vehicles = _parse_vehicle_rows(vehicle_source, catalog)
    pedestrians = _parse_pedestrian_rows(pedestrian_source, catalog)
    return DemandInput(
        vehicles=[
            VehicleDemandSegment(
                endpoint_id=row.endpoint_id,
                departures_per_hour=row.generated,
                arrivals_per_hour=row.attracted,
            )
            for row in vehicles
        ],
        pedestrians=[
            PedestrianDemandSegment(
                kind=row.kind,
                rate_kind=row.rate_kind,
                departures=row.generated,
                arrivals=row.attracted,
                side=row.side,
                start_m=row.start_m,
                end_m=row.end_m,
                endpoint_id=row.endpoint_id,
            )
            for row in pedestrians
        ],
    )


def parse_vehicle_demand(
    source: TextIO | Iterable[str], catalog: EndpointCatalog
) -> List[VehicleDemandSegment]:
    """Parse the vehicle CSV only."""

    return [
        VehicleDemandSegment(
            endpoint_id=row.endpoint_id,
            departures_per_hour=row.generated,
            arrivals_per_hour=row.attracted,
        )
        for row in _parse_vehicle_rows(source, catalog)
    ]


def parse_pedestrian_demand(
    source: TextIO | Iterable[str], catalog: EndpointCatalog
) -> List[PedestrianDemandSegment]:
    """Parse the pedestrian CSV only."""

    return [
        PedestrianDemandSegment(
            kind=row.kind,
            rate_kind=row.rate_kind,
            departures=row.generated,
            arrivals=row.attracted,
            side=row.side,
            start_m=row.start_m,
            end_m=row.end_m,
            endpoint_id=row.endpoint_id,
        )
        for row in _parse_pedestrian_rows(source, catalog)
    ]


def _parse_vehicle_rows(
    source: TextIO | Iterable[str], catalog: EndpointCatalog
) -> List[_VehicleRow]:
    header, rows = _read_csv(source)
    if not rows:
        return []

    collector = _ErrorCollector("vehicle demand")
    columns = _resolve_vehicle_columns(header, collector)
    if not columns.is_complete():
        collector.raise_if_any()
        return []

    endpoints = {endpoint.id: endpoint for endpoint in catalog.vehicle_endpoints}

    parsed: List[_VehicleRow] = []
    for row_index, row in rows:
        row_valid = True

        endpoint_id = _require_value(row, columns.endpoint_id, row_index, collector)
        if endpoint_id is None:
            row_valid = False
        elif endpoint_id not in endpoints:
            collector.add(
                f"row {row_index}: vehicle endpoint '{endpoint_id}' is not defined"
            )
            row_valid = False

        generated = _parse_float(row, columns.generated, row_index, collector)
        if generated is None:
            row_valid = False

        attracted = _parse_float(row, columns.attracted, row_index, collector)
        if attracted is None:
            row_valid = False

        if row_valid:
            parsed.append(
                _VehicleRow(
                    row_index=row_index,
                    endpoint_id=endpoint_id,
                    generated=generated,
                    attracted=attracted,
                )
            )

    collector.raise_if_any()
    return parsed


def _parse_pedestrian_rows(
    source: TextIO | Iterable[str], catalog: EndpointCatalog
) -> List[_PedestrianRow]:
    header, rows = _read_csv(source)
    if not rows:
        return []

    collector = _ErrorCollector("pedestrian demand")
    columns = _resolve_pedestrian_columns(header, collector)

    has_endpoint_column = "endpoint_id" in header
    has_location_column = "location_id" in header
    if not has_endpoint_column and not has_location_column:
        collector.add("missing required column 'endpoint_id' or 'location_id'")

    if collector.has_errors() and not rows:
        collector.raise_if_any()
        return []

    endpoints_by_id = {endpoint.id: endpoint for endpoint in catalog.pedestrian_endpoints}
    side_index = _build_pedestrian_side_index(catalog.pedestrian_endpoints)

    parsed: List[_PedestrianRow] = []
    for row_index, row in rows:
        row_valid = True
        endpoint_id = row.get("endpoint_id", "")
        location_id = row.get("location_id", "")
        start_field = row.get("start_m", "")
        end_field = row.get("end_m", "")

        has_endpoint = bool(endpoint_id)
        has_range = bool(location_id) and (bool(start_field) or bool(end_field))
        has_point = bool(location_id) and not has_range

        if sum(int(val) for val in (has_endpoint, has_point, has_range)) != 1:
            collector.add(
                f"row {row_index}: specify exactly one of endpoint_id, point location_id, or range location_id with start/end"
            )
            continue

        if has_endpoint:
            parsed_row = _parse_pedestrian_endpoint_row(
                row_index=row_index,
                endpoint_id=endpoint_id,
                row=row,
                columns=columns,
                endpoints_by_id=endpoints_by_id,
                collector=collector,
            )
            if parsed_row is not None:
                parsed.append(parsed_row)
            continue

        if has_point:
            parsed_row = _parse_pedestrian_point_row(
                row_index=row_index,
                location_id=location_id,
                row=row,
                columns=columns,
                side_index=side_index,
                collector=collector,
            )
            if parsed_row is not None:
                parsed.append(parsed_row)
            continue

        parsed_row = _parse_pedestrian_range_row(
            row_index=row_index,
            location_id=location_id,
            row=row,
            columns=columns,
            side_index=side_index,
            collector=collector,
        )
        if parsed_row is not None:
            parsed.append(parsed_row)

    collector.raise_if_any()
    return parsed


def _parse_pedestrian_endpoint_row(
    *,
    row_index: int,
    endpoint_id: str,
    row: Dict[str, str],
    columns: _PedestrianColumns,
    endpoints_by_id: Dict[str, PedestrianEndpoint],
    collector: _ErrorCollector,
) -> Optional[_PedestrianRow]:
    row_valid = True

    endpoint = endpoints_by_id.get(endpoint_id)
    if not endpoint:
        collector.add(
            f"row {row_index}: pedestrian endpoint '{endpoint_id}' is not defined"
        )
        row_valid = False

    if not columns.has_absolute():
        collector.add("pedestrian CSV must provide absolute generated/attracted columns for endpoint rows")
        row_valid = False

    generated = _parse_float(row, columns.generated, row_index, collector) if columns.generated else None
    attracted = _parse_float(row, columns.attracted, row_index, collector) if columns.attracted else None

    if generated is None or attracted is None:
        row_valid = False

    if not row_valid or endpoint is None:
        return None

    return _PedestrianRow(
        row_index=row_index,
        kind=PedestrianSegmentKind.ENDPOINT,
        rate_kind=PedestrianRateKind.ABSOLUTE,
        generated=generated,
        attracted=attracted,
        side=None,
        start_m=endpoint.pos,
        end_m=endpoint.pos,
        endpoint_id=endpoint.id,
    )


def _parse_pedestrian_point_row(
    *,
    row_index: int,
    location_id: str,
    row: Dict[str, str],
    columns: _PedestrianColumns,
    side_index: "_PedestrianSideIndex",
    collector: _ErrorCollector,
) -> Optional[_PedestrianRow]:
    row_valid = True

    location = _parse_point_location_id(location_id, row_index, collector)
    if location is None:
        return None

    side, position = location

    provided_position = row.get("position_m", "")
    if provided_position:
        try:
            declared = int(provided_position)
        except ValueError:
            collector.add(
                f"row {row_index}: position_m '{provided_position}' must be an integer"
            )
            row_valid = False
        else:
            if declared != position:
                collector.add(
                    f"row {row_index}: position_m {declared} does not match location_id position {position}"
                )
                row_valid = False

    endpoints = side_index.endpoints_by_side.get(side, {}).get(position, [])
    if not endpoints:
        collector.add(
            f"row {row_index}: no pedestrian endpoint on side {side.value} at position {position}"
        )
        row_valid = False
    elif len(endpoints) > 1:
        collector.add(
            f"row {row_index}: pedestrian endpoint on side {side.value} at position {position} is ambiguous"
        )
        row_valid = False

    if not columns.has_absolute():
        collector.add("pedestrian CSV must provide absolute generated/attracted columns for point rows")
        row_valid = False

    generated = _parse_float(row, columns.generated, row_index, collector) if columns.generated else None
    attracted = _parse_float(row, columns.attracted, row_index, collector) if columns.attracted else None

    if generated is None or attracted is None:
        row_valid = False

    if not row_valid or not endpoints or len(endpoints) != 1:
        return None

    endpoint = endpoints[0]

    return _PedestrianRow(
        row_index=row_index,
        kind=PedestrianSegmentKind.POSITION,
        rate_kind=PedestrianRateKind.ABSOLUTE,
        generated=generated,
        attracted=attracted,
        side=side,
        start_m=endpoint.pos,
        end_m=endpoint.pos,
        endpoint_id=endpoint.id,
    )


def _parse_pedestrian_range_row(
    *,
    row_index: int,
    location_id: str,
    row: Dict[str, str],
    columns: _PedestrianColumns,
    side_index: "_PedestrianSideIndex",
    collector: _ErrorCollector,
) -> Optional[_PedestrianRow]:
    row_valid = True

    location = _parse_range_location_id(location_id, row_index, collector)
    if location is None:
        return None

    side, start, end = location

    start_field = row.get("start_m", "")
    end_field = row.get("end_m", "")
    if start_field:
        try:
            declared_start = int(start_field)
        except ValueError:
            collector.add(
                f"row {row_index}: start_m '{start_field}' must be an integer"
            )
            row_valid = False
        else:
            if declared_start != start:
                collector.add(
                    f"row {row_index}: start_m {declared_start} does not match location_id start {start}"
                )
                row_valid = False
    if end_field:
        try:
            declared_end = int(end_field)
        except ValueError:
            collector.add(
                f"row {row_index}: end_m '{end_field}' must be an integer"
            )
            row_valid = False
        else:
            if declared_end != end:
                collector.add(
                    f"row {row_index}: end_m {declared_end} does not match location_id end {end}"
                )
                row_valid = False

    if start >= end:
        collector.add(
            f"row {row_index}: range start ({start}) must be less than end ({end})"
        )
        row_valid = False

    bounds = side_index.bounds_by_side.get(side)
    if not bounds:
        collector.add(
            f"row {row_index}: no pedestrian endpoints available on side {side.value}"
        )
        row_valid = False
    else:
        min_pos, max_pos = bounds
        if start < min_pos or end > max_pos:
            collector.add(
                f"row {row_index}: range {start}-{end} exceeds known endpoints on side {side.value} ({min_pos}-{max_pos})"
            )
            row_valid = False

    if columns.generated_per_m is None or columns.attracted_per_m is None:
        collector.add(
            "pedestrian CSV must provide generated_per_m/attracted_per_m columns for range rows"
        )
        row_valid = False

    generated = (
        _parse_float(row, columns.generated_per_m, row_index, collector)
        if columns.generated_per_m
        else None
    )
    attracted = (
        _parse_float(row, columns.attracted_per_m, row_index, collector)
        if columns.attracted_per_m
        else None
    )

    if generated is None or attracted is None:
        row_valid = False

    if not row_valid:
        return None

    return _PedestrianRow(
        row_index=row_index,
        kind=PedestrianSegmentKind.RANGE,
        rate_kind=PedestrianRateKind.PER_METER,
        generated=generated,
        attracted=attracted,
        side=side,
        start_m=start,
        end_m=end,
        endpoint_id=None,
    )


@dataclass
class _PedestrianSideIndex:
    endpoints_by_side: Dict[DirectionMain, Dict[int, List[PedestrianEndpoint]]]
    bounds_by_side: Dict[DirectionMain, Tuple[int, int]]


def _build_pedestrian_side_index(
    endpoints: Sequence[PedestrianEndpoint],
) -> _PedestrianSideIndex:
    endpoints_by_side: Dict[DirectionMain, Dict[int, List[PedestrianEndpoint]]] = {}
    positions_by_side: Dict[DirectionMain, List[int]] = {}

    for endpoint in endpoints:
        sides = _pedestrian_sides(endpoint)
        for side in sides:
            endpoints_by_side.setdefault(side, {}).setdefault(endpoint.pos, []).append(endpoint)
            positions_by_side.setdefault(side, []).append(endpoint.pos)

    bounds_by_side: Dict[DirectionMain, Tuple[int, int]] = {}
    for side, positions in positions_by_side.items():
        bounds_by_side[side] = (min(positions), max(positions))

    return _PedestrianSideIndex(endpoints_by_side=endpoints_by_side, bounds_by_side=bounds_by_side)


def _resolve_vehicle_columns(
    header: Sequence[str], collector: _ErrorCollector
) -> _VehicleColumns:
    endpoint_col = "endpoint_id" if "endpoint_id" in header else None
    if endpoint_col is None:
        collector.add("missing required column 'endpoint_id'")

    generated_col = _pick_column(header, "generated", collector, forbid_tokens=("per_m",))
    attracted_col = _pick_column(header, "attracted", collector, forbid_tokens=("per_m",))

    return _VehicleColumns(endpoint_col, generated_col, attracted_col)


def _resolve_pedestrian_columns(
    header: Sequence[str], collector: _ErrorCollector
) -> _PedestrianColumns:
    generated_col = _pick_column(header, "generated", collector, forbid_tokens=("per_m",))
    attracted_col = _pick_column(header, "attracted", collector, forbid_tokens=("per_m",))

    generated_per_m_col = _pick_column(header, "generated", collector, require_tokens=("per_m",), optional=True)
    attracted_per_m_col = _pick_column(header, "attracted", collector, require_tokens=("per_m",), optional=True)

    return _PedestrianColumns(
        generated=generated_col,
        attracted=attracted_col,
        generated_per_m=generated_per_m_col,
        attracted_per_m=attracted_per_m_col,
    )


def _pick_column(
    header: Sequence[str],
    token: str,
    collector: _ErrorCollector,
    *,
    forbid_tokens: Tuple[str, ...] = (),
    require_tokens: Tuple[str, ...] = (),
    optional: bool = False,
) -> Optional[str]:
    matches = [
        name
        for name in header
        if token in name
        and all(forbidden not in name for forbidden in forbid_tokens)
        and all(required in name for required in require_tokens)
    ]

    if not matches:
        if not optional:
            descriptor = f"containing '{token}'"
            if require_tokens:
                descriptor += " and " + " and ".join(f"'{req}'" for req in require_tokens)
            collector.add(f"missing required column {descriptor}")
        return None

    if len(matches) > 1:
        descriptor = f"containing '{token}'"
        if require_tokens:
            descriptor += " and " + " and ".join(f"'{req}'" for req in require_tokens)
        collector.add(f"multiple columns {descriptor}: {', '.join(matches)}")

    return matches[0]


def _read_csv(source: TextIO | Iterable[str]) -> Tuple[List[str], List[Tuple[int, Dict[str, str]]]]:
    if hasattr(source, "seek"):
        try:
            source.seek(0)
        except OSError:
            pass

    reader = csv.reader(source)
    try:
        header_row = next(reader)
    except StopIteration:
        return [], []

    header = [_normalize_header(cell) for cell in header_row]

    data: List[Tuple[int, Dict[str, str]]] = []
    row_number = 1
    for raw_row in reader:
        row_number += 1
        if not raw_row or all(not (cell or "").strip() for cell in raw_row):
            continue
        normalized = {
            header[idx]: _normalize_value(raw_row[idx]) if idx < len(raw_row) else ""
            for idx in range(len(header))
        }
        data.append((row_number, normalized))

    return header, data


def _normalize_header(value: str) -> str:
    value = (value or "").lstrip("\ufeff").strip().lower()
    return value.replace(" ", "_")


def _normalize_value(value: Optional[str]) -> str:
    return (value or "").strip()


def _require_value(
    row: Dict[str, str], column: Optional[str], row_index: int, collector: _ErrorCollector
) -> Optional[str]:
    if column is None:
        return None
    value = row.get(column, "")
    if value == "":
        collector.add(f"row {row_index}: column '{column}' must not be empty")
        return None
    return value


def _parse_float(
    row: Dict[str, str], column: Optional[str], row_index: int, collector: _ErrorCollector
) -> Optional[float]:
    text = _require_value(row, column, row_index, collector)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        collector.add(f"row {row_index}: column '{column}' must be a number")
        return None


def _parse_point_location_id(
    location_id: str, row_index: int, collector: _ErrorCollector
) -> Optional[Tuple[DirectionMain, int]]:
    match = _POINT_ID_PATTERN.fullmatch(location_id)
    if not match:
        collector.add(
            f"row {row_index}: location_id '{location_id}' must match Walk.Main.<side>.P<pos>"
        )
        return None
    side = DirectionMain(match.group(1))
    position = int(match.group("pos"))
    return side, position


def _parse_range_location_id(
    location_id: str, row_index: int, collector: _ErrorCollector
) -> Optional[Tuple[DirectionMain, int, int]]:
    match = _RANGE_ID_PATTERN.fullmatch(location_id)
    if not match:
        collector.add(
            f"row {row_index}: location_id '{location_id}' must match Walk.Main.<side>.R<start>-<end>"
        )
        return None
    side = DirectionMain(match.group(1))
    start = int(match.group("start"))
    end = int(match.group("end"))
    return side, start, end


def _pedestrian_sides(endpoint: PedestrianEndpoint) -> List[DirectionMain]:
    movement = endpoint.movement
    suffix_map = {
        "_EB": DirectionMain.EB,
        "_north": DirectionMain.EB,
        "_WB": DirectionMain.WB,
        "_south": DirectionMain.WB,
    }
    for suffix, side in suffix_map.items():
        if movement.endswith(suffix):
            return [side]
    if movement.startswith("ped_main") or movement.startswith("ped_mid"):
        return [DirectionMain.EB, DirectionMain.WB]
    return []
