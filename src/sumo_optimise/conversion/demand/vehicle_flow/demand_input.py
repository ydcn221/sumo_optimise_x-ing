"""Vehicle demand CSV loaders (endpoint flows and junction turn weights)."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, TextIO, Tuple

from ...domain.models import CardinalDirection, EndpointDemandRow, PersonFlowPattern
from ...utils.errors import DemandValidationError

EndpointDemandSource = TextIO | Path
TurnWeightSource = TextIO | Path

_ENDPOINT_ID_COLUMN = "EndID"
_FLOW_COLUMN = "vehFlow"
_LABEL_COLUMN = "Label"

_TURN_ID_COLUMN = "JunctionID"
_TURN_COLUMNS = {
    "ToNorth": CardinalDirection.NORTH,
    "ToWest": CardinalDirection.WEST,
    "ToSouth": CardinalDirection.SOUTH,
    "ToEast": CardinalDirection.EAST,
}


@dataclass(frozen=True)
class VehicleTurnWeights:
    junction_id: str
    weights: Dict[CardinalDirection, float]

    def weight(self, direction: CardinalDirection) -> float:
        return self.weights.get(direction, 0.0)


class _ErrorCollector:
    def __init__(self, context: str) -> None:
        self._context = context
        self._messages: List[str] = []

    def add(self, message: str) -> None:
        self._messages.append(message)

    def raise_if_any(self) -> None:
        if self._messages:
            raise DemandValidationError(f"{self._context}: {'; '.join(self._messages)}")


def _open_source(source: EndpointDemandSource | TurnWeightSource, *, context: str) -> Tuple[TextIO, bool]:
    if hasattr(source, "read"):
        return source, False  # type: ignore[return-value]
    path = Path(source)
    stream = path.open("r", encoding="utf-8-sig", newline="")
    return stream, True


def _parse_pattern_row(row: Sequence[str], errors: _ErrorCollector) -> PersonFlowPattern | None:
    if len(row) < 2 or row[0].strip().lower() != "pattern":
        errors.add("first row must declare the pattern, e.g., 'Pattern,steady'")
        return None
    token = row[1].strip().lower()
    for pattern in PersonFlowPattern:
        if token == pattern.value:
            return pattern
    errors.add(f"unsupported pattern value: {row[1]!r}")
    return None


def load_vehicle_endpoint_demands(source: EndpointDemandSource) -> Tuple[PersonFlowPattern, List[EndpointDemandRow]]:
    """Parse `veh_EP_demand` CSV into pattern + signed endpoint rows."""

    stream, should_close = _open_source(source, context="Vehicle endpoint demand CSV")
    errors = _ErrorCollector("invalid vehicle endpoint demand rows")
    rows: List[EndpointDemandRow] = []
    pattern: PersonFlowPattern | None = None

    try:
        reader = csv.reader(stream)
        first_row = next(reader, None)
        if first_row is None:
            errors.add("file is empty; expected a 'Pattern,<value>' row")
            errors.raise_if_any()
        pattern = _parse_pattern_row(first_row, errors)
        errors.raise_if_any()

        header = next(reader, None)
        if header is None:
            errors.add("missing header row; expected EndID and vehFlow columns on row 2")
            errors.raise_if_any()

        normalized = [cell.strip() for cell in header]
        if _ENDPOINT_ID_COLUMN not in normalized or _FLOW_COLUMN not in normalized:
            errors.add("header must contain 'EndID' and 'vehFlow'")
            errors.raise_if_any()

        dict_reader = csv.DictReader(stream, fieldnames=header)
        for index, raw_row in enumerate(dict_reader, start=3):
            endpoint_id = (raw_row.get(_ENDPOINT_ID_COLUMN) or "").strip()
            flow_token = (raw_row.get(_FLOW_COLUMN) or "").strip()
            label = (raw_row.get(_LABEL_COLUMN) or "").strip() or None

            if not endpoint_id:
                errors.add(f"row {index}: EndID is required")
                continue
            if not flow_token:
                errors.add(f"row {index}: vehFlow is required")
                continue
            try:
                flow = float(flow_token)
            except ValueError:
                errors.add(f"row {index}: vehFlow must be numeric (got {flow_token!r})")
                continue
            rows.append(
                EndpointDemandRow(
                    endpoint_id=endpoint_id,
                    flow_per_hour=flow,
                    label=label,
                    row_index=index,
                )
            )
    finally:
        if should_close:
            stream.close()

    errors.raise_if_any()
    if pattern is None:
        raise DemandValidationError("pattern declaration missing or invalid")
    return pattern, rows


def load_vehicle_turn_weights(source: TurnWeightSource) -> Dict[str, VehicleTurnWeights]:
    """Parse `veh_jct_turn_weight` CSV into cluster -> direction weights."""

    stream, should_close = _open_source(source, context="Vehicle junction turn-weight CSV")
    errors = _ErrorCollector("invalid vehicle junction turn-weight rows")
    turn_map: Dict[str, VehicleTurnWeights] = {}

    try:
        reader = csv.DictReader(stream)
        header = [cell.strip() for cell in (reader.fieldnames or [])]
        missing = {_TURN_ID_COLUMN, *_TURN_COLUMNS.keys()} - set(header)
        if missing:
            errors.add(f"missing columns: {', '.join(sorted(missing))}")
            errors.raise_if_any()

        for index, raw_row in enumerate(reader, start=2):
            junction_id = (raw_row.get(_TURN_ID_COLUMN) or "").strip()
            if not junction_id:
                errors.add(f"row {index}: JunctionID is required")
                continue
            if junction_id in turn_map:
                errors.add(f"row {index}: duplicate JunctionID {junction_id!r}")
                continue

            weights: Dict[CardinalDirection, float] = {}
            problems: List[str] = []
            for column, direction in _TURN_COLUMNS.items():
                token = (raw_row.get(column) or "").strip()
                if not token:
                    problems.append(f"{column} missing value")
                    continue
                try:
                    weights[direction] = float(token)
                except ValueError:
                    problems.append(f"{column} must be numeric (got {token!r})")
            if problems:
                errors.add(f"row {index} ({junction_id}): " + "; ".join(problems))
                continue

            turn_map[junction_id] = VehicleTurnWeights(junction_id=junction_id, weights=weights)
    finally:
        if should_close:
            stream.close()

    errors.raise_if_any()
    return turn_map


__all__ = ["load_vehicle_endpoint_demands", "load_vehicle_turn_weights", "VehicleTurnWeights"]
