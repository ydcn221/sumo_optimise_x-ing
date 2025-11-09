"""CSV loaders for pedestrian demand inputs."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, TextIO, Tuple, Union

from ...domain.models import (
    CardinalDirection,
    EndpointDemandRow,
    JunctionTurnWeights,
    PedestrianSide,
    PersonFlowPattern,
)
from ...utils.errors import DemandValidationError

EndpointDemandSource = Union[Path, TextIO]
JunctionTurnWeightSource = Union[Path, TextIO]

_ENDPOINT_ID_COLUMN = "SidewalkEndID"
_FLOW_COLUMN = "PedFlow"
_LABEL_COLUMN = "Label"
_PATTERN_KEY = "Pattern"

_TURN_WEIGHT_ID_COLUMN = "JunctionID"
_TURN_WEIGHT_COLUMNS: Dict[str, Tuple[CardinalDirection, PedestrianSide]] = {
    "ToNorth_EastSide": (CardinalDirection.NORTH, PedestrianSide.EAST_SIDE),
    "ToNorth_WestSide": (CardinalDirection.NORTH, PedestrianSide.WEST_SIDE),
    "ToWest_NorthSide": (CardinalDirection.WEST, PedestrianSide.NORTH_SIDE),
    "ToWest_SouthSide": (CardinalDirection.WEST, PedestrianSide.SOUTH_SIDE),
    "ToSouth_WestSide": (CardinalDirection.SOUTH, PedestrianSide.WEST_SIDE),
    "ToSouth_EastSide": (CardinalDirection.SOUTH, PedestrianSide.EAST_SIDE),
    "ToEast_SouthSide": (CardinalDirection.EAST, PedestrianSide.SOUTH_SIDE),
    "ToEast_NorthSide": (CardinalDirection.EAST, PedestrianSide.NORTH_SIDE),
}


class _ErrorCollector:
    def __init__(self, context: str) -> None:
        self._context = context
        self._messages: List[str] = []

    def add(self, message: str) -> None:
        self._messages.append(message)

    def extend(self, messages: Iterable[str]) -> None:
        for message in messages:
            self.add(message)

    def raise_if_any(self) -> None:
        if self._messages:
            detail = "\n".join(f"- {msg}" for msg in self._messages)
            raise DemandValidationError(f"{self._context}:\n{detail}")


def _open_source(source: Union[Path, TextIO], *, context: str) -> Tuple[TextIO, bool]:
    if isinstance(source, Path):
        try:
            return source.open("r", encoding="utf-8-sig", newline=""), True
        except OSError as exc:  # pragma: no cover - filesystem errors not deterministic
            raise DemandValidationError(f"{context}: unable to open {source}: {exc}") from exc
    return source, False


def _parse_pattern_row(row: Sequence[str], errors: _ErrorCollector) -> Optional[PersonFlowPattern]:
    key = (row[0] or "").strip() if row else ""
    if key != _PATTERN_KEY:
        errors.add("row 1: first column must be 'Pattern'")
        return None
    if len(row) < 2 or not row[1].strip():
        errors.add("row 1: Pattern value is required")
        return None
    token = row[1].strip().lower()
    for pattern in PersonFlowPattern:
        if token == pattern.value:
            return pattern
    errors.add(
        "row 1: Pattern value must be one of "
        + ", ".join(pattern.value for pattern in PersonFlowPattern)
    )
    return None


def load_endpoint_demands(source: EndpointDemandSource) -> Tuple[PersonFlowPattern, List[EndpointDemandRow]]:
    """Parse endpoint demand rows and declared pattern from the provided CSV source."""

    stream, should_close = _open_source(source, context="Endpoint demand CSV")
    errors = _ErrorCollector("invalid endpoint demand rows")
    rows: List[EndpointDemandRow] = []
    pattern: Optional[PersonFlowPattern] = None

    try:
        csv_reader = csv.reader(stream)
        first_row = next(csv_reader, None)
        if first_row is None:
            errors.add("file is empty; expected a 'Pattern,<value>' row")
            errors.raise_if_any()
        pattern = _parse_pattern_row(first_row, errors)
        errors.raise_if_any()

        header_row = next(csv_reader, None)
        if header_row is None:
            errors.add("missing header row; expected SidewalkEndID and PedFlow columns on row 2")
            errors.raise_if_any()

        expected = {_ENDPOINT_ID_COLUMN, _FLOW_COLUMN}
        missing = expected - set(cell.strip() for cell in header_row if cell)
        if missing:
            errors.add(f"missing columns: {', '.join(sorted(missing))}")
            errors.raise_if_any()

        reader = csv.DictReader(stream, fieldnames=header_row)

        for index, raw_row in enumerate(reader, start=3):
            endpoint_id = (raw_row.get(_ENDPOINT_ID_COLUMN) or "").strip()
            flow_token = (raw_row.get(_FLOW_COLUMN) or "").strip()
            label = (raw_row.get(_LABEL_COLUMN) or "").strip() or None

            if not endpoint_id:
                errors.add(f"row {index}: SidewalkEndID is required")
                continue
            if not flow_token:
                errors.add(f"row {index}: PedFlow is required")
                continue
            try:
                flow = float(flow_token)
            except ValueError:
                errors.add(f"row {index}: PedFlow must be numeric (got {flow_token!r})")
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
        errors.add("pattern declaration missing or invalid")
        errors.raise_if_any()
        # errors.raise_if_any() will raise; return fallback to satisfy type checker
        pattern = PersonFlowPattern.STEADY
    return pattern, rows


def load_junction_turn_weights(source: JunctionTurnWeightSource) -> Dict[str, JunctionTurnWeights]:
    """Parse junction turn weights from CSV."""

    stream, should_close = _open_source(source, context="Junction turn-weight CSV")
    errors = _ErrorCollector("invalid junction turn-weight rows")
    turn_weights: Dict[str, JunctionTurnWeights] = {}

    try:
        reader = csv.DictReader(stream)
        required = {_TURN_WEIGHT_ID_COLUMN, *_TURN_WEIGHT_COLUMNS.keys()}
        missing = required - set(reader.fieldnames or [])
        if missing:
            errors.add(f"missing columns: {', '.join(sorted(missing))}")
            errors.raise_if_any()

        for index, raw_row in enumerate(reader, start=2):
            junction_id = (raw_row.get(_TURN_WEIGHT_ID_COLUMN) or "").strip()
            if not junction_id:
                errors.add(f"row {index}: JunctionID is required")
                continue
            if junction_id in turn_weights:
                errors.add(f"row {index}: duplicate JunctionID {junction_id!r}")
                continue

            weights: Dict[Tuple[CardinalDirection, PedestrianSide], float] = {}
            parse_errors: List[str] = []
            for column, key in _TURN_WEIGHT_COLUMNS.items():
                token = (raw_row.get(column) or "").strip()
                if not token:
                    parse_errors.append(f"{column} missing value")
                    continue
                try:
                    weight = float(token)
                except ValueError:
                    parse_errors.append(f"{column} must be numeric (got {token!r})")
                    continue
                weights[key] = weight

            if parse_errors:
                errors.add(f"row {index} ({junction_id}): " + "; ".join(parse_errors))
                continue

            turn_weights[junction_id] = JunctionTurnWeights(junction_id=junction_id, weights=weights)
    finally:
        if should_close:
            stream.close()

    errors.raise_if_any()
    return turn_weights


__all__ = ["load_endpoint_demands", "load_junction_turn_weights"]
