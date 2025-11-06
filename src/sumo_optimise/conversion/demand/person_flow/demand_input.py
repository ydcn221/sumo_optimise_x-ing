"""CSV loaders for pedestrian demand inputs."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, TextIO, Tuple, Union

from ...domain.models import (
    CardinalDirection,
    EndpointDemandRow,
    JunctionDirectionRatios,
    PedestrianSide,
)
from ...utils.errors import DemandValidationError

EndpointDemandSource = Union[Path, TextIO]
JunctionRatioSource = Union[Path, TextIO]

_ENDPOINT_ID_COLUMN = "EndpointID"
_FLOW_COLUMN = "PedFlow"
_LABEL_COLUMN = "Label"

_RATIO_ID_COLUMN = "JunctionID"
_RATIO_COLUMNS: Dict[str, Tuple[CardinalDirection, PedestrianSide]] = {
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


def load_endpoint_demands(source: EndpointDemandSource) -> List[EndpointDemandRow]:
    """Parse endpoint demand rows from the provided CSV source."""

    stream, should_close = _open_source(source, context="Endpoint demand CSV")
    errors = _ErrorCollector("invalid endpoint demand rows")
    rows: List[EndpointDemandRow] = []

    try:
        reader = csv.DictReader(stream)
        expected = {_ENDPOINT_ID_COLUMN, _FLOW_COLUMN}
        missing = expected - set(reader.fieldnames or [])
        if missing:
            errors.add(f"missing columns: {', '.join(sorted(missing))}")
            errors.raise_if_any()

        for index, raw_row in enumerate(reader, start=2):
            endpoint_id = (raw_row.get(_ENDPOINT_ID_COLUMN) or "").strip()
            flow_token = (raw_row.get(_FLOW_COLUMN) or "").strip()
            label = (raw_row.get(_LABEL_COLUMN) or "").strip() or None

            if not endpoint_id:
                errors.add(f"row {index}: EndpointID is required")
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
    return rows


def load_junction_ratios(source: JunctionRatioSource) -> Dict[str, JunctionDirectionRatios]:
    """Parse junction direction ratios from CSV."""

    stream, should_close = _open_source(source, context="Junction ratio CSV")
    errors = _ErrorCollector("invalid junction ratio rows")
    ratios: Dict[str, JunctionDirectionRatios] = {}

    try:
        reader = csv.DictReader(stream)
        required = {_RATIO_ID_COLUMN, *_RATIO_COLUMNS.keys()}
        missing = required - set(reader.fieldnames or [])
        if missing:
            errors.add(f"missing columns: {', '.join(sorted(missing))}")
            errors.raise_if_any()

        for index, raw_row in enumerate(reader, start=2):
            junction_id = (raw_row.get(_RATIO_ID_COLUMN) or "").strip()
            if not junction_id:
                errors.add(f"row {index}: JunctionID is required")
                continue
            if junction_id in ratios:
                errors.add(f"row {index}: duplicate JunctionID {junction_id!r}")
                continue

            weights: Dict[Tuple[CardinalDirection, PedestrianSide], float] = {}
            parse_errors: List[str] = []
            for column, key in _RATIO_COLUMNS.items():
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

            ratios[junction_id] = JunctionDirectionRatios(junction_id=junction_id, weights=weights)
    finally:
        if should_close:
            stream.close()

    errors.raise_if_any()
    return ratios


__all__ = ["load_endpoint_demands", "load_junction_ratios"]
