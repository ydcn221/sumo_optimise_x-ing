"""Conflict table loader for traffic-light movement interactions."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from ..utils.logging import get_logger

LOG = get_logger()

_CARDINAL_SEQUENCE = ["NB", "EB", "SB", "WB"]


@dataclass(frozen=True)
class _ConflictEntry:
    movement_a: str
    movement_b: str
    state_a: str
    state_b: str


class ConflictMatrix:
    """Lookup table describing how two movements influence each other."""

    def __init__(self, entries: Iterable[_ConflictEntry]) -> None:
        self._matrix: Dict[Tuple[str, str], Tuple[str, str]] = {}
        for entry in entries:
            key = (entry.movement_a, entry.movement_b)
            self._matrix[key] = (entry.state_a, entry.state_b)
            reverse_key = (entry.movement_b, entry.movement_a)
            self._matrix[reverse_key] = (entry.state_b, entry.state_a)

    def relation(self, movement_a: str, movement_b: str) -> Tuple[str, str]:
        """Return (state_for_a, state_for_b) using 'P' if unknown."""

        if movement_a == movement_b:
            return "P", "P"

        variants_a = _token_variants(movement_a)
        variants_b = _token_variants(movement_b)
        for variant_a in variants_a:
            for variant_b in variants_b:
                key = (variant_a, variant_b)
                if key in self._matrix:
                    return self._matrix[key]

        LOG.debug(
            "[TLS] no conflict entry for %s vs %s; assuming priority for both",
            movement_a,
            movement_b,
        )
        return "P", "P"


def _strip_ped_suffix(token: str) -> str:
    if "_p" in token:
        base, _, suffix = token.partition("_p")
        if suffix in {"r", "g"}:
            return base
    return token


def _strip_turn_suffix(token: str) -> str:
    if "_" not in token:
        return token
    prefix, _, suffix = token.rpartition("_")
    if not prefix:
        return token
    if suffix.upper() in {"L", "T", "R", "U"} and prefix.endswith("B"):
        return prefix
    return token


def _token_variants(token: str) -> List[str]:
    variants: List[str] = []
    seen: Set[str] = set()

    def add(value: str) -> None:
        if value not in seen:
            variants.append(value)
            seen.add(value)

    add(token)
    stripped = _strip_ped_suffix(token)
    add(stripped)
    add(_strip_turn_suffix(stripped))
    return variants


def _prepare_replacements(rows: Sequence[List[str]]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for row in rows:
        if not row or len(row) < 2:
            continue
        base = row[0].strip()
        if not base:
            continue
        replacements = [cell.strip() for cell in row[1:]]
        if len(replacements) != len(_CARDINAL_SEQUENCE):
            continue
        mapping[base] = replacements
    return mapping


def _substitute(token: str, replacements: Dict[str, str]) -> str:
    # Replace the first matching prefix using the longest base token first.
    for base in sorted(replacements, key=len, reverse=True):
        if token.startswith(base):
            return replacements[base] + token[len(base) :]
    return token


def _expand_entries(
    base_entries: Sequence[_ConflictEntry],
    replacement_rows: Sequence[List[str]],
) -> List[_ConflictEntry]:
    templates = _prepare_replacements(replacement_rows)
    if not templates:
        LOG.warning("[TLS] conflict-table replacements not found; using base entries verbatim")
        return list(base_entries)

    expanded: List[_ConflictEntry] = []
    for orient_index, cardinal in enumerate(_CARDINAL_SEQUENCE):
        column_map: Dict[str, str] = {}
        for base_token, replacements in templates.items():
            column_map[base_token] = replacements[orient_index]
        for entry in base_entries:
            movement_a = _substitute(entry.movement_a, column_map)
            movement_b = _substitute(entry.movement_b, column_map)
            expanded.append(
                _ConflictEntry(
                    movement_a=movement_a,
                    movement_b=movement_b,
                    state_a=entry.state_a,
                    state_b=entry.state_b,
                )
            )
    return expanded


def _parse_conflict_file(path: Path) -> ConflictMatrix:
    text = path.read_text(encoding="cp932").replace("\r\n", "\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    replace_rows: List[List[str]] = []
    conflict_rows: List[List[str]] = []

    mode = None
    for line in lines:
        if "Equivalence_Replace_Table" in line and "END" not in line:
            mode = "replace"
            continue
        if "Equivalence-Replace-Table END" in line:
            mode = None
            continue
        if "Conflict_Table" in line and "END" not in line:
            mode = "conflict"
            continue
        if "Conflict_Table END" in line:
            mode = None
            continue

        if mode == "replace":
            cells = [cell.strip('" ') for cell in line.split("\t") if cell]
            if cells and cells[0] != "Base":
                replace_rows.append(cells)
        elif mode == "conflict":
            cells = [cell.strip('" ') for cell in line.split("\t") if cell]
            if cells and cells[0] != "allowed_movement_A":
                conflict_rows.append(cells)

    base_entries: List[_ConflictEntry] = []
    for row in conflict_rows:
        if len(row) < 4:
            continue
        base_entries.append(
            _ConflictEntry(
                movement_a=row[0],
                movement_b=row[1],
                state_a=row[2],
                state_b=row[3],
            )
        )

    if not base_entries:
        LOG.warning("[TLS] conflict-table entries missing; defaulting to permissive states")
        return ConflictMatrix([])

    expanded_entries = _expand_entries(base_entries, replace_rows)
    return ConflictMatrix(expanded_entries)


def load_conflict_matrix() -> ConflictMatrix:
    """Load the conflict matrix from the prototype table on disk."""

    data_dir = Path(__file__).resolve().parents[1] / "data"
    path = data_dir / "conflict_table(prototype).txt"
    if not path.exists():
        LOG.warning("[TLS] conflict-table file %s not found; defaulting to empty matrix", path)
        return ConflictMatrix([])
    return _parse_conflict_file(path)


GLOBAL_CONFLICT_MATRIX = load_conflict_matrix()
