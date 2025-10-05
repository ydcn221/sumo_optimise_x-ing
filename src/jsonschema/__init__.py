"""Minimal jsonschema stub used for offline testing.

This stub only exposes :class:`Draft7Validator` with an ``iter_errors``
method that yields no validation errors.  It is sufficient for environments
where the real :mod:`jsonschema` dependency cannot be installed.
"""
from __future__ import annotations

from typing import Iterator


class Draft7Validator:
    """Very small stub mimicking :class:`jsonschema.Draft7Validator`.

    The real jsonschema library performs extensive validation against the
    provided schema.  The legacy converter expects to instantiate the
    validator and iterate over validation errors.  For our offline fixtures
    we rely on pre-validated specifications, so the stub simply yields an
    empty iterator.
    """

    def __init__(self, schema: object) -> None:  # pragma: no cover - simple
        self.schema = schema

    def iter_errors(self, instance: object) -> Iterator[object]:
        return iter(())


__all__ = ["Draft7Validator"]
