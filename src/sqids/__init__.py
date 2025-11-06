"""Minimal Sqids fallback implementation used when the external dependency is unavailable."""
from __future__ import annotations

from typing import Iterable


class Sqids:
    def __init__(self, alphabet: str | None = None, min_length: int | None = None) -> None:
        self._alphabet = alphabet or "abcdefghijklmnopqrstuvwxyz0123456789"
        self._min_length = max(min_length or 0, 0)

    def encode(self, values: Iterable[int]) -> str:
        parts = [format(max(0, int(value)), "x") for value in values]
        token = "_".join(parts) if parts else "0"
        if self._min_length and len(token) < self._min_length:
            token = token.rjust(self._min_length, self._alphabet[0])
        return token


__all__ = ["Sqids"]
