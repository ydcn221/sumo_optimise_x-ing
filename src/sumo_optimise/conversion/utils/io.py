"""Filesystem helpers that reproduce and extend the legacy behaviour."""
from __future__ import annotations

import datetime
import json
import time
from itertools import count
from pathlib import Path
from typing import Mapping

from sqids import Sqids

from ..domain.models import OutputDirectoryTemplate
from .constants import (
    CONNECTIONS_FILE_NAME,
    EDGES_FILE_NAME,
    MANIFEST_NAME,
    NODES_FILE_NAME,
)


_SQIDS = Sqids()


def _current_time() -> datetime.datetime:
    return datetime.datetime.now()


def _time_ns() -> int:
    return time.time_ns()


class _DateComponent:
    def __init__(self, value: int, default_width: int) -> None:
        self._value = value
        self._default_width = default_width

    def __format__(self, format_spec: str) -> str:
        spec = format_spec or f"0{self._default_width}"
        return format(self._value, spec)


class _SequenceComponent:
    def __init__(self, value: int, default_width: int) -> None:
        self._value = value
        self._default_width = default_width

    def __format__(self, format_spec: str) -> str:  # pragma: no cover - exercised indirectly
        spec = format_spec or f"0{self._default_width}"
        return format(self._value, spec)


class _UidComponent:
    def __init__(self, token: str) -> None:
        self._token = token

    def __format__(self, format_spec: str) -> str:  # pragma: no cover - exercised indirectly
        if format_spec and format_spec not in {"s", ""}:
            raise ValueError("uid placeholder does not support custom format specifiers")
        return self._token


def _build_context(
    *,
    now: datetime.datetime,
    seq: int,
    seq_digits: int,
    uid: str,
) -> dict[str, object]:
    return {
        "year": _DateComponent(now.year, 4),
        "month": _DateComponent(now.month, 2),
        "day": _DateComponent(now.day, 2),
        "hour": _DateComponent(now.hour, 2),
        "minute": _DateComponent(now.minute, 2),
        "second": _DateComponent(now.second, 2),
        "microsecond": _DateComponent(now.microsecond, 6),
        "seq": _SequenceComponent(seq, seq_digits),
        "uid": _UidComponent(uid),
    }


def _format_template(template: str | Path, context: Mapping[str, object]) -> str:
    template_str = str(template)
    try:
        return template_str.format_map(context)
    except KeyError as exc:  # pragma: no cover - defensive
        missing = exc.args[0]
        raise ValueError(f"unknown placeholder {{{missing}}} in template '{template_str}'") from exc


class BuildArtifacts:
    """Represents materialised file paths for a build run."""

    def __init__(self, outdir: Path) -> None:
        self.outdir = outdir
        self.log_path = outdir / "build.log"
        self.nodes_path = outdir / NODES_FILE_NAME
        self.edges_path = outdir / EDGES_FILE_NAME
        self.connections_path = outdir / CONNECTIONS_FILE_NAME


def ensure_output_directory(
    template: OutputDirectoryTemplate | None = None,
) -> BuildArtifacts:
    config = template or OutputDirectoryTemplate()
    if config.seq_digits < 1:
        raise ValueError("seq_digits must be at least 1")

    now = _current_time()
    timestamp_ns = _time_ns()

    for seq in count(1):
        uid = _SQIDS.encode([timestamp_ns, seq])
        context = _build_context(now=now, seq=seq, seq_digits=config.seq_digits, uid=uid)
        root_name = _format_template(config.root, context)
        run_name = _format_template(config.run, context) if config.run else ""

        root_path = Path(root_name)
        root_path.mkdir(parents=True, exist_ok=True)

        if run_name:
            run_path = Path(run_name)
            if run_path.is_absolute():
                raise ValueError("run directory template must be relative")
            outdir = root_path / run_path
        else:
            outdir = root_path

        try:
            outdir.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            continue

        return BuildArtifacts(outdir)

    raise RuntimeError("unable to create unique output directory")  # pragma: no cover - defensive


def write_manifest(artifacts: BuildArtifacts, payload: dict) -> Path:
    path = artifacts.outdir / MANIFEST_NAME
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def persist_xml(artifacts: BuildArtifacts, *, nodes: str, edges: str, connections: str) -> None:
    artifacts.nodes_path.write_text(nodes, encoding="utf-8")
    artifacts.edges_path.write_text(edges, encoding="utf-8")
    artifacts.connections_path.write_text(connections, encoding="utf-8")


__all__ = [
    "BuildArtifacts",
    "ensure_output_directory",
    "persist_xml",
    "write_manifest",
]
