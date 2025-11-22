"""Filesystem helpers that reproduce and extend the legacy behaviour."""
from __future__ import annotations

import datetime
import json
import os
import time
from itertools import count
from pathlib import Path
from typing import Mapping, Sequence

from sqids import Sqids

from ..domain.models import OutputDirectoryTemplate, OutputFileTemplates


_SQIDS = Sqids()
_DEFAULT_SEQ_WIDTH = 3


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
    uid: str,
    epoch_ms: int,
) -> dict[str, object]:
    return {
        "year": _DateComponent(now.year, 4),
        "month": _DateComponent(now.month, 2),
        "day": _DateComponent(now.day, 2),
        "hour": _DateComponent(now.hour, 2),
        "minute": _DateComponent(now.minute, 2),
        "second": _DateComponent(now.second, 2),
        "millisecond": _DateComponent(now.microsecond // 1000, 3),
        "microsecond": _DateComponent(now.microsecond, 6),
        "seq": _SequenceComponent(seq, _DEFAULT_SEQ_WIDTH),
        "uid": _UidComponent(uid),
        "sqid": _UidComponent(uid),
        "squid": _UidComponent(uid),
        "epoch_ms": epoch_ms,
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

    def __init__(
        self,
        outdir: Path,
        *,
        context: Mapping[str, object],
        file_templates: OutputFileTemplates,
    ) -> None:
        self.outdir = outdir
        self._context = context
        self._file_templates = file_templates
        self._path_cache: dict[str, Path] = {}
        self._string_cache: dict[str, str] = {}

    def _resolve_path(self, key: str) -> Path:
        if key not in self._path_cache:
            template = getattr(self._file_templates, key)
            formatted = _format_template(template, self._context)
            candidate = Path(formatted)
            if not candidate.is_absolute():
                candidate = self.outdir / candidate
            self._path_cache[key] = candidate
        return self._path_cache[key]

    def _resolve_string(self, key: str) -> str:
        if key not in self._string_cache:
            template = getattr(self._file_templates, key)
            self._string_cache[key] = _format_template(template, self._context)
        return self._string_cache[key]

    @property
    def log_path(self) -> Path:
        return self._resolve_path("log")

    @property
    def manifest_path(self) -> Path:
        return self._resolve_path("manifest")

    @property
    def nodes_path(self) -> Path:
        return self._resolve_path("nodes")

    @property
    def edges_path(self) -> Path:
        return self._resolve_path("edges")

    @property
    def connections_path(self) -> Path:
        return self._resolve_path("connections")

    @property
    def tll_path(self) -> Path:
        return self._resolve_path("tll")

    @property
    def routes_path(self) -> Path:
        return self._resolve_path("routes")

    @property
    def sumocfg_path(self) -> Path:
        return self._resolve_path("sumocfg")

    @property
    def network_path(self) -> Path:
        return self._resolve_path("network")

    @property
    def pedestrian_network_path(self) -> Path:
        return self._resolve_path("pedestrian_network")

    @property
    def ped_endpoint_template_path(self) -> Path:
        return self._resolve_path("demand_endpoint_template")

    @property
    def ped_junction_template_path(self) -> Path:
        return self._resolve_path("demand_junction_template")

    @property
    def veh_endpoint_template_path(self) -> Path:
        return self._resolve_path("vehicle_endpoint_template")

    @property
    def veh_junction_template_path(self) -> Path:
        return self._resolve_path("vehicle_junction_template")

    @property
    def netconvert_prefix(self) -> str:
        return self._resolve_string("netconvert_plain_prefix")


def ensure_output_directory(
    template: OutputDirectoryTemplate | None = None,
    file_templates: OutputFileTemplates | None = None,
) -> BuildArtifacts:
    config = template or OutputDirectoryTemplate()
    file_config = file_templates or OutputFileTemplates()

    literal_run = bool(config.run) and "{" not in config.run

    for seq in count(1):
        now = _current_time()
        timestamp_ns = _time_ns()
        epoch_ms = timestamp_ns // 1_000_000
        uid = _SQIDS.encode([epoch_ms])
        context = _build_context(
            now=now,
            seq=seq,
            uid=uid,
            epoch_ms=epoch_ms,
        )
        root_name = _format_template(config.root, context)
        run_name = _format_template(config.run, context) if config.run else ""

        root_path = Path(root_name)
        root_path.mkdir(parents=True, exist_ok=True)

        if run_name:
            run_path = Path(run_name)
            anchor = getattr(run_path, "anchor", "")
            if run_path.is_absolute() or anchor not in {"", None}:
                raise ValueError("run directory template must be relative")
            outdir = root_path / run_path
        else:
            outdir = root_path

        if outdir.exists():
            if not outdir.is_dir():
                raise ValueError(f"output path exists and is not a directory: {outdir}")
            if literal_run:
                if any(outdir.iterdir()):
                    raise ValueError(f"output directory already exists and is not empty: {outdir}")
                return BuildArtifacts(outdir, context=context, file_templates=file_config)
            continue
        else:
            outdir.mkdir(parents=True, exist_ok=False)

        return BuildArtifacts(outdir, context=context, file_templates=file_config)

    raise RuntimeError("unable to create unique output directory")  # pragma: no cover - defensive


def write_manifest(artifacts: BuildArtifacts, payload: dict) -> Path:
    _write_text(artifacts.manifest_path, json.dumps(payload, indent=2, ensure_ascii=False))
    return artifacts.manifest_path


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def persist_xml(
    artifacts: BuildArtifacts,
    *,
    nodes: str,
    edges: str,
    connections: str,
    tll: str,
) -> None:
    _write_text(artifacts.nodes_path, nodes)
    _write_text(artifacts.edges_path, edges)
    _write_text(artifacts.connections_path, connections)
    _write_text(artifacts.tll_path, tll)


def persist_routes(artifacts: BuildArtifacts, *, demand: str) -> None:
    _write_text(artifacts.routes_path, demand)


def _config_value(path: Path, base: Path) -> str:
    try:
        relative = path.relative_to(base)
        return relative.as_posix()
    except ValueError:
        try:
            rel = Path(os.path.relpath(path, base))
            return rel.as_posix()
        except ValueError:
            return path.resolve().as_posix()


def write_sumocfg(artifacts: BuildArtifacts, *, net_path: Path, routes_path: Path) -> None:
    """Emit a minimal SUMO configuration referencing the generated net and routes."""

    net_value = _config_value(net_path, artifacts.sumocfg_path.parent)
    route_value = _config_value(routes_path, artifacts.sumocfg_path.parent)
    content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<configuration>\n"
        "    <input>\n"
        f'        <net-file value="{net_value}"/>\n'
        f'        <route-files value="{route_value}"/>\n'
        '        <junction-taz value="true"/>\n'
        "    </input>\n"
        "    <!-- <time>\n"
        '         <step-length value="0.01"/>\n'
        "    </time> -->\n"
        "    <!-- <output>\n"
        '         <summary-output value="summary.xml"/>\n'
        '         <person-summary-output value="personSummary.xml"/>\n'
        '         <fcd-output value="fcd.xml"/>\n'
        '         <tripinfo-output value="tripinfo.xml"/>\n'
        "    </output> -->\n"
        "</configuration>\n"
    )
    _write_text(artifacts.sumocfg_path, content)


__all__ = [
    "BuildArtifacts",
    "ensure_output_directory",
    "persist_xml",
    "persist_routes",
    "write_sumocfg",
    "write_manifest",
]
