from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from sqids import Sqids

from sumo_optimise.conversion.domain.models import OutputDirectoryTemplate
from sumo_optimise.conversion.utils import io


def _freeze_time(monkeypatch, *, now: datetime.datetime, ns: int) -> None:
    monkeypatch.setattr(io, "_current_time", lambda: now)
    monkeypatch.setattr(io, "_time_ns", lambda: ns)


def test_default_directory_sequence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    frozen = datetime.datetime(2024, 3, 5, 7, 8, 9, 123456)
    _freeze_time(monkeypatch, now=frozen, ns=9876543210)

    first = io.ensure_output_directory()
    second = io.ensure_output_directory()

    assert first.outdir == Path("plainXML_out/0305_001")
    assert second.outdir == Path("plainXML_out/0305_002")
    assert first.outdir.resolve() == tmp_path / "plainXML_out" / "0305_001"
    assert second.outdir.resolve() == tmp_path / "plainXML_out" / "0305_002"


def test_custom_templates_support_placeholders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    frozen = datetime.datetime(2023, 12, 31, 23, 59, 58, 654321)
    ns_value = 123456789
    _freeze_time(monkeypatch, now=frozen, ns=ns_value)

    template = OutputDirectoryTemplate(
        root=str(tmp_path / "runs" / "{year}{month}{day}"),
        run="{hour}{minute}{second}-{uid}-{seq:04}",
        seq_digits=4,
    )

    sqids = Sqids()
    expected_uid_first = sqids.encode([ns_value, 1])
    expected_uid_second = sqids.encode([ns_value, 2])

    first = io.ensure_output_directory(template)
    second = io.ensure_output_directory(template)

    expected_root = tmp_path / "runs" / "20231231"
    assert first.outdir == expected_root / f"235958-{expected_uid_first}-0001"
    assert second.outdir == expected_root / f"235958-{expected_uid_second}-0002"


def test_run_template_must_be_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    frozen = datetime.datetime(2024, 1, 1, 0, 0, 0)
    _freeze_time(monkeypatch, now=frozen, ns=42)

    template = OutputDirectoryTemplate(root="base", run="/absolute")

    with pytest.raises(ValueError):
        io.ensure_output_directory(template)


def test_build_artifacts_exposes_tll_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    frozen = datetime.datetime(2024, 3, 5, 7, 8, 9, 123456)
    _freeze_time(monkeypatch, now=frozen, ns=123)

    artifacts = io.ensure_output_directory()

    assert artifacts.tll_path == artifacts.outdir / "net.tll.xml"


def test_persist_xml_writes_all_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    frozen = datetime.datetime(2024, 6, 1, 12, 0, 0)
    _freeze_time(monkeypatch, now=frozen, ns=321)

    artifacts = io.ensure_output_directory()

    io.persist_xml(
        artifacts,
        nodes="<nodes/>",
        edges="<edges/>",
        connections="<connections/>",
        tll="<tlLogics/>",
    )

    assert artifacts.nodes_path.read_text(encoding="utf-8") == "<nodes/>"
    assert artifacts.edges_path.read_text(encoding="utf-8") == "<edges/>"
    assert artifacts.connections_path.read_text(encoding="utf-8") == "<connections/>"
    assert artifacts.tll_path.read_text(encoding="utf-8") == "<tlLogics/>"
