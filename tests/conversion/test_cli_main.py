from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from sumo_optimise.conversion.cli.main import (
    _build_options,
    _resolve_output_template,
    main,
    parse_args,
)
from sumo_optimise.conversion.domain.models import OutputDirectoryTemplate
from sumo_optimise.conversion.utils import io


def test_cli_defaults_to_standard_output_template() -> None:
    args = parse_args(["spec.json"])
    template = _resolve_output_template(args)

    assert template == OutputDirectoryTemplate()


def test_cli_run_netedit_flag_defaults_to_false() -> None:
    args = parse_args(["spec.json"])

    assert args.run_netedit is False


def test_cli_run_netedit_enables_netconvert() -> None:
    args = parse_args(["spec.json", "--run-netedit"])
    template = _resolve_output_template(args)
    options = _build_options(args, template)

    assert options.run_netedit is True
    assert options.run_netconvert is True


def test_cli_accepts_output_template_overrides() -> None:
    args = parse_args(
        [
            "spec.json",
            "--output-root",
            "runs/{year}{month}",
            "--output-run",
            "{hour}{minute}-{seq:04}",
            "--output-seq-digits",
            "4",
        ]
    )
    template = _resolve_output_template(args)

    assert template.root == "runs/{year}{month}"
    assert template.run == "{hour}{minute}-{seq:04}"
    assert template.seq_digits == 4


def test_cli_writes_tllogics_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: "pytest.CaptureFixture[str]"
) -> None:
    monkeypatch.chdir(tmp_path)
    fixed_time = datetime.datetime(2024, 5, 1, 12, 0, 0)
    monkeypatch.setattr(io, "_current_time", lambda: fixed_time)
    monkeypatch.setattr(io, "_time_ns", lambda: 123456789)
    monkeypatch.setattr(
        "sumo_optimise.conversion.pipeline.configure_logger",
        lambda *args, **kwargs: None,
    )

    spec_path = Path(__file__).resolve().parents[2] / "data" / "reference" / "schema_v1.3_sample.json"
    exit_code = main([str(spec_path)])

    assert exit_code == 0

    captured = capsys.readouterr()
    manifest_path = Path(captured.out.strip())
    tll_path = manifest_path.parent / "net.tll.xml"

    assert tll_path.exists()
    assert "<tlLogics" in tll_path.read_text(encoding="utf-8")
