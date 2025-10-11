from __future__ import annotations

from sumo_optimise.conversion.cli.main import _resolve_output_template, parse_args
from sumo_optimise.conversion.domain.models import OutputDirectoryTemplate


def test_cli_defaults_to_standard_output_template() -> None:
    args = parse_args(["spec.json"])
    template = _resolve_output_template(args)

    assert template == OutputDirectoryTemplate()


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
