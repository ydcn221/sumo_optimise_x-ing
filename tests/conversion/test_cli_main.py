from __future__ import annotations

from sumo_optimise.conversion.cli.main import (
    _build_options,
    _resolve_output_files,
    _resolve_output_template,
    parse_args,
)
from sumo_optimise.conversion.domain.models import OutputDirectoryTemplate, OutputFileTemplates


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
    file_templates = _resolve_output_files(args)
    options = _build_options(args, template, file_templates)

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


def test_cli_generate_demand_templates_flag() -> None:
    args = parse_args(["spec.json", "--generate-demand-templates"])
    template = _resolve_output_template(args)
    file_templates = _resolve_output_files(args)
    options = _build_options(args, template, file_templates)

    assert options.generate_demand_templates is True


def test_cli_output_file_template_overrides() -> None:
    args = parse_args(["spec.json", "--output-file-template", "routes=routes/{sqid}.xml"])
    file_templates = _resolve_output_files(args)

    assert isinstance(file_templates, OutputFileTemplates)
    assert file_templates.routes == "routes/{sqid}.xml"


def test_cli_task_defaults_to_all() -> None:
    args = parse_args(["spec.json"])

    assert args.task == "all"
