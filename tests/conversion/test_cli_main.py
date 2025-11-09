from __future__ import annotations

from pathlib import Path
import importlib

import pytest

cli_main_module = importlib.import_module("sumo_optimise.conversion.cli.main")
from sumo_optimise.conversion.cli.main import (
    _build_options,
    _resolve_output_files,
    _resolve_output_template,
    parse_args,
)
from sumo_optimise.conversion.domain.models import BuildResult, OutputDirectoryTemplate, OutputFileTemplates


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
        ]
    )
    template = _resolve_output_template(args)

    assert template.root == "runs/{year}{month}"
    assert template.run == "{hour}{minute}-{seq:04}"


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


def test_cli_short_aliases_parse_correctly(tmp_path) -> None:
    args = parse_args(
        [
            "spec.json",
            "-sc",
            "schema.json",
            "-nc",
            "-ne",
            "-nl",
            "-or",
            "root_dir",
            "-rr",
            "run_dir",
            "-ft",
            "routes=routes/{seq}.xml",
            "-pe",
            "ped.csv",
            "-pj",
            "ped_turn.csv",
            "-ve",
            "veh.csv",
            "-vj",
            "veh_turn.csv",
            "-ni",
            "net.xml",
            "-ds",
            "7200",
            "-gt",
            "-sg",
            "-ta",
            "network",
        ]
    )

    assert args.schema == Path("schema.json")
    assert args.run_netconvert is True
    assert args.run_netedit is True
    assert args.no_console_log is True
    assert args.output_root_template == "root_dir"
    assert args.output_run_template == "run_dir"
    assert args.output_file_template == ["routes=routes/{seq}.xml"]
    assert args.ped_endpoint_demand == Path("ped.csv")
    assert args.ped_junction_turn_weight == Path("ped_turn.csv")
    assert args.veh_endpoint_demand == Path("veh.csv")
    assert args.veh_junction_turn_weight == Path("veh_turn.csv")
    assert args.network_input == Path("net.xml")
    assert args.demand_sim_end == 7200
    assert args.generate_demand_templates is True
    assert args.run_sumo_gui is True
    assert args.task == "network"


def test_cli_demand_requires_network_for_netedit() -> None:
    spec = "data/reference/SUMO_OPTX_demo(connection_build)/SUMO_OPTX_v1.4_sample.json"
    with pytest.raises(SystemExit):
        cli_main_module.main([spec, "--task", "demand", "--run-netedit"])


def test_cli_demand_accepts_network_input_for_netedit(tmp_path) -> None:
    spec = "data/reference/SUMO_OPTX_demo(connection_build)/SUMO_OPTX_v1.4_sample.json"
    net = tmp_path / "existing.net.xml"
    net.write_text("<net/>", encoding="utf-8")

    called = {}

    def fake_build(spec_path, options, task):
        called["args"] = (spec_path, task, options.network_input)
        return BuildResult("", "", "", [], "", manifest_path=tmp_path / "manifest.json")

    original = cli_main_module.build_and_persist
    cli_main_module.build_and_persist = fake_build  # type: ignore[attr-defined]
    try:
        exit_code = cli_main_module.main(
            [
                spec,
                "--task",
                "demand",
                "--run-netedit",
                "--generate-demand-templates",
                "--network-input",
                str(net),
            ]
        )
    finally:
        cli_main_module.build_and_persist = original  # type: ignore[attr-defined]

    assert exit_code == 0
    assert Path(called["args"][0]) == Path(spec)
    assert str(called["args"][2]) == str(net.resolve())


def test_cli_network_rejects_network_input(tmp_path) -> None:
    spec = "data/reference/SUMO_OPTX_demo(connection_build)/SUMO_OPTX_v1.4_sample.json"
    net = tmp_path / "existing.net.xml"
    net.write_text("<net/>", encoding="utf-8")
    with pytest.raises(SystemExit):
        cli_main_module.main([spec, "--task", "network", "--network-input", str(net)])


def test_cli_run_sumo_gui_requires_demand_task() -> None:
    spec = "spec.json"
    with pytest.raises(SystemExit):
        cli_main_module.main([spec, "--task", "network", "--run-sumo-gui"])


def test_cli_demand_requires_network_for_sumo_gui() -> None:
    spec = "data/reference/SUMO_OPTX_demo(connection_build)/SUMO_OPTX_v1.4_sample.json"
    with pytest.raises(SystemExit):
        cli_main_module.main([spec, "--task", "demand", "--run-sumo-gui"])
