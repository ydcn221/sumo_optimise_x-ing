"""Command line interface for the modular corridor pipeline."""
from __future__ import annotations

import argparse
from dataclasses import fields, replace
from pathlib import Path

from ..domain.models import (
    BuildOptions,
    BuildTask,
    DemandOptions,
    OutputDirectoryTemplate,
    OutputFileTemplates,
)
from ..pipeline import build_and_persist
from ..utils.constants import SCHEMA_JSON_PATH


_FILE_TEMPLATE_KEYS = tuple(field.name for field in fields(OutputFileTemplates))


def _build_parser(
    *,
    include_netconvert: bool,
    include_netedit: bool,
    include_demand_flags: bool,
    include_network_input: bool,
    include_sumo_gui: bool,
    include_task: bool,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build SUMO PlainXML artefacts from corridor JSON specs")
    parser.add_argument("spec", type=Path, help="Path to the corridor JSON specification")
    parser.add_argument(
        "--schema",
        "-sc",
        type=Path,
        default=SCHEMA_JSON_PATH,
        help="Path to the JSON schema (default: schema.json)",
    )
    if include_netconvert:
        parser.add_argument("--run-netconvert", "-nc", action="store_true", help="Run two-step netconvert after emission")
    else:
        parser.set_defaults(run_netconvert=False)
    if include_netedit:
        parser.add_argument("--run-netedit", "-ne", action="store_true", help="Launch SUMO netedit with the generated network")
    else:
        parser.set_defaults(run_netedit=False)
    parser.add_argument("--no-console-log", "-nl", action="store_true", help="Disable console logging")
    parser.add_argument(
        "--output-root",
        "-or",
        dest="output_root_template",
        help=(
            "Template for the root output directory (default: plainXML_out). "
            "Supports placeholders such as {year}, {month}, {day}, {hour}, {minute}, {second}, {millisecond}, "
            "{seq}, {sqid}, and {epoch_ms}."
        ),
    )
    parser.add_argument(
        "--output-run",
        "-rr",
        dest="output_run_template",
        help=(
            "Template for the per-run directory relative to the root (default: {month}{day}_{seq:03}). "
            "Supports placeholders such as {hour}, {minute}, {second}, {millisecond}, {seq}, {sqid}, and {epoch_ms}."
        ),
    )
    parser.add_argument(
        "--output-file-template",
        "-ft",
        dest="output_file_template",
        metavar="KEY=VALUE",
        action="append",
        help=(
            "Override the template for a specific artefact. "
            f"Valid keys: {', '.join(_FILE_TEMPLATE_KEYS)}. "
            "Templates support the same placeholders as --output-root."
        ),
    )

    if include_demand_flags:
        parser.add_argument(
            "--ped-endpoint-demand",
            "-pe",
            type=Path,
            help="Path to pedestrian endpoint demand CSV (utf-8-sig)",
        )
        parser.add_argument(
            "--ped-junction-turn-weight",
            "-pj",
            type=Path,
            help="Path to pedestrian junction turn-weight CSV (utf-8-sig)",
        )
        parser.add_argument(
            "--veh-endpoint-demand",
            "-ve",
            type=Path,
            help="Path to vehicle endpoint demand CSV (utf-8-sig)",
        )
        parser.add_argument(
            "--veh-junction-turn-weight",
            "-vj",
            type=Path,
            help="Path to vehicle junction turn-weight CSV (utf-8-sig)",
        )
        parser.add_argument(
            "--demand-sim-end",
            "-ds",
            type=float,
            default=3600.0,
            help="Simulation end time for person flows (seconds, default: 3600)",
        )
        parser.add_argument(
            "--generate-demand-templates",
            "-gt",
            action="store_true",
            help="Emit CSV templates for endpoint demand and junction ratios instead of populated data",
        )
    else:
        parser.set_defaults(
            ped_endpoint_demand=None,
            ped_junction_turn_weight=None,
            veh_endpoint_demand=None,
            veh_junction_turn_weight=None,
            demand_sim_end=3600.0,
            generate_demand_templates=False,
        )

    if include_network_input:
        parser.add_argument(
            "--network-input",
            "-ni",
            type=Path,
            help="Path to an existing net.xml file to reuse (demand runs)",
        )
    else:
        parser.set_defaults(network_input=None)

    if include_sumo_gui:
        parser.add_argument(
            "--run-sumo-gui",
            "-sg",
            action="store_true",
            help="Launch sumo-gui with the generated SUMO config (requires demand outputs)",
        )
    else:
        parser.set_defaults(run_sumo_gui=False)

    if include_task:
        parser.add_argument(
            "--task",
            "-ta",
            choices=[task.value for task in BuildTask],
            default=BuildTask.ALL.value,
            help="Select which stage to run: network, demand, or all (default: all).",
        )
    else:
        parser.set_defaults(task=BuildTask.ALL.value)

    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = _build_parser(
        include_netconvert=True,
        include_netedit=True,
        include_demand_flags=True,
        include_network_input=True,
        include_sumo_gui=True,
        include_task=True,
    )
    return parser.parse_args(argv)


def _resolve_output_template(args: argparse.Namespace) -> OutputDirectoryTemplate:
    default_template = OutputDirectoryTemplate()
    return OutputDirectoryTemplate(
        root=args.output_root_template if args.output_root_template is not None else default_template.root,
        run=args.output_run_template if args.output_run_template is not None else default_template.run,
    )


def _resolve_output_files(args: argparse.Namespace) -> OutputFileTemplates:
    overrides: dict[str, str] = {}
    for raw in args.output_file_template or []:
        key, _, value = raw.partition("=")
        if not value:
            raise SystemExit(f"Invalid --output-file-template '{raw}'. Expected KEY=VALUE.")
        key = key.strip()
        if key not in _FILE_TEMPLATE_KEYS:
            raise SystemExit(f"Unknown output template key '{key}'. Expected one of: {', '.join(_FILE_TEMPLATE_KEYS)}.")
        overrides[key] = value

    template = OutputFileTemplates()
    if not overrides:
        return template
    return replace(template, **overrides)


def _build_options(
    args: argparse.Namespace,
    output_template: OutputDirectoryTemplate,
    file_templates: OutputFileTemplates,
) -> BuildOptions:
    demand_options = _resolve_demand_options(args)
    return BuildOptions(
        schema_path=args.schema,
        run_netconvert=args.run_netconvert or args.run_netedit,
        run_netedit=args.run_netedit,
        run_sumo_gui=args.run_sumo_gui,
        console_log=not args.no_console_log,
        output_template=output_template,
        output_files=file_templates,
        demand=demand_options,
        generate_demand_templates=args.generate_demand_templates,
        network_input=args.network_input,
    )


def _resolve_demand_options(args: argparse.Namespace) -> DemandOptions | None:
    ped_endpoint_csv = args.ped_endpoint_demand
    ped_ratio_csv = args.ped_junction_turn_weight
    veh_endpoint_csv = args.veh_endpoint_demand
    veh_ratio_csv = args.veh_junction_turn_weight

    if (
        ped_endpoint_csv is None
        and ped_ratio_csv is None
        and veh_endpoint_csv is None
        and veh_ratio_csv is None
    ):
        return None

    if (ped_endpoint_csv is None) != (ped_ratio_csv is None):
        raise SystemExit("Both --ped-endpoint-demand and --ped-junction-turn-weight must be provided together")
    if (veh_endpoint_csv is None) != (veh_ratio_csv is None):
        raise SystemExit("Both --veh-endpoint-demand and --veh-junction-turn-weight must be provided together")

    return DemandOptions(
        ped_endpoint_csv=ped_endpoint_csv,
        ped_junction_turn_weight_csv=ped_ratio_csv,
        veh_endpoint_csv=veh_endpoint_csv,
        veh_junction_turn_weight_csv=veh_ratio_csv,
        simulation_end_time=args.demand_sim_end,
    )


def _run_with_args(args: argparse.Namespace, *, forced_task: BuildTask | None = None) -> int:
    if forced_task is not None:
        args.task = forced_task.value

    output_template = _resolve_output_template(args)
    file_templates = _resolve_output_files(args)

    if args.network_input is not None:
        if not args.network_input.exists():
            raise SystemExit(f"--network-input path not found: {args.network_input}")
        if not args.network_input.is_file():
            raise SystemExit("--network-input must point to a file")
        args.network_input = args.network_input.resolve()

    options = _build_options(args, output_template, file_templates)
    task = BuildTask(args.task)
    network_input = args.network_input

    if task.includes_network() and network_input is not None:
        raise SystemExit("--network-input can only be used when running demand-only tasks")

    if args.run_netconvert and not task.includes_network():
        raise SystemExit("--run-netconvert is only available when building the network")

    if args.run_netedit and not (task.includes_network() or network_input):
        raise SystemExit("--run-netedit requires a generated network or --network-input")

    if args.run_sumo_gui and not task.includes_demand():
        raise SystemExit("--run-sumo-gui requires demand outputs (task must include demand)")

    if task is BuildTask.DEMAND and args.run_sumo_gui and network_input is None:
        raise SystemExit("--run-sumo-gui requires --network-input when task=demand")

    if (
        task is BuildTask.DEMAND
        and not options.generate_demand_templates
        and options.demand is None
    ):
        raise SystemExit("Demand task requires demand CSV inputs or --generate-demand-templates.")

    result = build_and_persist(args.spec, options, task=task)
    print(result.manifest_path)
    return 0


def main(argv: list[str] | None = None, *, forced_task: BuildTask | None = None) -> int:
    args = parse_args(argv)
    return _run_with_args(args, forced_task=forced_task)


if __name__ == "__main__":
    raise SystemExit(main())
