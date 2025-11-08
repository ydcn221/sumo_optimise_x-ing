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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build SUMO PlainXML artefacts from corridor JSON specs")
    parser.add_argument("spec", type=Path, help="Path to the corridor JSON specification")
    parser.add_argument(
        "--schema",
        type=Path,
        default=SCHEMA_JSON_PATH,
        help="Path to the JSON schema (default: schema.json)",
    )
    parser.add_argument("--run-netconvert", action="store_true", help="Run two-step netconvert after emission")
    parser.add_argument("--run-netedit", action="store_true", help="Launch SUMO netedit with the generated network")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    parser.add_argument(
        "--output-root",
        dest="output_root_template",
        help=(
            "Template for the root output directory (default: plainXML_out). "
            "Supports placeholders such as {year}, {month}, {day}, {hour}, {minute}, {second}, {millisecond}, "
            "{seq}, {sqid}, and {epoch_ms}."
        ),
    )
    parser.add_argument(
        "--output-run",
        dest="output_run_template",
        help=(
            "Template for the per-run directory relative to the root (default: {month}{day}_{seq:03}). "
            "Supports placeholders such as {hour}, {minute}, {second}, {millisecond}, {seq}, {sqid}, and {epoch_ms}."
        ),
    )
    parser.add_argument(
        "--output-seq-digits",
        dest="output_seq_digits",
        type=int,
        help="Number of digits for the zero-padded {seq} placeholder (default: 3)",
    )
    parser.add_argument(
        "--output-file-template",
        dest="output_file_template",
        metavar="KEY=VALUE",
        action="append",
        help=(
            "Override the template for a specific artefact. "
            f"Valid keys: {', '.join(_FILE_TEMPLATE_KEYS)}. "
            "Templates support the same placeholders as --output-root."
        ),
    )
    parser.add_argument(
        "--ped-endpoint-demand",
        type=Path,
        help="Path to pedestrian endpoint demand CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--ped-junction-turn-weight",
        type=Path,
        help="Path to pedestrian junction turn-weight CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--veh-endpoint-demand",
        type=Path,
        help="Path to vehicle endpoint demand CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--veh-junction-turn-weight",
        type=Path,
        help="Path to vehicle junction turn-weight CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--demand-sim-end",
        type=float,
        default=3600.0,
        help="Simulation end time for person flows (seconds, default: 3600)",
    )
    parser.add_argument(
        "--generate-demand-templates",
        action="store_true",
        help="Emit CSV templates for endpoint demand and junction ratios instead of populated data",
    )
    parser.add_argument(
        "--task",
        choices=[task.value for task in BuildTask],
        default=BuildTask.ALL.value,
        help="Select which stage to run: network, demand, or all (default: all).",
    )
    return parser.parse_args(argv)


def _resolve_output_template(args: argparse.Namespace) -> OutputDirectoryTemplate:
    default_template = OutputDirectoryTemplate()
    return OutputDirectoryTemplate(
        root=args.output_root_template if args.output_root_template is not None else default_template.root,
        run=args.output_run_template if args.output_run_template is not None else default_template.run,
        seq_digits=args.output_seq_digits if args.output_seq_digits is not None else default_template.seq_digits,
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
        console_log=not args.no_console_log,
        output_template=output_template,
        output_files=file_templates,
        demand=demand_options,
        generate_demand_templates=args.generate_demand_templates,
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_template = _resolve_output_template(args)
    file_templates = _resolve_output_files(args)
    options = _build_options(args, output_template, file_templates)
    task = BuildTask(args.task)

    if task is BuildTask.DEMAND and args.run_netconvert:
        raise SystemExit("--run-netconvert is only available for network or all tasks.")
    if task is BuildTask.DEMAND and args.run_netedit:
        raise SystemExit("--run-netedit is only available for network or all tasks.")
    if (
        task is BuildTask.DEMAND
        and not options.generate_demand_templates
        and options.demand is None
    ):
        raise SystemExit("Demand task requires demand CSV inputs or --generate-demand-templates.")

    result = build_and_persist(args.spec, options, task=task)
    print(result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
