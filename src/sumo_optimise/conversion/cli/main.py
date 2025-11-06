"""Command line interface for the modular corridor pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..domain.models import BuildOptions, DemandOptions, OutputDirectoryTemplate
from ..pipeline import build_and_persist
from ..utils.constants import SCHEMA_JSON_PATH


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
            "Supports placeholders such as {year}, {month}, {day}, {seq}, and {uid}."
        ),
    )
    parser.add_argument(
        "--output-run",
        dest="output_run_template",
        help=(
            "Template for the per-run directory relative to the root (default: {month}{day}_{seq:03}). "
            "Supports placeholders such as {hour}, {minute}, {second}, {seq}, and {uid}."
        ),
    )
    parser.add_argument(
        "--output-seq-digits",
        dest="output_seq_digits",
        type=int,
        help="Number of digits for the zero-padded {seq} placeholder (default: 3)",
    )
    parser.add_argument(
        "--ped-demand-endpoint",
        type=Path,
        help="Path to pedestrian endpoint demand CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--ped-direction-ratio",
        type=Path,
        help="Path to pedestrian direction ratio CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--veh-demand-endpoint",
        type=Path,
        help="Path to vehicle endpoint demand CSV (utf-8-sig)",
    )
    parser.add_argument(
        "--veh-turn-ratio",
        type=Path,
        help="Path to vehicle turn ratio CSV (utf-8-sig)",
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
    return parser.parse_args(argv)


def _resolve_output_template(args: argparse.Namespace) -> OutputDirectoryTemplate:
    default_template = OutputDirectoryTemplate()
    return OutputDirectoryTemplate(
        root=args.output_root_template if args.output_root_template is not None else default_template.root,
        run=args.output_run_template if args.output_run_template is not None else default_template.run,
        seq_digits=args.output_seq_digits if args.output_seq_digits is not None else default_template.seq_digits,
    )


def _build_options(args: argparse.Namespace, output_template: OutputDirectoryTemplate) -> BuildOptions:
    demand_options = _resolve_demand_options(args)
    return BuildOptions(
        schema_path=args.schema,
        run_netconvert=args.run_netconvert or args.run_netedit,
        run_netedit=args.run_netedit,
        console_log=not args.no_console_log,
        output_template=output_template,
        demand=demand_options,
        generate_demand_templates=args.generate_demand_templates,
    )


def _resolve_demand_options(args: argparse.Namespace) -> DemandOptions | None:
    ped_endpoint_csv = args.ped_demand_endpoint
    ped_ratio_csv = args.ped_direction_ratio
    veh_endpoint_csv = getattr(args, "veh_demand_endpoint", None)
    veh_ratio_csv = getattr(args, "veh_turn_ratio", None)

    if (
        ped_endpoint_csv is None
        and ped_ratio_csv is None
        and veh_endpoint_csv is None
        and veh_ratio_csv is None
    ):
        return None

    if (ped_endpoint_csv is None) != (ped_ratio_csv is None):
        raise SystemExit("Both --ped-demand-endpoint and --ped-direction-ratio must be provided together")
    if (veh_endpoint_csv is None) != (veh_ratio_csv is None):
        raise SystemExit("Both --veh-demand-endpoint and --veh-turn-ratio must be provided together")

    return DemandOptions(
        ped_endpoint_csv=ped_endpoint_csv,
        ped_direction_ratio_csv=ped_ratio_csv,
        veh_endpoint_csv=veh_endpoint_csv,
        veh_direction_ratio_csv=veh_ratio_csv,
        simulation_end_time=args.demand_sim_end,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_template = _resolve_output_template(args)
    options = _build_options(args, output_template)
    result = build_and_persist(args.spec, options)
    print(result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
