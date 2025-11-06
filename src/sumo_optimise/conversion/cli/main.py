"""Command line interface for the modular corridor pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..domain.models import (
    BuildOptions,
    DemandOptions,
    OutputDirectoryTemplate,
    PersonFlowPattern,
)
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
    parser.add_argument("--demand-endpoints", type=Path, help="Path to endpoint demand CSV (utf-8-sig)")
    parser.add_argument("--demand-junctions", type=Path, help="Path to junction ratio CSV (utf-8-sig)")
    parser.add_argument(
        "--demand-pattern",
        choices=[pattern.value for pattern in PersonFlowPattern],
        default=PersonFlowPattern.PERSONS_PER_HOUR.value,
        help="personFlow attribute to encode demand intensity (default: persons_per_hour)",
    )
    parser.add_argument(
        "--demand-sim-end",
        type=float,
        default=3600.0,
        help="Simulation end time for person flows (seconds, default: 3600)",
    )
    parser.add_argument(
        "--demand-endpoint-offset",
        type=float,
        default=0.10,
        help="Offset from edge extremities when spawning/arriving persons (default: 0.10m)",
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
    )


def _resolve_demand_options(args: argparse.Namespace) -> DemandOptions | None:
    endpoint_csv = args.demand_endpoints
    junction_csv = args.demand_junctions
    if endpoint_csv is None and junction_csv is None:
        return None
    if endpoint_csv is None or junction_csv is None:
        raise SystemExit("Both --demand-endpoints and --demand-junctions must be provided together")
    pattern = PersonFlowPattern(args.demand_pattern)
    return DemandOptions(
        endpoint_csv=endpoint_csv,
        junction_csv=junction_csv,
        pattern=pattern,
        simulation_end_time=args.demand_sim_end,
        endpoint_offset_m=args.demand_endpoint_offset,
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
