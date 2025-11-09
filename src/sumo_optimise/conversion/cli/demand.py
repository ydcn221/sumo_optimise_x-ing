"""Demand-only CLI entry point."""
from __future__ import annotations

import importlib

from ..domain.models import BuildTask

main_module = importlib.import_module("sumo_optimise.conversion.cli.main")


def main(argv: list[str] | None = None) -> int:
    parser = main_module._build_parser(  # type: ignore[attr-defined]
        include_netconvert=False,
        include_netedit=True,
        include_demand_flags=True,
        include_network_input=True,
        include_sumo_gui=True,
        include_task=False,
    )
    args = parser.parse_args(argv)
    args.task = BuildTask.DEMAND.value
    return main_module._run_with_args(args, forced_task=BuildTask.DEMAND)  # type: ignore[attr-defined]


if __name__ == "__main__":
    raise SystemExit(main())
