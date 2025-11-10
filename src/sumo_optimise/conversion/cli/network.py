"""Network-only CLI entry point."""
from __future__ import annotations

import importlib

from ..domain.models import BuildTask

main_module = importlib.import_module("sumo_optimise.conversion.cli.main")


def main(argv: list[str] | None = None) -> int:
    parser = main_module._build_parser(  # type: ignore[attr-defined]
        include_netconvert=True,
        include_netedit=True,
        include_demand_flags=False,
        include_network_input=False,
        include_sumo_gui=False,
        include_task=False,
    )
    args = parser.parse_args(argv)
    args.task = BuildTask.NETWORK.value
    return main_module._run_with_args(args, forced_task=BuildTask.NETWORK)  # type: ignore[attr-defined]


if __name__ == "__main__":
    raise SystemExit(main())
