"""Command line interface for the modular corridor pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..domain.models import BuildOptions
from ..pipeline import build_and_persist
from ..utils.constants import SCHEMA_JSON_PATH


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build SUMO PlainXML artefacts from corridor JSON specs")
    parser.add_argument("spec", type=Path, help="Path to the corridor JSON specification")
    parser.add_argument(
        "--schema",
        type=Path,
        default=SCHEMA_JSON_PATH,
        help="Path to the JSON schema (default: schema_v1.2.json)",
    )
    parser.add_argument("--run-netconvert", action="store_true", help="Run two-step netconvert after emission")
    parser.add_argument("--run-netedit", action="store_true", help="Open the generated network in netedit")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    options = BuildOptions(
        schema_path=args.schema,
        run_netconvert=args.run_netconvert,
        run_netedit=args.run_netedit,
        console_log=not args.no_console_log,
    )
    result = build_and_persist(args.spec, options)
    print(result.manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
