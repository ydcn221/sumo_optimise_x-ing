"""Command line entry point for the wrapper builder."""
from __future__ import annotations

import argparse
from pathlib import Path

from . import build_corridor_artifacts
from .options import BuildOptions


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate PlainXML via the legacy converter")
    parser.add_argument("spec", type=Path, help="Path to the corridor specification JSON")
    parser.add_argument("--schema", type=Path, help="Path to the JSON schema", default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory where nodes/edges/connections files will be written",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    options = BuildOptions(
        schema_path=args.schema,
        output_dir=args.output_dir,
        keep_output=args.output_dir is not None,
    )
    artifacts = build_corridor_artifacts(args.spec, options=options)

    if args.output_dir is not None:
        for filename, content in artifacts.as_mapping().items():
            target_path = args.output_dir / filename
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(content, encoding="utf-8")
    else:
        print(artifacts.nodes_xml)
        print(artifacts.edges_xml)
        print(artifacts.connections_xml)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
