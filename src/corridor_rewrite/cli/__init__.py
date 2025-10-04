"""Command line entry point for corridor rewrite pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..options import BuildOptions
from ..pipeline import build_corridor_artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build SUMO PlainXML from corridor JSON specs")
    parser.add_argument("spec", type=Path, help="Path to the corridor specification JSON file")
    parser.add_argument("--schema", type=Path, help="Optional JSON schema path", default=None)
    parser.add_argument("--workdir", type=Path, help="Output directory for artefacts", default=Path("build"))
    parser.add_argument("--run-netconvert", action="store_true", help="Invoke netconvert after emitting XML")
    args = parser.parse_args(argv)

    options = BuildOptions(run_netconvert=args.run_netconvert, schema_path=args.schema, workdir=args.workdir)
    artefacts = build_corridor_artifacts(args.spec, options)

    workdir = options.ensure_workdir()
    for name, content in artefacts.as_mapping().items():
        path = workdir / name
        path.write_text(content, encoding="utf-8")

    manifest = {
        "spec": str(args.spec),
        "schema": str(args.schema) if args.schema else None,
        "artefacts": list(artefacts.as_mapping().keys()),
    }
    (workdir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
