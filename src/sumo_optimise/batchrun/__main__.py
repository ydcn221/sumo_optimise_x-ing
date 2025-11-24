from __future__ import annotations

import argparse
from pathlib import Path

from .models import (
    DEFAULT_BEGIN_FILTER,
    DEFAULT_END_TIME,
    DEFAULT_MAX_WORKERS,
    WAITING_THRESHOLD_FIXED,
    WAITING_THRESHOLD_PCT,
    WaitingThresholds,
)
from .orchestrator import load_manifest, run_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch runner for SUMO scenarios")
    parser.add_argument("manifest", type=Path, help="Path to JSON or CSV manifest")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("scenario_runs"),
        help="Root directory for scenario outputs (default: scenario_runs)",
    )
    parser.add_argument(
        "--results",
        type=Path,
        help="Optional path for aggregated CSV (default: <output-root>/results.csv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Maximum parallel workers (default: 32)",
    )
    parser.add_argument(
        "--waiting-threshold-fixed",
        type=float,
        default=WAITING_THRESHOLD_FIXED,
        help="Fixed waiting threshold for summary evaluation (default: 1)",
    )
    parser.add_argument(
        "--waiting-threshold-pct",
        type=float,
        default=WAITING_THRESHOLD_PCT,
        help="Relative waiting threshold expressed as a fraction of running (default: 0.10)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root: Path = args.output_root
    results_path: Path = args.results or output_root / "results.csv"
    thresholds = WaitingThresholds(
        fixed=args.waiting_threshold_fixed,
        pct_of_running=args.waiting_threshold_pct,
    )
    scenarios = load_manifest(args.manifest)
    run_batch(
        scenarios,
        output_root=output_root,
        thresholds=thresholds,
        results_csv=results_path,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
