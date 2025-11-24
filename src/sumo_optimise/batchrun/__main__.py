from __future__ import annotations

import argparse
from pathlib import Path

from .models import (
    DEFAULT_BEGIN_FILTER,
    DEFAULT_END_TIME,
    DEFAULT_MAX_WORKERS,
    DEFAULT_QUEUE_THRESHOLD_LENGTH,
    DEFAULT_QUEUE_THRESHOLD_STEPS,
    DEFAULT_SCALE_PROBE_CEILING,
    DEFAULT_SCALE_PROBE_RESOLUTION,
    DEFAULT_SCALE_PROBE_START,
    WAITING_THRESHOLD_FIXED,
    WAITING_THRESHOLD_PCT,
    QueueDurabilityConfig,
    ScaleProbeConfig,
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
    parser.add_argument(
        "--queue-threshold-steps",
        type=int,
        default=DEFAULT_QUEUE_THRESHOLD_STEPS,
        help="Consecutive timesteps required above queueing_length threshold to mark as non-durable (default: 10)",
    )
    parser.add_argument(
        "--queue-threshold-length",
        type=float,
        default=DEFAULT_QUEUE_THRESHOLD_LENGTH,
        help="Queueing_length threshold in meters for durability check (default: 200)",
    )
    parser.add_argument(
        "--enable-scale-probe",
        action="store_true",
        help="Search for the minimal scale that violates queue durability (runs additional SUMO simulations)",
    )
    parser.add_argument(
        "--scale-probe-start",
        type=float,
        default=DEFAULT_SCALE_PROBE_START,
        help="Starting scale for durability probing (default: 1.0)",
    )
    parser.add_argument(
        "--scale-probe-ceiling",
        type=float,
        default=DEFAULT_SCALE_PROBE_CEILING,
        help="Maximum scale tested before giving up (default: 10.0)",
    )
    parser.add_argument(
        "--scale-probe-resolution",
        type=float,
        default=DEFAULT_SCALE_PROBE_RESOLUTION,
        help="Resolution for binary search when probing scales (default: 0.1)",
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
    queue_config = QueueDurabilityConfig(
        step_window=args.queue_threshold_steps,
        length_threshold=args.queue_threshold_length,
    )
    scale_probe_config = ScaleProbeConfig(
        enabled=args.enable_scale_probe,
        start=args.scale_probe_start,
        ceiling=args.scale_probe_ceiling,
        resolution=args.scale_probe_resolution,
    )
    scenarios = load_manifest(args.manifest)
    run_batch(
        scenarios,
        output_root=output_root,
        thresholds=thresholds,
        queue_config=queue_config,
        scale_probe=scale_probe_config,
        results_csv=results_path,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
