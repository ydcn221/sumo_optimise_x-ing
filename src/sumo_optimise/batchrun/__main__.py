from __future__ import annotations

import argparse
from pathlib import Path

from .models import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_QUEUE_THRESHOLD_LENGTH,
    DEFAULT_QUEUE_THRESHOLD_STEPS,
    OutputFormat,
    QueueDurabilityConfig,
    ScaleProbeConfig,
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
        "--waiting-ratio-steps",
        "--queue-threshold-steps",
        dest="waiting_ratio_steps",
        type=int,
        default=DEFAULT_QUEUE_THRESHOLD_STEPS,
        help="Consecutive timesteps required above waiting ratio threshold to mark as non-durable (default: 10)",
    )
    parser.add_argument(
        "--waiting-ratio-threshold",
        "--queue-threshold-length",
        dest="waiting_ratio_threshold",
        type=float,
        default=DEFAULT_QUEUE_THRESHOLD_LENGTH,
        help="Waiting ratio threshold (waiting/running) for durability check (default: 0.25)",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv.gz", "xml.gz", "csv.zst", "xml.zst"],
        default="csv.gz",
        help="Output format for SUMO artefacts (default: csv.gz)",
    )
    parser.add_argument(
        "--zstd-level",
        type=int,
        default=10,
        help="Zstandard level used when output-format ends with '.zst' (1-22, default: 10)",
    )
    parser.add_argument(
        "--metrics-trace",
        action="store_true",
        help="Temporarily log metrics parsing progress (debug; may be removed later)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root: Path = args.output_root
    results_path: Path = args.results or output_root / "results.csv"
    queue_config = QueueDurabilityConfig(
        step_window=args.waiting_ratio_steps,
        length_threshold=args.waiting_ratio_threshold,
    )
    scenarios = load_manifest(args.manifest)
    output_format = OutputFormat.from_string(args.output_format, zstd_level=args.zstd_level)
    run_batch(
        scenarios,
        output_root=output_root,
        queue_config=queue_config,
        scale_probe=ScaleProbeConfig(enabled=False),
        results_csv=results_path,
        max_workers=args.workers,
        metrics_trace=args.metrics_trace,
        output_format=output_format,
    )


if __name__ == "__main__":
    main()
