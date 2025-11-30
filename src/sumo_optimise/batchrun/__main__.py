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
    DEFAULT_SCALE_PROBE_COARSE_STEP,
    DEFAULT_SCALE_PROBE_FINE_STEP,
    DEFAULT_SCALE_PROBE_START,
    QueueDurabilityConfig,
    ScaleProbeConfig,
    ScaleMode,
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
        "--enable-scale-probe",
        action="store_true",
        help="Search for the minimal scale that violates queue durability (runs additional SUMO simulations)",
    )
    parser.add_argument(
        "--scale-probe-start",
        type=float,
        default=DEFAULT_SCALE_PROBE_START,
        help="Starting scale for durability probing (default: 0.1)",
    )
    parser.add_argument(
        "--scale-probe-ceiling",
        type=float,
        default=DEFAULT_SCALE_PROBE_CEILING,
        help="Maximum scale tested before giving up (default: 5.0)",
    )
    parser.add_argument(
        "--scale-probe-fine-step",
        dest="scale_probe_fine_step",
        type=float,
        default=DEFAULT_SCALE_PROBE_FINE_STEP,
        help="Fine step (binary search increment) when probing scales",
    )
    parser.add_argument(
        "--scale-probe-coarse-step",
        type=float,
        default=DEFAULT_SCALE_PROBE_COARSE_STEP,
        help="Coarse step for initial scanning before fine search (default: 1.0)",
    )
    parser.add_argument(
        "--abort-on-waiting-ratio",
        action="store_true",
        help="Abort SUMO during probe when waiting ratio condition is hit, then switch to fine search",
    )
    parser.add_argument(
        "--compress-zstd",
        nargs="?",
        const=10,
        type=int,
        help="Compress generated XML files with zstd at given level (1-22, default: 10); single-threaded",
    )
    parser.add_argument(
        "--metrics-trace",
        action="store_true",
        help="Temporarily log metrics parsing progress (debug; may be removed later)",
    )
    parser.add_argument(
        "--scale-mode",
        choices=[mode.value for mode in ScaleMode],
        default=ScaleMode.SUMO.value,
        help=(
            "Scale application strategy: 'sumo' passes --scale to SUMO (scales people + vehicles); "
            "'veh_only' multiplies vehicle demand CSV rows and runs SUMO with scale=1"
        ),
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
    scale_probe_config = ScaleProbeConfig(
        enabled=args.enable_scale_probe,
        start=args.scale_probe_start,
        ceiling=args.scale_probe_ceiling,
        fine_step=args.scale_probe_fine_step,
        coarse_step=args.scale_probe_coarse_step,
        abort_on_waiting=args.abort_on_waiting_ratio,
    )
    scale_mode = ScaleMode(args.scale_mode)
    scenarios = load_manifest(args.manifest, default_scale_mode=scale_mode)
    run_batch(
        scenarios,
        output_root=output_root,
        queue_config=queue_config,
        scale_probe=scale_probe_config,
        results_csv=results_path,
        max_workers=args.workers,
        metrics_trace=args.metrics_trace,
        compress_zstd_level=args.compress_zstd,
    )


if __name__ == "__main__":
    main()
