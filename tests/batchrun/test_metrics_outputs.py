from pathlib import Path
import gzip

import zstandard as zstd

from sumo_optimise.batchrun.models import (
    OutputFormat,
    PhaseTiming,
    QueueDurabilityMetrics,
    RunArtifacts,
    RunTimings,
    ScaleProbeResult,
    ScenarioResult,
    TripinfoMetrics,
)
from sumo_optimise.batchrun.orchestrator import (
    _format_timestamp,
    _materialize_metrics_inputs,
    _result_to_row,
)


def _build_artifacts(tmp_path: Path, trip_path: Path) -> RunArtifacts:
    return RunArtifacts(
        outdir=tmp_path,
        sumocfg=tmp_path / "config.sumocfg",
        network=tmp_path / "net.net.xml",
        tripinfo=trip_path,
        personinfo=tmp_path / "person_tripinfo.csv.gz",
        fcd=tmp_path / "fcd.csv.gz",
        summary=tmp_path / "vehicle_summary.csv.gz",
        person_summary=tmp_path / "person_summary.csv.gz",
        detector=tmp_path / "detector.xml",
        queue=tmp_path / "queue.xml",
        sumo_log=tmp_path / "sumo.log",
    )


def test_materialize_metrics_inputs_decompresses_gz_and_cleans_plain(tmp_path: Path) -> None:
    content = "id;arrival;timeLoss\nveh_0;5;1.0\n"
    trip_gz = tmp_path / "vehicle_tripinfo.csv.gz"
    with gzip.open(trip_gz, "wb") as fp:
        fp.write(content.encode("utf-8"))

    artifacts = _build_artifacts(tmp_path, trip_path=trip_gz)
    output_format = OutputFormat.from_string("csv.gz")

    with _materialize_metrics_inputs(
        artifacts,
        output_format=output_format,
        need_tripinfo=True,
        need_personinfo=False,
        need_summary=False,
    ) as (trip_path, person_path, summary_path):
        assert person_path is None
        assert summary_path is None
        assert trip_path is not None
        assert trip_path.exists()
        assert trip_path.suffix == ".csv"
        assert "timeLoss" in trip_path.read_text(encoding="utf-8")

    assert not (tmp_path / "vehicle_tripinfo.csv").exists()
    assert trip_gz.exists()


def test_materialize_metrics_inputs_compresses_zst_and_removes_plain(tmp_path: Path) -> None:
    content = "id;arrival;timeLoss\nveh_1;7;0.5\n"
    trip_plain = tmp_path / "vehicle_tripinfo.csv"
    trip_plain.write_text(content, encoding="utf-8")

    artifacts = _build_artifacts(tmp_path, trip_path=trip_plain)
    output_format = OutputFormat.from_string("csv.zst")

    with _materialize_metrics_inputs(
        artifacts,
        output_format=output_format,
        need_tripinfo=True,
        need_personinfo=False,
        need_summary=False,
    ) as (trip_path, _, _):
        assert trip_path == trip_plain
        assert trip_path.exists()

    compressed = tmp_path / "vehicle_tripinfo.csv.zst"
    assert compressed.exists()
    assert not trip_plain.exists()

    with compressed.open("rb") as fp, zstd.ZstdDecompressor().stream_reader(fp) as reader:
        data = reader.read()
    assert b"timeLoss" in data


def test_result_to_row_uses_metrics_timing() -> None:
    timings = RunTimings(
        build=PhaseTiming(start=1.0, end=2.0),
        sumo=PhaseTiming(start=3.0, end=4.0),
        metrics=PhaseTiming(start=5.0, end=6.0),
        probe=PhaseTiming(start=7.0, end=8.0),
    )
    result = ScenarioResult(
        scenario_id="s-1",
        scenario_base_id="s-1",
        seed=42,
        warmup_seconds=0.0,
        unsat_seconds=10.0,
        sat_seconds=0.0,
        ped_unsat_scale=1.0,
        ped_sat_scale=1.0,
        veh_unsat_scale=1.0,
        veh_sat_scale=1.0,
        demand_dir=Path("demand"),
        tripinfo=TripinfoMetrics(),
        queue=QueueDurabilityMetrics(),
        scale_probe=ScaleProbeResult(),
        waiting_p95_sat=None,
        fcd_note="",
        error=None,
        worker_id=0,
        timings=timings,
    )

    row = _result_to_row(result)

    assert row["metrics_start"] == _format_timestamp(timings.metrics.start)
    assert row["metrics_end"] == _format_timestamp(timings.metrics.end)
    assert row["build_start"] == _format_timestamp(timings.build.start)
    assert row["probe_end"] == _format_timestamp(timings.probe.end)
