from pathlib import Path

import pytest

from sumo_optimise.batchrun.models import QueueDurabilityConfig
from sumo_optimise.batchrun.parsers import parse_tripinfo, parse_waiting_ratio


@pytest.mark.parametrize(
    ("tripinfo_path", "begin_filter"),
    [
        (Path("workspace/temp_runs_INSPECTION/I-1s-1/001/tripinfo.xml"), 1200.0),
    ],
)
def test_tripinfo_parsing_handles_combined_tripinfo(tripinfo_path: Path, begin_filter: float) -> None:
    if not tripinfo_path.exists():
        pytest.skip(f"sample tripinfo not present: {tripinfo_path}")

    metrics = parse_tripinfo(tripinfo_path, begin_filter=begin_filter, personinfo=None)

    assert metrics.person_count > 0, "expected at least one pedestrian after begin_filter"
    assert metrics.person_mean_time_loss is not None and metrics.person_mean_time_loss > 0
    assert metrics.person_mean_route_length is not None and metrics.person_mean_route_length > 0


def test_tripinfo_parsing_prefers_separate_personinfo(tmp_path: Path) -> None:
    tripinfo_path = tmp_path / "tripinfo.xml"
    personinfo_path = tmp_path / "personinfo.xml"

    tripinfo_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<tripinfos>
  <tripinfo id="veh_0" arrival="100.0" timeLoss="10.0" routeLength="150.0"/>
</tripinfos>
""",
        encoding="utf-8",
    )
    personinfo_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<personinfos>
  <personinfo id="per_0" depart="50.0" duration="200.0">
    <walk depart="50.0" arrival="250.0" duration="200.0" routeLength="400.0" timeLoss="25.0"/>
  </personinfo>
</personinfos>
""",
        encoding="utf-8",
    )

    metrics = parse_tripinfo(tripinfo_path, begin_filter=0.0, personinfo=personinfo_path)

    assert metrics.vehicle_count == 1
    assert metrics.person_count == 1
    assert metrics.person_mean_time_loss == pytest.approx(25.0)
    assert metrics.person_mean_route_length == pytest.approx(400.0)


def test_tripinfo_parsing_supports_csv_reference() -> None:
    base = Path("data/reference/csv outputs")
    tripinfo_path = base / "vehicle_tripinfo.csv"
    personinfo_path = base / "person_tripinfo.csv"
    if not tripinfo_path.exists():
        pytest.skip(f"sample tripinfo not present: {tripinfo_path}")

    metrics = parse_tripinfo(tripinfo_path, begin_filter=0.0, personinfo=personinfo_path)

    assert metrics.vehicle_count > 0
    assert metrics.person_count > 0
    assert metrics.vehicle_mean_time_loss is not None
    assert metrics.person_mean_time_loss is not None
    assert metrics.person_mean_route_length is not None


def test_waiting_ratio_parsing_supports_csv_reference() -> None:
    base = Path("data/reference/csv outputs")
    summary_path = base / "vehicle_summary.csv"
    if not summary_path.exists():
        pytest.skip(f"sample summary not present: {summary_path}")

    metrics = parse_waiting_ratio(summary_path, config=QueueDurabilityConfig())

    assert metrics.max_queue_length >= 0.0
