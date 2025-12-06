from pathlib import Path

import pytest

from sumo_optimise.batchrun.parsers import parse_waiting_percentile


def test_waiting_percentile_trims_top_and_computes_95p(tmp_path: Path) -> None:
    summary_path = tmp_path / "vehicle_summary.xml"
    summary_path.write_text(
        """<summary>
  <step time="0" waiting="0"/>
  <step time="1" waiting="1"/>
  <step time="2" waiting="2"/>
  <step time="3" waiting="3"/>
  <step time="4" waiting="100"/>
</summary>
""",
        encoding="utf-8",
    )

    value = parse_waiting_percentile(summary_path, begin=1.0, end=4.0)

    # top 5% trimmed removes the largest sample (100), leaving [1,2,3]; 95p ~= 2.9
    assert value is not None
    assert value == pytest.approx(2.9, rel=1e-3)
