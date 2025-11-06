from __future__ import annotations

import csv
from pathlib import Path

from sumo_optimise.conversion.demand.person_flow.templates import write_demand_templates
from sumo_optimise.conversion.demand.person_flow.identifier import minor_endpoint_id
from sumo_optimise.conversion.domain.models import BuildOptions, PedestrianSide
from sumo_optimise.conversion.pipeline import build_corridor_artifacts


def test_write_demand_templates(tmp_path: Path) -> None:
    endpoint_ids = ["Node.0.MainN", "Node.100.MainS"]
    junction_ids = ["Cluster.100.Main"]

    write_demand_templates(tmp_path, endpoint_ids, junction_ids)

    endpoint_path = tmp_path / "DemandPerEndpoint_template.csv"
    junction_path = tmp_path / "JunctionDirectionRatio_template.csv"

    with endpoint_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0] == ["EndpointID", "PedFlow", "Label"]
    assert reader[1] == ["Node.0.MainN", "", ""]

    with junction_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0][0] == "JunctionID"
    assert reader[1][0] == "Cluster.100.Main"
    assert all(value == "" for value in reader[1][1:])


def test_build_corridor_artifacts_collects_template_ids(tmp_path: Path) -> None:
    spec_path = Path("data/reference/SUMO_OPTX_v1.3_sample.json")
    schema_path = Path("src/sumo_optimise/conversion/data/schema.json")

    options = BuildOptions(schema_path=schema_path, generate_demand_templates=True)
    result = build_corridor_artifacts(spec_path, options)

    assert result.endpoint_ids
    assert result.junction_ids
    assert minor_endpoint_id(350, "N", PedestrianSide.EAST_SIDE) in result.endpoint_ids
    assert minor_endpoint_id(350, "N", PedestrianSide.WEST_SIDE) in result.endpoint_ids
