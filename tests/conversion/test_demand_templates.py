from __future__ import annotations

import csv
from pathlib import Path

from sumo_optimise.conversion.demand.person_flow.templates import write_demand_templates
from sumo_optimise.conversion.demand.person_flow.identifier import minor_endpoint_id
from sumo_optimise.conversion.domain.models import BuildOptions, PedestrianSide
from sumo_optimise.conversion.pipeline import build_corridor_artifacts


def test_write_demand_templates(tmp_path: Path) -> None:
    ped_endpoint_ids = ["PedEnd.Main.W_end.N_sidewalk", "PedEnd.Minor.100.N_end.E_sidewalk"]
    veh_endpoint_ids = [
        "VehEnd_Main_W_end",
        "VehEnd_Minor_100_N_end",
    ]
    junction_ids = ["Cluster.100"]

    ped_endpoint_path = tmp_path / "template_ped_dem.csv"
    ped_junction_path = tmp_path / "template_ped_turn.csv"
    veh_endpoint_path = tmp_path / "template_veh_dem.csv"
    veh_junction_path = tmp_path / "template_veh_turn.csv"
    write_demand_templates(
        ped_endpoint_path,
        ped_junction_path,
        veh_endpoint_path,
        veh_junction_path,
        ped_endpoint_ids,
        veh_endpoint_ids,
        junction_ids,
    )

    with ped_endpoint_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0] == ["Pattern", "steady"]
    assert reader[1] == ["SidewalkEndID", "PedFlow", "Label"]
    assert reader[2] == ["PedEnd.Main.W_end.N_sidewalk", "", ""]

    with ped_junction_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0][0] == "JunctionID"
    assert reader[1][0] == "Cluster.100"
    assert all(value == "" for value in reader[1][1:])

    with veh_endpoint_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0] == ["Pattern", "steady"]
    assert reader[1] == ["EndID", "vehFlow", "Label"]
    assert reader[2] == ["VehEnd_Main_W_end", "", ""]

    with veh_junction_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = list(csv.reader(stream))
    assert reader[0] == ["JunctionID", "Main_L", "Main_T", "Main_R", "Minor_L", "Minor_T", "Minor_R"]
    assert reader[1][0] == "Cluster.100"
    assert all(value == "" for value in reader[1][1:])


def test_build_corridor_artifacts_collects_endpoint_ids(tmp_path: Path) -> None:
    spec_path = Path("data/reference/SUMO_OPTX_demo(connection_build)") / "SUMO_OPTX_v1.4_sample.json"
    schema_path = Path("src/sumo_optimise/conversion/data/schema.json")

    options = BuildOptions(schema_path=schema_path, generate_demand_templates=True)
    result = build_corridor_artifacts(spec_path, options)

    assert result.endpoint_ids
    assert result.vehicle_endpoint_ids
    assert result.junction_ids
    assert result.pedestrian_graph is not None
    assert "PedEnd.Main.W_end.N_sidewalk" in result.endpoint_ids
    assert "PedEnd.Main.E_end.S_sidewalk" in result.endpoint_ids
    assert minor_endpoint_id(350, "N", PedestrianSide.EAST_SIDE) in result.endpoint_ids
    assert minor_endpoint_id(350, "N", PedestrianSide.WEST_SIDE) in result.endpoint_ids
    assert "VehEnd_Main_W_end" in result.vehicle_endpoint_ids
    assert "VehEnd_Main_E_end" in result.vehicle_endpoint_ids
    assert any(endpoint.startswith("VehEnd_Minor_") for endpoint in result.vehicle_endpoint_ids)
    assert set(result.vehicle_endpoint_ids) != set(result.endpoint_ids)
