from __future__ import annotations

from pathlib import Path

from sumo_optimise.conversion.domain.models import (
    BuildOptions,
    BuildTask,
    DemandOptions,
    OutputDirectoryTemplate,
    OutputFileTemplates,
)
from sumo_optimise.conversion.pipeline import build_and_persist
from sumo_optimise.conversion.utils.constants import NETWORK_FILE_NAME


SPEC_PATH = Path("data/reference/SUMO_OPTX_demo(connection_build)/SUMO_OPTX_v1.4_sample.json")
SCHEMA_PATH = Path("src/sumo_optimise/conversion/data/schema.json")
PED_ENDPOINT_CSV = Path("data/sample_updated/ped_EP_demand_sampleUpd.csv")
PED_JUNCTION_CSV = Path("data/sample_updated/ped_jct_turn_weight_sampleUpd.csv")
VEH_ENDPOINT_CSV = Path("data/sample_updated/veh_EP_demand_sampleUpd.csv")
VEH_JUNCTION_CSV = Path("data/sample_updated/veh_jct_turn_weight_sampleUpd.csv")


def test_demand_run_reuses_existing_network(tmp_path):
    network_input = tmp_path / "existing.net.xml"
    content = "<net id=\"demo\"/>"
    network_input.write_text(content, encoding="utf-8")

    root = tmp_path / "runs"
    run_name = "demo"
    (root / run_name).mkdir(parents=True, exist_ok=True)

    options = BuildOptions(
        schema_path=SCHEMA_PATH,
        output_template=OutputDirectoryTemplate(root=str(root), run=run_name),
        output_files=OutputFileTemplates(),
        demand=DemandOptions(
            ped_endpoint_csv=PED_ENDPOINT_CSV,
            ped_junction_turn_weight_csv=PED_JUNCTION_CSV,
            veh_endpoint_csv=VEH_ENDPOINT_CSV,
            veh_junction_turn_weight_csv=VEH_JUNCTION_CSV,
            simulation_end_time=3600.0,
        ),
        network_input=network_input,
    )

    result = build_and_persist(SPEC_PATH, options, task=BuildTask.DEMAND)

    assert result.manifest_path is not None
    outdir = result.manifest_path.parent
    copied_net = outdir / NETWORK_FILE_NAME
    assert copied_net.exists()
    assert copied_net.read_text(encoding="utf-8") == content
    assert result.sumocfg_path is not None and result.sumocfg_path.exists()
