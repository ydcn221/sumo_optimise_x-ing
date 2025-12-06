from sumo_optimise.conversion.domain.models import EndpointDemandRow, PersonFlowPattern
from sumo_optimise.conversion.demand.vehicle_flow.route_output import (
    build_vehicle_flow_entries,
)


def test_vehicle_flow_entries_use_time_windows() -> None:
    row = EndpointDemandRow(endpoint_id="Node.Main.0.N", flow_per_hour=100.0)
    flows = [("o", "d", 5.0, row)]

    entries = build_vehicle_flow_entries(
        flows,
        vehicle_pattern=PersonFlowPattern.STEADY,
        begin_time=10.0,
        end_time=20.0,
    )

    assert len(entries) == 1
    entry = entries[0]
    assert 'begin="10.00"' in entry
    assert 'end="20.00"' in entry
    assert 'vehsPerHour="5.000000"' in entry
