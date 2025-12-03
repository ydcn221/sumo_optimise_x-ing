"""Vehicle demand routing orchestration."""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Sequence

from ...domain.models import (
    Cluster,
    DemandOptions,
    Defaults,
    EndpointCatalog,
    EndpointDemandRow,
    MainRoadConfig,
)
from ...utils.logging import get_logger
from .demand_input import load_vehicle_endpoint_demands, load_vehicle_turn_weights
from .flow_propagation import compute_vehicle_od_flows
from .reachability import VehicleOdFlowRecord, evaluate_vehicle_od_reachability
from .route_output import build_vehicle_flow_entries
from .topology import build_vehicle_network

LOG = get_logger()


@dataclass(frozen=True)
class VehicleRouteResult:
    entries: List[str]
    unreachable_od_pairs: List[VehicleOdFlowRecord]


def prepare_vehicle_routes(
    *,
    options: DemandOptions,
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    catalog: EndpointCatalog,
    edges_xml: str,
    connections_xml: str,
) -> VehicleRouteResult | None:
    veh_endpoint = options.veh_endpoint_csv
    veh_turn_weights = options.veh_junction_turn_weight_csv
    if not veh_endpoint or not veh_turn_weights:
        return None

    pattern, endpoint_rows = load_vehicle_endpoint_demands(veh_endpoint)
    vehicle_scale = options.vehicle_flow_scale if options.vehicle_flow_scale is not None else 1.0
    if vehicle_scale != 1.0:
        endpoint_rows = [
            replace(row, flow_per_hour=row.flow_per_hour * vehicle_scale)
            for row in endpoint_rows
        ]
    turn_weight_map = load_vehicle_turn_weights(veh_turn_weights)
    network = build_vehicle_network(breakpoints, clusters)
    od_flows = compute_vehicle_od_flows(endpoint_rows, network=network, turn_weights=turn_weight_map)
    reachability = evaluate_vehicle_od_reachability(
        od_flows,
        edges_xml=edges_xml,
        connections_xml=connections_xml,
    )
    if reachability.unreachable:
        LOG.warning(
            "[DEMAND] %d vehicle OD pairs have no viable route and will be removed from rou.xml",
            len(reachability.unreachable),
        )
        for record in reachability.unreachable:
            LOG.warning(
                "[DEMAND] unreachable vehicle OD: origin=%s destination=%s demand=%.2f (%s)",
                record.origin,
                record.destination,
                record.value,
                _describe_row(record.row),
            )

    entries = build_vehicle_flow_entries(
        [(rec.origin, rec.destination, rec.value, rec.row) for rec in reachability.reachable],
        vehicle_pattern=pattern,
        simulation_end_time=options.simulation_end_time,
    )
    return VehicleRouteResult(entries=entries, unreachable_od_pairs=reachability.unreachable)


def _describe_row(row: EndpointDemandRow) -> str:
    tokens: List[str] = []
    if row.row_index is not None:
        tokens.append(f"row={row.row_index}")
    if row.label:
        tokens.append(f"label={row.label}")
    tokens.append(f"endpoint={row.endpoint_id}")
    return ", ".join(tokens)


__all__ = ["prepare_vehicle_routes", "VehicleRouteResult"]
