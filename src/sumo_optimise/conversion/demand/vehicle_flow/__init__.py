"""Vehicle demand routing orchestration."""
from __future__ import annotations

from dataclasses import dataclass
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

    unsat_end = options.warmup_seconds + options.unsat_seconds
    sat_end = options.simulation_end_time
    segments = [(0.0, unsat_end, options.veh_unsat_scale)]
    if options.sat_seconds > 0:
        segments.append((unsat_end, sat_end, options.veh_sat_scale))

    reachable_flows = [
        (rec.origin, rec.destination, rec.value, rec.row) for rec in reachability.reachable
    ]
    entries: List[str] = []
    for idx, (begin_time, end_time, scale) in enumerate(segments):
        scaled = [
            (origin, destination, value * scale, row)
            for origin, destination, value, row in reachable_flows
        ]
        entries.extend(
            build_vehicle_flow_entries(
                scaled,
                vehicle_pattern=pattern,
                begin_time=begin_time,
                end_time=end_time,
                segment_tag=f"seg{idx}",
            )
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
