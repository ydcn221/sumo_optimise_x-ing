"""High-level helpers for personFlow generation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from ...domain.models import (
    Cluster,
    DemandOptions,
    Defaults,
    EndpointCatalog,
    MainRoadConfig,
)
from ...utils.logging import get_logger
from .demand_input import load_endpoint_demands, load_junction_turn_weights
from .flow_propagation import compute_od_flows
from .graph_builder import build_pedestrian_graph
from .route_output import build_person_flow_entries

LOG = get_logger()


@dataclass(frozen=True)
class PersonRouteResult:
    entries: List[str]


def prepare_person_flow_routes(
    *,
    options: DemandOptions,
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    catalog: EndpointCatalog,
) -> Optional[PersonRouteResult]:
    """Generate the personFlow routes document if demand inputs are provided."""

    ped_endpoint = options.ped_endpoint_csv
    ped_turn_weight_path = options.ped_junction_turn_weight_csv
    if not ped_endpoint or not ped_turn_weight_path:
        return None

    LOG.info("loading pedestrian endpoint demand CSV: %s", ped_endpoint)
    pattern, endpoint_rows = load_endpoint_demands(ped_endpoint)
    LOG.info("loading pedestrian junction turn-weight CSV: %s", ped_turn_weight_path)
    turn_weight_map = load_junction_turn_weights(ped_turn_weight_path)

    LOG.info("building pedestrian graph (nodes=%d, edges=%d)", len(breakpoints), len(breakpoints) - 1)
    graph = build_pedestrian_graph(
        main_road=main_road,
        defaults=defaults,
        clusters=clusters,
        breakpoints=breakpoints,
        catalog=catalog,
    )

    LOG.info("propagating %d demand rows across pedestrian network", len(endpoint_rows))
    od_flows = compute_od_flows(graph, turn_weight_map, endpoint_rows)
    LOG.info("derived %d OD flows", len(od_flows))

    unsat_end = options.warmup_seconds + options.unsat_seconds
    sat_end = options.simulation_end_time
    segments = [(0.0, unsat_end, options.ped_unsat_scale)]
    if options.sat_seconds > 0:
        segments.append((unsat_end, sat_end, options.ped_sat_scale))

    entries: List[str] = []
    for idx, (begin_time, end_time, scale) in enumerate(segments):
        scaled_flows = [
            (origin, destination, value * scale, row) for origin, destination, value, row in od_flows
        ]
        entries.extend(
            build_person_flow_entries(
                scaled_flows,
                ped_pattern=pattern,
                begin_time=begin_time,
                end_time=end_time,
                segment_tag=f"seg{idx}",
                endpoint_offset_m=defaults.ped_endpoint_offset_m,
                breakpoints=breakpoints,
                defaults=defaults,
            )
        )
    return PersonRouteResult(entries=entries)


__all__ = ["PersonRouteResult", "prepare_person_flow_routes"]
