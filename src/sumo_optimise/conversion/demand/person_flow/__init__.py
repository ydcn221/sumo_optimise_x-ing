"""High-level helpers for personFlow generation."""
from __future__ import annotations

from typing import Optional, Sequence

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
from .route_output import render_person_flows

LOG = get_logger()


def prepare_person_flow_routes(
    *,
    options: DemandOptions,
    main_road: MainRoadConfig,
    defaults: Defaults,
    clusters: Sequence[Cluster],
    breakpoints: Sequence[int],
    catalog: EndpointCatalog,
) -> Optional[str]:
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

    return render_person_flows(
        od_flows,
        ped_pattern=pattern,
        simulation_end_time=options.simulation_end_time,
        endpoint_offset_m=defaults.ped_endpoint_offset_m,
        breakpoints=breakpoints,
        defaults=defaults,
    )


__all__ = ["prepare_person_flow_routes"]
