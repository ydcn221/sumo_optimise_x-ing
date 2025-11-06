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
from .demand_input import load_endpoint_demands, load_junction_ratios
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

    if not options.endpoint_csv or not options.junction_csv:
        return None

    LOG.info("loading endpoint demand CSV: %s", options.endpoint_csv)
    endpoint_rows = load_endpoint_demands(options.endpoint_csv)
    LOG.info("loading junction ratio CSV: %s", options.junction_csv)
    ratio_map = load_junction_ratios(options.junction_csv)

    LOG.info("building pedestrian graph (nodes=%d, edges=%d)", len(breakpoints), len(breakpoints) - 1)
    graph = build_pedestrian_graph(
        main_road=main_road,
        defaults=defaults,
        clusters=clusters,
        breakpoints=breakpoints,
        catalog=catalog,
    )

    LOG.info("propagating %d demand rows across pedestrian network", len(endpoint_rows))
    od_flows = compute_od_flows(graph, ratio_map, endpoint_rows)
    LOG.info("derived %d OD flows", len(od_flows))

    return render_person_flows(
        od_flows,
        options=options,
        breakpoints=breakpoints,
        defaults=defaults,
    )


__all__ = ["prepare_person_flow_routes"]
