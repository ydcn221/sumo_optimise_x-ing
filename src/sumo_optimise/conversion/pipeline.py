"""High-level orchestration for building PlainXML artefacts."""
from __future__ import annotations

from pathlib import Path

from .checks.semantics import validate_semantics
from .domain.models import BuildOptions, BuildResult
from .demand.catalog import build_endpoint_catalog
from .demand.person_flow import prepare_person_flow_routes
from .demand.person_flow.graph_builder import build_pedestrian_graph
from .emitters.connections import render_connections_xml
from .emitters.edges import render_edges_xml
from .emitters.tll import render_tll_xml
from .emitters.nodes import render_nodes_xml
from .demand.person_flow.templates import write_demand_templates
from .builder.ids import cluster_id
from .parser.spec_loader import (
    build_clusters,
    load_json_file,
    load_schema_file,
    parse_defaults,
    parse_junction_templates,
    parse_layout_events,
    parse_main_road,
    parse_signal_profiles,
    parse_snap_rule,
    validate_json_schema,
)
from .planner.lanes import collect_breakpoints_and_reasons, compute_lane_overrides
from .sumo_integration.netconvert import run_two_step_netconvert
from .sumo_integration.netedit import launch_netedit
from .utils.constants import NETWORK_FILE_NAME
from .utils.io import ensure_output_directory, persist_xml, write_manifest
from .utils.logging import configure_logger, get_logger

LOG = get_logger()


def build_corridor_artifacts(spec_path: Path, options: BuildOptions) -> BuildResult:
    spec_json = load_json_file(spec_path)
    schema_json = load_schema_file(options.schema_path)
    validate_json_schema(spec_json, schema_json)

    snap_rule = parse_snap_rule(spec_json)
    defaults = parse_defaults(spec_json)
    main_road = parse_main_road(spec_json)
    junction_template_by_id = parse_junction_templates(spec_json)
    signal_profiles_by_kind = parse_signal_profiles(spec_json)

    validate_semantics(
        spec_json=spec_json,
        snap_rule=snap_rule,
        main_road=main_road,
        junction_template_by_id=junction_template_by_id,
        signal_profiles_by_kind=signal_profiles_by_kind,
    )

    layout_events = parse_layout_events(spec_json, snap_rule, main_road)
    clusters = build_clusters(layout_events)
    lane_overrides = compute_lane_overrides(main_road, clusters, junction_template_by_id, snap_rule)
    breakpoints, reason_by_pos = collect_breakpoints_and_reasons(main_road, clusters, lane_overrides, snap_rule)

    endpoint_catalog = build_endpoint_catalog(
        defaults=defaults,
        main_road=main_road,
        clusters=clusters,
        breakpoints=breakpoints,
        junction_template_by_id=junction_template_by_id,
        lane_overrides=lane_overrides,
        snap_rule=snap_rule,
    )

    junction_ids = sorted(
        {
            cluster_id(cluster.pos_m)
            for cluster in clusters
            if any(event.type.value in ("tee", "cross") for event in cluster.events)
        }
    )

    endpoint_ids: list[str] | None = None
    if options.generate_demand_templates:
        graph = build_pedestrian_graph(
            main_road=main_road,
            defaults=defaults,
            clusters=clusters,
            breakpoints=breakpoints,
            catalog=endpoint_catalog,
        )
        endpoint_ids = sorted(
            node_id for node_id, data in graph.nodes(data=True) if data.get("is_endpoint")
        )

    nodes_xml = render_nodes_xml(main_road, defaults, clusters, breakpoints, reason_by_pos)
    edges_xml = render_edges_xml(main_road, defaults, clusters, breakpoints, junction_template_by_id, lane_overrides)
    connections_result = render_connections_xml(
        defaults,
        clusters,
        breakpoints,
        junction_template_by_id,
        snap_rule,
        main_road,
        lane_overrides,
    )
    tll_xml = render_tll_xml(
        defaults=defaults,
        clusters=clusters,
        breakpoints=breakpoints,
        junction_template_by_id=junction_template_by_id,
        snap_rule=snap_rule,
        main_road=main_road,
        lane_overrides=lane_overrides,
        signal_profiles_by_kind=signal_profiles_by_kind,
        connection_links=connections_result.links,
        controlled_connections=connections_result.controlled_connections,
    )

    demand_xml = None
    if options.demand:
        demand_xml = prepare_person_flow_routes(
            options=options.demand,
            main_road=main_road,
            defaults=defaults,
            clusters=clusters,
            breakpoints=breakpoints,
            catalog=endpoint_catalog,
        )

    return BuildResult(
        nodes_xml=nodes_xml,
        edges_xml=edges_xml,
        connections_xml=connections_result.xml,
        connection_links=connections_result.links,
        tll_xml=tll_xml,
        demand_xml=demand_xml,
        endpoint_ids=endpoint_ids,
        junction_ids=junction_ids,
    )


def build_and_persist(spec_path: Path, options: BuildOptions) -> BuildResult:
    result = build_corridor_artifacts(spec_path, options)
    artifacts = ensure_output_directory(options.output_template)
    configure_logger(artifacts.log_path, console=options.console_log)
    LOG.info("outdir: %s", artifacts.outdir.resolve())

    persist_xml(
        artifacts,
        nodes=result.nodes_xml,
        edges=result.edges_xml,
        connections=result.connections_xml,
        tll=result.tll_xml,
        demand=result.demand_xml,
    )

    if options.generate_demand_templates:
        write_demand_templates(
            artifacts.outdir,
            result.endpoint_ids or [],
            result.junction_ids or [],
        )

    manifest = {
        "source": str(spec_path.resolve()),
        "schema": str(options.schema_path.resolve()),
    }
    manifest_path = write_manifest(artifacts, manifest)
    result.manifest_path = manifest_path

    if options.run_netconvert:
        run_two_step_netconvert(
            artifacts.outdir,
            artifacts.nodes_path,
            artifacts.edges_path,
            artifacts.connections_path,
            artifacts.tll_path,
        )

    if options.run_netedit:
        network_path = artifacts.outdir / NETWORK_FILE_NAME
        LOG.info("netedit requested. Looking for network file at %s", network_path.resolve())
        if network_path.exists():
            LOG.info("network file found. Launching netedit.")
            launch_netedit(network_path)
        else:
            LOG.warning("network file %s not found. Skip launching netedit.", network_path)

    return result
