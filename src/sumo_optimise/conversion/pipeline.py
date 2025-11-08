"""High-level orchestration for building PlainXML artefacts."""
from __future__ import annotations

from pathlib import Path
from typing import Sequence

from .builder.ids import cluster_id
from .checks.semantics import validate_semantics
from .demand.catalog import build_endpoint_catalog
from .demand.person_flow import PersonRouteResult, prepare_person_flow_routes
from .demand.person_flow.graph_builder import build_pedestrian_graph
from .demand.person_flow.templates import write_demand_templates
from .demand.routes import render_routes_document
from .demand.vehicle_flow import VehicleRouteResult, prepare_vehicle_routes
from .demand.visualization import render_pedestrian_network_image
from .domain.models import BuildOptions, BuildResult, BuildTask
from .emitters.connections import render_connections_xml
from .emitters.edges import render_edges_xml
from .emitters.nodes import render_nodes_xml
from .emitters.tll import render_tll_xml
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
from .utils.io import (
    ensure_output_directory,
    persist_routes,
    persist_xml,
    write_manifest,
    write_sumocfg,
)
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
    ped_graph = None
    if options.generate_demand_templates:
        ped_graph = build_pedestrian_graph(
            main_road=main_road,
            defaults=defaults,
            clusters=clusters,
            breakpoints=breakpoints,
            catalog=endpoint_catalog,
        )
        endpoint_ids = sorted(
            {
                _canonical_endpoint_id(node_id, breakpoints)
            for node_id, data in ped_graph.nodes(data=True)
            if data.get("is_endpoint")
            }
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
    person_routes: PersonRouteResult | None = None
    vehicle_routes: VehicleRouteResult | None = None
    if options.demand:
        person_routes = prepare_person_flow_routes(
            options=options.demand,
            main_road=main_road,
            defaults=defaults,
            clusters=clusters,
            breakpoints=breakpoints,
            catalog=endpoint_catalog,
        )
        vehicle_routes = prepare_vehicle_routes(
            options=options.demand,
            main_road=main_road,
            defaults=defaults,
            clusters=clusters,
            breakpoints=breakpoints,
            catalog=endpoint_catalog,
            edges_xml=edges_xml,
            connections_xml=connections_result.xml,
        )
        demand_xml = render_routes_document(
            person_entries=person_routes.entries if person_routes else None,
            vehicle_entries=vehicle_routes.entries if vehicle_routes else None,
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
        pedestrian_graph=ped_graph,
    )


def _canonical_endpoint_id(node_id: str, breakpoints: Sequence[int]) -> str:
    tokens = node_id.split(".")
    if len(tokens) == 4 and tokens[0] == "Node" and tokens[1] == "Main":
        try:
            pos = int(tokens[2])
        except ValueError:
            return node_id
        if not breakpoints:
            return node_id
        if pos == breakpoints[0]:
            anchor = "W_end"
        elif pos == breakpoints[-1]:
            anchor = "E_end"
        else:
            return node_id
        half = tokens[3]
        if half not in {"N", "S"}:
            return node_id
        sidewalk = "N_sidewalk" if half == "N" else "S_sidewalk"
        return f"PedEnd.Main.{anchor}.{sidewalk}"
    return node_id


def build_and_persist(
    spec_path: Path,
    options: BuildOptions,
    task: BuildTask = BuildTask.ALL,
) -> BuildResult:
    result = build_corridor_artifacts(spec_path, options)
    artifacts = ensure_output_directory(options.output_template, options.output_files)
    artifacts.log_path.parent.mkdir(parents=True, exist_ok=True)
    configure_logger(artifacts.log_path, console=options.console_log)
    LOG.info("outdir: %s", artifacts.outdir.resolve())

    if task.includes_network():
        persist_xml(
            artifacts,
            nodes=result.nodes_xml,
            edges=result.edges_xml,
            connections=result.connections_xml,
            tll=result.tll_xml,
        )

    if task.includes_demand() and result.demand_xml is not None:
        persist_routes(artifacts, demand=result.demand_xml)
        write_sumocfg(
            artifacts,
            net_path=artifacts.network_path,
            routes_path=artifacts.routes_path,
        )
        result.sumocfg_path = artifacts.sumocfg_path

    if options.generate_demand_templates:
        write_demand_templates(
            artifacts.ped_endpoint_template_path,
            artifacts.ped_junction_template_path,
            result.endpoint_ids or [],
            result.junction_ids or [],
        )

        visualization = render_pedestrian_network_image(
            result.pedestrian_graph,
            result.endpoint_ids or [],
            result.junction_ids or [],
            artifacts.pedestrian_network_path,
        )
        if visualization is not None:
            result.network_image_path = visualization.image_path

    manifest = {
        "source": str(spec_path.resolve()),
        "schema": str(options.schema_path.resolve()),
    }
    manifest_path = write_manifest(artifacts, manifest)
    result.manifest_path = manifest_path

    if options.run_netconvert and task.includes_network():
        run_two_step_netconvert(
            artifacts.outdir,
            artifacts.nodes_path,
            artifacts.edges_path,
            artifacts.connections_path,
            artifacts.tll_path,
            plain_prefix=artifacts.netconvert_prefix,
            network_output=artifacts.network_path,
        )

    if options.run_netedit and task.includes_network():
        network_path = artifacts.network_path
        LOG.info("netedit requested. Looking for network file at %s", network_path.resolve())
        if network_path.exists():
            LOG.info("network file found. Launching netedit.")
            launch_netedit(network_path)
        else:
            LOG.warning("network file %s not found. Skip launching netedit.", network_path)

    return result
