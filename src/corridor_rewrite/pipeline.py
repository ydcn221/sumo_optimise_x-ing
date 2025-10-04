"""Pure pipeline entry point orchestrating the corridor conversion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .builder.core import CorridorBuilder
from .domain.models import CorridorSpec
from .emitter.xml import emit_connections_xml, emit_edges_xml, emit_nodes_xml
from .options import BuildOptions
from .parser.loader import load_corridor_spec
from .planner.layout import CorridorPlan
from .planner.layout import build_corridor_plan


@dataclass(slots=True)
class CorridorArtifacts:
    """Generated PlainXML artefacts as strings."""

    nodes_xml: str
    edges_xml: str
    connections_xml: str

    def as_mapping(self) -> Mapping[str, str]:
        return {
            "nodes.nod.xml": self.nodes_xml,
            "edges.edg.xml": self.edges_xml,
            "connections.con.xml": self.connections_xml,
        }


def build_corridor_artifacts(spec_path: Path | str, options: BuildOptions | None = None) -> CorridorArtifacts:
    """Run the full conversion pipeline purely in memory."""

    opts = options or BuildOptions()
    spec = load_corridor_spec(Path(spec_path), schema_path=opts.schema_path)
    plan: CorridorPlan = build_corridor_plan(spec)
    builder = CorridorBuilder(plan)
    ir = builder.build()
    nodes_xml = emit_nodes_xml(ir.nodes)
    edges_xml = emit_edges_xml(ir.edges)
    connections_xml = emit_connections_xml(ir.connections)
    return CorridorArtifacts(nodes_xml=nodes_xml, edges_xml=edges_xml, connections_xml=connections_xml)
