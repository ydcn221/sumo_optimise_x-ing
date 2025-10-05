"""Wrapper API for generating SUMO PlainXML artefacts via the legacy builder."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .bridge import run_legacy_plainxml
from .options import BuildOptions


__all__ = ["CorridorArtifacts", "BuildOptions", "build_corridor_artifacts"]


@dataclass(slots=True)
class CorridorArtifacts:
    """PlainXML artefacts generated in-memory."""

    nodes_xml: str
    edges_xml: str
    connections_xml: str

    def as_mapping(self) -> dict[str, str]:
        return {
            "nodes.nod.xml": self.nodes_xml,
            "edges.edg.xml": self.edges_xml,
            "connections.con.xml": self.connections_xml,
        }


def build_corridor_artifacts(spec_path: Path | str, options: BuildOptions | None = None) -> CorridorArtifacts:
    """Run the legacy v1.2.11 builder and return its PlainXML output."""

    opts = options or BuildOptions()
    spec_path = Path(spec_path).resolve()
    schema_path = (
        Path(opts.schema_path).resolve()
        if opts.schema_path is not None
        else opts.default_schema_path.resolve()
    )

    result = run_legacy_plainxml(
        spec_path=spec_path,
        schema_path=schema_path,
        output_dir=opts.output_dir,
        keep_output=opts.keep_output,
    )
    return CorridorArtifacts(
        nodes_xml=result.nodes_xml,
        edges_xml=result.edges_xml,
        connections_xml=result.connections_xml,
    )
