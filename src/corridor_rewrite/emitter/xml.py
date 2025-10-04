"""Emit SUMO PlainXML files from IR objects."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Sequence

from ..domain import models


def emit_nodes_xml(nodes: Sequence[models.NodeIR]) -> str:
    root = ET.Element("nodes")
    for node in nodes:
        attrs = {
            "id": node.id,
            "x": f"{node.x:.2f}",
            "y": f"{node.y:.2f}",
            "type": node.type,
        }
        ET.SubElement(root, "node", attrib=attrs)
    return _to_string(root)


def emit_edges_xml(edges: Sequence[models.EdgeIR]) -> str:
    root = ET.Element("edges")
    for edge in edges:
        attrs = {
            "id": edge.id,
            "from": edge.from_node,
            "to": edge.to_node,
            "priority": str(edge.priority),
            "speed": f"{edge.speed_mps:.2f}",
            "numLanes": str(edge.num_lanes),
            "length": f"{edge.length_m:.2f}",
        }
        ET.SubElement(root, "edge", attrib=attrs)
    return _to_string(root)


def emit_connections_xml(connections: Sequence[models.ConnectionIR]) -> str:
    root = ET.Element("connections")
    for conn in connections:
        attrs = {
            "from": conn.from_edge,
            "to": conn.to_edge,
            "fromLane": str(conn.from_lane),
            "toLane": str(conn.to_lane),
        }
        ET.SubElement(root, "connection", attrib=attrs)
    return _to_string(root)


def _to_string(element: ET.Element) -> str:
    return ET.tostring(element, encoding="unicode")
