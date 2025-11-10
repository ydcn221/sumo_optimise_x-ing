"""Helpers for evaluating OD reachability against the generated vehicle network."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Sequence, Set, Tuple
from xml.etree import ElementTree as ET

from ...domain.models import EndpointDemandRow
from ...utils.errors import DemandValidationError

VehicleOdTuple = Tuple[str, str, float, EndpointDemandRow]


@dataclass(frozen=True)
class VehicleOdFlowRecord:
    """Materialised OD tuple that keeps a pointer to the source demand row."""

    origin: str
    destination: str
    value: float
    row: EndpointDemandRow


@dataclass(frozen=True)
class VehicleOdReachabilityReport:
    """Reachability buckets for the computed OD flows."""

    reachable: List[VehicleOdFlowRecord]
    unreachable: List[VehicleOdFlowRecord]


class VehicleConnectionGraph:
    """Edge-level graph that honours the emitted <connection> elements."""

    def __init__(
        self,
        edge_nodes: Dict[str, Tuple[str, str]],
        transitions: Dict[str, Set[str]],
        edges_from_node: Dict[str, Set[str]],
    ) -> None:
        self._edge_nodes = edge_nodes
        self._transitions = transitions
        self._edges_from_node = edges_from_node

    @classmethod
    def from_xml(cls, edges_xml: str, connections_xml: str) -> VehicleConnectionGraph:
        edge_nodes = _parse_edge_nodes(edges_xml)
        edges_from_node = _edges_from_node(edge_nodes)
        transitions = _parse_edge_transitions(connections_xml)
        _apply_default_transitions(transitions, edge_nodes, edges_from_node)
        return cls(edge_nodes, transitions, edges_from_node)

    def has_path(self, origin: str, destination: str) -> bool:
        if origin == destination:
            return True
        starts = self._edges_from_node.get(origin)
        if not starts:
            return False
        visited: Set[str] = set()
        queue: Deque[str] = deque(starts)
        while queue:
            edge_id = queue.popleft()
            if edge_id in visited:
                continue
            visited.add(edge_id)
            nodes = self._edge_nodes.get(edge_id)
            if not nodes:
                continue
            _, to_node = nodes
            if to_node == destination:
                return True
            for next_edge in self._transitions.get(edge_id, ()):
                if next_edge not in visited:
                    queue.append(next_edge)
        return False


def evaluate_vehicle_od_reachability(
    flows: Sequence[VehicleOdTuple],
    *,
    edges_xml: str,
    connections_xml: str,
) -> VehicleOdReachabilityReport:
    """Split OD flows into reachable/unreachable buckets for downstream filtering."""

    if not flows:
        return VehicleOdReachabilityReport(reachable=[], unreachable=[])
    graph = VehicleConnectionGraph.from_xml(edges_xml, connections_xml)
    reachable: List[VehicleOdFlowRecord] = []
    unreachable: List[VehicleOdFlowRecord] = []
    for origin, destination, value, row in flows:
        record = VehicleOdFlowRecord(
            origin=origin,
            destination=destination,
            value=value,
            row=row,
        )
        if graph.has_path(origin, destination):
            reachable.append(record)
        else:
            unreachable.append(record)
    return VehicleOdReachabilityReport(reachable=reachable, unreachable=unreachable)


def _parse_edge_nodes(xml_text: str) -> Dict[str, Tuple[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:  # pragma: no cover - malformed XML should fail validation earlier
        raise DemandValidationError("failed to parse edges XML for reachability checks") from exc
    if root.tag != "edges":
        raise DemandValidationError("expected <edges> root when building reachability graph")
    mapping: Dict[str, Tuple[str, str]] = {}
    for edge_elem in root.findall("edge"):
        edge_id = edge_elem.get("id")
        from_node = edge_elem.get("from")
        to_node = edge_elem.get("to")
        if not edge_id or not from_node or not to_node:
            continue
        mapping[edge_id] = (from_node, to_node)
    return mapping


def _parse_edge_transitions(xml_text: str) -> Dict[str, Set[str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:  # pragma: no cover - malformed XML should fail validation earlier
        raise DemandValidationError("failed to parse connections XML for reachability checks") from exc
    if root.tag != "connections":
        raise DemandValidationError("expected <connections> root when building reachability graph")
    transitions: Dict[str, Set[str]] = defaultdict(set)
    for conn_elem in root.findall("connection"):
        from_edge = conn_elem.get("from")
        to_edge = conn_elem.get("to")
        if not from_edge or not to_edge:
            continue
        transitions[from_edge].add(to_edge)
    return transitions


def _edges_from_node(edge_nodes: Dict[str, Tuple[str, str]]) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = defaultdict(set)
    for edge_id, (from_node, _) in edge_nodes.items():
        mapping[from_node].add(edge_id)
    return mapping


def _apply_default_transitions(
    transitions: Dict[str, Set[str]],
    edge_nodes: Dict[str, Tuple[str, str]],
    edges_from_node: Dict[str, Set[str]],
) -> None:
    """Fallback to node adjacency when no explicit connection exists for an edge."""

    for edge_id, (_, to_node) in edge_nodes.items():
        if transitions.get(edge_id):
            continue
        default_targets = edges_from_node.get(to_node)
        if not default_targets:
            continue
        transitions[edge_id] = set(default_targets)


__all__ = [
    "VehicleOdFlowRecord",
    "VehicleOdReachabilityReport",
    "evaluate_vehicle_od_reachability",
]
