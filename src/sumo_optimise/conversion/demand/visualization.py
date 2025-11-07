"""Utilities for rendering a pedestrian network snapshot as SVG."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

import networkx as nx

LABEL_ROTATION_DEG = -45.0
LABEL_DELTA = 10.0
LABEL_MARGIN = 6.0
MAJOR_VERTICAL_BIAS = 4.0
CANVAS_PADDING = 25.0


@dataclass
class NetworkVisualizationResult:
    image_path: Path
    endpoint_labels: Dict[str, Tuple[float, float]]
    junction_labels: Dict[str, Tuple[float, float]]
    label_rotation_deg: float = LABEL_ROTATION_DEG


@dataclass
class _Bounds:
    min_x: float
    max_x: float
    min_y: float
    max_y: float

    @property
    def width(self) -> float:
        return max(1.0, self.max_x - self.min_x)

    @property
    def height(self) -> float:
        return max(1.0, self.max_y - self.min_y)


def render_pedestrian_network_image(
    graph: Optional[nx.MultiGraph],
    endpoint_ids: Iterable[str],
    junction_ids: Iterable[str],
    output_path: Path,
) -> Optional[NetworkVisualizationResult]:
    """Render the pedestrian graph as an SVG diagram with rotated labels."""

    if graph is None or graph.number_of_nodes() == 0:
        return None

    positions = _collect_positions(graph)
    if not positions:
        return None

    endpoint_ids = list(endpoint_ids)
    junction_ids = list(junction_ids)
    endpoint_set = set(endpoint_ids)

    endpoint_label_positions = _compute_endpoint_labels(endpoint_ids, positions)
    junction_label_positions = _compute_junction_labels(graph, junction_ids, positions)
    bounds = _compute_bounds(
        list(positions.values())
        + list(endpoint_label_positions.values())
        + list(junction_label_positions.values())
    )

    svg_content = _render_svg(
        graph,
        positions,
        bounds,
        endpoint_set,
        endpoint_label_positions,
        junction_label_positions,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg_content, encoding="utf-8")

    return NetworkVisualizationResult(
        image_path=output_path,
        endpoint_labels=endpoint_label_positions,
        junction_labels=junction_label_positions,
    )


def _collect_positions(graph: nx.MultiGraph) -> Dict[str, Tuple[float, float]]:
    positions: Dict[str, Tuple[float, float]] = {}
    for node, data in graph.nodes(data=True):
        coord = data.get("coord")
        if coord is None:
            continue
        positions[node] = (float(coord[0]), float(coord[1]))

    if len(positions) != graph.number_of_nodes():
        layout = nx.spring_layout(graph, seed=42)
        for node, coord in layout.items():
            positions.setdefault(node, (float(coord[0]), float(coord[1])))

    return positions


def _compute_bounds(coords: Iterable[Tuple[float, float]]) -> _Bounds:
    coords = list(coords)
    if not coords:
        return _Bounds(0.0, 1.0, 0.0, 1.0)
    xs = [coord[0] for coord in coords]
    ys = [coord[1] for coord in coords]
    margin = LABEL_MARGIN
    return _Bounds(min(xs) - margin, max(xs) + margin, min(ys) - margin, max(ys) + margin)


def _compute_endpoint_labels(
    endpoint_ids: Iterable[str],
    positions: Mapping[str, Tuple[float, float]],
) -> Dict[str, Tuple[float, float]]:
    label_positions: Dict[str, Tuple[float, float]] = {}
    for endpoint_id in endpoint_ids:
        coord = positions.get(endpoint_id)
        if coord is None:
            continue
        dx, dy = _endpoint_label_offset(endpoint_id)
        label_positions[endpoint_id] = (coord[0] + dx, coord[1] + dy)
    return label_positions


def _compute_junction_labels(
    graph: nx.MultiGraph,
    junction_ids: Iterable[str],
    positions: Mapping[str, Tuple[float, float]],
) -> Dict[str, Tuple[float, float]]:
    primary: Dict[str, Tuple[float, float, int]] = {}
    fallback: Dict[str, Tuple[float, float, int]] = {}
    for node, data in graph.nodes(data=True):
        cluster_id = data.get("cluster_id")
        coord = positions.get(node)
        if not cluster_id or coord is None:
            continue
        total_x, total_y, count = fallback.get(cluster_id, (0.0, 0.0, 0))
        fallback[cluster_id] = (total_x + coord[0], total_y + coord[1], count + 1)

        node_type = str(data.get("node_type", ""))
        if node_type.startswith("main"):
            p_x, p_y, p_count = primary.get(cluster_id, (0.0, 0.0, 0))
            primary[cluster_id] = (p_x + coord[0], p_y + coord[1], p_count + 1)

    label_positions: Dict[str, Tuple[float, float]] = {}
    for junction_id in junction_ids:
        total_x, total_y, count = primary.get(junction_id, (0.0, 0.0, 0))
        if count == 0:
            total_x, total_y, count = fallback.get(junction_id, (0.0, 0.0, 0))
        if count == 0:
            continue
        centroid_x = total_x / count
        centroid_y = total_y / count
        label_positions[junction_id] = (centroid_x + LABEL_DELTA, centroid_y - LABEL_DELTA)
    return label_positions


def _render_svg(
    graph: nx.MultiGraph,
    positions: Mapping[str, Tuple[float, float]],
    bounds: _Bounds,
    endpoint_set: set[str],
    endpoint_label_positions: Mapping[str, Tuple[float, float]],
    junction_label_positions: Mapping[str, Tuple[float, float]],
) -> str:
    width = bounds.width + 2 * CANVAS_PADDING
    height = bounds.height + 2 * CANVAS_PADDING

    def to_svg_coord(x: float, y: float) -> Tuple[float, float]:
        svg_x = x - bounds.min_x + CANVAS_PADDING
        svg_y = (bounds.max_y - y) + CANVAS_PADDING
        return svg_x, svg_y

    elements: list[str] = [
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc" />',
    ]

    # Draw edges first so nodes appear on top.
    for u, v in graph.edges():
        x1, y1 = to_svg_coord(*positions[u])
        x2, y2 = to_svg_coord(*positions[v])
        elements.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            'stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" />'
        )

    # Draw nodes with category-specific styling.
    for node in graph.nodes:
        x, y = to_svg_coord(*positions[node])
        radius = 4.0
        fill = "#cbd5f5"
        stroke = "#1f2937"
        if node in endpoint_set:
            fill = "#fb923c"
            radius = 5.0
        else:
            node_type = str(graph.nodes[node].get("node_type", ""))
            if node_type.startswith("main"):
                fill = "#2563eb"
        elements.append(
            f'<circle cx="{x}" cy="{y}" r="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="0.75" />'
        )

    max_label_x = max(LABEL_MARGIN, width - LABEL_MARGIN)
    max_label_y = max(LABEL_MARGIN, height - LABEL_MARGIN)

    def append_labels(labels: Mapping[str, Tuple[float, float]], *, color: str, font_weight: str = "normal") -> None:
        for label, (lx, ly) in labels.items():
            sx, sy = to_svg_coord(lx, ly)
            sx = min(max(sx, LABEL_MARGIN), max_label_x)
            sy = min(max(sy, LABEL_MARGIN), max_label_y)
            elements.append(
                '<text '
                f'x="{sx}" y="{sy}" '
                f'text-anchor="start" fill="{color}" '
                f'font-size="10" font-weight="{font_weight}" '
                'dominant-baseline="text-after-edge" '
                f'transform="rotate({LABEL_ROTATION_DEG} {sx} {sy})">'
                f"{escape(label)}"
                "</text>"
            )

    append_labels(endpoint_label_positions, color="#0f172a")
    append_labels(junction_label_positions, color="#065f46", font_weight="bold")

    elements.append("</svg>")
    return "".join(elements)


def _endpoint_label_offset(endpoint_id: str) -> Tuple[float, float]:
    dx = LABEL_DELTA
    dy = -LABEL_DELTA

    if endpoint_id.endswith("WestSide"):
        dx = -LABEL_DELTA
    elif endpoint_id.endswith("EastSide"):
        dx = LABEL_DELTA

    if "MinorSEndpoint" in endpoint_id:
        dy = LABEL_DELTA
    elif "MinorNEndpoint" in endpoint_id:
        dy = -LABEL_DELTA

    if endpoint_id.endswith(".MainN"):
        dy = -LABEL_DELTA - MAJOR_VERTICAL_BIAS
    elif endpoint_id.endswith(".MainS"):
        dy = LABEL_DELTA + MAJOR_VERTICAL_BIAS

    return dx, dy


__all__ = [
    "NetworkVisualizationResult",
    "LABEL_ROTATION_DEG",
    "render_pedestrian_network_image",
]
