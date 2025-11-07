from __future__ import annotations

import re
from pathlib import Path

import networkx as nx
import pytest

from sumo_optimise.conversion.demand.visualization import (
    LABEL_ROTATION_DEG,
    render_pedestrian_network_image,
)


def _build_sample_graph() -> nx.MultiGraph:
    graph = nx.MultiGraph()
    graph.add_node(
        "Node.0.MainN",
        coord=(0.0, 10.0),
        cluster_id="Cluster.0.Main",
        node_type="main_north",
        is_endpoint=True,
    )
    graph.add_node(
        "Node.0.MainS",
        coord=(0.0, 0.0),
        cluster_id="Cluster.0.Main",
        node_type="main_south",
        is_endpoint=False,
    )
    graph.add_node(
        "Node.0.MinorNEndpoint.WestSide",
        coord=(-5.0, 20.0),
        cluster_id="Cluster.0.Main",
        node_type="minor_north_end",
        is_endpoint=True,
    )
    graph.add_node(
        "Node.0.MinorNEndpoint.EastSide",
        coord=(5.0, 20.0),
        cluster_id="Cluster.0.Main",
        node_type="minor_north_end",
        is_endpoint=True,
    )
    graph.add_edge("Node.0.MainN", "Node.0.MainS")
    graph.add_edge("Node.0.MainN", "Node.0.MinorNEndpoint.WestSide")
    graph.add_edge("Node.0.MainN", "Node.0.MinorNEndpoint.EastSide")
    return graph


def test_render_pedestrian_network_image_creates_svg_with_labels(tmp_path: Path) -> None:
    graph = _build_sample_graph()
    output_path = tmp_path / "network.svg"

    result = render_pedestrian_network_image(
        graph,
        endpoint_ids=[
            "Node.0.MainN",
            "Node.0.MainS",
            "Node.0.MinorNEndpoint.WestSide",
            "Node.0.MinorNEndpoint.EastSide",
        ],
        junction_ids=["Cluster.0.Main"],
        output_path=output_path,
    )

    assert result is not None
    assert output_path.exists()
    svg_text = output_path.read_text(encoding="utf-8")
    assert "rotate(-45.0" in svg_text
    assert 'text-anchor="start"' in svg_text
    assert 'dominant-baseline="text-after-edge"' in svg_text
    assert result.label_rotation_deg == LABEL_ROTATION_DEG
    endpoint_label = result.endpoint_labels["Node.0.MainN"]
    node_coord = graph.nodes["Node.0.MainN"]["coord"]
    assert endpoint_label[0] > node_coord[0]
    assert endpoint_label[1] < node_coord[1]
    main_s_label = result.endpoint_labels["Node.0.MainS"]
    assert main_s_label[1] > graph.nodes["Node.0.MainS"]["coord"][1]
    west_label = result.endpoint_labels["Node.0.MinorNEndpoint.WestSide"]
    east_label = result.endpoint_labels["Node.0.MinorNEndpoint.EastSide"]
    assert west_label[0] < graph.nodes["Node.0.MinorNEndpoint.WestSide"]["coord"][0]
    assert east_label[0] > graph.nodes["Node.0.MinorNEndpoint.EastSide"]["coord"][0]
    assert "Cluster.0.Main" in result.junction_labels


def test_render_pedestrian_network_image_ignores_missing_graph(tmp_path: Path) -> None:
    assert (
        render_pedestrian_network_image(
            graph=None,
            endpoint_ids=[],
            junction_ids=[],
            output_path=tmp_path / "unused.svg",
        )
        is None
    )


def test_label_positions_clamped_within_canvas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from sumo_optimise.conversion.demand import visualization as viz

    monkeypatch.setattr(viz, "LABEL_DELTA", 1000.0)
    graph = _build_sample_graph()
    output_path = tmp_path / "clamped.svg"
    render_pedestrian_network_image(
        graph,
        endpoint_ids=["Node.0.MainN"],
        junction_ids=["Cluster.0.Main"],
        output_path=output_path,
    )

    svg_text = output_path.read_text(encoding="utf-8")
    viewbox_match = re.search(r'viewBox="0 0 ([0-9.]+) ([0-9.]+)"', svg_text)
    assert viewbox_match is not None
    width = float(viewbox_match.group(1))
    height = float(viewbox_match.group(2))

    label_match = re.search(
        r'<text[^>]*x="([0-9.]+)"[^>]*y="([0-9.]+)"[^>]*>Node\.0\.MainN</text>',
        svg_text,
    )
    assert label_match is not None
    label_x = float(label_match.group(1))
    label_y = float(label_match.group(2))

    assert viz.LABEL_MARGIN <= label_x <= max(viz.LABEL_MARGIN, width - viz.LABEL_MARGIN)
    assert viz.LABEL_MARGIN <= label_y <= max(viz.LABEL_MARGIN, height - viz.LABEL_MARGIN)
