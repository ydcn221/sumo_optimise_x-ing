from pathlib import Path

from corridor_rewrite import build_corridor_artifacts


SPEC_PATH = Path("data/legacy(v1.2)/schema_v1.2_sample.json")


def test_build_corridor_artifacts(tmp_path):
    artefacts = build_corridor_artifacts(SPEC_PATH)
    assert "nodes" in artefacts.nodes_xml
    assert "edges" in artefacts.edges_xml
    assert "connections" in artefacts.connections_xml
    mapping = artefacts.as_mapping()
    assert set(mapping.keys()) == {"nodes.nod.xml", "edges.edg.xml", "connections.con.xml"}
    for name, content in mapping.items():
        assert content.strip().startswith("<")
        assert content.strip().endswith(">")

