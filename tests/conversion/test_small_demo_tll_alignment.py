from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import xml.etree.ElementTree as ET

import pytest

from sumo_optimise.conversion.domain.models import BuildOptions
from sumo_optimise.conversion.pipeline import build_corridor_artifacts


def _parse_connection_links(xml_text: str) -> Dict[Tuple[str, str, str, str, str], int]:
    root = ET.fromstring(xml_text)
    mapping: Dict[Tuple[str, str, str, str, str], int] = {}
    for conn in root.findall('.//connection'):
        tl = conn.attrib.get('tl')
        if not tl:
            continue
        key = (
            tl,
            conn.attrib['from'],
            conn.attrib['to'],
            conn.attrib.get('fromLane', '0'),
            conn.attrib.get('toLane', '0'),
        )
        mapping[key] = int(conn.attrib['linkIndex'])
    return mapping


def _build_options() -> BuildOptions:
    return BuildOptions(schema_path=Path('src/sumo_optimise/conversion/data/schema.json'))


_DEMO_REFERENCE_ROOT = Path('data/reference/SUMO_OPTX_demo(connection_build)')

_SMALL_DEMO_CASES: Iterable[Tuple[str, Path]] = [
    (
        'SUMO_OPTX_demo_1.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_1'
        / 'demo_1_noconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_1_conflict.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_1_conflict'
        / 'demo_1_allowconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_2.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_2'
        / 'demo_2_noconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_2_conflict.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_2_conflict'
        / 'demo_2_allowconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_3.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_3'
        / 'demo_3_noconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_3_conflict.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_3_conflict'
        / 'demo_3_allowconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_4.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_4'
        / 'demo_4_noconflict_ideal.tll.xml',
    ),
    (
        'SUMO_OPTX_demo_4_conflict.json',
        _DEMO_REFERENCE_ROOT
        / 'ideal_XML_demo_4_conflict'
        / 'demo_4_allowconflict_ideal.tll.xml',
    ),
]


@pytest.mark.parametrize(('spec_name', 'reference_path'), _SMALL_DEMO_CASES)
def test_small_demo_connection_indices_match_reference(spec_name: str, reference_path: Path) -> None:
    spec_path = _DEMO_REFERENCE_ROOT / spec_name
    options = _build_options()
    result = build_corridor_artifacts(spec_path, options)

    our_connections = _parse_connection_links(result.tll_xml)
    reference_text = reference_path.read_text(encoding='utf-8')
    reference_connections = _parse_connection_links(reference_text)

    assert our_connections == reference_connections

    crossings_by_tl: Dict[str, List[int]] = {}
    connection_counts: Dict[str, int] = {}
    for link in result.connection_links:
        if link.kind == 'connection':
            connection_counts[link.tl_id] = connection_counts.get(link.tl_id, 0) + 1
        if link.kind == 'crossing':
            crossings_by_tl.setdefault(link.tl_id, []).append(link.link_index)

    for indices in crossings_by_tl.values():
        indices.sort()

    for tl_id, indices in crossings_by_tl.items():
        connection_count = connection_counts.get(tl_id, 0)
        expected = list(range(connection_count, connection_count + len(indices)))
        assert indices == expected

    root = ET.fromstring(result.tll_xml)
    for tl_logic in root.findall('.//tlLogic'):
        tl_id = tl_logic.attrib['id']
        connection_count = connection_counts.get(tl_id, 0)
        crossing_count = len(crossings_by_tl.get(tl_id, []))
        expected_width = connection_count + crossing_count
        for phase in tl_logic.findall('phase'):
            assert len(phase.attrib['state']) == expected_width

    connections_root = ET.fromstring(result.connections_xml)
    crossing_indices = [
        int(crossing.attrib['linkIndex'])
        for crossing in connections_root.findall('.//crossing')
    ]
    expected_crossing_indices = [
        link.link_index for link in result.connection_links if link.kind == 'crossing'
    ]
    assert sorted(crossing_indices) == sorted(expected_crossing_indices)

    for crossing in connections_root.findall('.//crossing'):
        assert 'id' not in crossing.attrib
        assert crossing.attrib.get('priority') == 'true'
        assert 'tl' not in crossing.attrib
