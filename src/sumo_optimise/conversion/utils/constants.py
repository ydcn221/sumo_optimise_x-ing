"""Shared constants mirroring the legacy implementation."""
from __future__ import annotations

from pathlib import Path

INPUT_JSON_PATH = Path("v1.3.check.json")
SCHEMA_JSON_PATH = Path(__file__).resolve().parents[1] / "data" / "schema.json"
OUTPUT_DIR_PREFIX = "plainXML_out"
DATE_DIR_FORMAT = "%m%d"

NODES_FILE_NAME = "net.nod.xml"
EDGES_FILE_NAME = "net.edg.xml"
CONNECTIONS_FILE_NAME = "net.con.xml"
TLLOGICS_FILE_NAME = "net.tll.xml"
NETWORK_FILE_NAME = "network.net.xml"

MANIFEST_NAME = "manifest.json"
