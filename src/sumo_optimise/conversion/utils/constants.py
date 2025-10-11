"""Shared constants mirroring the legacy implementation."""
from __future__ import annotations

from pathlib import Path

INPUT_JSON_PATH = Path("v1.2.check.json")
SCHEMA_JSON_PATH = Path("src\sumo_optimise\conversion\data\schema.json")
OUTPUT_DIR_PREFIX = "plainXML_out"
DATE_DIR_FORMAT = "%m%d"

NODES_FILE_NAME = "net.nod.xml"
EDGES_FILE_NAME = "net.edg.xml"
CONNECTIONS_FILE_NAME = "net.con.xml"

MANIFEST_NAME = "manifest.json"
