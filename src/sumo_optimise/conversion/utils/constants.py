"""Shared constants mirroring the legacy implementation."""
from __future__ import annotations

from pathlib import Path

INPUT_JSON_PATH = Path("v1.3.check.json")
SCHEMA_JSON_PATH = Path(__file__).resolve().parents[1] / "data" / "schema.json"
OUTPUT_DIR_PREFIX = "plainXML_out"
DATE_DIR_FORMAT = "%m%d"

NODES_FILE_NAME = "1-generated.nod.xml"
EDGES_FILE_NAME = "1-generated.edg.xml"
CONNECTIONS_FILE_NAME = "1-generated.con.xml"
TLL_FILE_NAME = "1-generated.tll.xml"
ROUTES_FILE_NAME = "demandflow.rou.xml"
SUMO_CONFIG_FILE_NAME = "config.sumocfg"
PLAIN_NETCONVERT_PREFIX = "2-cooked"
NETWORK_FILE_NAME = "3-n+e+c+t.net.xml"

MANIFEST_NAME = "manifest.json"
LOG_FILE_NAME = "build.log"
PED_NETWORK_IMAGE_NAME = "pedestrian_network.svg"
DEMAND_ENDPOINT_TEMPLATE_NAME = "DemandPerEndpoint_template.csv"
DEMAND_JUNCTION_TEMPLATE_NAME = "JunctionTurnWeight_template.csv"
