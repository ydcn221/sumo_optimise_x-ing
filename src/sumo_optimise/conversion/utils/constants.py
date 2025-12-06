"""Shared constants mirroring the legacy implementation."""
from __future__ import annotations

from pathlib import Path

INPUT_JSON_PATH = Path("v1.4.check.json")
SCHEMA_JSON_PATH = Path(__file__).resolve().parents[1] / "data" / "schema.json"
OUTPUT_DIR_PREFIX = "plainXML_out"
DATE_DIR_FORMAT = "%m%d"

NODES_FILE_NAME = "PlainXML/1-generated.nod_{id}.xml"
EDGES_FILE_NAME = "PlainXML/1-generated.edg_{id}.xml"
CONNECTIONS_FILE_NAME = "PlainXML/1-generated.con_{id}.xml"
TLL_FILE_NAME = "PlainXML/1-generated.tll_{id}.xml"
ROUTES_FILE_NAME = "PlainXML/demandflow.rou_{id}.xml"
SUMO_CONFIG_FILE_NAME = "PlainXML/config_{id}.sumocfg"
PLAIN_NETCONVERT_PREFIX = "PlainXML/2-cooked_{id}"
NETWORK_FILE_NAME = "PlainXML/3-assembled.net_{id}.xml"

MANIFEST_NAME = "manifest_{id}.json"
LOG_FILE_NAME = "build_{id}.log"
PED_NETWORK_IMAGE_NAME = "pedestrian_network_{id}.svg"
PED_ENDPOINT_TEMPLATE_NAME = "template_ped_dem_{id}.csv"
PED_JUNCTION_TEMPLATE_NAME = "template_ped_turn_{id}.csv"
VEH_ENDPOINT_TEMPLATE_NAME = "template_veh_dem_{id}.csv"
VEH_JUNCTION_TEMPLATE_NAME = "template_veh_turn_{id}.csv"
