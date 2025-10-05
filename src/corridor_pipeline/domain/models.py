"""Domain models representing the JSON specification and derived artefacts."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set


class DirectionMain(str, Enum):
    EB = "EB"
    WB = "WB"


class SideMinor(str, Enum):
    NORTH = "north"
    SOUTH = "south"


class Movement(str, Enum):
    LEFT = "L"
    THROUGH = "T"
    RIGHT = "R"
    PEDESTRIAN = "PED"


class EventKind(str, Enum):
    TEE = "tee"
    CROSS = "cross"
    XWALK_MIDBLOCK = "xwalk_midblock"


class MedianKind(str, Enum):
    CONTINUOUS = "continuous"
    NONE = "none"


class TieBreak(str, Enum):
    TOWARD_WEST = "toward_west"
    TOWARD_EAST = "toward_east"


@dataclass(frozen=True)
class SnapRule:
    step_m: int
    tie_break: str


@dataclass(frozen=True)
class Defaults:
    minor_road_length_m: int
    ped_crossing_width_m: float
    speed_kmh: int
    sidewalk_width_m: Optional[float] = None


@dataclass(frozen=True)
class MainRoadConfig:
    length_m: float
    center_gap_m: float
    lanes: int


@dataclass(frozen=True)
class JunctionTemplate:
    id: str
    main_approach_begin_m: int
    main_approach_lanes: int
    minor_lanes_to_main: int
    minor_lanes_from_main: int
    split_ped_crossing_on_main: bool
    median_continuous: bool
    kind: EventKind


@dataclass(frozen=True)
class SignalPhaseDef:
    name: str
    duration_s: int
    allow_movements: List[str]


@dataclass(frozen=True)
class SignalProfileDef:
    id: str
    cycle_s: int
    phases: List[SignalPhaseDef]
    kind: EventKind


@dataclass(frozen=True)
class SignalRef:
    profile_id: str
    offset_s: int


@dataclass(frozen=True)
class LayoutEvent:
    type: EventKind
    pos_m_raw: float
    pos_m: int
    template_id: Optional[str] = None
    signalized: Optional[bool] = None
    signal: Optional[SignalRef] = None
    main_ped_crossing_placement: Optional[Dict[str, bool]] = None
    branch: Optional[SideMinor] = None
    split_ped_crossing_on_main: Optional[bool] = None


@dataclass
class Cluster:
    pos_m: int
    events: List[LayoutEvent]


@dataclass(frozen=True)
class LaneOverride:
    start: int
    end: int
    lanes: int


@dataclass(frozen=True)
class BreakpointInfo:
    pos: int
    reasons: Set[str]


@dataclass(frozen=True)
class CorridorSpec:
    version: str
    snap: SnapRule
    defaults: Defaults
    main_road: MainRoadConfig
    junction_templates: Dict[str, JunctionTemplate]
    signal_profiles: Dict[str, Dict[str, SignalProfileDef]]
    layout: List[LayoutEvent]


@dataclass(frozen=True)
class BuildOptions:
    schema_path: Path
    run_netconvert: bool = False
    console_log: bool = False


@dataclass
class BuildResult:
    nodes_xml: str
    edges_xml: str
    connections_xml: str
    manifest_path: Optional[Path] = None


@dataclass
class CorridorArtifacts:
    nodes_path: Path
    edges_path: Path
    connections_path: Path

