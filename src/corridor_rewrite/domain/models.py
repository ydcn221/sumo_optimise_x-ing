"""Domain models describing the JSON specification and derived IR."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence


class TieBreak(str, Enum):
    TOWARD_WEST = "toward_west"
    TOWARD_EAST = "toward_east"


class LayoutKind(str, Enum):
    TEE = "tee"
    CROSS = "cross"
    XWALK_MIDBLOCK = "xwalk_midblock"


class BranchSide(str, Enum):
    NORTH = "north"
    SOUTH = "south"


@dataclass(slots=True)
class SnapSpec:
    step_m: int
    tie_break: TieBreak


@dataclass(slots=True)
class Defaults:
    minor_road_length_m: float
    ped_crossing_width_m: float
    sidewalk_width_m: float
    speed_kmh: float


@dataclass(slots=True)
class MainRoadSpec:
    length_m: float
    center_gap_m: float
    lanes: int


@dataclass(slots=True)
class JunctionTemplate:
    id: str
    main_approach_begin_m: float
    main_approach_lanes: int
    minor_lanes_to_main: int
    minor_lanes_from_main: int
    split_ped_crossing_on_main: bool
    median_continuous: bool


@dataclass(slots=True)
class SignalPhase:
    name: str
    duration_s: float
    allow_movements: Sequence[str]


@dataclass(slots=True)
class SignalProfile:
    id: str
    cycle_s: float
    phases: Sequence[SignalPhase]


@dataclass(slots=True)
class SignalRef:
    profile_id: str
    offset_s: float


@dataclass(slots=True)
class CrossingPlacement:
    west: bool
    east: bool


@dataclass(slots=True)
class LayoutEvent:
    kind: LayoutKind
    pos_m: float
    template_id: str | None
    branch: BranchSide | None
    signalized: bool
    signal: SignalRef | None
    split_ped_crossing_on_main: bool | None
    main_ped_crossing_placement: CrossingPlacement | None


@dataclass(slots=True)
class CorridorSpec:
    version: str
    snap: SnapSpec
    defaults: Defaults
    main_road: MainRoadSpec
    junction_templates: Mapping[LayoutKind, Mapping[str, JunctionTemplate]]
    signal_profiles: Mapping[LayoutKind, Mapping[str, SignalProfile]]
    layout: Sequence[LayoutEvent]


@dataclass(slots=True)
class BreakPoint:
    pos_m: float
    reasons: tuple[str, ...]


@dataclass(slots=True)
class LaneOverlay:
    start_m: float
    end_m: float
    lanes: int


@dataclass(slots=True)
class Segment:
    start_m: float
    end_m: float
    lanes: int


@dataclass(slots=True)
class NodeIR:
    id: str
    x: float
    y: float
    type: str


@dataclass(slots=True)
class EdgeIR:
    id: str
    from_node: str
    to_node: str
    num_lanes: int
    speed_mps: float
    length_m: float
    priority: int


@dataclass(slots=True)
class ConnectionIR:
    from_edge: str
    to_edge: str
    from_lane: int
    to_lane: int


@dataclass(slots=True)
class CorridorIR:
    nodes: Sequence[NodeIR]
    edges: Sequence[EdgeIR]
    connections: Sequence[ConnectionIR]
