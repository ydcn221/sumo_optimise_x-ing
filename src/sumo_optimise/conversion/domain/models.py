"""Domain models representing the JSON specification and derived artefacts."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ..utils.constants import OUTPUT_DIR_PREFIX


@dataclass(frozen=True)
class OutputDirectoryTemplate:
    """Templates used to materialise output directories for each build run."""

    root: str = OUTPUT_DIR_PREFIX
    run: str = "{month}{day}_{seq:03}"
    seq_digits: int = 3


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


class CardinalDirection(str, Enum):
    NORTH = "North"
    SOUTH = "South"
    EAST = "East"
    WEST = "West"


class PedestrianSide(str, Enum):
    NORTH_SIDE = "NorthSide"
    SOUTH_SIDE = "SouthSide"
    EAST_SIDE = "EastSide"
    WEST_SIDE = "WestSide"


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
    ped_endpoint_offset_m: float = 0.10
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
    median_continuous: bool
    kind: EventKind


@dataclass(frozen=True)
class SignalPhaseDef:
    duration_s: int
    allow_movements: List[str]


@dataclass(frozen=True)
class PedestrianConflictConfig:
    left: bool
    right: bool


@dataclass(frozen=True)
class SignalProfileDef:
    id: str
    cycle_s: int
    ped_early_cutoff_s: int
    yellow_duration_s: int
    phases: List[SignalPhaseDef]
    kind: EventKind
    pedestrian_conflicts: PedestrianConflictConfig


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
    main_u_turn_allowed: Optional[bool] = None
    refuge_island_on_main: Optional[bool] = None
    two_stage_tll_control: Optional[bool] = None


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
class SignalLink:
    """Metadata describing the ordering of a controllable signal element."""

    tl_id: str
    movement: str
    slot_index: int
    link_index: int
    kind: str
    element_id: str


@dataclass(frozen=True)
class ControlledConnection:
    """Descriptor for a vehicle connection controlled by a traffic light."""

    tl_id: str
    from_edge: str
    to_edge: str
    from_lane: int
    to_lane: int
    link_index: int


@dataclass(frozen=True)
class ConnectionsRenderResult:
    """Rendered XML and signal metadata for vehicle connections and crossings."""

    xml: str
    links: List[SignalLink]
    controlled_connections: List[ControlledConnection]


@dataclass(frozen=True)
class VehicleEndpoint:
    """Descriptor for a vehicle endpoint used when building demand flows."""

    id: str
    pos: int
    category: str
    edge_id: str
    lane_count: int
    is_inbound: bool
    tl_id: Optional[str] = None


@dataclass(frozen=True)
class PedestrianEndpoint:
    """Descriptor for a pedestrian crossing endpoint in the demand model."""

    id: str
    pos: int
    movement: str
    node_id: str
    edges: Tuple[str, ...]
    width: float
    tl_id: Optional[str] = None


class PedestrianSegmentKind(str, Enum):
    ENDPOINT = "endpoint"
    POSITION = "position"
    RANGE = "range"


class PedestrianRateKind(str, Enum):
    ABSOLUTE = "per_hour"
    PER_METER = "per_hour_per_m"


@dataclass(frozen=True)
class VehicleDemandSegment:
    """Demand magnitude tied to a vehicle endpoint."""

    endpoint_id: str
    departures_per_hour: float
    arrivals_per_hour: float


@dataclass(frozen=True)
class PedestrianDemandSegment:
    """Demand magnitude for pedestrians, covering point or ranged scopes."""

    kind: PedestrianSegmentKind
    rate_kind: PedestrianRateKind
    departures: float
    arrivals: float
    side: Optional[DirectionMain] = None
    start_m: Optional[int] = None
    end_m: Optional[int] = None
    endpoint_id: Optional[str] = None


@dataclass(frozen=True)
class DemandInput:
    """Aggregate view of vehicle and pedestrian demand parsed from CSV inputs."""

    vehicles: List[VehicleDemandSegment]
    pedestrians: List[PedestrianDemandSegment]


@dataclass(frozen=True)
class VehicleFlow:
    """Placeholder descriptor for future vehicle demand flows."""

    id: str
    origin: str
    destination: str
    description: Optional[str] = None


@dataclass(frozen=True)
class PedestrianFlow:
    """Placeholder descriptor for future pedestrian demand flows."""

    id: str
    origin: str
    destination: str
    description: Optional[str] = None


@dataclass(frozen=True)
class EndpointCatalog:
    """Collection of vehicle and pedestrian endpoints/flows for demand planning."""

    vehicle_endpoints: List[VehicleEndpoint]
    pedestrian_endpoints: List[PedestrianEndpoint]
    vehicle_flows: List[VehicleFlow] = field(default_factory=list)
    pedestrian_flows: List[PedestrianFlow] = field(default_factory=list)


class PersonFlowPattern(str, Enum):
    PERSONS_PER_HOUR = "persons_per_hour"
    PERIOD = "period"
    POISSON = "poisson"


@dataclass(frozen=True)
class EndpointDemandRow:
    """Parsed demand row for a specific endpoint."""

    endpoint_id: str
    flow_per_hour: float
    label: Optional[str] = None
    row_index: Optional[int] = None


@dataclass(frozen=True)
class JunctionTurnWeights:
    """Turn weights for distributing pedestrian flows at a junction."""

    junction_id: str
    weights: Dict[Tuple[CardinalDirection, PedestrianSide], float]

    def weight(self, direction: CardinalDirection, side: PedestrianSide) -> float:
        return self.weights.get((direction, side), 0.0)


@dataclass(frozen=True)
class DemandOptions:
    """CLI-level options controlling demand ingestion and emission."""

    ped_endpoint_csv: Optional[Path] = None
    ped_junction_turn_weight_csv: Optional[Path] = None
    veh_endpoint_csv: Optional[Path] = None
    veh_junction_turn_weight_csv: Optional[Path] = None
    simulation_end_time: float = 3600.0


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
    run_netedit: bool = False
    console_log: bool = False
    output_template: OutputDirectoryTemplate = field(default_factory=OutputDirectoryTemplate)
    demand: Optional[DemandOptions] = None
    generate_demand_templates: bool = False


@dataclass
class BuildResult:
    nodes_xml: str
    edges_xml: str
    connections_xml: str
    connection_links: List[SignalLink]
    tll_xml: str
    demand_xml: Optional[str] = None
    manifest_path: Optional[Path] = None
    endpoint_ids: Optional[List[str]] = None
    junction_ids: Optional[List[str]] = None
    pedestrian_graph: Optional[Any] = None
    network_image_path: Optional[Path] = None


@dataclass
class CorridorArtifacts:
    nodes_path: Path
    edges_path: Path
    connections_path: Path
    tll_path: Path
