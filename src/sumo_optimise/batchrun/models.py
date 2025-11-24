from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_BEGIN_FILTER = 1200.0
DEFAULT_END_TIME = 2400.0
DEFAULT_MAX_WORKERS = 32
WAITING_THRESHOLD_FIXED = 1.0
WAITING_THRESHOLD_PCT = 0.10
DEFAULT_QUEUE_THRESHOLD_STEPS = 10
DEFAULT_QUEUE_THRESHOLD_LENGTH = 200.0
DEFAULT_SCALE_PROBE_START = 1.0
DEFAULT_SCALE_PROBE_CEILING = 10.0
DEFAULT_SCALE_PROBE_RESOLUTION = 0.1


@dataclass(frozen=True)
class DemandFiles:
    ped_endpoint: Path
    ped_junction: Path
    veh_endpoint: Path
    veh_junction: Path


@dataclass(frozen=True)
class ScenarioConfig:
    spec: Path
    scenario_id: str
    seed: int
    demand_dir: Path
    scale: float
    begin_filter: float = DEFAULT_BEGIN_FILTER
    end_time: float = DEFAULT_END_TIME


@dataclass(frozen=True)
class WaitingThresholds:
    fixed: float = WAITING_THRESHOLD_FIXED
    pct_of_running: float = WAITING_THRESHOLD_PCT


@dataclass(frozen=True)
class QueueDurabilityConfig:
    step_window: int = DEFAULT_QUEUE_THRESHOLD_STEPS
    length_threshold: float = DEFAULT_QUEUE_THRESHOLD_LENGTH


@dataclass
class TripinfoMetrics:
    vehicle_count: int = 0
    vehicle_time_loss_sum: float = 0.0
    person_count: int = 0
    person_time_loss_sum: float = 0.0
    person_route_length_sum: float = 0.0

    @property
    def vehicle_mean_time_loss(self) -> Optional[float]:
        if self.vehicle_count == 0:
            return None
        return self.vehicle_time_loss_sum / self.vehicle_count

    @property
    def person_mean_time_loss(self) -> Optional[float]:
        if self.person_count == 0:
            return None
        return self.person_time_loss_sum / self.person_count

    @property
    def person_mean_route_length(self) -> Optional[float]:
        if self.person_count == 0:
            return None
        return self.person_route_length_sum / self.person_count


@dataclass
class WaitingMetrics:
    first_fixed_time: Optional[float] = None
    first_fixed_value: Optional[float] = None
    first_pct_time: Optional[float] = None
    first_pct_value: Optional[float] = None
    max_waiting: float = 0.0


@dataclass
class QueueDurabilityMetrics:
    first_failure_time: Optional[float] = None
    max_queue_length: float = 0.0
    threshold_steps: int = DEFAULT_QUEUE_THRESHOLD_STEPS
    threshold_length: float = DEFAULT_QUEUE_THRESHOLD_LENGTH

    @property
    def is_durable(self) -> bool:
        return self.first_failure_time is None


@dataclass
class RunArtifacts:
    outdir: Path
    sumocfg: Path
    tripinfo: Path
    fcd: Path
    summary: Path
    person_summary: Path
    detector: Path
    queue: Path


@dataclass(frozen=True)
class ScaleProbeConfig:
    enabled: bool = False
    start: float = DEFAULT_SCALE_PROBE_START
    ceiling: float = DEFAULT_SCALE_PROBE_CEILING
    resolution: float = DEFAULT_SCALE_PROBE_RESOLUTION


@dataclass
class ScaleProbeResult:
    enabled: bool = False
    min_failure_scale: Optional[float] = None
    attempts: int = 0


@dataclass
class ScenarioResult:
    scenario_id: str
    seed: int
    scale: float
    begin_filter: float
    end_time: float
    demand_dir: Path
    tripinfo: TripinfoMetrics
    waiting: WaitingMetrics
    waiting_thresholds: WaitingThresholds
    queue: QueueDurabilityMetrics
    scale_probe: ScaleProbeResult
    fcd_note: str = ""
    error: Optional[str] = None
