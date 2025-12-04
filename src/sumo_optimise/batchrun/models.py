from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_BEGIN_FILTER = 1200.0
DEFAULT_END_TIME = 2400.0
DEFAULT_MAX_WORKERS = 32
DEFAULT_QUEUE_THRESHOLD_STEPS = 10
DEFAULT_QUEUE_THRESHOLD_LENGTH = 0.25  # ratio threshold (waiting/running)
DEFAULT_SCALE_PROBE_START = 0.1
DEFAULT_SCALE_PROBE_CEILING = 5.0
DEFAULT_SCALE_PROBE_FINE_STEP = 0.1
DEFAULT_SCALE_PROBE_COARSE_STEP = 1.0


class ScaleMode(str, Enum):
    SUMO = "sumo"
    VEH_ONLY = "veh_only"


class OutputFileType(str, Enum):
    CSV = "csv"
    XML = "xml"


class OutputCompression(str, Enum):
    GZ = "gz"
    ZST = "zst"


@dataclass(frozen=True)
class OutputFormat:
    file_type: OutputFileType = OutputFileType.CSV
    compression: OutputCompression = OutputCompression.GZ
    zstd_level: int = 10  # single-threaded zstd

    @classmethod
    def from_string(cls, value: str, *, zstd_level: int | None = None) -> "OutputFormat":
        normalized = value.strip().lower()
        if normalized not in {"xml.gz", "csv.gz", "xml.zst", "csv.zst"}:
            raise ValueError(f"unsupported output format: {value}")
        file_type = OutputFileType.XML if normalized.startswith("xml") else OutputFileType.CSV
        compression = OutputCompression.GZ if normalized.endswith("gz") else OutputCompression.ZST
        level = 10 if zstd_level is None else max(1, min(22, zstd_level))
        return cls(file_type=file_type, compression=compression, zstd_level=level)

    @property
    def base_suffix(self) -> str:
        return ".csv" if self.file_type is OutputFileType.CSV else ".xml"

    @property
    def sumo_output_suffix(self) -> str:
        if self.compression is OutputCompression.GZ:
            return f"{self.base_suffix}.gz"
        return self.base_suffix

    @property
    def compressed_suffix(self) -> str:
        if self.compression is OutputCompression.ZST:
            return f"{self.base_suffix}.zst"
        return f"{self.base_suffix}.gz"


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
    scenario_base_id: str
    seed: int
    demand_dir: Path
    scale: float
    scale_mode: ScaleMode = ScaleMode.SUMO
    begin_filter: float = DEFAULT_BEGIN_FILTER
    end_time: float = DEFAULT_END_TIME


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
class QueueDurabilityMetrics:
    first_failure_time: Optional[float] = None  # time when over-saturation is first detected; None means durable
    max_queue_length: float = 0.0  # when using summary-based ratio, this stores max ratio
    threshold_steps: int = DEFAULT_QUEUE_THRESHOLD_STEPS
    threshold_length: float = DEFAULT_QUEUE_THRESHOLD_LENGTH  # ratio threshold

    @property
    def is_durable(self) -> bool:
        return self.first_failure_time is None


@dataclass
class RunArtifacts:
    outdir: Path
    sumocfg: Path
    network: Path
    tripinfo: Path
    personinfo: Path
    fcd: Path
    summary: Path
    person_summary: Path
    detector: Path
    queue: Path
    sumo_log: Path


@dataclass(frozen=True)
class ScaleProbeConfig:
    enabled: bool = False
    start: float = DEFAULT_SCALE_PROBE_START
    ceiling: float = DEFAULT_SCALE_PROBE_CEILING
    fine_step: float = DEFAULT_SCALE_PROBE_FINE_STEP
    coarse_step: float = DEFAULT_SCALE_PROBE_COARSE_STEP
    abort_on_waiting: bool = False


@dataclass
class PhaseTiming:
    start: float | None = None
    end: float | None = None


@dataclass
class RunTimings:
    build: PhaseTiming = field(default_factory=PhaseTiming)
    sumo: PhaseTiming = field(default_factory=PhaseTiming)
    postprocess: PhaseTiming = field(default_factory=PhaseTiming)
    probe: PhaseTiming = field(default_factory=PhaseTiming)


@dataclass
class ScaleProbeResult:
    enabled: bool = False
    max_durable_scale: Optional[float] = None
    attempts: int = 0


class WorkerPhase(str, Enum):
    IDLE = "idle"
    BUILD = "build"
    SUMO = "sumo"
    PROBE = "probe"
    PARSE = "parse"
    DONE = "done"
    ERROR = "error"


@dataclass
class WorkerStatus:
    worker_id: int
    scenario_id: str = ""
    seed: int = 0
    scale: float = 0.0
    affinity_cpu: Optional[int] = None
    phase: WorkerPhase = WorkerPhase.IDLE
    step: Optional[float] = None
    label: str = ""
    last_update: float = 0.0
    done: bool = False
    error: Optional[str] = None
    probe_scale: Optional[float] = None


@dataclass
class ScenarioResult:
    scenario_id: str
    scenario_base_id: str
    seed: int
    scale: float
    begin_filter: float
    end_time: float
    demand_dir: Path
    tripinfo: TripinfoMetrics
    queue: QueueDurabilityMetrics
    scale_probe: ScaleProbeResult
    fcd_note: str = ""
    error: Optional[str] = None
    worker_id: Optional[int] = None
    timings: RunTimings = field(default_factory=RunTimings)
