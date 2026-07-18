"""Data structures emitted by the causal activity detector."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Mapping

from marketleak.domain import ActivityStatus


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    bucket_width: timedelta = timedelta(minutes=5)
    max_carry_age: timedelta = timedelta(minutes=10)
    baseline_window: timedelta = timedelta(days=7)
    min_baseline_observations: int = 288
    min_baseline_elapsed: timedelta = timedelta(hours=24)
    empirical_tail_min_observations: int = 100
    fdr_alpha: float = 0.05
    incident_cooldown: timedelta = timedelta(minutes=30)
    scheduled_window: timedelta = timedelta(hours=1)
    near_close_window: timedelta = timedelta(hours=1)
    thin_liquidity_threshold: float = 100.0
    sibling_control_ratio: float = 0.25

    def __post_init__(self) -> None:
        if self.bucket_width != timedelta(minutes=5):
            raise ValueError("v2 detector buckets are fixed at five minutes")
        if self.max_carry_age < timedelta(0):
            raise ValueError("max_carry_age cannot be negative")
        if self.baseline_window <= timedelta(0):
            raise ValueError("baseline_window must be positive")
        if self.min_baseline_observations < 2:
            raise ValueError("min_baseline_observations must be at least two")
        if self.empirical_tail_min_observations < 2:
            raise ValueError("empirical_tail_min_observations must be at least two")
        if not 0 < self.fdr_alpha < 1:
            raise ValueError("fdr_alpha must lie strictly between zero and one")


@dataclass(frozen=True, slots=True)
class MarketControl:
    market_uid: str
    event_uid: str | None = None
    scheduled_announcement_at: datetime | None = None
    live_event: bool = False
    closes_at: datetime | None = None
    liquidity: float | None = None


@dataclass(frozen=True, slots=True)
class ComponentDiagnostic:
    value: float
    baseline_count: int
    baseline_median: float
    baseline_mad: float
    method: str
    score: float
    p_value: float


@dataclass(frozen=True, slots=True)
class BucketFeatures:
    market_uid: str
    outcome_uid: str
    bucket_time: datetime
    price: float | None
    source_event_time: datetime | None
    carry_age_seconds: float | None
    move_5m_logit: float | None
    move_30m_logit: float | None
    volume: float
    notional: float
    aggressor_imbalance: float | None
    spread: float | None
    depth: float | None
    impact: float | None
    unique_actor_count: int | None
    actor_concentration: float | None
    sibling_residual: float | None = None
    sibling_count: int = 0
    control_tags: tuple[str, ...] = ()
    source_observation_count: int = 0
    source_fill_count: int = 0

    @property
    def primary_price_move(self) -> float | None:
        value = self.sibling_residual if self.sibling_count else self.move_5m_logit
        return None if value is None else abs(value)


@dataclass(frozen=True, slots=True)
class DetectorSignal:
    market_uid: str
    outcome_uid: str
    bucket_time: datetime
    status: ActivityStatus
    score: float | None
    p_value: float | None
    q_value: float | None
    rank: int | None
    component_scores: Mapping[str, float] = field(default_factory=dict)
    component_p_values: Mapping[str, float] = field(default_factory=dict)
    diagnostics: Mapping[str, ComponentDiagnostic] = field(default_factory=dict)
    control_tags: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    incident_uid: str | None = None

    @property
    def actionable(self) -> bool:
        return self.status == ActivityStatus.ABNORMAL and self.incident_uid is not None


@dataclass(frozen=True, slots=True)
class Incident:
    incident_uid: str
    market_uid: str
    outcome_uid: str
    started_at: datetime
    ended_at: datetime
    peak_time: datetime
    peak_score: float
    signal_count: int
    control_tags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DetectionBatch:
    buckets: tuple[BucketFeatures, ...]
    signals: tuple[DetectorSignal, ...]
    incidents: tuple[Incident, ...]

    @property
    def actionable(self) -> tuple[Incident, ...]:
        return self.incidents
