"""Strict label contracts; unknown and unmapped are first-class states."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class LabelTarget(str, Enum):
    ACTIVITY_A = "A_activity"
    PUBLIC_EXPLANATION_B = "B_public_explanation"
    ACTOR_EVIDENCE_C = "C_actor_evidence"


class LabelValue(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    UNKNOWN = "unknown"
    UNMAPPED = "unmapped"

    @property
    def is_binary(self) -> bool:
        return self in {LabelValue.POSITIVE, LabelValue.NEGATIVE}


class CaseOrigin(str, Enum):
    PUBLIC_ADJUDICATION = "public_adjudication"
    INTERNAL_ADJUDICATION = "internal_adjudication"
    SYNTHETIC_MECHANIC = "synthetic_mechanic"


class HardNegativeType(str, Enum):
    SCHEDULED_NEWS = "scheduled_news"
    LIVE_SPORTS = "live_sports"
    SIBLING_REPRICING = "sibling_repricing"
    LOW_LIQUIDITY_PRINT = "low_liquidity_print"
    STALE_CATCH_UP = "stale_catch_up"
    RESOLUTION = "resolution"
    OUTAGE_RECOVERY = "outage_recovery"
    MARKET_MAKER_REBALANCE = "market_maker_rebalance"


class LabelModel(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    @staticmethod
    def utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)


class Case(LabelModel):
    case_uid: str = Field(min_length=3, pattern=r"^labels:[^\s]+$")
    title: str = Field(min_length=1)
    origin: CaseOrigin
    description: str = Field(min_length=1)
    source_urls: tuple[str, ...]
    platform: str | None = None
    category: str
    event_cluster_uid: str | None = None
    market_uids: tuple[str, ...] = ()
    actor_uids: tuple[str, ...] = ()
    occurred_start: datetime | None = None
    occurred_end: datetime | None = None
    exact_market_mapping_available: bool
    exact_actor_mapping_available: bool
    training_eligible: bool
    limitations: tuple[str, ...] = ()
    created_at: datetime

    @field_validator("created_at", "occurred_start", "occurred_end")
    @classmethod
    def validate_times(cls, value: datetime | None) -> datetime | None:
        return None if value is None else cls.utc(value)

    @model_validator(mode="after")
    def validate_mapping_and_origin(self):
        if self.exact_market_mapping_available != bool(self.market_uids):
            raise ValueError("exact_market_mapping_available must match market_uids availability")
        if self.exact_actor_mapping_available != bool(self.actor_uids):
            raise ValueError("exact_actor_mapping_available must match actor_uids availability")
        if self.occurred_start and self.occurred_end and self.occurred_end < self.occurred_start:
            raise ValueError("occurred_end cannot precede occurred_start")
        if self.origin == CaseOrigin.SYNTHETIC_MECHANIC and self.training_eligible:
            raise ValueError("synthetic mechanics are never training eligible")
        return self


class Adjudication(LabelModel):
    adjudication_uid: str = Field(min_length=3, pattern=r"^labels:[^\s]+$")
    case_uid: str = Field(min_length=3, pattern=r"^labels:[^\s]+$")
    target: LabelTarget
    value: LabelValue
    rationale: str = Field(min_length=1)
    evidence_urls: tuple[str, ...] = ()
    adjudicator: str = Field(min_length=1)
    adjudicated_at: datetime
    training_eligible: bool
    supersedes_uid: str | None = Field(default=None, pattern=r"^labels:[^\s]+$")

    @field_validator("adjudicated_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return cls.utc(value)

    @model_validator(mode="after")
    def unknown_is_never_trainable(self):
        if not self.value.is_binary and self.training_eligible:
            raise ValueError("unknown and unmapped adjudications cannot be training eligible")
        return self


class LabelWindow(LabelModel):
    window_uid: str = Field(min_length=3, pattern=r"^labels:[^\s]+$")
    case_uid: str = Field(min_length=3, pattern=r"^labels:[^\s]+$")
    starts_at: datetime
    ends_at: datetime
    event_cluster_uid: str
    market_uid: str | None = None
    actor_uids: tuple[str, ...] = ()
    labels: dict[LabelTarget, LabelValue]
    sampling_weight: Decimal = Field(strict=True, gt=Decimal("0"))
    hard_negative_type: HardNegativeType | None = None
    synthetic: bool = False
    training_eligible: bool
    evidence_refs: tuple[str, ...] = ()

    @field_validator("starts_at", "ends_at")
    @classmethod
    def validate_times(cls, value: datetime) -> datetime:
        return cls.utc(value)

    @model_validator(mode="after")
    def validate_window(self):
        if self.ends_at <= self.starts_at:
            raise ValueError("ends_at must be after starts_at")
        if self.synthetic and self.training_eligible:
            raise ValueError("synthetic windows test mechanics only and cannot train models")
        if self.hard_negative_type is not None and not self.synthetic:
            raise ValueError("generated hard-negative mechanics must be marked synthetic")
        return self
