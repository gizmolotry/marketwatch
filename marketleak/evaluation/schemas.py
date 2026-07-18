from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from marketleak.labels.schemas import LabelTarget, LabelValue


class EvaluationRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, str_strip_whitespace=True)

    row_uid: str = Field(min_length=1)
    target: LabelTarget
    label: LabelValue
    score: Decimal = Field(strict=True, ge=Decimal("0"), le=Decimal("1"))
    event_time: datetime
    detected_at: datetime | None = None
    market_uid: str
    event_cluster_uid: str
    actor_uids: tuple[str, ...] = ()
    platform: str
    category: str
    exposure_market_days: Decimal = Field(default=Decimal("0"), strict=True, ge=Decimal("0"))
    duplicate_group_uid: str | None = None
    abstained: bool = False
    actor_visible: bool = False
    evidence_supported: bool = False

    @field_validator("event_time", "detected_at")
    @classmethod
    def utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("evaluation timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_detection_time(self):
        if self.detected_at is not None and self.detected_at < self.event_time:
            raise ValueError("detected_at cannot precede event_time")
        return self

