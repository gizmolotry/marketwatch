"""Raw provenance, point-in-time coverage, and evidence graph DTOs."""

from __future__ import annotations

from datetime import datetime
from typing import Self

from pydantic import field_validator, model_validator

from .common import LineageRecord, NonEmptyStr, StableUID
from .enums import AnalysisTarget, CoverageStatus, EvidenceRelation, EvidenceType


class RawArtifact(LineageRecord):
    content_hash: NonEmptyStr
    media_type: NonEmptyStr
    storage_uri: NonEmptyStr
    byte_length: int | None = None
    retrieved_at: datetime

    @field_validator("byte_length")
    @classmethod
    def validate_byte_length(cls, value: int | None) -> int | None:
        if value is not None and (isinstance(value, bool) or value < 0):
            raise ValueError("byte_length must be a non-negative integer")
        return value

    @field_validator("retrieved_at")
    @classmethod
    def validate_retrieved_at(cls, value: datetime) -> datetime:
        return cls.require_utc(value)


class EvidenceItem(LineageRecord):
    evidence_uid: StableUID
    evidence_type: EvidenceType
    target: AnalysisTarget
    summary: NonEmptyStr
    content_hash: NonEmptyStr | None = None
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: datetime) -> datetime:
        return cls.require_utc(value)


class CoverageWindow(LineageRecord):
    coverage_uid: StableUID
    coverage_type: NonEmptyStr
    status: CoverageStatus
    starts_at: datetime
    ends_at: datetime
    query: NonEmptyStr | None = None
    limitations: tuple[NonEmptyStr, ...] = ()

    @field_validator("starts_at", "ends_at")
    @classmethod
    def validate_window_time(cls, value: datetime) -> datetime:
        return cls.require_utc(value)

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.ends_at <= self.starts_at:
            raise ValueError("ends_at must be after starts_at")
        return self


class EvidenceLink(LineageRecord):
    link_uid: StableUID
    from_uid: StableUID
    to_uid: StableUID
    relation: EvidenceRelation
    is_inferred: bool
    rationale: NonEmptyStr | None = None

    @model_validator(mode="after")
    def reject_self_link(self) -> Self:
        if self.from_uid == self.to_uid:
            raise ValueError("evidence links cannot be self-referential")
        if self.is_inferred and self.rationale is None:
            raise ValueError("inferred links require a rationale")
        return self


class EvidenceObservation(EvidenceItem):
    """Canonical descriptive name for a point-in-time evidence item."""


class SourceCoverageInterval(CoverageWindow):
    """Canonical descriptive name for a source coverage window."""
