"""Reproducibility contracts for datasets, models, and frozen shadow runs."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import Field, field_validator, model_validator

from .common import LineageRecord, NonEmptyStr, StableUID
from .enums import AnalysisTarget, ShadowRunStatus


class DatasetManifest(LineageRecord):
    dataset_uid: StableUID
    name: NonEmptyStr
    version: NonEmptyStr
    created_at: datetime
    window_start: datetime
    window_end: datetime
    source_uids: tuple[StableUID, ...]
    raw_artifact_uids: tuple[StableUID, ...]
    record_counts: dict[NonEmptyStr, int]
    checksums: dict[NonEmptyStr, NonEmptyStr]
    deduplication_keys: tuple[NonEmptyStr, ...]
    known_limitations: tuple[NonEmptyStr, ...] = ()

    @field_validator("created_at", "window_start", "window_end")
    @classmethod
    def validate_manifest_time(cls, value: datetime) -> datetime:
        return cls.require_utc(value)

    @field_validator("record_counts")
    @classmethod
    def validate_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("record counts must be non-negative integers")
        return value

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be after window_start")
        return self


class ModelManifest(LineageRecord):
    model_uid: StableUID
    name: NonEmptyStr
    version: NonEmptyStr
    target: AnalysisTarget
    trained_on_dataset_uid: StableUID
    feature_names: tuple[NonEmptyStr, ...]
    metrics: dict[NonEmptyStr, Decimal] = Field(default_factory=dict)
    thresholds: dict[NonEmptyStr, Decimal] = Field(default_factory=dict)
    artifact_hash: NonEmptyStr
    code_revision: NonEmptyStr
    frozen_config_hash: NonEmptyStr
    known_limitations: tuple[NonEmptyStr, ...] = ()


class ShadowManifest(LineageRecord):
    shadow_run_uid: StableUID
    model_uid: StableUID
    dataset_uid: StableUID
    status: ShadowRunStatus
    started_at: datetime
    ended_at: datetime | None = None
    frozen_config_hash: NonEmptyStr
    analyst_capacity: int = Field(strict=True, ge=0)
    alert_count: int = Field(strict=True, ge=0)
    reviewed_count: int = Field(strict=True, ge=0)
    metrics: dict[NonEmptyStr, Decimal] = Field(default_factory=dict)
    notes: tuple[NonEmptyStr, ...] = ()

    @field_validator("started_at", "ended_at")
    @classmethod
    def validate_run_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else cls.require_utc(value)

    @model_validator(mode="after")
    def validate_run(self) -> Self:
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot precede started_at")
        if self.status == ShadowRunStatus.COMPLETED and self.ended_at is None:
            raise ValueError("completed shadow runs require ended_at")
        if self.reviewed_count > self.alert_count:
            raise ValueError("reviewed_count cannot exceed alert_count")
        return self


class ShadowRunManifest(ShadowManifest):
    """Explicit alias retained for callers that prefer the longer name."""

