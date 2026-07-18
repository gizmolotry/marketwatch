"""Independent A/B/C assessment records.

These records deliberately do not expose an aggregate "fraud" or "escalation"
status. They capture separate questions that require different evidence.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from .common import LineageRecord, NonEmptyStr, StableUID
from .enums import (
    ActivityStatus,
    ActorEvidenceStatus,
    ActorVisibility,
    PublicExplanationStatus,
)


class ActivitySignal(LineageRecord):
    signal_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    status: ActivityStatus
    detector_name: NonEmptyStr
    detector_version: NonEmptyStr
    score: Decimal | None = Field(default=None, strict=True)
    threshold: Decimal | None = Field(default=None, strict=True)
    reason_codes: tuple[NonEmptyStr, ...] = ()
    evidence_uids: tuple[StableUID, ...] = ()
    limitations: tuple[NonEmptyStr, ...] = ()

    @model_validator(mode="after")
    def validate_score_availability(self) -> Self:
        if self.status == ActivityStatus.NOT_SCORABLE and self.score is not None:
            raise ValueError("NOT_SCORABLE activity cannot carry a score")
        if self.status != ActivityStatus.NOT_SCORABLE and self.score is None:
            raise ValueError("scored activity requires score")
        return self


class PublicExplanation(LineageRecord):
    explanation_uid: StableUID
    signal_uid: StableUID
    status: PublicExplanationStatus
    shock_time: datetime
    earliest_matching_public_time: datetime | None = None
    evidence_uids: tuple[StableUID, ...] = ()
    coverage_uids: tuple[StableUID, ...] = ()
    summary: NonEmptyStr
    limitations: tuple[NonEmptyStr, ...] = ()

    @field_validator("shock_time", "earliest_matching_public_time")
    @classmethod
    def validate_public_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else cls.require_utc(value)

    @model_validator(mode="after")
    def validate_timing_claim(self) -> Self:
        if self.status == PublicExplanationStatus.OBSERVED_PRE_SHOCK:
            if self.earliest_matching_public_time is None:
                raise ValueError("OBSERVED_PRE_SHOCK requires earliest_matching_public_time")
            if self.earliest_matching_public_time > self.shock_time:
                raise ValueError("pre-shock public information cannot occur after shock_time")
        if self.status == PublicExplanationStatus.NO_MATCH_OBSERVED_PRE_SHOCK and not self.coverage_uids:
            raise ValueError("NO_MATCH_OBSERVED_PRE_SHOCK requires documented coverage")
        return self


class ActorEvidence(LineageRecord):
    actor_evidence_uid: StableUID
    signal_uid: StableUID
    status: ActorEvidenceStatus
    actor_visibility: ActorVisibility
    actor_uid: StableUID | None = None
    evidence_uids: tuple[StableUID, ...] = ()
    summary: NonEmptyStr
    limitations: tuple[NonEmptyStr, ...] = ()

    @model_validator(mode="after")
    def validate_actor_claim(self) -> Self:
        visible = {ActorVisibility.PUBLIC_WALLET, ActorVisibility.OWN_ACCOUNT_ONLY}
        if self.actor_visibility in visible and self.actor_uid is None:
            raise ValueError("visible actor evidence requires actor_uid")
        if self.actor_visibility == ActorVisibility.NOT_AVAILABLE and self.actor_uid is not None:
            raise ValueError("actor_uid must be absent when actor data is unavailable")
        if self.status == ActorEvidenceStatus.NO_ACTOR_DATA:
            if self.actor_visibility != ActorVisibility.NOT_AVAILABLE or self.actor_uid is not None:
                raise ValueError("NO_ACTOR_DATA requires NOT_AVAILABLE visibility")
        if self.status == ActorEvidenceStatus.CORROBORATED_ACCESS_SIGNAL and not self.evidence_uids:
            raise ValueError("corroborated access signal requires evidence")
        return self


class IntegrityAssessment(LineageRecord):
    assessment_uid: StableUID
    market_uid: StableUID
    activity: ActivitySignal
    public_explanation: PublicExplanation
    actor_evidence: ActorEvidence
    limitations: tuple[NonEmptyStr, ...] = ()
    not_proof_of_fraud: Literal[True] = True

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        if self.activity.market_uid != self.market_uid:
            raise ValueError("activity market_uid must match assessment market_uid")
        if self.public_explanation.signal_uid != self.activity.signal_uid:
            raise ValueError("public explanation must refer to the assessment activity signal")
        if self.actor_evidence.signal_uid != self.activity.signal_uid:
            raise ValueError("actor evidence must refer to the assessment activity signal")
        return self
