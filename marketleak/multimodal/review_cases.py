"""Frozen, redacted Phase 15 review-case packets for read-only serving.

This module is deliberately a presentation contract, not an inference path.
It reads one hash-checked JSON document produced outside the serving process
and returns only market-mechanism review material that was available by the
case cutoff.  It never connects to a venue, resolves an identity, looks up an
address, or computes a score.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
from typing import Literal, Mapping, Sequence

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, Probability, StableUID
from marketleak.multimodal.schemas import Phase15Model, canonical_hash


REVIEW_CASE_SCHEMA_VERSION = "phase15-review-case-v1"
REVIEW_CASE_CONFIG_SCHEMA_VERSION = "phase15-review-cases-config-v1"

# These packets must not become a backdoor for actor attribution or a claim
# about wrongdoing.  The checks apply to identifiers as well as narrative
# fields because every configured value can be returned by the API.
_PROHIBITED_TERMS = (
    "fraud",
    "insider",
    "intent",
    "identity",
    "wallet",
    "address",
    "owner",
    "ownership",
    "misconduct",
    "mnpi",
    "risk score",
    "fraud probability",
)


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _safe_text(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    lowered = normalized.lower()
    if any(term in lowered for term in _PROHIBITED_TERMS):
        raise ValueError(f"{field_name} contains a prohibited attribution or conclusion term")
    return normalized


class ReviewCaseStatus(str, Enum):
    AVAILABLE_PRECOMPUTED_REVIEW_CASES = "available_precomputed_review_cases"
    REVIEW_CASES_NOT_CONFIGURED = "review_cases_not_configured"
    UNAVAILABLE = "unavailable"
    UNAVAILABLE_LATE_CONFIGURATION = "unavailable_late_configuration"


class ReviewCoverageStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class ReviewDecision(str, Enum):
    TRIAGE = "triage"
    ABSTAIN_INSUFFICIENT_EVIDENCE = "abstain_insufficient_evidence"
    ABSTAIN_OOD = "abstain_ood"
    ABSTAIN_UNCERTAIN = "abstain_uncertain"
    BLOCKED_BY_PROVENANCE_POLICY = "blocked_by_provenance_policy"


class ReviewCheckStatus(str, Enum):
    OBSERVED = "observed"
    PLAUSIBLE = "plausible"
    NOT_OBSERVED = "not_observed"
    UNAVAILABLE = "unavailable"


class ReviewCaseKind(str, Enum):
    RECORDED_SNAPSHOT = "recorded_snapshot"
    SIMULATED_WALKTHROUGH = "simulated_walkthrough"


class _ReviewModel(Phase15Model):
    """Strict, immutable base for a safe review packet."""


class ReviewTrigger(_ReviewModel):
    """Observed market movement values; never a person or intent signal."""

    trigger_uid: StableUID
    event_uid: StableUID
    window_starts_at: datetime
    window_ends_at: datetime
    observed_at: datetime
    available_at: datetime
    price_open: Probability
    price_close: Probability
    price_change: Decimal = Field(strict=True)
    observation_count: int = Field(strict=True, ge=0)
    fill_count: int = Field(strict=True, ge=0)
    orderbook_snapshot_count: int = Field(strict=True, ge=0)
    trade_notional: Decimal | None = Field(default=None, strict=True, ge=Decimal("0"))
    raw_artifact_uids: tuple[StableUID, ...]

    @field_validator("trigger_uid", "event_uid", "raw_artifact_uids")
    @classmethod
    def require_safe_identifiers(cls, value, info):
        values = value if isinstance(value, tuple) else (value,)
        for item in values:
            _safe_text(str(item), field_name=info.field_name)
        return value

    @field_validator("window_starts_at", "window_ends_at", "observed_at", "available_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_trigger(self) -> "ReviewTrigger":
        if self.window_ends_at <= self.window_starts_at:
            raise ValueError("trigger window must have positive duration")
        if self.window_ends_at > self.observed_at:
            raise ValueError("trigger window cannot end after observed_at")
        if self.observed_at > self.available_at:
            raise ValueError("trigger observed_at cannot be after available_at")
        if self.price_change != self.price_close - self.price_open:
            raise ValueError("trigger price_change must equal price_close minus price_open")
        if not self.raw_artifact_uids or len(set(self.raw_artifact_uids)) != len(self.raw_artifact_uids):
            raise ValueError("trigger requires distinct raw artifact UIDs")
        return self


class OrdinaryExplanationCheck(_ReviewModel):
    """A bounded ordinary-mechanism check, not a conclusion."""

    check_uid: StableUID
    kind: Literal[
        "market_mechanics",
        "underlying_reference",
        "public_evidence",
        "market_context",
        "collection_health",
    ]
    status: ReviewCheckStatus
    coverage_status: ReviewCoverageStatus
    summary: NonEmptyStr
    evidence_uids: tuple[StableUID, ...] = ()

    @field_validator("check_uid", "evidence_uids")
    @classmethod
    def require_safe_identifiers(cls, value, info):
        values = value if isinstance(value, tuple) else (value,)
        for item in values:
            _safe_text(str(item), field_name=info.field_name)
        return value

    @field_validator("summary")
    @classmethod
    def require_safe_summary(cls, value: str) -> str:
        return _safe_text(value, field_name="summary")

    @model_validator(mode="after")
    def validate_check(self) -> "OrdinaryExplanationCheck":
        if len(set(self.evidence_uids)) != len(self.evidence_uids):
            raise ValueError("ordinary check evidence_uids must be unique")
        if self.status in {ReviewCheckStatus.OBSERVED, ReviewCheckStatus.PLAUSIBLE} and not self.evidence_uids:
            raise ValueError("observed or plausible ordinary checks require evidence_uids")
        if self.status in {ReviewCheckStatus.NOT_OBSERVED, ReviewCheckStatus.UNAVAILABLE} and self.evidence_uids:
            raise ValueError("not_observed and unavailable checks cannot attach evidence_uids")
        if self.status == ReviewCheckStatus.UNAVAILABLE and self.coverage_status == ReviewCoverageStatus.COMPLETE:
            raise ValueError("an unavailable check cannot claim complete coverage")
        return self


class CoverageModalityState(_ReviewModel):
    modality: Literal[
        "market_state",
        "public_evidence",
        "onchain_settlement",
        "reference_price",
        "market_context",
        "collection_health",
    ]
    status: ReviewCoverageStatus
    reason: NonEmptyStr

    @field_validator("reason")
    @classmethod
    def require_safe_reason(cls, value: str) -> str:
        return _safe_text(value, field_name="reason")


class ReviewCoverage(_ReviewModel):
    overall_status: ReviewCoverageStatus
    modality_states: tuple[CoverageModalityState, ...]
    source_high_watermarks: dict[StableUID, datetime]
    gap_reasons: tuple[NonEmptyStr, ...] = ()
    raw_receipt_count: int = Field(strict=True, ge=0)
    late_excluded_count: int = Field(strict=True, ge=0)

    @field_validator("source_high_watermarks")
    @classmethod
    def require_safe_watermarks(cls, value: dict[str, datetime]) -> dict[str, datetime]:
        normalized: dict[str, datetime] = {}
        for source_uid, timestamp in value.items():
            _safe_text(str(source_uid), field_name="source_high_watermarks")
            normalized[str(source_uid)] = _utc(timestamp, field_name=f"source_high_watermarks[{source_uid}]")
        return normalized

    @field_validator("gap_reasons")
    @classmethod
    def require_safe_gap_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_safe_text(item, field_name="gap_reasons") for item in value)

    @model_validator(mode="after")
    def validate_coverage(self) -> "ReviewCoverage":
        modalities = tuple(item.modality for item in self.modality_states)
        if not modalities or len(set(modalities)) != len(modalities):
            raise ValueError("coverage requires distinct modality states")
        if self.overall_status == ReviewCoverageStatus.COMPLETE and (
            any(item.status != ReviewCoverageStatus.COMPLETE for item in self.modality_states) or self.gap_reasons
        ):
            raise ValueError("complete coverage cannot include incomplete modalities or gap reasons")
        if self.overall_status != ReviewCoverageStatus.COMPLETE and not self.gap_reasons and all(
            item.status == ReviewCoverageStatus.COMPLETE for item in self.modality_states
        ):
            raise ValueError("partial or unavailable coverage requires a gap or incomplete modality")
        return self


class ReviewRouting(_ReviewModel):
    decision: ReviewDecision
    coverage_status: ReviewCoverageStatus
    abstention_reasons: tuple[NonEmptyStr, ...] = ()
    mechanism_hypotheses: tuple[
        Literal[
            "thin_liquidity_artifact",
            "public_information_response",
            "scheduled_event_response",
            "underlying_reference_move",
            "sibling_market_repricing",
            "market_maker_rebalance",
            "outage_or_recovery",
            "unexplained_activity",
            "mixed",
        ],
        ...,
    ] = ()

    @field_validator("abstention_reasons")
    @classmethod
    def require_safe_abstention_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_safe_text(item, field_name="abstention_reasons") for item in value)

    @model_validator(mode="after")
    def validate_routing(self) -> "ReviewRouting":
        if len(set(self.abstention_reasons)) != len(self.abstention_reasons):
            raise ValueError("abstention_reasons must be unique")
        if len(set(self.mechanism_hypotheses)) != len(self.mechanism_hypotheses):
            raise ValueError("mechanism_hypotheses must be unique")
        if self.decision == ReviewDecision.TRIAGE:
            if self.abstention_reasons:
                raise ValueError("triage cannot carry abstention reasons")
            if self.coverage_status != ReviewCoverageStatus.COMPLETE:
                raise ValueError("triage requires complete coverage")
        elif not self.abstention_reasons:
            raise ValueError("an abstention or policy block requires explicit reasons")
        return self


class RedactedEvidenceLedgerEntry(_ReviewModel):
    """Lineage summary without raw contents, actor fields, or addresses."""

    evidence_uid: StableUID
    modality: Literal[
        "market_state",
        "public_evidence",
        "onchain_settlement",
        "reference_price",
        "market_context",
        "collection_health",
    ]
    event_time: datetime
    available_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reliability_tier: Literal["high", "medium", "low", "unassessed"]

    @field_validator("evidence_uid", "source_uid", "raw_artifact_uid")
    @classmethod
    def require_safe_identifiers(cls, value: str, info) -> str:
        return _safe_text(value, field_name=info.field_name)

    @field_validator("event_time", "available_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_evidence_time(self) -> "RedactedEvidenceLedgerEntry":
        if self.event_time > self.available_at:
            raise ValueError("ledger event_time cannot be after available_at")
        return self


class FrozenReviewCase(_ReviewModel):
    """One immutable market-mechanism review packet at a fixed cutoff."""

    schema_version: Literal[REVIEW_CASE_SCHEMA_VERSION] = REVIEW_CASE_SCHEMA_VERSION
    case_uid: StableUID
    case_kind: ReviewCaseKind
    published_at: datetime
    as_of: datetime
    venue: Literal["polymarket", "kalshi"]
    market_uid: StableUID
    question: NonEmptyStr
    outcome_uid: StableUID | None = None
    trigger: ReviewTrigger
    ordinary_checks: tuple[OrdinaryExplanationCheck, ...]
    coverage: ReviewCoverage
    routing: ReviewRouting
    evidence_ledger: tuple[RedactedEvidenceLedgerEntry, ...]

    @field_validator("case_uid", "market_uid", "outcome_uid")
    @classmethod
    def require_safe_identifiers(cls, value: str | None, info) -> str | None:
        return None if value is None else _safe_text(value, field_name=info.field_name)

    @field_validator("question")
    @classmethod
    def require_safe_question(cls, value: str) -> str:
        return _safe_text(value, field_name="question")

    @field_validator("published_at", "as_of")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_case(self) -> "FrozenReviewCase":
        if self.published_at < self.as_of:
            raise ValueError("review case published_at cannot predate its as_of cutoff")
        if self.trigger.available_at > self.as_of:
            raise ValueError("trigger was not available by the review case as_of cutoff")
        if any(timestamp > self.as_of for timestamp in self.coverage.source_high_watermarks.values()):
            raise ValueError("coverage source high-watermarks cannot be later than as_of")
        if self.routing.coverage_status != self.coverage.overall_status:
            raise ValueError("routing coverage_status must match case coverage")
        check_uids = tuple(item.check_uid for item in self.ordinary_checks)
        if not check_uids or len(set(check_uids)) != len(check_uids):
            raise ValueError("review case requires distinct ordinary checks")
        evidence_uids = tuple(item.evidence_uid for item in self.evidence_ledger)
        if not evidence_uids or len(set(evidence_uids)) != len(evidence_uids):
            raise ValueError("review case requires distinct redacted evidence ledger entries")
        evidence_by_uid = {item.evidence_uid: item for item in self.evidence_ledger}
        if self.trigger.event_uid not in evidence_by_uid:
            raise ValueError("trigger event_uid must appear in the evidence ledger")
        ledger_raw = {item.raw_artifact_uid for item in self.evidence_ledger}
        if not set(self.trigger.raw_artifact_uids).issubset(ledger_raw):
            raise ValueError("trigger raw artifacts must appear in the redacted evidence ledger")
        for entry in self.evidence_ledger:
            if entry.available_at > self.as_of:
                raise ValueError("late evidence cannot appear in a frozen review case")
        for check in self.ordinary_checks:
            if not set(check.evidence_uids).issubset(evidence_by_uid):
                raise ValueError("ordinary checks may reference only evidence in the redacted ledger")
        return self

    def safe_payload(self) -> dict[str, object]:
        """Return the closed review contract; no raw contents are present."""

        return self.model_dump(mode="json")


@dataclass(frozen=True)
class ReviewCasesLoadState:
    status: ReviewCaseStatus
    reason: str
    cases: tuple[FrozenReviewCase, ...] = ()


class FrozenReviewCaseRepository:
    """Read one hash-checked review-case configuration without side effects."""

    def __init__(self, configuration_path: str | Path | None) -> None:
        self.configuration_path = None if configuration_path is None else Path(configuration_path)

    def load(self) -> ReviewCasesLoadState:
        if self.configuration_path is None:
            return ReviewCasesLoadState(
                ReviewCaseStatus.REVIEW_CASES_NOT_CONFIGURED,
                "no frozen review-case configuration is configured",
            )
        try:
            document = json.loads(self.configuration_path.read_text(encoding="utf-8"))
            expected_keys = {"schema_version", "review_cases", "review_cases_sha256"}
            if not isinstance(document, Mapping) or set(document) != expected_keys:
                raise ValueError("review-case configuration has an invalid shape")
            if document["schema_version"] != REVIEW_CASE_CONFIG_SCHEMA_VERSION:
                raise ValueError("review-case configuration schema version is unsupported")
            raw_cases = document["review_cases"]
            if not isinstance(raw_cases, list) or not raw_cases:
                raise ValueError("review-case configuration requires a non-empty review_cases list")
            supplied_hash = document["review_cases_sha256"]
            if not isinstance(supplied_hash, str) or len(supplied_hash) != 64:
                raise ValueError("review-case configuration hash is invalid")
            if canonical_hash(raw_cases) != supplied_hash:
                raise ValueError("review-case configuration content hash does not match")
            cases = tuple(FrozenReviewCase.model_validate_json(json.dumps(item)) for item in raw_cases)
            if len({item.case_uid for item in cases}) != len(cases):
                raise ValueError("review-case configuration contains duplicate case UIDs")
            return ReviewCasesLoadState(
                ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES,
                "hash-checked precomputed review cases are available",
                cases,
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return ReviewCasesLoadState(
                ReviewCaseStatus.UNAVAILABLE,
                "frozen review-case configuration is unavailable or failed provenance validation",
            )

    def payload(self, *, as_of: datetime) -> dict[str, object]:
        """Return precomputed cases only if each was published by the cutoff."""

        cutoff = _utc(as_of, field_name="as_of")
        state = self.load()
        if state.status != ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES:
            return self._unavailable_payload(state.status, state.reason)
        if any(case.published_at > cutoff for case in state.cases):
            return self._unavailable_payload(
                ReviewCaseStatus.UNAVAILABLE_LATE_CONFIGURATION,
                "a frozen review case was published after the requested cutoff",
            )
        return {
            "status": ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES.value,
            "reason": state.reason,
            "read_only": True,
            "live_fetch": False,
            "live_inference": False,
            "case_kind_required": True,
            "review_cases": [case.safe_payload() for case in state.cases],
        }

    def case_payload(self, case_uid: str, *, as_of: datetime) -> dict[str, object]:
        """Return one exact precomputed case; no market or actor lookup exists."""

        packet = self.payload(as_of=as_of)
        if packet["status"] != ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES.value:
            return {**packet, "case": None}
        try:
            requested_uid = _safe_text(case_uid, field_name="case_uid")
        except ValueError:
            return self._unavailable_payload(ReviewCaseStatus.UNAVAILABLE, "review case UID is invalid", include_case=True)
        case = next((item for item in packet["review_cases"] if item["case_uid"] == requested_uid), None)
        if case is None:
            return self._unavailable_payload(ReviewCaseStatus.UNAVAILABLE, "review case is not available", include_case=True)
        return {
            "status": ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES.value,
            "reason": "hash-checked precomputed review case is available",
            "read_only": True,
            "live_fetch": False,
            "live_inference": False,
            "case_kind_required": True,
            "case": case,
        }

    @staticmethod
    def _unavailable_payload(
        status: ReviewCaseStatus,
        reason: str,
        *,
        include_case: bool = False,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "status": status.value,
            "reason": reason,
            "read_only": True,
            "live_fetch": False,
            "live_inference": False,
            "case_kind_required": True,
            "review_cases": [],
        }
        if include_case:
            payload["case"] = None
        return payload


def review_cases_config_document(cases: Sequence[FrozenReviewCase]) -> dict[str, object]:
    """Create the explicit immutable configuration envelope for local export."""

    payload = [case.model_dump(mode="json") for case in cases]
    if not payload:
        raise ValueError("at least one frozen review case is required")
    return {
        "schema_version": REVIEW_CASE_CONFIG_SCHEMA_VERSION,
        "review_cases": payload,
        "review_cases_sha256": canonical_hash(payload),
    }


__all__ = [
    "CoverageModalityState",
    "FrozenReviewCase",
    "FrozenReviewCaseRepository",
    "OrdinaryExplanationCheck",
    "REVIEW_CASE_CONFIG_SCHEMA_VERSION",
    "REVIEW_CASE_SCHEMA_VERSION",
    "RedactedEvidenceLedgerEntry",
    "ReviewCaseKind",
    "ReviewCaseStatus",
    "ReviewCheckStatus",
    "ReviewCoverage",
    "ReviewCoverageStatus",
    "ReviewDecision",
    "ReviewRouting",
    "ReviewTrigger",
    "review_cases_config_document",
]
