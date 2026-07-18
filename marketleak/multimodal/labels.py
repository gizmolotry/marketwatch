"""Human-only, multi-axis Phase 15 adjudication contracts.

The labels describe observable mechanisms and review decisions.  They never
turn a model score into a legal finding, and unknown/unmapped remains distinct
from a negative example.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.multimodal.schemas import Phase15Model, _utc


class ObservableMechanism(str, Enum):
    THIN_LIQUIDITY_ARTIFACT = "thin_liquidity_artifact"
    PUBLIC_INFORMATION_RESPONSE = "public_information_response"
    SCHEDULED_EVENT_RESPONSE = "scheduled_event_response"
    UNDERLYING_REFERENCE_MOVE = "underlying_reference_move"
    SIBLING_MARKET_REPRICING = "sibling_market_repricing"
    MARKET_MAKER_REBALANCE = "market_maker_rebalance"
    OUTAGE_OR_RECOVERY = "outage_or_recovery"
    UNEXPLAINED_ACTIVITY = "unexplained_activity"
    MIXED = "mixed"
    UNKNOWN = "unknown"
    UNMAPPED = "unmapped"


class EvidenceStrength(str, Enum):
    ABSENT = "absent"
    CONFLICTING = "conflicting"
    LIMITED = "limited"
    CORROBORATED = "corroborated"
    UNKNOWN = "unknown"
    UNMAPPED = "unmapped"


class HumanDisposition(str, Enum):
    BENIGN_MECHANICAL = "benign_mechanical"
    BENIGN_PUBLIC_RESPONSE = "benign_public_response"
    ESCALATE_FOR_REVIEW = "escalate_for_review"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    UNRESOLVED = "unresolved"
    UNKNOWN = "unknown"
    UNMAPPED = "unmapped"


class ExternalLegalOutcome(str, Enum):
    PENDING = "pending"
    EXTERNALLY_ADJUDICATED_MISCONDUCT = "externally_adjudicated_misconduct"
    EXTERNALLY_ADJUDICATED_NO_MISCONDUCT = "externally_adjudicated_no_misconduct"


class ExternalLegalAudit(Phase15Model):
    """Optional, audit-only external disposition; never a predicted target."""

    authority: NonEmptyStr
    outcome: ExternalLegalOutcome
    public_reference: NonEmptyStr
    adjudicated_at: datetime
    audit_only: Literal[True] = True
    training_eligible: Literal[False] = False

    @field_validator("adjudicated_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="adjudicated_at")


class MultiAxisAdjudication(Phase15Model):
    """Append-only human review label for one event or explicitly linked case."""

    label_uid: StableUID
    event_uid: StableUID
    case_uid: StableUID | None = None
    observable_mechanism: ObservableMechanism
    evidence_strength: EvidenceStrength
    disposition: HumanDisposition
    evidence_uids: tuple[StableUID, ...] = ()
    rationale: NonEmptyStr
    adjudicator: NonEmptyStr
    adjudicated_at: datetime
    human_adjudicated: Literal[True] = True
    model_generated: Literal[False] = False
    training_eligible: bool
    external_legal_audit: ExternalLegalAudit | None = None
    supersedes_uid: StableUID | None = None

    @field_validator("adjudicated_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="adjudicated_at")

    @model_validator(mode="after")
    def validate_human_label_policy(self) -> "MultiAxisAdjudication":
        unknown_or_unmapped = {
            ObservableMechanism.UNKNOWN,
            ObservableMechanism.UNMAPPED,
            EvidenceStrength.UNKNOWN,
            EvidenceStrength.UNMAPPED,
            HumanDisposition.UNKNOWN,
            HumanDisposition.UNMAPPED,
        }
        if self.training_eligible and (
            self.observable_mechanism in unknown_or_unmapped
            or self.evidence_strength in unknown_or_unmapped
            or self.disposition in unknown_or_unmapped
        ):
            raise ValueError("unknown and unmapped labels cannot be training eligible")
        if self.external_legal_audit is not None and self.training_eligible:
            raise ValueError("external legal outcomes are audit-only and cannot make a label trainable")
        if self.supersedes_uid == self.label_uid:
            raise ValueError("an adjudication cannot supersede itself")
        return self


__all__ = [
    "EvidenceStrength",
    "ExternalLegalAudit",
    "ExternalLegalOutcome",
    "HumanDisposition",
    "MultiAxisAdjudication",
    "ObservableMechanism",
]
