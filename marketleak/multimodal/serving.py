"""Policy-gated, read-only metadata serving for frozen multimodal bundles.

Only JSON pointer/manifest metadata is read at request time.  This module does
not train, construct an index, call a language model, or deserialize a model
artifact.  A verified manifest therefore establishes artifact identity, not a
claim that a model is ready to make substantive conclusions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterable, Literal

from .baselines import MechanismSupport
from .bundles import FrozenBundleRepository, ServingBundleManifest
from .event_store import EventMemoryStore
from .fusion import validate_mechanism_name
from .schemas import EventFact


_DECISIONS = {
    "triage",
    "abstain_insufficient_evidence",
    "abstain_ood",
    "abstain_uncertain",
    "blocked_by_provenance_policy",
}
_COVERAGE = {"complete", "partial", "unknown", "unavailable"}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class LabelSufficiency:
    """Externally measured label availability; manifests never imply it."""

    status: Literal["adequate", "insufficient", "unknown"] = "unknown"
    labeled_count: int | None = None
    reason: str = "label_sufficiency_not_attested"

    def __post_init__(self) -> None:
        if self.labeled_count is not None and self.labeled_count < 0:
            raise ValueError("labeled_count must be non-negative")
        if not str(self.reason).strip():
            raise ValueError("label sufficiency reason is required")

    def to_payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "labeled_count": self.labeled_count,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class FrozenServingAssessment:
    """A precomputed, immutable routing record for a one-way serving surface."""

    event_uid: str
    as_of: datetime
    decision: str
    coverage_status: str
    ranked_mechanisms: tuple[MechanismSupport, ...] = ()
    abstention_reasons: tuple[str, ...] = ()
    ood_score: float | None = None
    retrieval_event_uids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not str(self.event_uid).strip():
            raise ValueError("event_uid is required")
        object.__setattr__(self, "as_of", _utc(self.as_of))
        if self.decision not in _DECISIONS:
            raise ValueError("decision is not an approved serving action")
        if self.coverage_status not in _COVERAGE:
            raise ValueError("coverage_status is invalid")
        if self.decision == "triage" and self.abstention_reasons:
            raise ValueError("triage records cannot carry abstention reasons")
        if self.decision != "triage" and not self.abstention_reasons:
            raise ValueError("abstention and policy-block records require reasons")
        if self.ood_score is not None and not 0.0 <= float(self.ood_score) <= 1.0:
            raise ValueError("ood_score must be in [0, 1]")
        normalized = tuple(
            MechanismSupport(validate_mechanism_name(item.mechanism), float(item.support))
            for item in self.ranked_mechanisms
        )
        if any(item.support < 0.0 or item.support > 1.0 for item in normalized):
            raise ValueError("mechanism support must be in [0, 1]")
        object.__setattr__(self, "ranked_mechanisms", normalized)
        if len(set(self.retrieval_event_uids)) != len(self.retrieval_event_uids):
            raise ValueError("retrieval event references must be unique")

    def to_payload(self) -> dict[str, object]:
        return {
            "event_uid": self.event_uid,
            "as_of": _iso(self.as_of),
            "decision": self.decision,
            "coverage_status": self.coverage_status,
            "ranked_mechanisms": [
                {"mechanism": item.mechanism, "support": item.support}
                for item in self.ranked_mechanisms
            ],
            "abstention_reasons": list(self.abstention_reasons),
            "ood_score": self.ood_score,
            "retrieval_count": len(self.retrieval_event_uids),
        }


@dataclass(frozen=True)
class BundleLoadState:
    available: bool
    reason: str | None
    manifest: ServingBundleManifest | None


class ServingBundleRepository:
    """Read only approved pointer/manifest metadata and frozen event records."""

    def __init__(
        self,
        root: str | Path,
        *,
        event_store: EventMemoryStore | None = None,
        assessments: Iterable[FrozenServingAssessment] = (),
        label_sufficiency: LabelSufficiency | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._bundles = FrozenBundleRepository(root)
        self._events = event_store or EventMemoryStore()
        self._assessments = tuple(sorted(assessments, key=lambda item: (item.as_of, item.event_uid)))
        if len({item.event_uid for item in self._assessments}) != len(self._assessments):
            raise ValueError("frozen serving assessments must have unique event_uids")
        self._labels = label_sufficiency or LabelSufficiency()
        self._clock = clock or (lambda: datetime.now(UTC))

    def bundle_state(self) -> BundleLoadState:
        try:
            return BundleLoadState(True, None, self._bundles.read_current())
        except FileNotFoundError:
            return BundleLoadState(False, "bundle_unavailable", None)
        except (OSError, ValueError):
            # Do not expose malformed pointer or manifest bytes to callers.
            return BundleLoadState(False, "bundle_verification_failed", None)

    def as_of(self) -> datetime:
        return _utc(self._clock())

    def readiness_payload(self) -> dict[str, object]:
        state = self.bundle_state()
        return {
            "status": "bundle_verified_policy_blocked" if state.available else "bundle_unavailable",
            "bundle": self._bundle_payload(state.manifest),
            "label_sufficiency": self._labels.to_payload(),
            "serving_mode": "read_only_verified_metadata",
            "as_of": _iso(self.as_of()),
            "assessment_count": len(self.assessments_as_of()),
            "training_in_request": False,
            "retrieval_index_build_in_request": False,
            "model_deserialization_in_request": False,
        }

    def response_context(self) -> dict[str, object]:
        """Invariant context carried by every v3 response."""

        state = self.bundle_state()
        return {
            "bundle": self._bundle_payload(state.manifest),
            "as_of": _iso(self.as_of()),
            "label_sufficiency": self._labels.to_payload(),
        }

    def model_status_payload(self) -> dict[str, object]:
        state = self.bundle_state()
        return {
            "status": "verified_metadata_only" if state.available else "unavailable",
            "reason": state.reason if not state.available else "artifact_bytes_are_not_loaded_by_serving_api",
            "bundle": self._bundle_payload(state.manifest),
            "label_sufficiency": self._labels.to_payload(),
            "model_deserialization_in_request": False,
            "effectiveness_measurement_available": False,
        }

    def assessments_as_of(self, *, as_of: datetime | None = None) -> tuple[FrozenServingAssessment, ...]:
        cutoff = self.as_of() if as_of is None else _utc(as_of)
        return tuple(item for item in self._assessments if item.as_of <= cutoff)

    def assessments_payload(self) -> dict[str, object]:
        state = self.bundle_state()
        if not state.available:
            return {"status": "unavailable", "reason": state.reason, "assessments": []}
        return {
            "status": "available_metadata_only",
            "reason": "precomputed_frozen_assessments_only",
            "assessments": [item.to_payload() for item in self.assessments_as_of()],
        }

    def event_payload(self, event_uid: str) -> dict[str, object] | None:
        state = self.bundle_state()
        if not state.available:
            return None
        record = self._event_as_of(event_uid, self.as_of())
        if record is None:
            return None
        return self._safe_event_payload(record)

    def retrieval_payload(self, event_uid: str) -> dict[str, object] | None:
        state = self.bundle_state()
        if not state.available:
            return None
        assessment = next((item for item in self.assessments_as_of() if item.event_uid == event_uid), None)
        if assessment is None:
            return {
                "status": "unavailable",
                "reason": "no_frozen_retrieval_result",
                "event_uid": event_uid,
                "coverage_status": "unknown",
                "records": [],
            }
        cutoff = min(self.as_of(), assessment.as_of)
        records = [
            self._safe_event_payload(record)
            for uid in assessment.retrieval_event_uids
            if (record := self._event_as_of(uid, cutoff)) is not None
        ]
        return {
            "status": "available_metadata_only",
            "reason": "frozen_retrieval_references",
            "event_uid": event_uid,
            "as_of": _iso(cutoff),
            "coverage_status": assessment.coverage_status,
            "records": records,
            "omitted_references": len(assessment.retrieval_event_uids) - len(records),
        }

    def _event_as_of(self, event_uid: str, as_of: datetime) -> EventFact | None:
        return next((record for record in self._events.records_as_of(as_of) if record.event_uid == event_uid), None)

    @staticmethod
    def _safe_event_payload(record: EventFact) -> dict[str, object]:
        """Expose lineage summaries only, never raw payload/artifact contents."""

        return {
            "event_uid": record.event_uid,
            "modality": record.modality.value,
            "event_time": _iso(record.event_time),
            "available_at": _iso(record.available_at),
            "source_uid": record.provenance.source_uid,
            "content_hash": record.provenance.content_hash,
            "reliability": {
                "tier": record.reliability.tier.value,
                "score": str(record.reliability.score),
            },
            "missingness": [
                {"modality": item.modality.value, "status": item.status.value, "reason": item.reason}
                for item in record.missingness
            ],
        }

    @staticmethod
    def _bundle_payload(manifest: ServingBundleManifest | None) -> dict[str, object]:
        if manifest is None:
            return {"available": False, "bundle_uid": None, "bundle_version": None, "manifest_sha256": None}
        return {
            "available": True,
            "bundle_uid": manifest.bundle_uid,
            "bundle_version": manifest.bundle_version,
            "manifest_sha256": manifest.manifest_hash,
            "model_hash": manifest.model_hash,
            "model_id": f"sha256:{manifest.model_hash}",
            "feature_spec_hash": manifest.feature_spec_hash,
            "calibration_hash": manifest.calibration_hash,
            "ood_hash": manifest.ood_hash,
            "conformal_hash": manifest.conformal_hash,
            "retrieval_hash": manifest.retrieval_hash,
            "code_hash": manifest.code_hash,
            "mechanisms": list(manifest.mechanisms),
        }


__all__ = [
    "BundleLoadState",
    "FrozenServingAssessment",
    "LabelSufficiency",
    "ServingBundleRepository",
]
