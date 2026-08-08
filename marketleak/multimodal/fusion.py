"""Deterministic, mechanism-oriented late fusion.

This module deliberately does *not* estimate a person's intent or any prohibited
outcome.  It combines independently-produced modality signals into an ordered
list of operational mechanisms (for example, ``scheduled_release`` or
``low_liquidity_print``), while retaining missingness, source freshness, and
cross-modality disagreement for a later restraint policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import exp, log, sqrt
from typing import Iterable, Mapping, Sequence

import numpy as np


# These are outcomes the multimodal layer is not allowed to rank or emit.  A
# caller must supply an operational mechanism instead.
PROHIBITED_OUTCOME_TERMS = (
    "fraud",
    "insider",
    "misconduct",
    "guilt",
    "criminal",
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _unit_interval(value: float, *, field: str) -> float:
    result = float(value)
    if not np.isfinite(result) or not 0.0 <= result <= 1.0:
        raise ValueError(f"{field} must be finite and in [0, 1]")
    return result


def validate_mechanism_name(name: str) -> str:
    """Validate a neutral, operational mechanism identifier."""

    normalized = str(name).strip().lower()
    if not normalized:
        raise ValueError("mechanism name is required")
    if any(term in normalized for term in PROHIBITED_OUTCOME_TERMS):
        raise ValueError("prohibited outcome language is not a mechanism")
    return normalized


@dataclass(frozen=True)
class ModalityEvidence:
    """A model-agnostic observation from one modality.

    ``mechanism_scores`` are bounded operational compatibility scores, not
    probabilities. ``features`` remain optional because specialists may expose
    very different representations.
    """

    modality: str
    observed_at: datetime
    reliability: float
    mechanism_scores: Mapping[str, float]
    features: Mapping[str, float | None] | None = None
    source_uid: str | None = None

    def __post_init__(self) -> None:
        if not str(self.modality).strip():
            raise ValueError("modality is required")
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        object.__setattr__(self, "reliability", _unit_interval(self.reliability, field="reliability"))
        clean_scores: dict[str, float] = {}
        for mechanism, value in self.mechanism_scores.items():
            clean_scores[validate_mechanism_name(mechanism)] = _unit_interval(
                value, field=f"mechanism score for {mechanism}"
            )
        if not clean_scores:
            raise ValueError("at least one operational mechanism score is required")
        object.__setattr__(self, "mechanism_scores", clean_scores)
        if self.features is not None:
            clean_features: dict[str, float | None] = {}
            for key, value in self.features.items():
                if not str(key).strip():
                    raise ValueError("feature names must be non-empty")
                if value is None:
                    clean_features[str(key)] = None
                    continue
                numeric = float(value)
                if not np.isfinite(numeric):
                    raise ValueError(f"feature {key} must be finite or None")
                clean_features[str(key)] = numeric
            object.__setattr__(self, "features", clean_features)


@dataclass(frozen=True)
class ModalityStatus:
    modality: str
    age_seconds: float
    freshness: float
    reliability: float
    missing: bool
    effective_weight: float
    source_uid: str | None = None


@dataclass(frozen=True)
class RankedMechanism:
    mechanism: str
    fused_score: float
    contributing_modalities: tuple[str, ...]


@dataclass(frozen=True)
class FusionResult:
    """Fusion output intended for operational routing, not a finding."""

    as_of: datetime
    ranked_mechanisms: tuple[RankedMechanism, ...]
    modality_status: tuple[ModalityStatus, ...]
    disagreement: float
    observed_modalities: int
    expected_modalities: int
    missing_modalities: tuple[str, ...]
    calibration_available: bool
    calibration_reason: str | None

    @property
    def top_score(self) -> float | None:
        return self.ranked_mechanisms[0].fused_score if self.ranked_mechanisms else None

    @property
    def top_mechanism(self) -> str | None:
        return self.ranked_mechanisms[0].mechanism if self.ranked_mechanisms else None


class OperationalScoreCalibrator:
    """Optional monotone calibration for a neutral analyst-routing label.

    The calibrator is unavailable until labels meet explicit gates.  Its output
    remains an operational support estimate for a mechanism, never a claim
    about a person or a prohibited outcome.
    """

    def __init__(
        self,
        *,
        min_labels: int = 50,
        min_positive: int = 10,
        min_negative: int = 10,
    ) -> None:
        if min_labels < 2 or min_positive < 1 or min_negative < 1:
            raise ValueError("calibration gates must be positive and meaningful")
        self.min_labels = int(min_labels)
        self.min_positive = int(min_positive)
        self.min_negative = int(min_negative)
        self._model: object | None = None
        self.reason: str = "not_fit"

    @property
    def available(self) -> bool:
        return self._model is not None

    def fit(self, scores: Sequence[float], supported_labels: Sequence[int | bool]) -> "OperationalScoreCalibrator":
        if len(scores) != len(supported_labels):
            raise ValueError("scores and labels must have equal length")
        values = np.asarray([_unit_interval(score, field="calibration score") for score in scores], dtype=float)
        labels = np.asarray([int(bool(label)) for label in supported_labels], dtype=int)
        positives = int(labels.sum())
        negatives = int(len(labels) - positives)
        self._model = None
        if len(labels) < self.min_labels:
            self.reason = "insufficient_labels"
            return self
        if positives < self.min_positive or negatives < self.min_negative:
            self.reason = "insufficient_label_balance"
            return self
        # Imported lazily so the module remains usable with only NumPy for
        # non-calibrated demonstrations.
        from sklearn.isotonic import IsotonicRegression

        self._model = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(values, labels)
        self.reason = "available"
        return self

    def transform(self, score: float) -> float:
        value = _unit_interval(score, field="fusion score")
        if self._model is None:
            return value
        return float(np.asarray(self._model.predict([value]), dtype=float)[0])


class DeterministicLateFusion:
    """Reliability- and freshness-weighted late fusion for specialist models."""

    def __init__(
        self,
        *,
        expected_modalities: Iterable[str] = (),
        freshness_half_life_seconds: float = 900.0,
        calibrator: OperationalScoreCalibrator | None = None,
    ) -> None:
        if freshness_half_life_seconds <= 0:
            raise ValueError("freshness_half_life_seconds must be positive")
        expected = tuple(sorted({str(item).strip() for item in expected_modalities if str(item).strip()}))
        self.expected_modalities = expected
        self.freshness_half_life_seconds = float(freshness_half_life_seconds)
        self.calibrator = calibrator

    def status_for(self, evidence: ModalityEvidence, *, as_of: datetime) -> ModalityStatus:
        cutoff = _utc(as_of)
        age_seconds = max(0.0, (cutoff - evidence.observed_at).total_seconds())
        freshness = exp(-np.log(2.0) * age_seconds / self.freshness_half_life_seconds)
        effective_weight = evidence.reliability * freshness
        return ModalityStatus(
            modality=evidence.modality,
            age_seconds=age_seconds,
            freshness=float(freshness),
            reliability=evidence.reliability,
            missing=False,
            effective_weight=float(effective_weight),
            source_uid=evidence.source_uid,
        )

    def fuse(self, evidence: Sequence[ModalityEvidence], *, as_of: datetime) -> FusionResult:
        cutoff = _utc(as_of)
        by_modality: dict[str, ModalityEvidence] = {}
        for item in evidence:
            if item.observed_at > cutoff:
                raise ValueError("future modality evidence cannot be fused")
            # One deterministic representative per modality: the freshest
            # record wins; source UID breaks an exact timestamp tie.
            prior = by_modality.get(item.modality)
            if prior is None or (item.observed_at, item.source_uid or "") > (prior.observed_at, prior.source_uid or ""):
                by_modality[item.modality] = item

        statuses = [self.status_for(item, as_of=cutoff) for item in by_modality.values()]
        statuses.sort(key=lambda status: status.modality)
        observed = set(by_modality)
        expected = set(self.expected_modalities) | observed
        missing = tuple(sorted(expected - observed))
        statuses.extend(
            ModalityStatus(
                modality=modality,
                age_seconds=float("inf"),
                freshness=0.0,
                reliability=0.0,
                missing=True,
                effective_weight=0.0,
            )
            for modality in missing
        )

        per_mechanism: dict[str, list[tuple[float, float, str]]] = {}
        for status in statuses:
            if status.missing or status.effective_weight <= 0.0:
                continue
            item = by_modality[status.modality]
            for mechanism, score in item.mechanism_scores.items():
                per_mechanism.setdefault(mechanism, []).append((score, status.effective_weight, status.modality))

        ranked: list[RankedMechanism] = []
        for mechanism, contributions in per_mechanism.items():
            values = np.asarray([row[0] for row in contributions], dtype=float)
            weights = np.asarray([row[1] for row in contributions], dtype=float)
            fused = float(np.average(values, weights=weights))
            if self.calibrator is not None:
                fused = self.calibrator.transform(fused)
            ranked.append(
                RankedMechanism(
                    mechanism=mechanism,
                    fused_score=fused,
                    contributing_modalities=tuple(sorted(row[2] for row in contributions)),
                )
            )
        ranked.sort(key=lambda item: (-item.fused_score, item.mechanism))
        calibration_available = bool(self.calibrator and self.calibrator.available)
        calibration_reason = self.calibrator.reason if self.calibrator is not None else "no_calibrator_configured"
        return FusionResult(
            as_of=cutoff,
            ranked_mechanisms=tuple(ranked),
            modality_status=tuple(statuses),
            disagreement=_cross_modal_mechanism_disagreement(by_modality, statuses),
            observed_modalities=len(observed),
            expected_modalities=len(expected),
            missing_modalities=missing,
            calibration_available=calibration_available,
            calibration_reason=calibration_reason,
        )


def _cross_modal_mechanism_disagreement(
    by_modality: Mapping[str, ModalityEvidence],
    statuses: Sequence[ModalityStatus],
) -> float:
    """Return the maximum bounded Jensen-Shannon distance across modalities.

    Every observed modality is projected onto the same sorted mechanism axis.
    A mechanism omitted by an otherwise observed specialist has zero declared
    support, while an entirely missing or zero-weight modality is excluded by
    the observation mask.  An explicit no-support bin distinguishes an
    observed all-zero specialist output from missing modality evidence.

    Normalizing the declared support isolates disagreement about *which*
    mechanism is supported; absolute support remains represented by the fused
    scores and the weak-support restraint.  The returned distance is in [0, 1],
    and disjoint support such as ``{x: 1}`` versus ``{y: 1}`` is exactly 1.
    """

    observed_modalities = tuple(
        status.modality
        for status in statuses
        if not status.missing and status.effective_weight > 0.0
    )
    if len(observed_modalities) < 2:
        return 0.0
    mechanisms = tuple(
        sorted(
            {
                mechanism
                for modality in observed_modalities
                for mechanism in by_modality[modality].mechanism_scores
            }
        )
    )
    support = np.asarray(
        [
            [by_modality[modality].mechanism_scores.get(mechanism, 0.0) for mechanism in mechanisms]
            for modality in observed_modalities
        ],
        dtype=float,
    )
    totals = support.sum(axis=1, keepdims=True)
    normalized = np.divide(support, totals, out=np.zeros_like(support), where=totals > 0.0)
    no_support = (totals[:, 0] <= 0.0).astype(float).reshape(-1, 1)
    distributions = np.concatenate((normalized, no_support), axis=1)

    maximum = 0.0
    for left_index in range(len(distributions) - 1):
        for right_index in range(left_index + 1, len(distributions)):
            left = distributions[left_index]
            right = distributions[right_index]
            midpoint = 0.5 * (left + right)
            left_positive = left > 0.0
            right_positive = right > 0.0
            divergence = 0.5 * float(
                np.sum(left[left_positive] * np.log(left[left_positive] / midpoint[left_positive]))
                + np.sum(right[right_positive] * np.log(right[right_positive] / midpoint[right_positive]))
            )
            maximum = max(maximum, sqrt(max(0.0, divergence) / log(2.0)))
    return float(min(1.0, maximum))


__all__ = [
    "DeterministicLateFusion",
    "FusionResult",
    "ModalityEvidence",
    "ModalityStatus",
    "OperationalScoreCalibrator",
    "PROHIBITED_OUTCOME_TERMS",
    "RankedMechanism",
    "validate_mechanism_name",
]
