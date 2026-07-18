"""Out-of-distribution and selective-routing restraints for multimodal fusion.

The routines here are intentionally model-agnostic.  They measure whether an
input resembles a reference feature population and decide whether a neutral
operational mechanism may enter an analyst queue.  They do not create outcome
findings about people.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil, exp, log
from typing import Iterable, Mapping, Sequence

import numpy as np

from .fusion import FusionResult


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _bounded(value: float, *, field: str) -> float:
    result = float(value)
    if not np.isfinite(result) or not 0.0 <= result <= 1.0:
        raise ValueError(f"{field} must be finite and in [0, 1]")
    return result


def deterministic_feature_vector(
    features: Mapping[str, float | None],
    *,
    feature_order: Sequence[str] | None = None,
    include_missingness: bool = True,
) -> tuple[np.ndarray, tuple[str, ...]]:
    """Encode sparse specialist features without silently discarding gaps.

    Missing numeric values are zero-filled *with a paired missingness flag*.
    The returned names are stable and sorted unless an explicit order is
    supplied, which makes reference and query vectors reproducible.
    """

    names = tuple(feature_order) if feature_order is not None else tuple(sorted(str(key) for key in features))
    if len(set(names)) != len(names) or any(not name for name in names):
        raise ValueError("feature_order must contain unique non-empty names")
    values: list[float] = []
    encoded_names: list[str] = []
    for name in names:
        raw = features.get(name)
        missing = raw is None
        value = 0.0 if missing else float(raw)
        if not np.isfinite(value):
            raise ValueError(f"feature {name} must be finite or None")
        values.append(value)
        encoded_names.append(name)
        if include_missingness:
            values.append(1.0 if missing else 0.0)
            encoded_names.append(f"{name}__missing")
    return np.asarray(values, dtype=float), tuple(encoded_names)


@dataclass(frozen=True)
class OODResult:
    knn_distance: float | None
    energy: float | None
    ood_score: float | None
    reference_size: int
    sufficient_reference: bool
    reason: str | None = None


class ReferenceOODScorer:
    """Distance and energy-like OOD scorer against an explicit reference set."""

    def __init__(self, *, k_neighbors: int = 5, min_reference: int = 20, temperature: float = 1.0) -> None:
        if k_neighbors < 1 or min_reference < 2 or temperature <= 0:
            raise ValueError("invalid OOD scorer parameters")
        self.k_neighbors = int(k_neighbors)
        self.min_reference = int(min_reference)
        self.temperature = float(temperature)
        self._reference: np.ndarray | None = None
        self._knn_scale: float | None = None
        self._energy_center: float | None = None
        self._energy_scale: float | None = None

    @property
    def reference_size(self) -> int:
        return 0 if self._reference is None else int(self._reference.shape[0])

    def fit(self, reference_vectors: Sequence[Sequence[float] | np.ndarray]) -> "ReferenceOODScorer":
        array = np.asarray(reference_vectors, dtype=float)
        if array.ndim != 2 or array.shape[0] == 0 or array.shape[1] == 0:
            raise ValueError("reference_vectors must be a non-empty 2D numeric array")
        if not np.isfinite(array).all():
            raise ValueError("reference_vectors must be finite")
        self._reference = array
        # Leave-one-out distances establish a deterministic in-distribution
        # scale.  A robust percentile avoids a single duplicate collapsing it.
        if len(array) > 1:
            distances = self._pairwise_distances(array, array)
            np.fill_diagonal(distances, np.inf)
            kth = np.partition(distances, min(self.k_neighbors - 1, len(array) - 2), axis=1)[:, min(self.k_neighbors - 1, len(array) - 2)]
            self._knn_scale = max(float(np.quantile(kth, 0.90)), 1e-12)
            energies = np.asarray([self._energy(row) for row in array], dtype=float)
            self._energy_center = float(np.median(energies))
            self._energy_scale = max(float(np.quantile(np.abs(energies - self._energy_center), 0.90)), 1e-12)
        else:
            self._knn_scale = None
            self._energy_center = None
            self._energy_scale = None
        return self

    def score(self, vector: Sequence[float] | np.ndarray) -> OODResult:
        if self._reference is None:
            return OODResult(None, None, None, 0, False, "reference_unavailable")
        query = np.asarray(vector, dtype=float)
        if query.ndim != 1 or query.shape[0] != self._reference.shape[1] or not np.isfinite(query).all():
            raise ValueError("query vector must be finite and match fitted feature dimension")
        count = self.reference_size
        if count < self.min_reference or self._knn_scale is None or self._energy_center is None or self._energy_scale is None:
            return OODResult(None, None, None, count, False, "insufficient_reference")
        distances = np.linalg.norm(self._reference - query, axis=1)
        k = min(self.k_neighbors, count)
        knn_distance = float(np.partition(distances, k - 1)[k - 1])
        energy = self._energy(query)
        # Both components rise outside the reference population.  Logistic
        # squashing makes the restraint threshold portable without treating it
        # as a probability of any substantive outcome.
        knn_component = knn_distance / self._knn_scale
        energy_component = max(0.0, (energy - self._energy_center) / self._energy_scale)
        ood_score = 1.0 - exp(-0.5 * max(0.0, knn_component - 1.0) - 0.5 * energy_component)
        return OODResult(knn_distance, energy, float(min(1.0, ood_score)), count, True)

    def _energy(self, query: np.ndarray) -> float:
        assert self._reference is not None
        squared = np.sum((self._reference - query) ** 2, axis=1)
        logits = -squared / (2.0 * self.temperature * self.temperature)
        maximum = float(np.max(logits))
        # Negative log mean kernel density.  Values increase for far-away
        # vectors, an energy-like OOD quantity rather than a class score.
        return float(-self.temperature * (maximum + log(float(np.mean(np.exp(logits - maximum))))))

    @staticmethod
    def _pairwise_distances(left: np.ndarray, right: np.ndarray) -> np.ndarray:
        return np.linalg.norm(left[:, None, :] - right[None, :, :], axis=2)


@dataclass(frozen=True)
class AbstentionDecision:
    abstain: bool
    reasons: tuple[str, ...]
    top_mechanism: str | None
    top_score: float | None
    ood_score: float | None


class SelectiveAbstentionPolicy:
    """Conservative, explainable gate before analyst escalation."""

    def __init__(
        self,
        *,
        min_observed_modalities: int = 2,
        min_top_score: float = 0.50,
        max_disagreement: float = 0.40,
        max_ood_score: float = 0.50,
        require_calibration: bool = False,
    ) -> None:
        if min_observed_modalities < 1:
            raise ValueError("min_observed_modalities must be positive")
        self.min_observed_modalities = int(min_observed_modalities)
        self.min_top_score = _bounded(min_top_score, field="min_top_score")
        self.max_disagreement = _bounded(max_disagreement, field="max_disagreement")
        self.max_ood_score = _bounded(max_ood_score, field="max_ood_score")
        self.require_calibration = bool(require_calibration)

    def decide(self, fusion: FusionResult, ood: OODResult | None = None) -> AbstentionDecision:
        reasons: list[str] = []
        if fusion.top_mechanism is None or fusion.top_score is None:
            reasons.append("no_mechanism_ranked")
        if fusion.observed_modalities < self.min_observed_modalities:
            reasons.append("insufficient_modalities")
        if fusion.disagreement > self.max_disagreement:
            reasons.append("cross_modality_disagreement")
        if fusion.top_score is not None and fusion.top_score < self.min_top_score:
            reasons.append("weak_operational_support")
        if self.require_calibration and not fusion.calibration_available:
            reasons.append("calibration_unavailable")
        ood_score = None if ood is None else ood.ood_score
        if ood is None or not ood.sufficient_reference:
            reasons.append("ood_reference_unavailable")
        elif ood.ood_score is not None and ood.ood_score > self.max_ood_score:
            reasons.append("out_of_distribution")
        return AbstentionDecision(
            abstain=bool(reasons),
            reasons=tuple(reasons),
            top_mechanism=fusion.top_mechanism,
            top_score=fusion.top_score,
            ood_score=ood_score,
        )


@dataclass(frozen=True)
class ConformalFeedback:
    """A post-review operational feedback record.

    ``supported`` indicates only whether the escalation proved useful to the
    analyst workflow.  It is not a label for a prohibited outcome.
    """

    decision_time: datetime
    feedback_time: datetime
    score: float
    supported: bool
    candidate_uid: str

    def __post_init__(self) -> None:
        decision = _utc(self.decision_time)
        feedback = _utc(self.feedback_time)
        if feedback < decision:
            raise ValueError("feedback cannot predate the decision")
        if not str(self.candidate_uid).strip():
            raise ValueError("candidate_uid is required")
        object.__setattr__(self, "decision_time", decision)
        object.__setattr__(self, "feedback_time", feedback)
        object.__setattr__(self, "score", _bounded(self.score, field="score"))


@dataclass(frozen=True)
class QueueCandidate:
    candidate_uid: str
    score: float
    decision: AbstentionDecision

    def __post_init__(self) -> None:
        if not str(self.candidate_uid).strip():
            raise ValueError("candidate_uid is required")
        object.__setattr__(self, "score", _bounded(self.score, field="score"))


@dataclass(frozen=True)
class QueueDecision:
    candidate_uid: str
    escalate: bool
    threshold: float | None
    reasons: tuple[str, ...]


class RollingConformalRiskController:
    """Future-free adaptive thresholding with an explicit daily queue budget.

    The conformal component uses only feedback available at ``as_of``.  It
    derives a conservative score cutoff from recent unsupported escalations and
    nudges that cutoff upward if their observed rate exceeds the configured
    operational risk budget.  With no feedback it abstains rather than inventing
    validation.
    """

    def __init__(
        self,
        *,
        target_unsupported_rate: float = 0.20,
        base_threshold: float = 0.50,
        window_size: int = 200,
        learning_rate: float = 0.20,
        min_feedback: int = 20,
    ) -> None:
        self.target_unsupported_rate = _bounded(target_unsupported_rate, field="target_unsupported_rate")
        self.base_threshold = _bounded(base_threshold, field="base_threshold")
        if window_size < 1 or min_feedback < 1 or learning_rate <= 0:
            raise ValueError("window_size, min_feedback, and learning_rate must be positive")
        self.window_size = int(window_size)
        self.learning_rate = float(learning_rate)
        self.min_feedback = int(min_feedback)
        self._feedback: list[ConformalFeedback] = []

    def record(self, feedback: ConformalFeedback) -> None:
        """Append immutable analyst feedback; ordering is resolved at query time."""

        self._feedback.append(feedback)

    def available_feedback(self, *, as_of: datetime) -> tuple[ConformalFeedback, ...]:
        cutoff = _utc(as_of)
        # Future-free: feedback must have arrived by the decision cutoff, and
        # decisions themselves cannot originate after it.
        usable = [
            item
            for item in self._feedback
            if item.feedback_time <= cutoff and item.decision_time <= cutoff
        ]
        usable.sort(key=lambda item: (item.feedback_time, item.decision_time, item.candidate_uid))
        return tuple(usable[-self.window_size :])

    def threshold(self, *, as_of: datetime) -> tuple[float | None, str | None]:
        feedback = self.available_feedback(as_of=as_of)
        if len(feedback) < self.min_feedback:
            return None, "conformal_history_unavailable"
        unsupported_scores = np.asarray([item.score for item in feedback if not item.supported], dtype=float)
        observed_rate = 1.0 - (sum(item.supported for item in feedback) / len(feedback))
        adaptive = self.learning_rate * (observed_rate - self.target_unsupported_rate)
        threshold = self.base_threshold + adaptive
        if len(unsupported_scores):
            # Finite-sample conformal-style upper order statistic: selecting
            # scores at or below this boundary would resemble known unsupported
            # escalations, so escalation requires a strictly higher score.
            rank = min(len(unsupported_scores) - 1, max(0, ceil((len(unsupported_scores) + 1) * (1.0 - self.target_unsupported_rate)) - 1))
            threshold = max(threshold, float(np.sort(unsupported_scores)[rank]))
        return float(min(1.0, max(0.0, threshold))), None

    def route(
        self,
        candidates: Iterable[QueueCandidate],
        *,
        as_of: datetime,
        analyst_daily_budget: int,
    ) -> tuple[QueueDecision, ...]:
        if analyst_daily_budget < 0:
            raise ValueError("analyst_daily_budget must be non-negative")
        threshold, unavailable_reason = self.threshold(as_of=as_of)
        ordered = sorted(candidates, key=lambda item: (-item.score, item.candidate_uid))
        decisions: list[QueueDecision] = []
        admitted = 0
        for candidate in ordered:
            reasons = list(candidate.decision.reasons)
            if unavailable_reason:
                reasons.append(unavailable_reason)
            elif candidate.score < float(threshold):
                reasons.append("below_conformal_threshold")
            elif admitted >= analyst_daily_budget:
                reasons.append("analyst_budget_exhausted")
            escalate = not reasons
            if escalate:
                admitted += 1
            decisions.append(
                QueueDecision(
                    candidate_uid=candidate.candidate_uid,
                    escalate=escalate,
                    threshold=threshold,
                    reasons=tuple(reasons),
                )
            )
        return tuple(decisions)


__all__ = [
    "AbstentionDecision",
    "ConformalFeedback",
    "OODResult",
    "QueueCandidate",
    "QueueDecision",
    "ReferenceOODScorer",
    "RollingConformalRiskController",
    "SelectiveAbstentionPolicy",
    "deterministic_feature_vector",
]
