"""Chronology-separated market-mechanism baseline models.

The only learned target in this module is a human-adjudicated, observable
market mechanism.  Public-evidence and on-chain inputs remain explicit,
deterministic availability/corroboration fields; no text model or chain
identity model is trained here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Mapping, Sequence

import numpy as np

from .fusion import validate_mechanism_name
from .labels import ObservableMechanism


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _mechanism_name(value: ObservableMechanism | str | None) -> str | None:
    if value is None:
        return None
    raw = value.value if isinstance(value, ObservableMechanism) else str(value)
    normalized = validate_mechanism_name(raw)
    if normalized in {ObservableMechanism.UNKNOWN.value, ObservableMechanism.UNMAPPED.value}:
        return None
    return normalized


class BaselineAvailability(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class DeterministicContextFeatures:
    """Availability and corroboration indicators; never model-generated facts."""

    public_evidence_available: bool
    public_evidence_corroborated: bool
    onchain_settlement_available: bool
    onchain_settlement_corroborated: bool

    def __post_init__(self) -> None:
        if self.public_evidence_corroborated and not self.public_evidence_available:
            raise ValueError("public evidence cannot corroborate when unavailable")
        if self.onchain_settlement_corroborated and not self.onchain_settlement_available:
            raise ValueError("on-chain settlement cannot corroborate when unavailable")

    def as_features(self) -> dict[str, float]:
        return {
            "public_evidence_available": float(self.public_evidence_available),
            "public_evidence_corroborated": float(self.public_evidence_corroborated),
            "onchain_settlement_available": float(self.onchain_settlement_available),
            "onchain_settlement_corroborated": float(self.onchain_settlement_corroborated),
        }


def with_deterministic_context(
    market_features: Mapping[str, float], context: DeterministicContextFeatures
) -> dict[str, float]:
    """Combine numeric market features with fixed availability masks.

    The caller must build public-evidence and on-chain booleans from provenance
    checks.  This helper has no text, wallet, or network inference path.
    """

    result = {str(name): float(value) for name, value in market_features.items()}
    if not result or any(not name or not np.isfinite(value) for name, value in result.items()):
        raise ValueError("market_features must be non-empty, named, and finite")
    overlap = set(result) & set(context.as_features())
    if overlap:
        raise ValueError(f"market feature names collide with deterministic context: {sorted(overlap)}")
    result.update(context.as_features())
    return result


@dataclass(frozen=True)
class MechanismExample:
    event_uid: str
    group_uid: str
    observed_at: datetime
    features: Mapping[str, float]
    mechanism: ObservableMechanism | str | None
    training_eligible: bool = True

    def __post_init__(self) -> None:
        if not str(self.event_uid).strip() or not str(self.group_uid).strip():
            raise ValueError("event_uid and group_uid are required")
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        clean: dict[str, float] = {}
        for name, value in self.features.items():
            key = str(name).strip()
            numeric = float(value)
            if not key or not np.isfinite(numeric):
                raise ValueError("features must have non-empty names and finite values")
            clean[key] = numeric
        if not clean:
            raise ValueError("features are required")
        object.__setattr__(self, "features", clean)
        raw = self.mechanism.value if isinstance(self.mechanism, ObservableMechanism) else self.mechanism
        object.__setattr__(self, "mechanism", _mechanism_name(raw))


@dataclass(frozen=True)
class BaselineStatus:
    availability: BaselineAvailability
    reason: str | None
    fit_count: int
    calibration_count: int
    evaluation_count: int
    mechanisms: tuple[str, ...] = ()


@dataclass(frozen=True)
class MechanismSupport:
    mechanism: str
    support: float


@dataclass(frozen=True)
class MechanismPrediction:
    availability: BaselineAvailability
    reason: str | None
    ranked_mechanisms: tuple[MechanismSupport, ...]
    feature_spec: tuple[str, ...]
    algorithm: str


class _OneVsRestIsotonicCalibrator:
    """Independent, post-fit calibration on a later chronological stream."""

    def __init__(self, classes: Sequence[str]) -> None:
        self.classes = tuple(classes)
        self._models: dict[str, object] = {}

    def fit(self, support: np.ndarray, labels: Sequence[str]) -> "_OneVsRestIsotonicCalibrator":
        from sklearn.isotonic import IsotonicRegression

        if support.ndim != 2 or support.shape[1] != len(self.classes) or support.shape[0] != len(labels):
            raise ValueError("calibration support and labels do not match class layout")
        for index, mechanism in enumerate(self.classes):
            target = np.asarray([int(label == mechanism) for label in labels], dtype=int)
            if target.min() == target.max():
                raise ValueError("calibration requires positive and negative examples for every trained mechanism")
            self._models[mechanism] = IsotonicRegression(
                y_min=0.0, y_max=1.0, out_of_bounds="clip"
            ).fit(support[:, index], target)
        return self

    def transform(self, support: np.ndarray) -> np.ndarray:
        if support.ndim != 2 or support.shape[1] != len(self.classes):
            raise ValueError("support does not match calibrated class layout")
        calibrated = np.column_stack(
            [np.asarray(self._models[name].predict(support[:, index]), dtype=float) for index, name in enumerate(self.classes)]
        )
        totals = calibrated.sum(axis=1, keepdims=True)
        if not np.isfinite(calibrated).all() or np.any(totals <= 0.0):
            raise ValueError("all_calibrated_support_zero")
        return calibrated / totals


class MarketMechanismBaseline:
    """Regularized logistic or histogram-gradient baseline for mechanisms only.

    Inputs must arrive in three independently frozen chronological partitions:
    fit, calibration, and evaluation.  The evaluation partition is validated
    but never used by model or calibrator fitting.
    """

    def __init__(
        self,
        *,
        algorithm: str = "logistic",
        regularization_c: float = 1.0,
        random_state: int = 0,
        min_fit_examples: int = 30,
        min_calibration_examples: int = 20,
        min_evaluation_examples: int = 20,
        min_examples_per_mechanism: int = 5,
    ) -> None:
        if algorithm not in {"logistic", "hist_gradient_boosting"}:
            raise ValueError("algorithm must be logistic or hist_gradient_boosting")
        if regularization_c <= 0 or min_fit_examples < 2 or min_calibration_examples < 2 or min_evaluation_examples < 1 or min_examples_per_mechanism < 1:
            raise ValueError("baseline gates and regularization must be positive")
        self.algorithm = algorithm
        self.regularization_c = float(regularization_c)
        self.random_state = int(random_state)
        self.min_fit_examples = int(min_fit_examples)
        self.min_calibration_examples = int(min_calibration_examples)
        self.min_evaluation_examples = int(min_evaluation_examples)
        self.min_examples_per_mechanism = int(min_examples_per_mechanism)
        self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, "not_fit", 0, 0, 0)
        self._feature_spec: tuple[str, ...] = ()
        self._classes: tuple[str, ...] = ()
        self._model: object | None = None
        self._calibrator: _OneVsRestIsotonicCalibrator | None = None

    @property
    def feature_spec(self) -> tuple[str, ...]:
        return self._feature_spec

    def fit(
        self,
        fit_examples: Sequence[MechanismExample],
        calibration_examples: Sequence[MechanismExample],
        evaluation_examples: Sequence[MechanismExample],
    ) -> BaselineStatus:
        self._model = None
        self._calibrator = None
        self._feature_spec = ()
        self._classes = ()
        counts = (len(fit_examples), len(calibration_examples), len(evaluation_examples))
        partitions = (tuple(fit_examples), tuple(calibration_examples), tuple(evaluation_examples))
        invalid_reason = self._validate_partitions(*partitions)
        if invalid_reason:
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, invalid_reason, *counts)
            return self.status
        if counts[0] < self.min_fit_examples or counts[1] < self.min_calibration_examples or counts[2] < self.min_evaluation_examples:
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, "insufficient_labeled_mechanism_data", *counts)
            return self.status
        fit_labels = [item.mechanism for item in fit_examples]
        assert all(label is not None for label in fit_labels)
        mechanisms = tuple(sorted(set(str(label) for label in fit_labels)))
        if len(mechanisms) < 2:
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, "insufficient_mechanism_classes", *counts)
            return self.status
        if any(fit_labels.count(name) < self.min_examples_per_mechanism for name in mechanisms):
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, "insufficient_examples_per_mechanism", *counts, mechanisms)
            return self.status
        calibration_labels = [item.mechanism for item in calibration_examples]
        if set(calibration_labels) != set(mechanisms):
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, "calibration_mechanism_coverage_incomplete", *counts, mechanisms)
            return self.status
        self._feature_spec = tuple(sorted(fit_examples[0].features))
        try:
            fit_x = self._matrix(fit_examples)
            calibration_x = self._matrix(calibration_examples)
            self._matrix(evaluation_examples)
        except ValueError as exc:
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, f"invalid_feature_partition:{exc}", *counts, mechanisms)
            return self.status
        fit_y = np.asarray([str(label) for label in fit_labels], dtype=object)
        model = self._new_model()
        model.fit(fit_x, fit_y)
        classes = tuple(str(item) for item in model.classes_)
        raw_support = np.asarray(model.predict_proba(calibration_x), dtype=float)
        try:
            calibrator = _OneVsRestIsotonicCalibrator(classes).fit(raw_support, [str(label) for label in calibration_labels])
            calibrator.transform(raw_support)
        except ValueError as exc:
            self.status = BaselineStatus(BaselineAvailability.UNAVAILABLE, f"calibration_unavailable:{exc}", *counts, mechanisms)
            return self.status
        self._model = model
        self._calibrator = calibrator
        self._classes = classes
        self.status = BaselineStatus(BaselineAvailability.AVAILABLE, None, *counts, classes)
        return self.status

    def predict(self, features: Mapping[str, float]) -> MechanismPrediction:
        if self._model is None or self._calibrator is None:
            return MechanismPrediction(
                availability=BaselineAvailability.UNAVAILABLE,
                reason=self.status.reason,
                ranked_mechanisms=(),
                feature_spec=self._feature_spec,
                algorithm=self.algorithm,
            )
        try:
            vector = self._matrix_from_mapping(features).reshape(1, -1)
        except ValueError as exc:
            return MechanismPrediction(
                availability=BaselineAvailability.UNAVAILABLE,
                reason=f"invalid_prediction_features:{exc}",
                ranked_mechanisms=(),
                feature_spec=self._feature_spec,
                algorithm=self.algorithm,
            )
        try:
            support = self._calibrator.transform(np.asarray(self._model.predict_proba(vector), dtype=float))[0]
        except ValueError as exc:
            return MechanismPrediction(
                availability=BaselineAvailability.UNAVAILABLE,
                reason=f"calibration_unavailable:{exc}",
                ranked_mechanisms=(),
                feature_spec=self._feature_spec,
                algorithm=self.algorithm,
            )
        ranked = tuple(
            MechanismSupport(mechanism=name, support=float(value))
            for name, value in sorted(zip(self._classes, support, strict=True), key=lambda pair: (-pair[1], pair[0]))
        )
        return MechanismPrediction(
            availability=BaselineAvailability.AVAILABLE,
            reason=None,
            ranked_mechanisms=ranked,
            feature_spec=self._feature_spec,
            algorithm=self.algorithm,
        )

    def _new_model(self):
        if self.algorithm == "logistic":
            from sklearn.linear_model import LogisticRegression
            from sklearn.pipeline import make_pipeline
            from sklearn.preprocessing import StandardScaler

            return make_pipeline(
                StandardScaler(),
                LogisticRegression(
                    C=self.regularization_c,
                    max_iter=1000,
                    random_state=self.random_state,
                ),
            )
        from sklearn.ensemble import HistGradientBoostingClassifier

        return HistGradientBoostingClassifier(random_state=self.random_state)

    def _validate_partitions(
        self,
        fit_examples: Sequence[MechanismExample],
        calibration_examples: Sequence[MechanismExample],
        evaluation_examples: Sequence[MechanismExample],
    ) -> str | None:
        partitions = (fit_examples, calibration_examples, evaluation_examples)
        if any(not partition for partition in partitions):
            return "empty_chronological_partition"
        for partition in partitions:
            if any(not item.training_eligible or item.mechanism is None for item in partition):
                return "non_trainable_or_unknown_mechanism_label"
        group_sets = [set(item.group_uid for item in partition) for partition in partitions]
        if group_sets[0] & group_sets[1] or group_sets[0] & group_sets[2] or group_sets[1] & group_sets[2]:
            return "partitions_not_group_disjoint"
        latest_fit = max(item.observed_at for item in fit_examples)
        earliest_calibration = min(item.observed_at for item in calibration_examples)
        latest_calibration = max(item.observed_at for item in calibration_examples)
        earliest_evaluation = min(item.observed_at for item in evaluation_examples)
        if not latest_fit < earliest_calibration or not latest_calibration < earliest_evaluation:
            return "partitions_not_strictly_chronological"
        return None

    def _matrix(self, examples: Sequence[MechanismExample]) -> np.ndarray:
        return np.vstack([self._matrix_from_mapping(item.features) for item in examples])

    def _matrix_from_mapping(self, features: Mapping[str, float]) -> np.ndarray:
        if not self._feature_spec:
            raise ValueError("feature spec unavailable")
        keys = {str(name) for name in features}
        expected = set(self._feature_spec)
        if keys != expected:
            missing = sorted(expected - keys)
            extra = sorted(keys - expected)
            raise ValueError(f"feature names differ; missing={missing}, extra={extra}")
        vector = np.asarray([float(features[name]) for name in self._feature_spec], dtype=float)
        if not np.isfinite(vector).all():
            raise ValueError("features must be finite")
        return vector


__all__ = [
    "BaselineAvailability",
    "BaselineStatus",
    "DeterministicContextFeatures",
    "MarketMechanismBaseline",
    "MechanismExample",
    "MechanismPrediction",
    "MechanismSupport",
    "with_deterministic_context",
]
