"""Leakage-aware evaluation helpers for operational mechanism routing.

These functions report validation availability and queue workload.  They do not
claim trained performance when labels, temporal separation, or group separation
are inadequate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable, Mapping, Sequence

import numpy as np


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _score(value: float) -> float:
    score = float(value)
    if not np.isfinite(score) or not 0.0 <= score <= 1.0:
        raise ValueError("score must be finite and in [0, 1]")
    return score


@dataclass(frozen=True)
class EvaluationRecord:
    record_uid: str
    event_time: datetime
    group_uid: str
    score: float
    supported: bool | None
    escalated: bool
    analyst_day: str | None = None
    coverage_slice: str = "all"

    def __post_init__(self) -> None:
        if not str(self.record_uid).strip() or not str(self.group_uid).strip():
            raise ValueError("record_uid and group_uid are required")
        if not str(self.coverage_slice).strip():
            raise ValueError("coverage_slice is required")
        object.__setattr__(self, "event_time", _utc(self.event_time))
        object.__setattr__(self, "score", _score(self.score))
        if self.analyst_day is None:
            object.__setattr__(self, "analyst_day", self.event_time.date().isoformat())


@dataclass(frozen=True)
class TemporalGroupSplit:
    cutoff: datetime
    train: tuple[EvaluationRecord, ...]
    test: tuple[EvaluationRecord, ...]
    dropped: tuple[EvaluationRecord, ...]


def temporal_group_disjoint_split(
    records: Sequence[EvaluationRecord],
    *,
    cutoff: datetime | None = None,
    test_fraction: float = 0.20,
) -> TemporalGroupSplit:
    """Create a forward-only split and drop groups crossing the time boundary."""

    if not records:
        raise ValueError("records are required")
    if not 0.0 < test_fraction < 1.0:
        raise ValueError("test_fraction must be in (0, 1)")
    ordered = sorted(records, key=lambda row: (row.event_time, row.record_uid))
    if cutoff is None:
        index = min(len(ordered) - 1, max(1, int(np.floor(len(ordered) * (1.0 - test_fraction)))))
        boundary = ordered[index].event_time
    else:
        boundary = _utc(cutoff)
    train_candidates = [row for row in ordered if row.event_time < boundary]
    test_candidates = [row for row in ordered if row.event_time >= boundary]
    overlap = {row.group_uid for row in train_candidates} & {row.group_uid for row in test_candidates}
    train = tuple(row for row in train_candidates if row.group_uid not in overlap)
    test = tuple(row for row in test_candidates if row.group_uid not in overlap)
    dropped = tuple(row for row in ordered if row.group_uid in overlap)
    return TemporalGroupSplit(cutoff=boundary, train=train, test=test, dropped=dropped)


@dataclass(frozen=True)
class CalibrationMetrics:
    available: bool
    reason: str | None
    labeled_count: int
    positive_count: int
    negative_count: int
    brier: float | None
    ece: float | None


def brier_ece_when_adequate(
    records: Iterable[EvaluationRecord],
    *,
    min_labels: int = 50,
    min_positive: int = 10,
    min_negative: int = 10,
    bins: int = 10,
) -> CalibrationMetrics:
    """Return calibration diagnostics only after explicit label gates pass."""

    if min_labels < 2 or min_positive < 1 or min_negative < 1 or bins < 2:
        raise ValueError("invalid calibration metric gates")
    labeled = [row for row in records if row.supported is not None]
    labels = np.asarray([int(bool(row.supported)) for row in labeled], dtype=float)
    scores = np.asarray([row.score for row in labeled], dtype=float)
    positives = int(labels.sum())
    negatives = len(labels) - positives
    if len(labeled) < min_labels:
        return CalibrationMetrics(False, "insufficient_labels", len(labeled), positives, negatives, None, None)
    if positives < min_positive or negatives < min_negative:
        return CalibrationMetrics(False, "insufficient_label_balance", len(labeled), positives, negatives, None, None)
    brier = float(np.mean((scores - labels) ** 2))
    ece = 0.0
    boundaries = np.linspace(0.0, 1.0, bins + 1)
    for index in range(bins):
        lower, upper = boundaries[index], boundaries[index + 1]
        if index == bins - 1:
            mask = (scores >= lower) & (scores <= upper)
        else:
            mask = (scores >= lower) & (scores < upper)
        if not np.any(mask):
            continue
        ece += float(mask.mean()) * abs(float(scores[mask].mean()) - float(labels[mask].mean()))
    return CalibrationMetrics(True, None, len(labeled), positives, negatives, brier, float(ece))


@dataclass(frozen=True)
class SelectiveQueueMetrics:
    total_records: int
    labeled_records: int
    escalated_records: int
    abstained_records: int
    coverage: float
    selective_risk: float | None
    false_escalations: int
    analyst_days: int
    false_escalations_per_analyst_day: float | None


def selective_queue_metrics(records: Iterable[EvaluationRecord]) -> SelectiveQueueMetrics:
    """Measure abstention and analyst workload without treating unknowns as noes."""

    rows = list(records)
    escalated = [row for row in rows if row.escalated]
    labeled_escalated = [row for row in escalated if row.supported is not None]
    false_escalations = sum(not bool(row.supported) for row in labeled_escalated)
    analyst_days = len({row.analyst_day for row in escalated if row.analyst_day})
    risk = (false_escalations / len(labeled_escalated)) if labeled_escalated else None
    per_day = (false_escalations / analyst_days) if analyst_days else None
    return SelectiveQueueMetrics(
        total_records=len(rows),
        labeled_records=sum(row.supported is not None for row in rows),
        escalated_records=len(escalated),
        abstained_records=len(rows) - len(escalated),
        coverage=(len(escalated) / len(rows)) if rows else 0.0,
        selective_risk=risk,
        false_escalations=false_escalations,
        analyst_days=analyst_days,
        false_escalations_per_analyst_day=per_day,
    )


def coverage_slice_metrics(records: Iterable[EvaluationRecord]) -> Mapping[str, SelectiveQueueMetrics]:
    """Return queue metrics per explicit coverage slice without pooling gaps."""

    buckets: dict[str, list[EvaluationRecord]] = {}
    for row in records:
        buckets.setdefault(row.coverage_slice, []).append(row)
    return {name: selective_queue_metrics(buckets[name]) for name in sorted(buckets)}


@dataclass(frozen=True)
class OperationalEvaluation:
    split: TemporalGroupSplit
    calibration: CalibrationMetrics
    overall: SelectiveQueueMetrics
    coverage_slices: Mapping[str, SelectiveQueueMetrics]
    performance_claim_available: bool
    performance_claim_reason: str


def evaluate_operational_queue(
    records: Sequence[EvaluationRecord],
    *,
    cutoff: datetime | None = None,
    test_fraction: float = 0.20,
    min_labels: int = 50,
    min_positive: int = 10,
    min_negative: int = 10,
) -> OperationalEvaluation:
    """Evaluate only the held-out, temporally future, group-disjoint portion."""

    split = temporal_group_disjoint_split(records, cutoff=cutoff, test_fraction=test_fraction)
    calibration = brier_ece_when_adequate(
        split.test,
        min_labels=min_labels,
        min_positive=min_positive,
        min_negative=min_negative,
    )
    overall = selective_queue_metrics(split.test)
    slices = coverage_slice_metrics(split.test)
    # A numerical metric is not sufficient to claim trained performance.  The
    # caller must also supply separate prospective governance evidence.
    if not split.test:
        reason = "no_group_disjoint_future_test_records"
    elif not calibration.available:
        reason = f"calibration_unavailable:{calibration.reason}"
    else:
        reason = "prospective_governance_evidence_required"
    return OperationalEvaluation(
        split=split,
        calibration=calibration,
        overall=overall,
        coverage_slices=slices,
        performance_claim_available=False,
        performance_claim_reason=reason,
    )


__all__ = [
    "CalibrationMetrics",
    "EvaluationRecord",
    "OperationalEvaluation",
    "SelectiveQueueMetrics",
    "TemporalGroupSplit",
    "brier_ece_when_adequate",
    "coverage_slice_metrics",
    "evaluate_operational_queue",
    "selective_queue_metrics",
    "temporal_group_disjoint_split",
]
