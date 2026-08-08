"""Operational metrics that keep unreviewed labels out of the negative class."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from decimal import Decimal
from statistics import median
from typing import Iterable

from marketleak.labels.schemas import LabelTarget, LabelValue

from .schemas import EvaluationRow, unique_evaluation_rows


@dataclass(frozen=True, slots=True)
class CalibrationGate:
    min_labeled: int = 100
    min_positive: int = 20
    min_negative: int = 20
    ece_bins: int = 10

    def reasons(self, rows: Iterable[EvaluationRow]) -> tuple[str, ...]:
        binary = [row for row in unique_evaluation_rows(rows) if row.label.is_binary]
        positives = sum(row.label == LabelValue.POSITIVE for row in binary)
        negatives = sum(row.label == LabelValue.NEGATIVE for row in binary)
        reasons: list[str] = []
        if len(binary) < self.min_labeled:
            reasons.append(f"labeled={len(binary)} below {self.min_labeled}")
        if positives < self.min_positive:
            reasons.append(f"positives={positives} below {self.min_positive}")
        if negatives < self.min_negative:
            reasons.append(f"negatives={negatives} below {self.min_negative}")
        return tuple(reasons)


@dataclass(slots=True)
class EvaluationReport:
    target: LabelTarget
    effectiveness_status: str
    effectiveness_reason: str
    total_rows: int
    labeled_rows: int
    positive_rows: int
    negative_rows: int
    unknown_rows: int
    unmapped_rows: int
    analyst_capacity: int
    precision_at_capacity: float | None
    labeled_fraction_at_capacity: float
    recall_at_capacity: float | None
    false_alerts_per_1000_market_days: float | None
    median_detection_delay_seconds: float | None
    duplicate_rate: float
    pr_auc: float | None
    brier_score: float | None
    ece: float | None
    calibration_status: str
    timeline_b: dict[str, float | None] = field(default_factory=dict)
    actor_evidence_c: dict[str, float | None] = field(default_factory=dict)
    slices: dict[str, dict[str, float | int | None]] = field(default_factory=dict)
    bootstrap_cis: dict[str, tuple[float, float] | None] = field(default_factory=dict)


def _rank(rows: Iterable[EvaluationRow]) -> list[EvaluationRow]:
    return sorted(rows, key=lambda row: (-float(row.score), row.event_time, row.row_uid))


def _average_precision(rows: list[EvaluationRow]) -> float | None:
    binary = [row for row in _rank(rows) if row.label.is_binary]
    total_positive = sum(row.label == LabelValue.POSITIVE for row in binary)
    if total_positive == 0:
        return None
    positives_seen = 0
    precision_sum = 0.0
    for rank, row in enumerate(binary, 1):
        if row.label == LabelValue.POSITIVE:
            positives_seen += 1
            precision_sum += positives_seen / rank
    return precision_sum / total_positive


def _ece(rows: list[EvaluationRow], bins: int) -> float:
    total = len(rows)
    result = 0.0
    for index in range(bins):
        lower, upper = index / bins, (index + 1) / bins
        bucket = [
            row for row in rows
            if float(row.score) >= lower
            and (float(row.score) < upper or (index == bins - 1 and float(row.score) <= upper))
        ]
        if not bucket:
            continue
        accuracy = sum(row.label == LabelValue.POSITIVE for row in bucket) / len(bucket)
        confidence = sum(float(row.score) for row in bucket) / len(bucket)
        result += len(bucket) / total * abs(accuracy - confidence)
    return result


def _core_metrics(rows: list[EvaluationRow], capacity: int) -> dict[str, float | int | None]:
    ranked = _rank(rows)
    top = ranked[:capacity]
    labeled_top = [row for row in top if row.label.is_binary]
    positives = [row for row in rows if row.label == LabelValue.POSITIVE]
    true_top = [row for row in top if row.label == LabelValue.POSITIVE]
    precision = (
        sum(row.label == LabelValue.POSITIVE for row in labeled_top) / len(labeled_top)
        if labeled_top else None
    )
    recall = len(true_top) / len(positives) if positives else None
    exposure = sum(float(row.exposure_market_days) for row in rows)
    false_alerts = sum(row.label == LabelValue.NEGATIVE for row in top)
    false_rate = false_alerts / exposure * 1000 if exposure > 0 else None
    delays = [
        (row.detected_at - row.event_time).total_seconds()
        for row in true_top if row.detected_at is not None
    ]
    seen: set[str] = set()
    duplicates = 0
    for row in top:
        key = row.duplicate_group_uid or row.row_uid
        if key in seen:
            duplicates += 1
        seen.add(key)
    return {
        "precision": precision,
        "labeled_fraction": len(labeled_top) / len(top) if top else 0.0,
        "recall": recall,
        "false_rate": false_rate,
        "delay": median(delays) if delays else None,
        "duplicate_rate": duplicates / len(top) if top else 0.0,
        "pr_auc": _average_precision(rows),
    }


def _bootstrap(
    rows: list[EvaluationRow], capacity: int, *, samples: int, seed: int
) -> dict[str, tuple[float, float] | None]:
    clusters: dict[str, list[EvaluationRow]] = {}
    for row in rows:
        clusters.setdefault(row.event_cluster_uid, []).append(row)
    if len(clusters) < 2 or samples < 2:
        return {"precision_at_capacity": None, "recall_at_capacity": None}
    rng = random.Random(seed)
    names = sorted(clusters)
    values: dict[str, list[float]] = {"precision": [], "recall": []}
    for _ in range(samples):
        sampled: list[EvaluationRow] = []
        for name in (rng.choice(names) for _ in names):
            sampled.extend(clusters[name])
        metrics = _core_metrics(sampled, capacity)
        for key in values:
            if metrics[key] is not None:
                values[key].append(float(metrics[key]))

    def interval(items: list[float]) -> tuple[float, float] | None:
        if not items:
            return None
        ordered = sorted(items)
        low = ordered[max(0, math.floor(0.025 * (len(ordered) - 1)))]
        high = ordered[min(len(ordered) - 1, math.ceil(0.975 * (len(ordered) - 1)))]
        return low, high

    return {
        "precision_at_capacity": interval(values["precision"]),
        "recall_at_capacity": interval(values["recall"]),
    }


def evaluate(
    rows: Iterable[EvaluationRow],
    *,
    target: LabelTarget,
    analyst_capacity: int,
    calibration_gate: CalibrationGate | None = None,
    decision_threshold: float = 0.5,
    bootstrap_samples: int = 200,
    bootstrap_seed: int = 7,
) -> EvaluationReport:
    if analyst_capacity < 0:
        raise ValueError("analyst_capacity must be non-negative")
    selected = [row for row in unique_evaluation_rows(rows) if row.target == target]
    binary = [row for row in selected if row.label.is_binary]
    positives = [row for row in binary if row.label == LabelValue.POSITIVE]
    negatives = [row for row in binary if row.label == LabelValue.NEGATIVE]
    gate = calibration_gate or CalibrationGate()
    reasons = gate.reasons(selected)
    status = "effectiveness_unknown_insufficient_labels" if reasons else "estimated_from_available_labels"
    reason = "; ".join(reasons) if reasons else "label sufficiency gate passed; estimate is not proof of fraud"
    core = _core_metrics(selected, analyst_capacity)
    if reasons:
        brier = None
        ece = None
        calibration_status = "not_computed_insufficient_labels"
    else:
        brier_decimal = sum(
            (row.score - (Decimal(1) if row.label == LabelValue.POSITIVE else Decimal(0))) ** 2
            for row in binary
        ) / Decimal(len(binary))
        brier = float(brier_decimal)
        ece = _ece(binary, gate.ece_bins)
        calibration_status = "computed"

    timeline_b: dict[str, float | None] = {}
    if target == LabelTarget.PUBLIC_EXPLANATION_B:
        claimed = [row for row in selected if not row.abstained and float(row.score) >= decision_threshold]
        mapped_claimed = [row for row in claimed if row.label.is_binary]
        timeline_b = {
            "label_coverage": len(binary) / len(selected) if selected else 0.0,
            "abstention_rate": sum(row.abstained for row in selected) / len(selected) if selected else 0.0,
            "false_claim_rate": (
                sum(row.label == LabelValue.NEGATIVE for row in mapped_claimed) / len(mapped_claimed)
                if mapped_claimed else None
            ),
        }
    actor_c: dict[str, float | None] = {}
    if target == LabelTarget.ACTOR_EVIDENCE_C:
        predicted = [row for row in selected if not row.abstained and float(row.score) >= decision_threshold]
        evidenced = [row for row in predicted if row.evidence_supported and row.label.is_binary]
        actor_c = {
            "actor_visibility_rate": sum(row.actor_visible for row in selected) / len(selected) if selected else 0.0,
            "evidence_support_rate": sum(row.evidence_supported for row in predicted) / len(predicted) if predicted else None,
            "evidence_supported_precision": (
                sum(row.label == LabelValue.POSITIVE for row in evidenced) / len(evidenced)
                if evidenced else None
            ),
            "abstention_rate": sum(row.abstained for row in selected) / len(selected) if selected else 0.0,
        }

    slices: dict[str, dict[str, float | int | None]] = {}
    for dimension in ("platform", "category"):
        values = sorted({getattr(row, dimension) for row in selected})
        for value in values:
            subset = [row for row in selected if getattr(row, dimension) == value]
            sliced = _core_metrics(subset, min(analyst_capacity, len(subset)))
            slices[f"{dimension}={value}"] = {
                "rows": len(subset),
                "labeled_rows": sum(row.label.is_binary for row in subset),
                "precision_at_capacity": sliced["precision"],
                "recall_at_capacity": sliced["recall"],
                "pr_auc": sliced["pr_auc"],
            }

    return EvaluationReport(
        target=target,
        effectiveness_status=status,
        effectiveness_reason=reason,
        total_rows=len(selected),
        labeled_rows=len(binary),
        positive_rows=len(positives),
        negative_rows=len(negatives),
        unknown_rows=sum(row.label == LabelValue.UNKNOWN for row in selected),
        unmapped_rows=sum(row.label == LabelValue.UNMAPPED for row in selected),
        analyst_capacity=analyst_capacity,
        precision_at_capacity=core["precision"],
        labeled_fraction_at_capacity=float(core["labeled_fraction"]),
        recall_at_capacity=core["recall"],
        false_alerts_per_1000_market_days=core["false_rate"],
        median_detection_delay_seconds=core["delay"],
        duplicate_rate=float(core["duplicate_rate"]),
        pr_auc=core["pr_auc"],
        brier_score=brier,
        ece=ece,
        calibration_status=calibration_status,
        timeline_b=timeline_b,
        actor_evidence_c=actor_c,
        slices=slices,
        bootstrap_cis=_bootstrap(
            selected, analyst_capacity, samples=bootstrap_samples, seed=bootstrap_seed
        ),
    )
