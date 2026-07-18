"""Read-only Phase 15 assembly, readiness, and baseline-candidate orchestration.

This layer freezes as-of inputs and makes readiness explicit.  It creates
metadata for review only: it neither writes model artifacts nor approves or
serves a candidate.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable, Literal

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.domain.enums import CoverageStatus
from marketleak.evaluation.baselines import score_baselines
from marketleak.evaluation.metrics import CalibrationGate, evaluate
from marketleak.evaluation.schemas import EvaluationRow
from marketleak.evaluation.splits import DatasetSplit, forward_disjoint_split
from marketleak.labels.schemas import LabelTarget, LabelValue
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.labels import MultiAxisAdjudication
from marketleak.multimodal.schemas import EventFact, EventMemorySnapshot, Phase15Model, canonical_hash


ORCHESTRATION_SCHEMA_VERSION = "15.0.0-orchestration-v1"


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class FeatureAssemblyInput(Phase15Model):
    """Caller-supplied, immutable features tied to one observed event UID."""

    feature_uid: StableUID
    event_uid: StableUID
    event_cluster_uid: StableUID
    market_uid: StableUID
    platform: NonEmptyStr
    category: NonEmptyStr
    features: dict[NonEmptyStr, Decimal]
    coverage_status: CoverageStatus
    context_complete: bool
    adjudication: MultiAxisAdjudication | None = None
    baseline_target: LabelTarget | None = None
    baseline_label: LabelValue | None = None
    exposure_market_days: Decimal = Field(default=Decimal("0"), strict=True, ge=Decimal("0"))

    @field_validator("features")
    @classmethod
    def validate_features(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        if not value:
            raise ValueError("features must not be empty")
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def validate_human_mapping(self) -> "FeatureAssemblyInput":
        if (self.baseline_target is None) != (self.baseline_label is None):
            raise ValueError("baseline_target and baseline_label must be supplied together")
        if self.adjudication is not None and self.adjudication.event_uid != self.event_uid:
            raise ValueError("adjudication.event_uid must match feature event_uid")
        return self


class BaselinePlan(Phase15Model):
    """Fixed forward-time boundaries; callers may not infer them from outcomes."""

    target: LabelTarget
    train_end: datetime
    validation_end: datetime
    analyst_capacity: int = Field(default=25, strict=True, ge=1)

    @field_validator("train_end", "validation_end")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_order(self) -> "BaselinePlan":
        if self.validation_end <= self.train_end:
            raise ValueError("validation_end must be after train_end")
        return self


class AssembledFeatureRow(Phase15Model):
    feature: FeatureAssemblyInput
    event_time: datetime
    available_at: datetime
    modality: NonEmptyStr

    @field_validator("event_time", "available_at")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)


class ExcludedFeatureInput(Phase15Model):
    feature_uid: StableUID
    event_uid: StableUID
    reason: NonEmptyStr


class AsOfAssembly(Phase15Model):
    """Frozen causal selection of event facts and caller-provided feature rows."""

    schema_version: NonEmptyStr = ORCHESTRATION_SCHEMA_VERSION
    run_uid: StableUID
    as_of: datetime
    event_snapshot: EventMemorySnapshot
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: tuple[AssembledFeatureRow, ...]
    excluded: tuple[ExcludedFeatureInput, ...]

    @field_validator("as_of")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")

    @model_validator(mode="after")
    def validate_as_of(self) -> "AsOfAssembly":
        if any(row.event_time > self.as_of or row.available_at > self.as_of for row in self.rows):
            raise ValueError("assembly rows cannot use future or later-available event facts")
        return self


class ReadinessReport(Phase15Model):
    """A deliberate gate: serving remains blocked even after a candidate is evaluated."""

    assembly_run_uid: StableUID
    status: Literal["not_ready", "ready_for_candidate_evaluation"]
    training_allowed: bool
    serving_allowed: Literal[False] = False
    reason_codes: tuple[NonEmptyStr, ...]
    total_rows: int = Field(ge=0)
    binary_label_rows: int = Field(ge=0)
    calibration_reason_codes: tuple[NonEmptyStr, ...] = ()


class BaselineCandidate(Phase15Model):
    """Candidate metadata only; review outside this process is required."""

    candidate_uid: StableUID
    assembly_run_uid: StableUID
    status: Literal["not_ready", "unapproved_candidate"]
    approved: Literal[False] = False
    published: Literal[False] = False
    methods: tuple[Literal["naive_price_change", "naive_volume"], ...] = (
        "naive_price_change",
        "naive_volume",
    )
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class BaselineEvaluationSummary(Phase15Model):
    method: Literal["naive_price_change", "naive_volume"]
    test_rows: int = Field(ge=0)
    labeled_test_rows: int = Field(ge=0)
    calibration_status: NonEmptyStr
    precision_at_capacity: float | None = None
    recall_at_capacity: float | None = None
    pr_auc: float | None = None
    brier_score: float | None = None
    ece: float | None = None


class BaselineRun(Phase15Model):
    status: Literal["not_ready", "candidate_evaluated"]
    assembly: AsOfAssembly
    readiness: ReadinessReport
    candidate: BaselineCandidate
    evaluation: tuple[BaselineEvaluationSummary, ...] = ()


def build_as_of_assembly(
    store: EventMemoryStore,
    inputs: Iterable[FeatureAssemblyInput],
    *,
    as_of: datetime,
) -> AsOfAssembly:
    """Freeze admissible facts first, then attach only matching feature inputs."""

    cutoff = _utc(as_of, field_name="as_of")
    snapshot = store.snapshot_as_of(cutoff)
    facts = {record.event_uid: record for record in snapshot.records}
    ordered_inputs = tuple(sorted(inputs, key=lambda item: item.feature_uid))
    rows: list[AssembledFeatureRow] = []
    excluded: list[ExcludedFeatureInput] = []
    for item in ordered_inputs:
        fact = facts.get(item.event_uid)
        if fact is None:
            excluded.append(
                ExcludedFeatureInput(
                    feature_uid=item.feature_uid,
                    event_uid=item.event_uid,
                    reason="event_not_admissible_as_of",
                )
            )
            continue
        rows.append(
            AssembledFeatureRow(
                feature=item,
                event_time=fact.event_time,
                available_at=fact.available_at,
                modality=fact.modality.value,
            )
        )
    input_hash = canonical_hash([item.model_dump(mode="json") for item in ordered_inputs])
    fingerprint = {
        "schema_version": ORCHESTRATION_SCHEMA_VERSION,
        "as_of": cutoff,
        "event_snapshot": snapshot.manifest.model_dump(mode="json"),
        "input_hash": input_hash,
        "row_uids": tuple(row.feature.feature_uid for row in rows),
        "excluded_uids": tuple(item.feature_uid for item in excluded),
    }
    return AsOfAssembly(
        run_uid=f"assembly:{canonical_hash(fingerprint)}",
        as_of=cutoff,
        event_snapshot=snapshot,
        input_hash=input_hash,
        rows=tuple(rows),
        excluded=tuple(excluded),
    )


def _evaluation_rows(assembly: AsOfAssembly, plan: BaselinePlan) -> tuple[EvaluationRow, ...]:
    rows: list[EvaluationRow] = []
    for assembled in assembly.rows:
        item = assembled.feature
        if item.baseline_target != plan.target or item.baseline_label is None:
            continue
        rows.append(
            EvaluationRow(
                row_uid=item.feature_uid,
                target=item.baseline_target,
                label=item.baseline_label,
                score=Decimal("0"),
                event_time=assembled.event_time,
                market_uid=item.market_uid,
                event_cluster_uid=item.event_cluster_uid,
                platform=item.platform,
                category=item.category,
                exposure_market_days=item.exposure_market_days,
                abstained=False,
                actor_visible=False,
                evidence_supported=False,
            )
        )
    return tuple(rows)


def assess_readiness(assembly: AsOfAssembly, *, plan: BaselinePlan | None) -> ReadinessReport:
    """Block when causal context, coverage, labels, or gated validation are insufficient."""

    reasons: list[str] = []
    if not assembly.event_snapshot.records:
        reasons.append("no_admissible_event_facts")
    if not assembly.rows:
        reasons.append("no_admissible_feature_rows")
    if assembly.excluded:
        reasons.append("feature_inputs_excluded_by_as_of")
    if any(row.feature.coverage_status != CoverageStatus.COMPLETE for row in assembly.rows):
        reasons.append("coverage_incomplete")
    if any(not row.feature.context_complete for row in assembly.rows):
        reasons.append("context_incomplete")
    if any(row.feature.adjudication is None for row in assembly.rows):
        reasons.append("human_labels_missing")
    if any(
        row.feature.adjudication is not None and not row.feature.adjudication.training_eligible
        for row in assembly.rows
    ):
        reasons.append("human_labels_not_eligible")
    if plan is None:
        reasons.append("evaluation_plan_missing")
        evaluation_rows: tuple[EvaluationRow, ...] = ()
        calibration_reasons: tuple[str, ...] = ()
    else:
        evaluation_rows = _evaluation_rows(assembly, plan)
        if len(evaluation_rows) != len(assembly.rows):
            reasons.append("explicit_baseline_labels_missing_or_mismatched")
        split = forward_disjoint_split(
            evaluation_rows,
            train_end=plan.train_end,
            validation_end=plan.validation_end,
        )
        if not split.train or not split.validation or not split.test:
            reasons.append("disjoint_forward_holdouts_incomplete")
        calibration_reasons = CalibrationGate().reasons(split.validation)
        if calibration_reasons:
            reasons.append("calibration_gate_not_met")
    binary_rows = sum(row.label.is_binary for row in evaluation_rows)
    unique_reasons = tuple(dict.fromkeys(reasons))
    return ReadinessReport(
        assembly_run_uid=assembly.run_uid,
        status="ready_for_candidate_evaluation" if not unique_reasons else "not_ready",
        training_allowed=not unique_reasons,
        reason_codes=unique_reasons,
        total_rows=len(assembly.rows),
        binary_label_rows=binary_rows,
        calibration_reason_codes=calibration_reasons,
    )


def _candidate(assembly: AsOfAssembly, readiness: ReadinessReport) -> BaselineCandidate:
    payload = {
        "assembly_run_uid": assembly.run_uid,
        "input_hash": assembly.input_hash,
        "readiness_status": readiness.status,
        "methods": ("naive_price_change", "naive_volume"),
    }
    return BaselineCandidate(
        candidate_uid=f"candidate:{canonical_hash(payload)}",
        assembly_run_uid=assembly.run_uid,
        status="unapproved_candidate" if readiness.training_allowed else "not_ready",
        input_hash=assembly.input_hash,
    )


def _summaries(
    split: DatasetSplit,
    feature_by_uid: dict[str, dict[str, Decimal]],
    plan: BaselinePlan,
) -> tuple[BaselineEvaluationSummary, ...]:
    """Use the existing naive baselines and calibration-gated evaluator only."""

    test_uids = {row.row_uid for row in split.test}
    scored = score_baselines((row, feature_by_uid[row.row_uid]) for row in split.test)
    summaries: list[BaselineEvaluationSummary] = []
    for method in ("naive_price_change", "naive_volume"):
        report = evaluate(
            scored[method],
            target=plan.target,
            analyst_capacity=plan.analyst_capacity,
            calibration_gate=CalibrationGate(),
            bootstrap_samples=2,
            bootstrap_seed=7,
        )
        summaries.append(
            BaselineEvaluationSummary(
                method=method,
                test_rows=len(test_uids),
                labeled_test_rows=report.labeled_rows,
                calibration_status=report.calibration_status,
                precision_at_capacity=report.precision_at_capacity,
                recall_at_capacity=report.recall_at_capacity,
                pr_auc=report.pr_auc,
                brier_score=report.brier_score,
                ece=report.ece,
            )
        )
    return tuple(summaries)


def train_baseline_candidate(assembly: AsOfAssembly, *, plan: BaselinePlan | None) -> BaselineRun:
    """Evaluate a candidate only after readiness; no weights, approval, or publication occurs."""

    readiness = assess_readiness(assembly, plan=plan)
    candidate = _candidate(assembly, readiness)
    if not readiness.training_allowed or plan is None:
        return BaselineRun(status="not_ready", assembly=assembly, readiness=readiness, candidate=candidate)
    rows = _evaluation_rows(assembly, plan)
    split = forward_disjoint_split(rows, train_end=plan.train_end, validation_end=plan.validation_end)
    feature_by_uid = {row.feature.feature_uid: row.feature.features for row in assembly.rows}
    return BaselineRun(
        status="candidate_evaluated",
        assembly=assembly,
        readiness=readiness,
        candidate=candidate,
        evaluation=_summaries(split, feature_by_uid, plan),
    )


__all__ = [
    "AsOfAssembly",
    "AssembledFeatureRow",
    "BaselineCandidate",
    "BaselineEvaluationSummary",
    "BaselinePlan",
    "BaselineRun",
    "ExcludedFeatureInput",
    "FeatureAssemblyInput",
    "ORCHESTRATION_SCHEMA_VERSION",
    "ReadinessReport",
    "assess_readiness",
    "build_as_of_assembly",
    "train_baseline_candidate",
]
