"""Read-only Phase 15 assembly, readiness, and baseline-candidate orchestration.

This layer freezes as-of inputs and makes readiness explicit.  It creates
metadata for review only: it neither writes model artifacts nor approves or
serves a candidate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
from marketleak.multimodal.schemas import (
    EventFact,
    EventMemorySnapshot,
    MarketStateSlice,
    OnChainSettlementFact,
    Phase15Model,
    Sha256,
    canonical_hash,
)


ORCHESTRATION_SCHEMA_VERSION = "15.0.0-orchestration-v5"

class NaiveBaselineFeatureSpec(Phase15Model):
    """Immutable checked-in derivation contract for the compatibility baselines."""

    schema_version: Literal["phase15-naive-baseline-feature-spec-v1"] = (
        "phase15-naive-baseline-feature-spec-v1"
    )
    source_modality: Literal["market_state"] = "market_state"
    actor_provenance_modality: Literal[
        "onchain_settlement_exact_market_outcome_with_observed_wallet"
    ] = "onchain_settlement_exact_market_outcome_with_observed_wallet"
    identity_scope: Literal[
        "FeatureAssemblyInput.market_uid+FeatureAssemblyInput.outcome_uid"
    ] = "FeatureAssemblyInput.market_uid+FeatureAssemblyInput.outcome_uid"
    source_order: tuple[
        Literal["window_starts_at"],
        Literal["window_ends_at"],
        Literal["event_uid"],
    ] = ("window_starts_at", "window_ends_at", "event_uid")
    lookback_seconds: Literal[300] = 300
    window_end_definition: Literal[
        "primary_market_state_event_time_equals_window_ends_at"
    ] = "primary_market_state_event_time_equals_window_ends_at"
    market_slice_coverage: Literal[
        "ordered_non_overlapping_contiguous_exact_fixed_window"
    ] = "ordered_non_overlapping_contiguous_exact_fixed_window"
    price_change_definition: Literal[
        "last_non_null_last_trade_price_minus_first_non_null_last_trade_price"
    ] = "last_non_null_last_trade_price_minus_first_non_null_last_trade_price"
    price_change_minimum_distinct_event_times: Literal[2] = 2
    volume_definition: Literal["sum_non_null_trade_notional"] = "sum_non_null_trade_notional"
    volume_minimum_observations: Literal[1] = 1
    feature_observed_at_definition: Literal[
        "primary_market_state_window_ends_at"
    ] = "primary_market_state_window_ends_at"
    feature_available_at_definition: Literal[
        "maximum_available_at_across_exact_window_market_slices"
    ] = "maximum_available_at_across_exact_window_market_slices"
    actor_temporal_admission: Literal[
        "event_within_fixed_window_and_available_no_later_than_market_feature"
    ] = "event_within_fixed_window_and_available_no_later_than_market_feature"


# Its canonical hash identifies derivation semantics, not a caller-selected
# list of column names.  The model and its nested values are immutable.
NAIVE_BASELINE_FEATURE_SPEC = NaiveBaselineFeatureSpec()
NAIVE_BASELINE_FEATURE_SPEC_HASH = canonical_hash(NAIVE_BASELINE_FEATURE_SPEC)


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
    outcome_uid: StableUID
    platform: NonEmptyStr
    category: NonEmptyStr
    features: dict[NonEmptyStr, Decimal]
    feature_observed_at: datetime | None = None
    feature_available_at: datetime | None = None
    source_fact_uids: tuple[StableUID, ...] = ()
    raw_artifact_uids: tuple[StableUID, ...] = ()
    feature_spec_hash: Sha256 | None = None
    bound_snapshot_uid: StableUID | None = None
    bound_as_of: datetime | None = None
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

    @field_validator("feature_observed_at", "feature_available_at", "bound_as_of")
    @classmethod
    def validate_optional_times(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @field_validator("source_fact_uids", "raw_artifact_uids")
    @classmethod
    def validate_unique_lineage(cls, value: tuple[str, ...], info) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError(f"{info.field_name} must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_human_mapping(self) -> "FeatureAssemblyInput":
        if (self.baseline_target is None) != (self.baseline_label is None):
            raise ValueError("baseline_target and baseline_label must be supplied together")
        if self.adjudication is not None and self.adjudication.event_uid != self.event_uid:
            raise ValueError("adjudication.event_uid must match feature event_uid")
        if (
            self.feature_observed_at is not None
            and self.feature_available_at is not None
            and self.feature_available_at < self.feature_observed_at
        ):
            raise ValueError("feature_available_at cannot precede feature_observed_at")
        return self


class BaselinePlan(Phase15Model):
    """Fixed forward-time boundaries; callers may not infer them from outcomes."""

    target: LabelTarget
    train_end: datetime
    validation_end: datetime
    analyst_capacity: int = Field(default=25, strict=True, ge=1)
    validation_min_labeled: int = Field(default=100, strict=True, ge=2)
    validation_min_positive: int = Field(default=20, strict=True, ge=1)
    validation_min_negative: int = Field(default=20, strict=True, ge=1)
    test_min_labeled: int = Field(default=100, strict=True, ge=2)
    test_min_positive: int = Field(default=20, strict=True, ge=1)
    test_min_negative: int = Field(default=20, strict=True, ge=1)

    @field_validator("train_end", "validation_end")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_order(self) -> "BaselinePlan":
        if self.validation_end <= self.train_end:
            raise ValueError("validation_end must be after train_end")
        if self.validation_min_labeled < self.validation_min_positive + self.validation_min_negative:
            raise ValueError("validation_min_labeled must cover positive and negative minimums")
        if self.test_min_labeled < self.test_min_positive + self.test_min_negative:
            raise ValueError("test_min_labeled must cover positive and negative minimums")
        return self

    def validation_gate(self) -> CalibrationGate:
        return CalibrationGate(
            min_labeled=self.validation_min_labeled,
            min_positive=self.validation_min_positive,
            min_negative=self.validation_min_negative,
        )

    def test_gate(self) -> CalibrationGate:
        return CalibrationGate(
            min_labeled=self.test_min_labeled,
            min_positive=self.test_min_positive,
            min_negative=self.test_min_negative,
        )


@dataclass(frozen=True, slots=True)
class _DerivedNaiveFeatures:
    values: dict[str, Decimal]
    window_starts_at: datetime
    window_ends_at: datetime
    observed_at: datetime
    available_at: datetime
    actor_uids: tuple[str, ...]


def _derive_naive_features(
    item: FeatureAssemblyInput,
    source_facts: tuple[EventFact, ...],
) -> tuple[_DerivedNaiveFeatures | None, str | None]:
    """Derive the checked-in naive feature contract from frozen source facts."""

    primary_fact = next(fact for fact in source_facts if fact.event_uid == item.event_uid)
    if not isinstance(primary_fact, MarketStateSlice):
        return None, "primary_event_not_market_state"
    if primary_fact.event_time != primary_fact.window_ends_at:
        return None, "primary_event_not_at_window_end"
    window_ends_at = primary_fact.window_ends_at
    window_starts_at = window_ends_at - timedelta(seconds=NAIVE_BASELINE_FEATURE_SPEC.lookback_seconds)
    market_facts = tuple(
        sorted(
            (
                fact
                for fact in source_facts
                if isinstance(fact, MarketStateSlice)
                and fact.market_uid == item.market_uid
                and fact.outcome_uid == item.outcome_uid
            ),
            key=lambda fact: (fact.window_starts_at, fact.window_ends_at, fact.event_uid),
        )
    )
    if not market_facts or market_facts[-1].event_uid != primary_fact.event_uid:
        return None, "primary_event_not_latest_market_slice"
    if any(fact.event_time != fact.window_ends_at for fact in market_facts):
        return None, "market_slice_event_time_mismatch"
    if any(
        fact.window_starts_at < window_starts_at or fact.window_ends_at > window_ends_at
        for fact in market_facts
    ):
        return None, "market_slice_outside_fixed_window"
    if market_facts[0].window_starts_at != window_starts_at:
        return None, "market_window_coverage_incomplete"
    for previous, current in zip(market_facts, market_facts[1:]):
        if current.window_starts_at < previous.window_ends_at:
            return None, "market_slice_overlap"
        if current.window_starts_at > previous.window_ends_at:
            return None, "market_slice_gap"
    if market_facts[-1].window_ends_at != window_ends_at:
        return None, "market_window_coverage_incomplete"
    price_points = tuple(fact for fact in market_facts if fact.last_trade_price is not None)
    if (
        len(price_points) < 2
        or len({fact.event_time for fact in price_points}) < 2
    ):
        return None, "required_price_change_unavailable"
    volume_points = tuple(fact.trade_notional for fact in market_facts if fact.trade_notional is not None)
    if not volume_points:
        return None, "required_volume_unavailable"
    first_price = price_points[0].last_trade_price
    last_price = price_points[-1].last_trade_price
    assert first_price is not None and last_price is not None
    values = {
        "price_change": last_price - first_price,
        "volume": sum(volume_points, Decimal("0")),
    }
    market_available_at = max(fact.available_at for fact in market_facts)
    actor_facts = tuple(fact for fact in source_facts if isinstance(fact, OnChainSettlementFact))
    if any(
        fact.event_time < window_starts_at or fact.event_time > window_ends_at
        for fact in actor_facts
    ):
        return None, "actor_fact_outside_feature_window"
    if any(fact.available_at > market_available_at for fact in actor_facts):
        return None, "actor_fact_available_after_feature"
    actors = tuple(
        sorted(
            {
                actor_uid
                for fact in source_facts
                if isinstance(fact, OnChainSettlementFact)
                and fact.market_uid == item.market_uid
                and fact.outcome_uid == item.outcome_uid
                and (actor_uid := fact.observed_wallet_uid) is not None
            }
        )
    )
    return (
        _DerivedNaiveFeatures(
            values=dict(sorted(values.items())),
            window_starts_at=window_starts_at,
            window_ends_at=window_ends_at,
            observed_at=window_ends_at,
            available_at=market_available_at,
            actor_uids=actors,
        ),
        None,
    )


class AssembledFeatureRow(Phase15Model):
    feature: FeatureAssemblyInput
    derived_features: dict[NonEmptyStr, Decimal]
    window_starts_at: datetime
    window_ends_at: datetime
    event_time: datetime
    available_at: datetime
    modality: NonEmptyStr
    actor_uids: tuple[StableUID, ...] = ()

    @field_validator("window_starts_at", "window_ends_at", "event_time", "available_at")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @field_validator("derived_features")
    @classmethod
    def validate_derived_features(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        if not value:
            raise ValueError("derived_features must not be empty")
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def validate_window(self) -> "AssembledFeatureRow":
        if self.window_ends_at <= self.window_starts_at:
            raise ValueError("assembled feature window must have positive duration")
        if self.event_time != self.window_ends_at:
            raise ValueError("assembled feature event_time must equal window_ends_at")
        return self


class ExcludedFeatureInput(Phase15Model):
    feature_uid: StableUID
    event_uid: StableUID
    feature: FeatureAssemblyInput
    reason: NonEmptyStr

    @model_validator(mode="after")
    def validate_feature_identity(self) -> "ExcludedFeatureInput":
        if self.feature_uid != self.feature.feature_uid or self.event_uid != self.feature.event_uid:
            raise ValueError("excluded feature IDs must match the retained caller payload")
        return self


def _canonical_assembly_payload(
    rows: Iterable[AssembledFeatureRow],
    excluded: Iterable[ExcludedFeatureInput],
) -> dict[str, object]:
    """Canonical payload available to both the builder and direct validator."""

    ordered_rows = sorted(rows, key=lambda row: (row.feature.feature_uid, row.feature.event_uid))
    ordered_excluded = sorted(excluded, key=lambda item: (item.feature_uid, item.event_uid, item.reason))
    return {
        "accepted_rows": [row.model_dump(mode="json") for row in ordered_rows],
        "excluded_inputs": [item.model_dump(mode="json") for item in ordered_excluded],
    }


def _assembly_input_hash(
    rows: Iterable[AssembledFeatureRow],
    excluded: Iterable[ExcludedFeatureInput],
) -> str:
    return canonical_hash(_canonical_assembly_payload(rows, excluded))


def _assembly_fingerprint(
    *,
    as_of: datetime,
    event_snapshot: EventMemorySnapshot,
    feature_spec_hash: str | None,
    input_hash: str,
    rows: Iterable[AssembledFeatureRow],
    excluded: Iterable[ExcludedFeatureInput],
) -> dict[str, object]:
    ordered_rows = sorted(rows, key=lambda row: (row.feature.feature_uid, row.feature.event_uid))
    ordered_excluded = sorted(excluded, key=lambda item: (item.feature_uid, item.event_uid, item.reason))
    return {
        "schema_version": ORCHESTRATION_SCHEMA_VERSION,
        "as_of": as_of,
        "event_snapshot": event_snapshot.manifest.model_dump(mode="json"),
        "feature_spec_hash": feature_spec_hash,
        "input_hash": input_hash,
        "row_uids": tuple((row.feature.feature_uid, row.feature.event_uid) for row in ordered_rows),
        "excluded_uids": tuple((item.feature_uid, item.event_uid) for item in ordered_excluded),
    }


def _assembly_run_uid(
    *,
    as_of: datetime,
    event_snapshot: EventMemorySnapshot,
    feature_spec_hash: str | None,
    input_hash: str,
    rows: Iterable[AssembledFeatureRow],
    excluded: Iterable[ExcludedFeatureInput],
) -> str:
    fingerprint = _assembly_fingerprint(
        as_of=as_of,
        event_snapshot=event_snapshot,
        feature_spec_hash=feature_spec_hash,
        input_hash=input_hash,
        rows=rows,
        excluded=excluded,
    )
    return f"assembly:{canonical_hash(fingerprint)}"


class AsOfAssembly(Phase15Model):
    """Frozen causal selection of event facts and caller-provided feature rows."""

    schema_version: Literal["15.0.0-orchestration-v5"] = ORCHESTRATION_SCHEMA_VERSION
    run_uid: StableUID
    as_of: datetime
    event_snapshot: EventMemorySnapshot
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feature_spec_hash: Sha256 | None = None
    rows: tuple[AssembledFeatureRow, ...]
    excluded: tuple[ExcludedFeatureInput, ...]

    @field_validator("as_of")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")

    @model_validator(mode="after")
    def validate_as_of(self) -> "AsOfAssembly":
        if self.event_snapshot.manifest.as_of != self.as_of:
            raise ValueError("event snapshot cutoff must exactly match assembly as_of")
        if any(row.event_time > self.as_of or row.available_at > self.as_of for row in self.rows):
            raise ValueError("assembly rows cannot use future or later-available event facts")
        row_uids = tuple(row.feature.feature_uid for row in self.rows)
        if len(set(row_uids)) != len(row_uids):
            raise ValueError("assembly row feature_uids must be unique")
        excluded_uids = tuple(item.feature_uid for item in self.excluded)
        if len(set(excluded_uids)) != len(excluded_uids):
            raise ValueError("assembly excluded feature_uids must be unique")
        if set(row_uids) & set(excluded_uids):
            raise ValueError("a feature_uid cannot be both accepted and excluded")
        if self.rows and self.feature_spec_hash != NAIVE_BASELINE_FEATURE_SPEC_HASH:
            raise ValueError("assembly rows require the checked-in naive feature spec")
        facts = {record.event_uid: record for record in self.event_snapshot.records}
        for row in self.rows:
            reason, derived = _validated_feature_derivation(
                row.feature,
                snapshot=self.event_snapshot,
                facts=facts,
                cutoff=self.as_of,
            )
            if reason is not None:
                raise ValueError(f"assembly row is not causally bound: {reason}")
            assert derived is not None
            if row.derived_features != derived.values:
                raise ValueError("assembly derived_features do not match frozen source facts")
            if (
                row.window_starts_at != derived.window_starts_at
                or row.window_ends_at != derived.window_ends_at
            ):
                raise ValueError("assembly feature window does not match frozen source facts")
            if row.event_time != derived.observed_at:
                raise ValueError("assembly event_time must equal the derived observation clock")
            if row.available_at != derived.available_at:
                raise ValueError("assembly available_at must equal the derived availability clock")
            primary_fact = facts[row.feature.event_uid]
            if row.modality != primary_fact.modality.value:
                raise ValueError("assembly modality must match the primary source fact")
            if row.actor_uids != derived.actor_uids:
                raise ValueError("assembly actor_uids must be derived from source fact provenance")
        expected_input_hash = _assembly_input_hash(self.rows, self.excluded)
        if self.input_hash != expected_input_hash:
            raise ValueError("assembly input_hash does not match accepted and excluded payloads")
        expected_run_uid = _assembly_run_uid(
            as_of=self.as_of,
            event_snapshot=self.event_snapshot,
            feature_spec_hash=self.feature_spec_hash,
            input_hash=self.input_hash,
            rows=self.rows,
            excluded=self.excluded,
        )
        if self.run_uid != expected_run_uid:
            raise ValueError("assembly run_uid does not match its canonical fingerprint")
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
    test_reason_codes: tuple[NonEmptyStr, ...] = ()
    validation_rows: int = Field(default=0, ge=0)
    validation_labeled_rows: int = Field(default=0, ge=0)
    validation_positive_rows: int = Field(default=0, ge=0)
    validation_negative_rows: int = Field(default=0, ge=0)
    test_rows: int = Field(default=0, ge=0)
    test_labeled_rows: int = Field(default=0, ge=0)
    test_positive_rows: int = Field(default=0, ge=0)
    test_negative_rows: int = Field(default=0, ge=0)
    dropped_cross_boundary_rows: int = Field(default=0, ge=0)


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
    positive_test_rows: int = Field(ge=0)
    negative_test_rows: int = Field(ge=0)
    unknown_test_rows: int = Field(ge=0)
    unmapped_test_rows: int = Field(ge=0)
    effectiveness_status: NonEmptyStr
    effectiveness_reason: NonEmptyStr
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
    unique_inputs: dict[str, FeatureAssemblyInput] = {}
    for item in inputs:
        previous = unique_inputs.get(item.feature_uid)
        if previous is None:
            unique_inputs[item.feature_uid] = item
        elif previous != item:
            raise ValueError(f"conflicting duplicate feature_uid: {item.feature_uid}")
    ordered_inputs = tuple(sorted(unique_inputs.values(), key=lambda item: item.feature_uid))
    rows: list[AssembledFeatureRow] = []
    excluded: list[ExcludedFeatureInput] = []
    for item in ordered_inputs:
        fact = facts.get(item.event_uid)
        if fact is None:
            excluded.append(
                ExcludedFeatureInput(
                    feature_uid=item.feature_uid,
                    event_uid=item.event_uid,
                    feature=item,
                    reason="event_not_admissible_as_of",
                )
            )
            continue
        reason, derived = _validated_feature_derivation(
            item,
            snapshot=snapshot,
            facts=facts,
            cutoff=cutoff,
        )
        if reason is not None:
            excluded.append(
                ExcludedFeatureInput(
                    feature_uid=item.feature_uid,
                    event_uid=item.event_uid,
                    feature=item,
                    reason=reason,
                )
            )
            continue
        assert derived is not None
        rows.append(
            AssembledFeatureRow(
                feature=item,
                derived_features=derived.values,
                window_starts_at=derived.window_starts_at,
                window_ends_at=derived.window_ends_at,
                event_time=derived.observed_at,
                available_at=derived.available_at,
                modality=fact.modality.value,
                actor_uids=derived.actor_uids,
            )
        )
    frozen_rows = tuple(rows)
    frozen_excluded = tuple(excluded)
    feature_spec_hash = NAIVE_BASELINE_FEATURE_SPEC_HASH if frozen_rows else None
    input_hash = _assembly_input_hash(frozen_rows, frozen_excluded)
    run_uid = _assembly_run_uid(
        as_of=cutoff,
        event_snapshot=snapshot,
        feature_spec_hash=feature_spec_hash,
        input_hash=input_hash,
        rows=frozen_rows,
        excluded=frozen_excluded,
    )
    return AsOfAssembly(
        run_uid=run_uid,
        as_of=cutoff,
        event_snapshot=snapshot,
        input_hash=input_hash,
        feature_spec_hash=feature_spec_hash,
        rows=frozen_rows,
        excluded=frozen_excluded,
    )


def _validated_feature_derivation(
    item: FeatureAssemblyInput,
    *,
    snapshot: EventMemorySnapshot,
    facts: dict[str, EventFact],
    cutoff: datetime,
) -> tuple[str | None, _DerivedNaiveFeatures | None]:
    """Validate declarations and recompute the only admitted feature contract."""

    if item.feature_observed_at is None:
        return "feature_observation_time_missing", None
    if item.feature_available_at is None:
        return "feature_availability_time_missing", None
    if not item.source_fact_uids:
        return "source_fact_lineage_missing", None
    if not item.raw_artifact_uids:
        return "raw_artifact_lineage_missing", None
    if item.feature_spec_hash is None:
        return "feature_spec_hash_missing", None
    if item.feature_spec_hash != NAIVE_BASELINE_FEATURE_SPEC_HASH:
        return "feature_spec_hash_unknown", None
    if item.bound_snapshot_uid is None:
        return "feature_snapshot_binding_missing", None
    if item.bound_as_of is None:
        return "feature_cutoff_binding_missing", None
    if item.feature_observed_at > cutoff:
        return "feature_observed_after_as_of", None
    if item.feature_available_at > cutoff:
        return "feature_available_after_as_of", None
    if item.bound_as_of != cutoff:
        return "feature_cutoff_mismatch", None
    if item.bound_snapshot_uid != snapshot.manifest.snapshot_uid:
        return "feature_snapshot_mismatch", None
    if item.event_uid not in item.source_fact_uids:
        return "feature_event_not_in_source_facts", None
    if any(uid not in facts for uid in item.source_fact_uids):
        return "source_fact_not_admissible_as_of", None
    source_facts = tuple(facts[uid] for uid in item.source_fact_uids)
    if any(
        not isinstance(fact, (MarketStateSlice, OnChainSettlementFact))
        for fact in source_facts
    ):
        return "source_fact_modality_unsupported", None
    event_fact = facts[item.event_uid]
    if (
        getattr(event_fact, "market_uid", None) != item.market_uid
        or getattr(event_fact, "outcome_uid", None) != item.outcome_uid
    ):
        return "event_fact_identity_mismatch", None
    if any(
        fact.market_uid != item.market_uid or fact.outcome_uid != item.outcome_uid
        for fact in source_facts
        if isinstance(fact, MarketStateSlice)
    ):
        return "market_fact_identity_mismatch", None
    actor_facts = tuple(fact for fact in source_facts if isinstance(fact, OnChainSettlementFact))
    if any(
        fact.market_uid != item.market_uid or fact.outcome_uid != item.outcome_uid
        for fact in actor_facts
    ):
        return "actor_fact_identity_mismatch", None
    if any(fact.observed_wallet_uid is None for fact in actor_facts):
        return "actor_fact_identifier_missing", None
    expected_raw_uids = {source_fact.provenance.raw_artifact_uid for source_fact in source_facts}
    if set(item.raw_artifact_uids) != expected_raw_uids:
        return "raw_artifact_lineage_mismatch", None
    derived, reason = _derive_naive_features(item, source_facts)
    if reason is not None:
        return reason, None
    assert derived is not None
    if item.feature_observed_at != derived.observed_at:
        return "feature_observation_time_mismatch", None
    if item.feature_available_at != derived.available_at:
        return "feature_availability_time_mismatch", None
    if item.features != derived.values:
        return "feature_values_mismatch", None
    return None, derived


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
                actor_uids=assembled.actor_uids,
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
        test_reasons: tuple[str, ...] = ()
        split = DatasetSplit(train=(), validation=(), test=(), dropped_cross_boundary=())
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
        if plan.target == LabelTarget.ACTOR_EVIDENCE_C and any(
            not row.actor_uids for row in evaluation_rows
        ):
            reasons.append("actor_identifiers_missing_for_actor_target")
        calibration_reasons = plan.validation_gate().reasons(split.validation)
        if calibration_reasons:
            reasons.extend(("validation_label_support_gate_not_met", "calibration_gate_not_met"))
        test_reasons = plan.test_gate().reasons(split.test)
        if test_reasons:
            reasons.append("test_label_support_gate_not_met")
    binary_rows = sum(row.label.is_binary for row in evaluation_rows)
    validation_binary = tuple(row for row in split.validation if row.label.is_binary)
    test_binary = tuple(row for row in split.test if row.label.is_binary)
    unique_reasons = tuple(dict.fromkeys(reasons))
    return ReadinessReport(
        assembly_run_uid=assembly.run_uid,
        status="ready_for_candidate_evaluation" if not unique_reasons else "not_ready",
        training_allowed=not unique_reasons,
        reason_codes=unique_reasons,
        total_rows=len(assembly.rows),
        binary_label_rows=binary_rows,
        calibration_reason_codes=calibration_reasons,
        test_reason_codes=test_reasons,
        validation_rows=len(split.validation),
        validation_labeled_rows=len(validation_binary),
        validation_positive_rows=sum(row.label == LabelValue.POSITIVE for row in validation_binary),
        validation_negative_rows=sum(row.label == LabelValue.NEGATIVE for row in validation_binary),
        test_rows=len(split.test),
        test_labeled_rows=len(test_binary),
        test_positive_rows=sum(row.label == LabelValue.POSITIVE for row in test_binary),
        test_negative_rows=sum(row.label == LabelValue.NEGATIVE for row in test_binary),
        dropped_cross_boundary_rows=len(split.dropped_cross_boundary),
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
            calibration_gate=plan.test_gate(),
            bootstrap_samples=2,
            bootstrap_seed=7,
        )
        summaries.append(
            BaselineEvaluationSummary(
                method=method,
                test_rows=len(test_uids),
                labeled_test_rows=report.labeled_rows,
                positive_test_rows=report.positive_rows,
                negative_test_rows=report.negative_rows,
                unknown_test_rows=report.unknown_rows,
                unmapped_test_rows=report.unmapped_rows,
                effectiveness_status=report.effectiveness_status,
                effectiveness_reason=report.effectiveness_reason,
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
    feature_by_uid = {row.feature.feature_uid: row.derived_features for row in assembly.rows}
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
    "NAIVE_BASELINE_FEATURE_SPEC",
    "NAIVE_BASELINE_FEATURE_SPEC_HASH",
    "NaiveBaselineFeatureSpec",
    "ORCHESTRATION_SCHEMA_VERSION",
    "ReadinessReport",
    "assess_readiness",
    "build_as_of_assembly",
    "train_baseline_candidate",
]
