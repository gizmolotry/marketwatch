"""Deterministic population-wide analyst review-priority queues.

This layer allocates a declared human-review budget over a *complete* wallet
cohort report.  It preserves abstentions and missing longitudinal evidence and
never interprets a rank as a probability, identity claim, or legal conclusion.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Iterable, Literal

from marketleak.actors.wallet_cohort import CohortRankingReport, RankedWallet
from marketleak.domain import CoverageStatus
from marketleak.ingestion.normalize import canonical_json_bytes

if TYPE_CHECKING:
    from marketleak.wallet_tracking import WalletBehaviorOutput, WalletHistorySnapshot


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: datetime) -> str:
    return _utc(value, "datetime").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _digest(value: object) -> str:
    return sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class WalletPriorityPolicy:
    as_of: datetime
    available_at: datetime
    frozen_at: datetime
    mode: Literal["operational_shadow", "hindsight_descriptive"]
    declared_population_size: int
    analyst_budget: int
    require_longitudinal: bool = False
    use_longitudinal_tiebreaker: bool = True
    longitudinal_method_uid: str | None = None
    longitudinal_source_scope_uid: str | None = None
    longitudinal_source_uids: tuple[str, ...] = ()
    history_window_starts_at: datetime | None = None
    history_window_ends_at: datetime | None = None
    policy_uid: str = field(init=False)

    def __post_init__(self) -> None:
        cutoff = _utc(self.as_of, "priority as_of")
        available = _utc(self.available_at, "priority available_at")
        frozen = _utc(self.frozen_at, "priority frozen_at")
        object.__setattr__(self, "as_of", cutoff)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "frozen_at", frozen)
        history_start = None if self.history_window_starts_at is None else _utc(
            self.history_window_starts_at, "history_window_starts_at"
        )
        history_end = None if self.history_window_ends_at is None else _utc(
            self.history_window_ends_at, "history_window_ends_at"
        )
        object.__setattr__(self, "history_window_starts_at", history_start)
        object.__setattr__(self, "history_window_ends_at", history_end)
        source_uids = tuple(str(item).strip() for item in self.longitudinal_source_uids)
        if any(not item for item in source_uids):
            raise ValueError("longitudinal_source_uids must contain non-empty values")
        if len(source_uids) != len(set(source_uids)):
            raise ValueError("longitudinal_source_uids must contain distinct values")
        object.__setattr__(self, "longitudinal_source_uids", tuple(sorted(source_uids)))
        if self.declared_population_size < 1:
            raise ValueError("declared_population_size must be positive")
        if self.analyst_budget < 1 or self.analyst_budget > self.declared_population_size:
            raise ValueError("analyst_budget must be within the declared population")
        if self.mode == "operational_shadow":
            if available > cutoff or frozen > available:
                raise ValueError("operational shadow policy must be available by as_of and frozen by available_at")
        elif self.mode == "hindsight_descriptive":
            if available <= cutoff:
                raise ValueError("hindsight descriptive policy requires availability later than as_of")
            if frozen > available:
                raise ValueError("hindsight priority policy must be frozen by available_at")
        else:  # pragma: no cover - runtime guard beyond Literal typing
            raise ValueError("unsupported wallet priority mode")
        if self.require_longitudinal or self.use_longitudinal_tiebreaker:
            if not self.longitudinal_method_uid or not self.longitudinal_method_uid.strip():
                raise ValueError("longitudinal_method_uid is required when longitudinal ranking is enabled")
            if not self.longitudinal_source_scope_uid or not self.longitudinal_source_scope_uid.strip():
                raise ValueError("longitudinal_source_scope_uid is required when longitudinal ranking is enabled")
            if not source_uids:
                raise ValueError("longitudinal_source_uids is required when longitudinal ranking is enabled")
            if history_start is None or history_end is None:
                raise ValueError("exact history window is required when longitudinal ranking is enabled")
        if (history_start is None) != (history_end is None):
            raise ValueError("history window endpoints must be supplied together")
        if history_start is not None and history_end is not None:
            if history_end <= history_start:
                raise ValueError("history window must have positive duration")
            if history_end > cutoff:
                raise ValueError("history window cannot extend beyond priority as_of")
        object.__setattr__(self, "policy_uid", f"wallet-priority-policy:{_digest(self.to_payload(include_uid=False))}")

    def to_payload(self, *, include_uid: bool = True) -> dict[str, Any]:
        payload = {
            "as_of": _time(self.as_of),
            "available_at": _time(self.available_at),
            "frozen_at": _time(self.frozen_at),
            "mode": self.mode,
            "declared_population_size": self.declared_population_size,
            "analyst_budget": self.analyst_budget,
            "require_longitudinal": self.require_longitudinal,
            "use_longitudinal_tiebreaker": self.use_longitudinal_tiebreaker,
            "longitudinal_method_uid": self.longitudinal_method_uid,
            "longitudinal_source_scope_uid": self.longitudinal_source_scope_uid,
            "longitudinal_source_uids": list(self.longitudinal_source_uids),
            "history_window_starts_at": None if self.history_window_starts_at is None else _time(self.history_window_starts_at),
            "history_window_ends_at": None if self.history_window_ends_at is None else _time(self.history_window_ends_at),
        }
        return {"policy_uid": self.policy_uid, **payload} if include_uid else payload


@dataclass(frozen=True, slots=True)
class LongitudinalPriorityInput:
    """Immutable binding between one behavior payload and its exact snapshot."""

    behavior_output: "WalletBehaviorOutput"
    history_snapshot: "WalletHistorySnapshot"
    actor_uid: str = field(init=False)
    behavior_output_sha256: str = field(init=False)
    history_snapshot_uid: str = field(init=False)
    history_snapshot_sha256: str = field(init=False)
    binding_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        # Kept local so importing ``marketleak.wallet_tracking`` does not cycle
        # through the actors package back into this module while it initializes.
        from marketleak.wallet_tracking import WalletBehaviorOutput, WalletHistorySnapshot

        output = self.behavior_output
        snapshot = self.history_snapshot
        if not isinstance(output, WalletBehaviorOutput):
            raise TypeError("behavior_output must be a WalletBehaviorOutput")
        if not isinstance(snapshot, WalletHistorySnapshot):
            raise TypeError("history_snapshot must be a WalletHistorySnapshot")
        if output.actor_uid != snapshot.actor_uid:
            raise ValueError("longitudinal binding actor mismatch")
        if output.registration_uid != snapshot.registration_uid:
            raise ValueError("longitudinal binding registration mismatch")
        if output.as_of != snapshot.as_of or output.available_at != snapshot.available_at:
            raise ValueError("longitudinal binding clock mismatch")
        if output.background_cohort != snapshot.background_cohort:
            raise ValueError("longitudinal binding background cohort mismatch")
        output_hash = _digest(output)
        snapshot_hash = _digest(snapshot)
        object.__setattr__(self, "actor_uid", output.actor_uid)
        object.__setattr__(self, "behavior_output_sha256", output_hash)
        object.__setattr__(self, "history_snapshot_uid", snapshot.snapshot_uid)
        object.__setattr__(self, "history_snapshot_sha256", snapshot_hash)
        object.__setattr__(
            self,
            "binding_sha256",
            _digest(
                {
                    "actor_uid": output.actor_uid,
                    "registration_uid": output.registration_uid,
                    "behavior_output_sha256": output_hash,
                    "history_snapshot_uid": snapshot.snapshot_uid,
                    "history_snapshot_sha256": snapshot_hash,
                }
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "actor_uid": self.actor_uid,
            "registration_uid": self.behavior_output.registration_uid,
            "behavior_output_sha256": self.behavior_output_sha256,
            "history_snapshot_uid": self.history_snapshot_uid,
            "history_snapshot_sha256": self.history_snapshot_sha256,
            "binding_sha256": self.binding_sha256,
        }


@dataclass(frozen=True, slots=True)
class WalletPriorityRow:
    actor_uid: str
    status: Literal["eligible", "abstain"]
    abstention_reasons: tuple[str, ...]
    cohort_population_rank: int | None
    cohort_population_size: int | None
    longitudinal_status: Literal["available", "missing", "unavailable", "ineligible"]
    longitudinal_reasons: tuple[str, ...]
    longitudinal_review_priority: Decimal | None
    longitudinal_output_sha256: str | None
    history_snapshot_sha256: str | None
    longitudinal_contributed_to_rank: bool = False
    priority_rank: int | None = None
    tie_group: int | None = None
    display_order: int | None = None
    selected_for_review: bool = False
    is_probability: Literal[False] = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "actor_uid": self.actor_uid,
            "status": self.status,
            "abstention_reasons": list(self.abstention_reasons),
            "cohort_population_rank": self.cohort_population_rank,
            "cohort_population_size": self.cohort_population_size,
            "longitudinal_status": self.longitudinal_status,
            "longitudinal_reasons": list(self.longitudinal_reasons),
            "longitudinal_review_priority": _decimal(self.longitudinal_review_priority),
            "longitudinal_output_sha256": self.longitudinal_output_sha256,
            "history_snapshot_sha256": self.history_snapshot_sha256,
            "longitudinal_contributed_to_rank": self.longitudinal_contributed_to_rank,
            "priority_rank": self.priority_rank,
            "tie_group": self.tie_group,
            "display_order": self.display_order,
            "selected_for_review": self.selected_for_review,
            "is_probability": self.is_probability,
        }


@dataclass(frozen=True, slots=True)
class WalletPriorityQueue:
    policy: WalletPriorityPolicy
    status: Literal["available", "abstain"]
    abstention_reasons: tuple[str, ...]
    cohort_report_sha256: str
    population_actor_set_sha256: str
    longitudinal_input_sha256: str
    rows: tuple[WalletPriorityRow, ...]
    selected_count: int
    selection_boundary_actor_uid: str | None
    selection_boundary_priority_rank: int | None
    prospective_shadow_eligible: bool
    descriptive_only: bool
    effectiveness_unknown: Literal[True] = True
    not_training: Literal[True] = True
    human_review_required: Literal[True] = True
    limitations: tuple[str, ...] = (
        "Review priority is a deterministic analyst-allocation rank, not a probability or finding of misconduct.",
        "Public wallet identifiers are pseudonymous and do not establish identity or common control.",
        "Missing longitudinal evidence remains explicit and is never represented as zero activity.",
    )

    @property
    def queue_sha256(self) -> str:
        return _digest(self._unsigned_payload())

    def _unsigned_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "wallet-priority-queue-v1",
            "policy": self.policy.to_payload(),
            "status": self.status,
            "abstention_reasons": list(self.abstention_reasons),
            "cohort_report_sha256": self.cohort_report_sha256,
            "population_actor_set_sha256": self.population_actor_set_sha256,
            "longitudinal_input_sha256": self.longitudinal_input_sha256,
            "rows": [row.to_payload() for row in self.rows],
            "selected_count": self.selected_count,
            "selection_boundary_actor_uid": self.selection_boundary_actor_uid,
            "selection_boundary_priority_rank": self.selection_boundary_priority_rank,
            "prospective_shadow_eligible": self.prospective_shadow_eligible,
            "descriptive_only": self.descriptive_only,
            "effectiveness_unknown": self.effectiveness_unknown,
            "not_training": self.not_training,
            "human_review_required": self.human_review_required,
            "limitations": list(self.limitations),
        }

    def to_payload(self) -> dict[str, Any]:
        return {**self._unsigned_payload(), "queue_sha256": self.queue_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_payload())


def _review_metric(
    output: "WalletBehaviorOutput",
    *,
    expected_method_uid: str | None,
) -> tuple[Decimal | None, str]:
    metric = next((item for item in output.metrics if item.name == "review_priority"), None)
    if metric is None or metric.status != "available" or metric.value is None:
        return None, "unavailable"
    if metric.method_uid != expected_method_uid:
        return None, "method_mismatch"
    return metric.value, "available"


def _validate_longitudinal_lineage(
    *,
    snapshot: "WalletHistorySnapshot",
    policy: WalletPriorityPolicy,
) -> tuple[str, ...]:
    """Independently re-check causal lineage before admitting a metric.

    ``WalletHistorySnapshot`` is an immutable transport object, but callers can
    still construct it directly.  Priority admission therefore cannot rely on
    the snapshot builder having enforced its filters.  ``FillLineage`` does not
    carry actor/visibility or market fields, so those dimensions are bound by
    the snapshot/output actor equality and frozen source scope checks elsewhere;
    the source UID is the strongest independently checkable source assertion.
    """

    reasons: set[str] = set()
    lineage_uids = tuple(item.fill_uid for item in snapshot.fill_lineage)
    trigger_uids = tuple(snapshot.trigger_eligible_fill_uids)
    follow_up_uids = tuple(snapshot.follow_up_only_fill_uids)
    lineage_set = set(lineage_uids)
    trigger_set = set(trigger_uids)
    follow_up_set = set(follow_up_uids)

    if len(lineage_uids) != len(lineage_set):
        reasons.add("longitudinal_lineage_fill_uid_duplicate")
    if (
        len(trigger_uids) != len(trigger_set)
        or len(follow_up_uids) != len(follow_up_set)
        or bool(trigger_set & follow_up_set)
        or trigger_set | follow_up_set != lineage_set
    ):
        reasons.add("longitudinal_lineage_role_partition_invalid")

    for item in snapshot.fill_lineage:
        if item.event_time > snapshot.as_of:
            reasons.add("longitudinal_fill_event_after_snapshot_cutoff")
        if item.event_time > policy.as_of:
            reasons.add("longitudinal_fill_event_after_priority_cutoff")
        if item.available_at > snapshot.available_at:
            reasons.add("longitudinal_fill_available_after_snapshot_cutoff")
        if item.available_at > policy.available_at:
            reasons.add("longitudinal_fill_available_after_priority_cutoff")
        if not snapshot.window_starts_at <= item.event_time <= snapshot.window_ends_at:
            reasons.add("longitudinal_fill_outside_history_window")
        if item.source_uid not in snapshot.source_scope.source_uids:
            reasons.add("longitudinal_fill_source_not_permitted")
        if item.source_uid not in policy.longitudinal_source_uids:
            reasons.add("longitudinal_fill_source_not_frozen")

        if item.fill_uid in trigger_set:
            if item.event_time > snapshot.as_of or item.event_time > policy.as_of:
                reasons.add("longitudinal_trigger_role_event_after_cutoff")
            if item.available_at > snapshot.available_at or item.available_at > policy.available_at:
                reasons.add("longitudinal_trigger_role_available_after_cutoff")

    if policy.mode == "operational_shadow" and follow_up_set:
        reasons.add("operational_shadow_rejects_follow_up_only")
    return tuple(sorted(reasons))


def _longitudinal_evidence(
    *,
    actor_uid: str,
    binding: LongitudinalPriorityInput | None,
    policy: WalletPriorityPolicy,
    conflicted: bool,
) -> tuple[
    Literal["available", "missing", "unavailable", "ineligible"],
    tuple[str, ...],
    Decimal | None,
]:
    """Admit a longitudinal metric only with its exact complete snapshot."""

    reasons: set[str] = set()
    if conflicted:
        reasons.add("longitudinal_input_conflict")
        return "ineligible", tuple(sorted(reasons)), None
    if binding is None:
        reasons.add("longitudinal_binding_missing")
        return "missing", tuple(sorted(reasons)), None

    output = binding.behavior_output
    snapshot = binding.history_snapshot
    ineligible = False
    if output.actor_uid != actor_uid or snapshot.actor_uid != actor_uid:
        reasons.add("longitudinal_actor_mismatch")
        ineligible = True
    if (
        output.registration_uid != snapshot.registration_uid
        or output.as_of != snapshot.as_of
        or output.available_at != snapshot.available_at
        or output.background_cohort != snapshot.background_cohort
    ):
        reasons.add("behavior_output_snapshot_mismatch")
        ineligible = True
    if snapshot.source_scope.scope_uid != policy.longitudinal_source_scope_uid:
        reasons.add("longitudinal_source_scope_mismatch")
        ineligible = True
    if tuple(snapshot.source_scope.source_uids) != policy.longitudinal_source_uids:
        reasons.add("longitudinal_source_uids_mismatch")
        ineligible = True
    if (
        snapshot.window_starts_at != policy.history_window_starts_at
        or snapshot.window_ends_at != policy.history_window_ends_at
    ):
        reasons.add("longitudinal_history_window_mismatch")
        ineligible = True
    for source in (output, snapshot):
        if source.available_at > source.as_of:
            reasons.add("longitudinal_availability_after_its_event_cutoff")
            ineligible = True
        if source.as_of > policy.as_of:
            reasons.add("longitudinal_event_after_priority_cutoff")
            ineligible = True
        if source.available_at > policy.available_at:
            reasons.add("longitudinal_input_after_priority_availability_cutoff")
            ineligible = True
    if policy.mode == "operational_shadow" and (
        output.follow_up_only
        or bool(snapshot.follow_up_only_fill_uids)
    ):
        reasons.add("operational_shadow_rejects_follow_up_only")
        ineligible = True
    lineage_reasons = _validate_longitudinal_lineage(snapshot=snapshot, policy=policy)
    if lineage_reasons:
        reasons.update(lineage_reasons)
        ineligible = True
    if ineligible:
        return "ineligible", tuple(sorted(reasons)), None

    if not snapshot.complete:
        reasons.add("history_snapshot_not_complete")
    if snapshot.coverage_status != CoverageStatus.COMPLETE or snapshot.missing_reasons:
        reasons.add("history_snapshot_coverage_incomplete")
    if output.coverage_status != CoverageStatus.COMPLETE or output.missing_reasons:
        reasons.add("behavior_output_coverage_incomplete")
    value, metric_status = _review_metric(
        output,
        expected_method_uid=policy.longitudinal_method_uid,
    )
    if metric_status == "method_mismatch":
        reasons.add("longitudinal_method_mismatch")
    elif metric_status != "available":
        reasons.add("review_priority_metric_unavailable")
    if reasons:
        return "unavailable", tuple(sorted(reasons)), None
    return "available", (), value


def build_wallet_priority_queue(
    *,
    cohort_report: CohortRankingReport,
    policy: WalletPriorityPolicy,
    longitudinal_inputs: Iterable[LongitudinalPriorityInput] = (),
) -> WalletPriorityQueue:
    """Build one immutable, full-population analyst queue at a declared cutoff."""

    rows_by_actor: dict[str, RankedWallet] = {}
    global_reasons: set[str] = set()
    for row in cohort_report.rows:
        if row.actor_uid in rows_by_actor:
            global_reasons.add("duplicate_actor_in_cohort_report")
        rows_by_actor.setdefault(row.actor_uid, row)
    actors = tuple(sorted(rows_by_actor))
    actor_set_hash = _digest(actors)
    if len(actors) != policy.declared_population_size:
        global_reasons.add("cohort_report_is_not_full_declared_population")
    if cohort_report.population_actor_count != policy.declared_population_size:
        global_reasons.add("cohort_population_actor_count_mismatch")
    if cohort_report.population_actor_set_sha256 != actor_set_hash:
        global_reasons.add("cohort_rows_do_not_equal_population_actor_set")
    if cohort_report.policy.as_of != policy.as_of:
        global_reasons.add("priority_and_cohort_decision_cutoff_mismatch")
    if cohort_report.policy.availability_cutoff != policy.available_at:
        global_reasons.add("priority_and_cohort_availability_cutoff_mismatch")
    if cohort_report.status == "abstain":
        global_reasons.add("cohort_report_abstained")
    if (
        cohort_report.coverage.status != CoverageStatus.COMPLETE
        or cohort_report.coverage.continuation is not None
        or cohort_report.coverage.gap_intervals
        or cohort_report.coverage.truncated
        or cohort_report.coverage.complete_through < cohort_report.coverage.interval_end
    ):
        global_reasons.add("cohort_population_coverage_not_complete")
    if cohort_report.input_fill_count != cohort_report.coverage.canonical_record_count:
        global_reasons.add("cohort_input_count_does_not_bind_complete_population")
    if any(
        row.status == "available"
        and (
            row.population_size != policy.declared_population_size
            or row.population_rank is None
            or not 1 <= row.population_rank <= policy.declared_population_size
        )
        for row in rows_by_actor.values()
    ):
        global_reasons.add("cohort_population_rank_contract_invalid")
    if policy.mode == "operational_shadow":
        if cohort_report.policy.analysis_mode != "operational" or not cohort_report.prospective_eligible:
            global_reasons.add("cohort_report_not_prospective_eligible")
        if cohort_report.coverage.retrieved_at > policy.as_of:
            global_reasons.add("cohort_coverage_late_for_shadow_cutoff")
    elif cohort_report.policy.analysis_mode != "hindsight_reconstructed":
        global_reasons.add("hindsight_priority_requires_hindsight_cohort")

    raw_bindings = tuple(longitudinal_inputs)
    bindings: dict[str, LongitudinalPriorityInput] = {}
    binding_conflicts: set[str] = set()
    for item in raw_bindings:
        if not isinstance(item, LongitudinalPriorityInput):
            global_reasons.add("non_longitudinal_binding_input")
            continue
        existing = bindings.get(item.actor_uid)
        if existing is not None and existing.binding_sha256 != item.binding_sha256:
            binding_conflicts.add(item.actor_uid)
        else:
            bindings.setdefault(item.actor_uid, item)
    extra_actors = set(bindings) - set(actors)
    if extra_actors:
        global_reasons.add("longitudinal_actor_outside_population")
    longitudinal_payload = {
        "bindings": sorted(
            [item.actor_uid, item.binding_sha256]
            for item in raw_bindings
            if isinstance(item, LongitudinalPriorityInput)
        ),
    }
    longitudinal_hash = _digest(longitudinal_payload)

    provisional: list[tuple[WalletPriorityRow, tuple[Any, ...] | None]] = []
    for actor_uid in actors:
        cohort_row = rows_by_actor[actor_uid]
        reasons: set[str] = set()
        if cohort_row.status != "available":
            reasons.update(f"cohort:{reason}" for reason in cohort_row.abstention_reasons)
        if cohort_row.population_rank is None or cohort_row.population_size is None:
            reasons.add("cohort_population_rank_unavailable")
        if cohort_row.scores_are_probabilities:
            reasons.add("cohort_score_contract_invalid")

        binding = bindings.get(actor_uid)
        output = None if binding is None else binding.behavior_output
        snapshot = None if binding is None else binding.history_snapshot
        output_hash = None if output is None else _digest(output)
        snapshot_hash = None if snapshot is None else _digest(snapshot)
        longitudinal_status, longitudinal_reasons, longitudinal_value = _longitudinal_evidence(
            actor_uid=actor_uid,
            binding=binding,
            policy=policy,
            conflicted=actor_uid in binding_conflicts,
        )
        if policy.require_longitudinal and longitudinal_status != "available":
            reasons.add("required_longitudinal_evidence_unavailable")
            reasons.update(longitudinal_reasons)

        eligible = not reasons
        row = WalletPriorityRow(
            actor_uid=actor_uid,
            status="eligible" if eligible else "abstain",
            abstention_reasons=tuple(sorted(reasons)),
            cohort_population_rank=cohort_row.population_rank,
            cohort_population_size=cohort_row.population_size,
            longitudinal_status=longitudinal_status,
            longitudinal_reasons=longitudinal_reasons,
            longitudinal_review_priority=longitudinal_value,
            longitudinal_output_sha256=output_hash,
            history_snapshot_sha256=snapshot_hash,
            longitudinal_contributed_to_rank=(
                eligible
                and policy.use_longitudinal_tiebreaker
                and longitudinal_status == "available"
            ),
        )
        if not eligible:
            provisional.append((row, None))
            continue
        if policy.use_longitudinal_tiebreaker:
            missing_bucket = 0 if longitudinal_status == "available" else 1
            longitudinal_key = -(longitudinal_value or Decimal("0")) if longitudinal_status == "available" else Decimal("0")
            key: tuple[Any, ...] = (cohort_row.population_rank, missing_bucket, longitudinal_key)
        else:
            key = (cohort_row.population_rank,)
        provisional.append((row, key))

    if global_reasons:
        abstained = tuple(
            WalletPriorityRow(
                actor_uid=row.actor_uid,
                status="abstain",
                abstention_reasons=tuple(sorted({*row.abstention_reasons, *global_reasons})),
                cohort_population_rank=row.cohort_population_rank,
                cohort_population_size=row.cohort_population_size,
                longitudinal_status=row.longitudinal_status,
                longitudinal_reasons=row.longitudinal_reasons,
                longitudinal_review_priority=row.longitudinal_review_priority,
                longitudinal_output_sha256=row.longitudinal_output_sha256,
                history_snapshot_sha256=row.history_snapshot_sha256,
                longitudinal_contributed_to_rank=False,
            )
            for row, _key in provisional
        )
        return WalletPriorityQueue(
            policy=policy,
            status="abstain",
            abstention_reasons=tuple(sorted(global_reasons)),
            cohort_report_sha256=cohort_report.report_sha256,
            population_actor_set_sha256=actor_set_hash,
            longitudinal_input_sha256=longitudinal_hash,
            rows=abstained,
            selected_count=0,
            selection_boundary_actor_uid=None,
            selection_boundary_priority_rank=None,
            prospective_shadow_eligible=False,
            descriptive_only=policy.mode == "hindsight_descriptive",
        )

    eligible = sorted(
        ((row, key) for row, key in provisional if key is not None),
        key=lambda item: (*item[1], item[0].actor_uid),
    )
    ranked: dict[str, WalletPriorityRow] = {}
    previous_key: tuple[Any, ...] | None = None
    tie_group = 0
    statistical_rank = 0
    for display_order, (row, key) in enumerate(eligible, start=1):
        if key != previous_key:
            tie_group += 1
            statistical_rank = display_order
            previous_key = key
        ranked[row.actor_uid] = WalletPriorityRow(
            actor_uid=row.actor_uid,
            status=row.status,
            abstention_reasons=row.abstention_reasons,
            cohort_population_rank=row.cohort_population_rank,
            cohort_population_size=row.cohort_population_size,
            longitudinal_status=row.longitudinal_status,
            longitudinal_reasons=row.longitudinal_reasons,
            longitudinal_review_priority=row.longitudinal_review_priority,
            longitudinal_output_sha256=row.longitudinal_output_sha256,
            history_snapshot_sha256=row.history_snapshot_sha256,
            longitudinal_contributed_to_rank=row.longitudinal_contributed_to_rank,
            priority_rank=statistical_rank,
            tie_group=tie_group,
            display_order=display_order,
            selected_for_review=(
                policy.mode == "operational_shadow"
                and display_order <= policy.analyst_budget
            ),
        )
    final_rows = tuple(
        [ranked[row.actor_uid] for row, _key in eligible]
        + sorted(
            (row for row, key in provisional if key is None),
            key=lambda item: item.actor_uid,
        )
    )
    selected_count = sum(row.selected_for_review for row in final_rows)
    selected_rows = tuple(row for row in final_rows if row.selected_for_review)
    boundary = selected_rows[-1] if selected_rows else None
    status: Literal["available", "abstain"] = "available" if eligible else "abstain"
    return WalletPriorityQueue(
        policy=policy,
        status=status,
        abstention_reasons=() if status == "available" else ("no_priority_eligible_wallets",),
        cohort_report_sha256=cohort_report.report_sha256,
        population_actor_set_sha256=actor_set_hash,
        longitudinal_input_sha256=longitudinal_hash,
        rows=final_rows,
        selected_count=selected_count,
        selection_boundary_actor_uid=None if boundary is None else boundary.actor_uid,
        selection_boundary_priority_rank=None if boundary is None else boundary.priority_rank,
        prospective_shadow_eligible=(policy.mode == "operational_shadow" and status == "available"),
        descriptive_only=policy.mode == "hindsight_descriptive",
    )


__all__ = [
    "LongitudinalPriorityInput",
    "WalletPriorityPolicy",
    "WalletPriorityQueue",
    "WalletPriorityRow",
    "build_wallet_priority_queue",
]
