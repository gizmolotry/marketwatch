"""Minimal priority-queue mechanics fixtures; never effectiveness evidence."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import subprocess
import sys
from time import perf_counter

from marketleak.actors import (
    CohortQueryFilter,
    CohortRankingReport,
    LongitudinalPriorityInput,
    PopulationCoverage,
    RankedWallet,
    WalletCohortPolicy,
    WalletPriorityPolicy,
    build_wallet_priority_queue,
)
from marketleak.domain import CoverageStatus
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.wallet_tracking import (
    BackgroundCohortMarker,
    BehaviorMetricMetadata,
    FillLineage,
    WalletBehaviorOutput,
    WalletHistorySnapshot,
    WalletMarketScope,
    WalletSourceScope,
)


AS_OF = datetime(2026, 7, 19, 12, tzinfo=timezone.utc)
START = AS_OF - timedelta(hours=1)
SOURCE = "polymarket:data-api-trades"
EVIL_SOURCE = "polymarket:substituted-evil-source"
RAW = "a" * 64


def _digest(value: object) -> str:
    return sha256(canonical_json_bytes(value)).hexdigest()


def coverage(count: int, *, retrieved_at: datetime = AS_OF) -> PopulationCoverage:
    return PopulationCoverage(
        platform="polymarket",
        dataset="public_market_trades",
        source_uid=SOURCE,
        scope_kind="market",
        scope_market_uids=("polymarket:market-1",),
        interval_start=START,
        interval_end=AS_OF,
        retrieved_at=retrieved_at,
        as_of=AS_OF,
        complete_through=AS_OF,
        status=CoverageStatus.COMPLETE,
        record_count=count,
        raw_sha256=(RAW,),
        query_filters=(
            CohortQueryFilter.from_value("market", "polymarket:market-1"),
            CohortQueryFilter.from_value("start", int(START.timestamp())),
            CohortQueryFilter.from_value("end", int(AS_OF.timestamp())),
            CohortQueryFilter.from_value("takerOnly", False),
        ),
        exhausted=True,
    )


def cohort_policy(*, hindsight: bool = False) -> WalletCohortPolicy:
    return WalletCohortPolicy(
        as_of=AS_OF,
        frozen_at=AS_OF,
        focus_market_uid="polymarket:market-1",
        focus_event_time=START,
        focus_published_at=START,
        focus_available_at=START,
        analysis_mode="hindsight_reconstructed" if hindsight else "operational",
        availability_cutoff=AS_OF + timedelta(hours=1) if hindsight else AS_OF,
        min_peer_count=2,
        nuisance_k=1,
    )


def ranked(actor: str, rank: int | None, *, status: str = "available", reason: str | None = None,
           size: int | None = None) -> RankedWallet:
    return RankedWallet(
        actor_uid=actor,
        status=status,  # type: ignore[arg-type]
        abstention_reasons=() if reason is None else (reason,),
        feature_vector=None,
        feature_surprises=(),
        novelty_composite=None if rank is None else Decimal(100 - rank),
        cohort_percentile=None if rank is None else Decimal("0.5"),
        nuisance_kth_distance=None,
        nuisance_neighbor_uids=(),
        case_resemblance=None,
        case_resemblance_status="unavailable_not_supplied",
        population_rank=rank,
        population_size=size,
    )


def report(actors: tuple[str, ...], *, ranks: tuple[int | None, ...] | None = None,
           statuses: tuple[str, ...] | None = None, hindsight: bool = False,
           status: str = "available", count: int | None = None,
           candidate_hash: str | None = None,
           population_count: int | None = None,
           population_hash: str | None = None) -> CohortRankingReport:
    ranks = ranks or tuple(range(1, len(actors) + 1))
    statuses = statuses or ("available",) * len(actors)
    rows = tuple(
        ranked(
            actor,
            rank,
            status=row_status,
            reason="nuisance_support_ood" if row_status == "abstain" else None,
            size=len(actors) if rank is not None else None,
        )
        for actor, rank, row_status in zip(actors, ranks, statuses, strict=True)
    )
    admitted = len(actors) if count is None else count
    return CohortRankingReport(
        policy=cohort_policy(hindsight=hindsight),
        coverage=coverage(admitted, retrieved_at=AS_OF + timedelta(hours=1) if hindsight else AS_OF),
        status=status,  # type: ignore[arg-type]
        abstention_reasons=() if status == "available" else ("population_input_incomplete",),
        input_fill_count=admitted,
        input_sha256="1" * 64,
        feature_snapshot_sha256="2" * 64,
        peer_profile_sha256="3" * 64,
        nuisance_snapshot_sha256="4" * 64,
        candidate_set_sha256=candidate_hash or _digest(tuple(sorted(actors))),
        population_actor_count=len(actors) if population_count is None else population_count,
        population_actor_set_sha256=population_hash or _digest(tuple(sorted(actors))),
        rows=rows,
        prospective_eligible=not hindsight and status == "available",
    )


def priority_policy(size: int, *, budget: int = 2, hindsight: bool = False,
                    require_longitudinal: bool = False) -> WalletPriorityPolicy:
    return WalletPriorityPolicy(
        as_of=AS_OF,
        available_at=AS_OF + timedelta(hours=1) if hindsight else AS_OF,
        frozen_at=AS_OF,
        mode="hindsight_descriptive" if hindsight else "operational_shadow",
        declared_population_size=size,
        analyst_budget=budget,
        require_longitudinal=require_longitudinal,
        longitudinal_method_uid="fixture-method",
        longitudinal_source_scope_uid="scope",
        longitudinal_source_uids=(SOURCE,),
        history_window_starts_at=START,
        history_window_ends_at=AS_OF,
    )


def longitudinal(actor: str, value: str, *, follow_up_only: bool = False,
                 partial: bool = False, late: bool = False) -> tuple[WalletBehaviorOutput, WalletHistorySnapshot]:
    background = BackgroundCohortMarker("cohort", "selection", "scope", START, START)
    scope = WalletSourceScope("scope", "polymarket", (SOURCE,), WalletMarketScope.ALL_PLATFORM_MARKETS)
    source_as_of = AS_OF + timedelta(minutes=1) if late else AS_OF
    source_available = source_as_of
    coverage_status = CoverageStatus.PARTIAL if partial else CoverageStatus.COMPLETE
    missing = ("fixture_incomplete",) if partial else ()
    snapshot = WalletHistorySnapshot(
        snapshot_uid=f"snapshot:{actor}", registration_uid=f"registration:{actor}", actor_uid=actor,
        as_of=source_as_of, available_at=source_available, source_scope=scope,
        window_starts_at=START, window_ends_at=source_as_of,
        fill_lineage=(), trigger_eligible_fill_uids=(), follow_up_only_fill_uids=(),
        coverage_status=coverage_status,
        complete_through=None if partial else source_as_of,
        continuation_token="next" if partial else None, missing_reasons=missing,
        late_excluded_count=0, supports_original_trigger=not follow_up_only,
        background_cohort=background,
    )
    metrics = (
        BehaviorMetricMetadata("behavior_novelty", None, None, "not_evaluated"),
        BehaviorMetricMetadata("case_resemblance", None, None, "not_evaluated"),
        BehaviorMetricMetadata("review_priority", Decimal(value), "fixture-method", "available"),
    )
    output = WalletBehaviorOutput(
        registration_uid=snapshot.registration_uid, actor_uid=actor,
        as_of=source_as_of, available_at=source_available, actor_features=None, metrics=metrics,
        coverage_status=coverage_status, missing_reasons=missing,
        background_cohort=background, follow_up_only=follow_up_only,
    )
    return output, snapshot


def bound_longitudinal(actor: str, value: str, **kwargs: object) -> LongitudinalPriorityInput:
    output, snapshot = longitudinal(actor, value, **kwargs)  # type: ignore[arg-type]
    return LongitudinalPriorityInput(output, snapshot)


def lineage(
    *,
    fill_uid: str = "polymarket:fill-lineage",
    event_time: datetime = START + timedelta(minutes=1),
    available_at: datetime = START + timedelta(minutes=1, seconds=1),
    source_uid: str = SOURCE,
) -> FillLineage:
    """Minimal hostile-input mechanics fixture, never effectiveness evidence."""

    return FillLineage(
        fill_uid=fill_uid,
        canonical_sha256="b" * 64,
        event_time=event_time,
        available_at=available_at,
        source_uid=source_uid,
        raw_artifact_uid="polymarket:raw/" + "c" * 64,
        transaction_uid="polymarket:tx-lineage",
    )


def bound_with_lineage(
    actor: str,
    item: FillLineage,
    *,
    role: str = "trigger",
) -> LongitudinalPriorityInput:
    output, snapshot = longitudinal(actor, "9")
    snapshot = replace(
        snapshot,
        fill_lineage=(item,),
        trigger_eligible_fill_uids=(item.fill_uid,) if role == "trigger" else (),
        follow_up_only_fill_uids=(item.fill_uid,) if role == "follow_up" else (),
        supports_original_trigger=role == "trigger",
    )
    return LongitudinalPriorityInput(output, snapshot)


def test_rejects_non_full_population_count_and_actor_set_hash() -> None:
    actors = ("wallet:a", "wallet:b", "wallet:c")
    bad_count = build_wallet_priority_queue(
        cohort_report=report(actors), policy=replace(priority_policy(3), declared_population_size=4, analyst_budget=2)
    )
    assert bad_count.status == "abstain"
    assert "cohort_report_is_not_full_declared_population" in bad_count.abstention_reasons
    bad_hash = build_wallet_priority_queue(
        cohort_report=report(actors, population_hash="f" * 64), policy=priority_policy(3)
    )
    assert "cohort_rows_do_not_equal_population_actor_set" in bad_hash.abstention_reasons


def test_candidate_subset_cannot_masquerade_as_complete_population() -> None:
    population = ("wallet:a", "wallet:b", "wallet:c")
    subset = population[:2]
    queue = build_wallet_priority_queue(
        cohort_report=report(
            subset,
            population_count=len(population),
            population_hash=_digest(population),
        ),
        policy=priority_policy(len(population), budget=1),
    )
    assert queue.status == "abstain"
    assert "cohort_report_is_not_full_declared_population" in queue.abstention_reasons
    assert "cohort_rows_do_not_equal_population_actor_set" in queue.abstention_reasons


def test_stable_ties_and_exact_top_k_selection() -> None:
    actors = ("wallet:c", "wallet:a", "wallet:b", "wallet:d")
    queue = build_wallet_priority_queue(
        cohort_report=report(actors, ranks=(1, 1, 1, 4)), policy=priority_policy(4, budget=2)
    )
    assert [row.actor_uid for row in queue.rows] == ["wallet:a", "wallet:b", "wallet:c", "wallet:d"]
    assert [row.priority_rank for row in queue.rows[:3]] == [1, 1, 1]
    assert [row.selected_for_review for row in queue.rows] == [True, True, False, False]
    assert queue.selected_count == 2


def test_cohort_ood_and_global_abstention_are_propagated() -> None:
    actors = ("wallet:a", "wallet:b")
    queue = build_wallet_priority_queue(
        cohort_report=report(actors, ranks=(None, 1), statuses=("abstain", "available")),
        policy=priority_policy(2, budget=1),
    )
    ood = next(row for row in queue.rows if row.actor_uid == "wallet:a")
    assert ood.status == "abstain"
    assert "cohort:nuisance_support_ood" in ood.abstention_reasons
    global_queue = build_wallet_priority_queue(
        cohort_report=report(actors, status="abstain"), policy=priority_policy(2, budget=1)
    )
    assert global_queue.status == "abstain" and global_queue.selected_count == 0


def test_missing_longitudinal_is_explicit_none_and_reasoned() -> None:
    queue = build_wallet_priority_queue(
        cohort_report=report(("wallet:a", "wallet:b")), policy=priority_policy(2, budget=1)
    )
    assert all(row.longitudinal_status == "missing" for row in queue.rows)
    assert all(row.longitudinal_review_priority is None for row in queue.rows)
    assert all(row.longitudinal_reasons == ("longitudinal_binding_missing",) for row in queue.rows)


def test_matching_complete_longitudinal_pair_breaks_only_cohort_rank_ties() -> None:
    actors = ("wallet:a", "wallet:b", "wallet:c")
    a = longitudinal("wallet:a", "2")
    b = longitudinal("wallet:b", "9")
    queue = build_wallet_priority_queue(
        cohort_report=report(actors, ranks=(1, 1, 3)), policy=priority_policy(3, budget=1),
        longitudinal_inputs=(LongitudinalPriorityInput(*a), LongitudinalPriorityInput(*b)),
    )
    assert [row.actor_uid for row in queue.rows[:2]] == ["wallet:b", "wallet:a"]
    assert queue.rows[0].longitudinal_contributed_to_rank is True


def test_longitudinal_binding_rejects_swapped_snapshot_and_binds_exact_hashes() -> None:
    a = longitudinal("wallet:a", "2")
    b = longitudinal("wallet:b", "3")
    binding = LongitudinalPriorityInput(*a)
    assert binding.actor_uid == "wallet:a"
    assert binding.behavior_output_sha256 == _digest(a[0])
    assert binding.history_snapshot_uid == a[1].snapshot_uid
    assert binding.history_snapshot_sha256 == _digest(a[1])
    import pytest

    with pytest.raises(ValueError, match="actor mismatch"):
        LongitudinalPriorityInput(a[0], b[1])


def test_longitudinal_method_scope_and_window_are_exact_policy_inputs() -> None:
    actors = ("wallet:a", "wallet:b", "wallet:c")
    method_output, method_snapshot = longitudinal(actors[0], "1")
    method_metrics = tuple(
        replace(metric, method_uid="other-method") if metric.name == "review_priority" else metric
        for metric in method_output.metrics
    )
    method_binding = LongitudinalPriorityInput(
        replace(method_output, metrics=method_metrics), method_snapshot
    )

    scope_output, scope_snapshot = longitudinal(actors[1], "1")
    wrong_scope = WalletSourceScope(
        "other-scope", "polymarket", (SOURCE,), WalletMarketScope.ALL_PLATFORM_MARKETS
    )
    scope_binding = LongitudinalPriorityInput(
        scope_output, replace(scope_snapshot, source_scope=wrong_scope)
    )

    window_output, window_snapshot = longitudinal(actors[2], "1")
    window_binding = LongitudinalPriorityInput(
        window_output,
        replace(window_snapshot, window_starts_at=START - timedelta(seconds=1)),
    )
    queue = build_wallet_priority_queue(
        cohort_report=report(actors),
        policy=priority_policy(3, budget=1, require_longitudinal=True),
        longitudinal_inputs=(method_binding, scope_binding, window_binding),
    )
    by_actor = {row.actor_uid: row for row in queue.rows}
    assert "longitudinal_method_mismatch" in by_actor[actors[0]].longitudinal_reasons
    assert "longitudinal_source_scope_mismatch" in by_actor[actors[1]].longitudinal_reasons
    assert "longitudinal_history_window_mismatch" in by_actor[actors[2]].longitudinal_reasons
    assert queue.selected_count == 0


def test_policy_requires_frozen_longitudinal_comparability_contract() -> None:
    import pytest

    with pytest.raises(ValueError, match="longitudinal_method_uid"):
        WalletPriorityPolicy(
            as_of=AS_OF,
            available_at=AS_OF,
            frozen_at=AS_OF,
            mode="operational_shadow",
            declared_population_size=2,
            analyst_budget=1,
        )


def test_same_scope_uid_cannot_substitute_an_unfrozen_source() -> None:
    """Hostile source-substitution mechanics fixture; never effectiveness evidence."""

    output, snapshot = longitudinal("wallet:a", "9")
    substituted_scope = replace(snapshot.source_scope, source_uids=(EVIL_SOURCE,))
    binding = LongitudinalPriorityInput(
        output,
        replace(snapshot, source_scope=substituted_scope),
    )
    queue = build_wallet_priority_queue(
        cohort_report=report(("wallet:a", "wallet:b")),
        policy=priority_policy(2, budget=1, require_longitudinal=True),
        longitudinal_inputs=(binding,),
    )
    row = next(item for item in queue.rows if item.actor_uid == "wallet:a")
    assert row.longitudinal_status == "ineligible"
    assert "longitudinal_source_uids_mismatch" in row.longitudinal_reasons


def test_wallet_tracking_imports_cleanly_in_a_fresh_interpreter() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", "import marketleak.wallet_tracking"],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[2]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_late_incomplete_and_follow_up_only_longitudinal_inputs_are_rejected() -> None:
    actors = ("wallet:a", "wallet:b", "wallet:c")
    pairs = (
        longitudinal(actors[0], "1", late=True),
        longitudinal(actors[1], "1", partial=True),
        longitudinal(actors[2], "1", follow_up_only=True),
    )
    queue = build_wallet_priority_queue(
        cohort_report=report(actors), policy=priority_policy(3, budget=1, require_longitudinal=True),
        longitudinal_inputs=tuple(LongitudinalPriorityInput(*pair) for pair in pairs),
    )
    by_actor = {row.actor_uid: row for row in queue.rows}
    assert "longitudinal_event_after_priority_cutoff" in by_actor[actors[0]].longitudinal_reasons
    assert by_actor[actors[1]].longitudinal_status == "unavailable"
    assert "operational_shadow_rejects_follow_up_only" in by_actor[actors[2]].longitudinal_reasons
    assert queue.selected_count == 0


def test_priority_independently_rejects_malicious_lineage_clocks_window_and_source() -> None:
    actors = ("wallet:future", "wallet:late", "wallet:window", "wallet:source")
    bindings = (
        bound_with_lineage(
            actors[0],
            lineage(event_time=AS_OF + timedelta(seconds=1), available_at=AS_OF + timedelta(seconds=1)),
        ),
        bound_with_lineage(
            actors[1],
            lineage(available_at=AS_OF + timedelta(seconds=1)),
        ),
        bound_with_lineage(
            actors[2],
            lineage(event_time=START - timedelta(seconds=1), available_at=START - timedelta(seconds=1)),
        ),
        bound_with_lineage(actors[3], lineage(source_uid="polymarket:unscoped-source")),
    )
    queue = build_wallet_priority_queue(
        cohort_report=report(actors),
        policy=priority_policy(len(actors), budget=1, require_longitudinal=True),
        longitudinal_inputs=bindings,
    )
    by_actor = {row.actor_uid: row for row in queue.rows}
    assert "longitudinal_fill_event_after_snapshot_cutoff" in by_actor[actors[0]].longitudinal_reasons
    assert "longitudinal_fill_event_after_priority_cutoff" in by_actor[actors[0]].longitudinal_reasons
    assert "longitudinal_fill_available_after_snapshot_cutoff" in by_actor[actors[1]].longitudinal_reasons
    assert "longitudinal_fill_available_after_priority_cutoff" in by_actor[actors[1]].longitudinal_reasons
    assert "longitudinal_fill_outside_history_window" in by_actor[actors[2]].longitudinal_reasons
    assert "longitudinal_fill_source_not_permitted" in by_actor[actors[3]].longitudinal_reasons
    assert all(row.longitudinal_status == "ineligible" for row in queue.rows)
    assert queue.selected_count == 0


def test_operational_priority_rejects_follow_up_roles_and_rechecks_exact_partition() -> None:
    actors = ("wallet:follow-up", "wallet:partition")
    follow_up = bound_with_lineage(actors[0], lineage(fill_uid="polymarket:fill-follow-up"), role="follow_up")
    partition = bound_with_lineage(actors[1], lineage(fill_uid="polymarket:fill-partition"))
    # Simulate a hostile deserializer bypassing the frozen dataclass constructor;
    # priority admission must still verify role partition independently.
    object.__setattr__(partition.history_snapshot, "trigger_eligible_fill_uids", ())
    queue = build_wallet_priority_queue(
        cohort_report=report(actors),
        policy=priority_policy(len(actors), budget=1, require_longitudinal=True),
        longitudinal_inputs=(follow_up, partition),
    )
    by_actor = {row.actor_uid: row for row in queue.rows}
    assert "operational_shadow_rejects_follow_up_only" in by_actor[actors[0]].longitudinal_reasons
    assert "longitudinal_lineage_role_partition_invalid" in by_actor[actors[1]].longitudinal_reasons
    assert queue.selected_count == 0


def test_operational_selects_but_hindsight_is_descriptive_only() -> None:
    actors = ("wallet:a", "wallet:b")
    operational = build_wallet_priority_queue(cohort_report=report(actors), policy=priority_policy(2, budget=1))
    hindsight = build_wallet_priority_queue(
        cohort_report=report(actors, hindsight=True), policy=priority_policy(2, budget=1, hindsight=True)
    )
    assert operational.selected_count == 1 and operational.prospective_shadow_eligible
    assert hindsight.status == "available" and hindsight.selected_count == 0
    assert hindsight.descriptive_only and not hindsight.prospective_shadow_eligible


def test_determinism_hash_mutation_and_nonclaims() -> None:
    actors = ("wallet:c", "wallet:a", "wallet:b")
    source = report(actors, ranks=(2, 1, 3))
    first = build_wallet_priority_queue(cohort_report=source, policy=priority_policy(3, budget=2))
    second = build_wallet_priority_queue(cohort_report=source, policy=priority_policy(3, budget=2))
    mutated = build_wallet_priority_queue(cohort_report=source, policy=priority_policy(3, budget=1))
    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.queue_sha256 != mutated.queue_sha256
    payload = first.to_payload()
    assert payload["effectiveness_unknown"] is True
    assert payload["not_training"] is True and payload["human_review_required"] is True
    assert all(row["is_probability"] is False for row in payload["rows"])


def test_ten_thousand_row_population_queue_runtime() -> None:
    actors = tuple(f"wallet:{index:05d}" for index in range(10_000))
    started = perf_counter()
    queue = build_wallet_priority_queue(
        cohort_report=report(actors), policy=priority_policy(len(actors), budget=10)
    )
    elapsed = perf_counter() - started
    assert queue.status == "available" and len(queue.rows) == 10_000 and queue.selected_count == 10
    assert elapsed < 15
