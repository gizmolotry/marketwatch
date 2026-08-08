"""Minimal cohort mechanics fixtures; never evidence of model effectiveness."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
from time import perf_counter

import pytest

import marketleak.actors.wallet_cohort as cohort_module
from marketleak.actors import (
    CohortMarketScopeBinding,
    CohortQueryFilter,
    NuisanceVector,
    PopulationCoverage,
    PopulationCoverageSlice,
    WalletCohortPolicy,
    rank_wallet_cohort,
)
from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill, TradeSide
from marketleak.ingestion.normalize import canonical_json_bytes


AS_OF = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
START = AS_OF - timedelta(hours=1)
RAW_HASH = "a" * 64
SOURCE = "polymarket:data-api-trades"
ACTORS = tuple(f"polymarket:0x{letter * 4}" for letter in "abcd")


def fill(
    actor_uid: str,
    index: int,
    *,
    market_uid: str = "polymarket:market-1",
    outcome_uid: str = "polymarket:yes",
    price: str = "0.50",
    size: str = "10",
    side: TradeSide = TradeSide.BUY,
    event_time: datetime | None = None,
    ingested_at: datetime | None = None,
) -> TradeFill:
    observed = event_time or START + timedelta(minutes=5 + index)
    return TradeFill(
        fill_uid=f"polymarket:fill-{actor_uid.rsplit(':', 1)[1]}-{index}",
        market_uid=market_uid,
        outcome_uid=outcome_uid,
        platform="polymarket",
        price=Decimal(price),
        size=Decimal(size),
        side=side,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid=actor_uid,
        transaction_uid=f"polymarket:tx-{actor_uid.rsplit(':', 1)[1]}-{index}",
        event_time=observed,
        ingested_at=ingested_at or observed + timedelta(seconds=1),
        source_uid=SOURCE,
        raw_artifact_uid=f"polymarket:raw/{RAW_HASH}",
        parser_version="mechanics-fixture-v1",
    )


def coverage(record_count: int, **changes: object) -> PopulationCoverage:
    values = {
        "platform": "polymarket",
        "dataset": "public_market_trades",
        "source_uid": SOURCE,
        "scope_kind": "market_set",
        "scope_market_uids": ("polymarket:market-1", "polymarket:market-2"),
        "interval_start": START,
        "interval_end": AS_OF,
        "retrieved_at": AS_OF,
        "as_of": AS_OF,
        "complete_through": AS_OF,
        "status": CoverageStatus.COMPLETE,
        "record_count": record_count,
        "raw_sha256": (RAW_HASH,),
        "query_filters": (
            CohortQueryFilter.from_value("market", ["polymarket:market-1", "polymarket:market-2"]),
            CohortQueryFilter.from_value("start", int(START.timestamp())),
            CohortQueryFilter.from_value("end", int(AS_OF.timestamp())),
            CohortQueryFilter.from_value("takerOnly", False),
        ),
        "exhausted": True,
    }
    values.update(changes)
    return PopulationCoverage(**values)  # type: ignore[arg-type]


def policy(**changes: object) -> WalletCohortPolicy:
    values = {
        "as_of": AS_OF,
        "frozen_at": AS_OF - timedelta(minutes=2),
        "focus_market_uid": "polymarket:market-1",
        "focus_event_time": START,
        "focus_published_at": START,
        "focus_available_at": START,
        "min_peer_count": 2,
        "min_supported_features": 6,
        "composite_top_k": 5,
        "nuisance_k": 1,
        "nuisance_max_distance": Decimal("5"),
    }
    values.update(changes)
    return WalletCohortPolicy(**values)  # type: ignore[arg-type]


def nuisance(actors: tuple[str, ...] = ACTORS, *, far_actor: str | None = None) -> tuple[NuisanceVector, ...]:
    return tuple(
        NuisanceVector(
            actor_uid=actor,
            values=(("time_to_close_hours", Decimal("100") if actor == far_actor else Decimal(index) / 10),),
            event_time=START,
            available_at=START + timedelta(seconds=1),
            source_uid="polymarket:market-metadata",
            raw_artifact_uid=f"polymarket:raw/{RAW_HASH}",
        )
        for index, actor in enumerate(actors)
    )


def basic_fills() -> tuple[TradeFill, ...]:
    return tuple(fill(actor, index, size=str(10 + index)) for index, actor in enumerate(ACTORS))


def test_population_coverage_rejects_non_population_and_incomplete_contracts() -> None:
    with pytest.raises(ValueError, match="user-only"):
        coverage(
            0,
            query_filters=(
                CohortQueryFilter.from_value("user", "polymarket:0xaaaa"),
                CohortQueryFilter.from_value("market", "polymarket:market-1"),
                CohortQueryFilter.from_value("start", int(START.timestamp())),
                CohortQueryFilter.from_value("end", int(AS_OF.timestamp())),
                CohortQueryFilter.from_value("takerOnly", False),
            ),
        )
    with pytest.raises(ValueError, match="complete coverage"):
        coverage(0, status=CoverageStatus.PARTIAL)
    with pytest.raises(ValueError, match="gapped, or truncated"):
        coverage(0, truncated=True)


def test_population_coverage_accepts_only_exact_contiguous_matching_slice_unions() -> None:
    middle = START + timedelta(minutes=30)

    def one_slice(start: datetime, end: datetime, digest: str, count: int, retrieved: datetime) -> PopulationCoverageSlice:
        return PopulationCoverageSlice(
            source_uid=SOURCE,
            interval_start=start,
            interval_end=end,
            retrieved_at=retrieved,
            complete_through=end,
            status=CoverageStatus.COMPLETE,
            record_count=count,
            raw_sha256=(digest,),
            query_filters=(
                CohortQueryFilter.from_value("market", ["polymarket:market-1", "polymarket:market-2"]),
                CohortQueryFilter.from_value("start", int(start.timestamp())),
                CohortQueryFilter.from_value("end", int(end.timestamp())),
                CohortQueryFilter.from_value("takerOnly", False),
            ),
        )

    first = one_slice(START, middle, "b" * 64, 2, AS_OF - timedelta(minutes=2))
    second = one_slice(middle + timedelta(seconds=1), AS_OF, "c" * 64, 3, AS_OF)
    union = coverage(
        5,
        raw_sha256=("b" * 64, "c" * 64),
        retrieved_at=second.retrieved_at,
        slices=(first, second),
    )
    assert len(union.slices) == 2
    assert union.to_payload()["slices"][0]["slice_sha256"] == first.slice_sha256

    with pytest.raises(ValueError, match="gap"):
        coverage(
            5,
            raw_sha256=("b" * 64, "c" * 64),
            retrieved_at=second.retrieved_at,
            slices=(first, one_slice(middle + timedelta(seconds=2), AS_OF, "c" * 64, 3, second.retrieved_at)),
        )
    with pytest.raises(ValueError, match="overlap"):
        coverage(
            5,
            raw_sha256=("b" * 64, "c" * 64),
            retrieved_at=second.retrieved_at,
            slices=(first, one_slice(middle, AS_OF, "c" * 64, 3, second.retrieved_at)),
        )
    mismatched = replace(
        second,
        query_filters=tuple(
            CohortQueryFilter.from_value(item.name, ["polymarket:market-1"] if item.name == "market" else item.value)
            for item in second.query_filters
        ),
    )
    with pytest.raises(ValueError, match="share market and taker policy"):
        coverage(
            5,
            raw_sha256=("b" * 64, "c" * 64),
            retrieved_at=mismatched.retrieved_at,
            slices=(first, mismatched),
        )


def test_each_canonical_fill_is_bound_to_the_exact_inclusive_event_slice_and_raw_hash() -> None:
    middle = START + timedelta(minutes=30)

    def part(start: datetime, end: datetime, digest: str, count: int) -> PopulationCoverageSlice:
        return PopulationCoverageSlice(
            source_uid=SOURCE,
            interval_start=start,
            interval_end=end,
            retrieved_at=AS_OF,
            complete_through=end,
            status=CoverageStatus.COMPLETE,
            record_count=count,
            raw_sha256=(digest,),
            query_filters=(
                CohortQueryFilter.from_value("market", ["polymarket:market-1", "polymarket:market-2"]),
                CohortQueryFilter.from_value("start", int(start.timestamp())),
                CohortQueryFilter.from_value("end", int(end.timestamp())),
                CohortQueryFilter.from_value("takerOnly", False),
            ),
        )

    slices = (
        part(START, middle, "b" * 64, 1),
        part(middle + timedelta(seconds=1), AS_OF, "c" * 64, 2),
    )
    items = (
        fill(ACTORS[0], 0, event_time=middle).model_copy(update={"raw_artifact_uid": f"polymarket:raw/{'b' * 64}"}),
        fill(ACTORS[1], 1, event_time=middle + timedelta(seconds=1)).model_copy(update={"raw_artifact_uid": f"polymarket:raw/{'c' * 64}"}),
        fill(ACTORS[2], 2, event_time=middle + timedelta(seconds=2)).model_copy(update={"raw_artifact_uid": f"polymarket:raw/{'c' * 64}"}),
    )
    union = coverage(3, raw_sha256=("b" * 64, "c" * 64), slices=slices)
    valid = rank_wallet_cohort(
        coverage=union, policy=policy(min_peer_count=2, nuisance_k=1), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(ACTORS[:3]),
    )
    assert valid.status == "available"
    wrong_slice = (items[0].model_copy(update={"raw_artifact_uid": f"polymarket:raw/{'c' * 64}"}), *items[1:])
    invalid = rank_wallet_cohort(
        coverage=union, policy=policy(min_peer_count=2, nuisance_k=1), fills=wrong_slice,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(ACTORS[:3]),
    )
    assert "population_input_raw_lineage_not_bound_to_event_slice" in invalid.abstention_reasons


def test_ranking_iterable_is_canonical_while_raw_and_duplicate_counts_remain_coverage_metadata() -> None:
    items = basic_fills()[:3]
    provenance = coverage(
        3,
        raw_record_count=5,
        duplicate_record_count=2,
        conflict_record_count=0,
        canonical_record_count=3,
    )
    accepted = rank_wallet_cohort(
        coverage=provenance, policy=policy(min_peer_count=2, nuisance_k=1),
        fills=(*items, items[0]),
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(ACTORS[:3]),
    )
    assert accepted.status == "available"
    conflict = items[0].model_copy(update={"price": Decimal("0.75")})
    rejected = rank_wallet_cohort(
        coverage=provenance, policy=policy(min_peer_count=2, nuisance_k=1),
        fills=(*items, conflict),
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(ACTORS[:3]),
    )
    assert "population_fill_uid_conflict" in rejected.abstention_reasons


def test_coverage_clocks_counts_and_declared_scope_are_exact() -> None:
    with pytest.raises(ValueError, match="before it is complete"):
        coverage(0, retrieved_at=AS_OF - timedelta(seconds=1))
    with pytest.raises(ValueError, match="exactly match"):
        coverage(
            0,
            query_filters=(
                CohortQueryFilter.from_value("market", ["polymarket:market-1"]),
                CohortQueryFilter.from_value("start", int(START.timestamp())),
                CohortQueryFilter.from_value("end", int(AS_OF.timestamp())),
                CohortQueryFilter.from_value("takerOnly", False),
            ),
        )
    real_shape = coverage(
        21785,
        raw_record_count=22156,
        duplicate_record_count=371,
        conflict_record_count=0,
        canonical_record_count=21785,
    )
    assert real_shape.raw_record_count == 22156
    assert real_shape.canonical_record_count == 21785
    with pytest.raises(ValueError, match="reconcile"):
        coverage(10, raw_record_count=12, duplicate_record_count=1, conflict_record_count=0)


def test_raw_polymarket_market_id_is_preserved_and_bound_to_canonical_uid() -> None:
    source_id = "0x" + "1" * 64
    canonical_uid = f"polymarket:market/{source_id}"
    bound = coverage(
        0,
        scope_kind="market",
        scope_market_uids=(canonical_uid,),
        scope_market_bindings=(CohortMarketScopeBinding(canonical_uid, source_id),),
        query_filters=(
            CohortQueryFilter.from_value("market", source_id),
            CohortQueryFilter.from_value("start", int(START.timestamp())),
            CohortQueryFilter.from_value("end", int(AS_OF.timestamp())),
            CohortQueryFilter.from_value("takerOnly", False),
        ),
    )
    assert dict((item.name, item.value) for item in bound.query_filters)["market"] == source_id
    assert bound.scope_market_bindings[0].canonical_market_uid == canonical_uid
    with pytest.raises(ValueError, match="deterministic canonical mapping"):
        coverage(
            0,
            scope_kind="market",
            scope_market_uids=("polymarket:wrong",),
            scope_market_bindings=(CohortMarketScopeBinding("polymarket:wrong", source_id),),
            query_filters=bound.query_filters,
        )


def test_unsliced_coverage_requires_exhaustion_and_polymarket_filters_are_not_narrowed() -> None:
    with pytest.raises(ValueError, match="exhausted proof"):
        coverage(0, exhausted=False)
    with pytest.raises(ValueError, match="takerOnly=false"):
        coverage(
            0,
            query_filters=tuple(
                CohortQueryFilter.from_value(item.name, True if item.name == "takerOnly" else item.value)
                for item in coverage(0).query_filters
            ),
        )
    with pytest.raises(ValueError, match="cohort-narrowing"):
        coverage(
            0,
            query_filters=coverage(0).query_filters + (CohortQueryFilter.from_value("outcome", "YES"),),
        )
    with pytest.raises(ValueError, match="unsupported Polymarket dataset"):
        coverage(0, dataset="renamed_to_bypass_policy")
    with pytest.raises(ValueError, match="unsupported Polymarket source"):
        coverage(0, source_uid="polymarket:unreviewed-trades")


def test_operational_mode_fails_closed_on_late_coverage_or_fill() -> None:
    items = basic_fills()
    late_coverage = coverage(len(items), retrieved_at=AS_OF + timedelta(minutes=1))
    report = rank_wallet_cohort(
        coverage=late_coverage,
        policy=policy(),
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert report.status == "abstain"
    assert "population_coverage_late_for_operational_ranking" in report.abstention_reasons
    assert report.prospective_eligible is False

    late = items[:-1] + (
        fill(ACTORS[-1], 3, size="13", ingested_at=AS_OF + timedelta(seconds=1)),
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(late)), policy=policy(), fills=late,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    assert "population_input_clock_ineligible" in report.abstention_reasons
    assert report.rows[0].novelty_composite is None


def test_hindsight_reconstruction_uses_explicit_later_availability_and_never_becomes_live_priority() -> None:
    later = AS_OF + timedelta(hours=2)
    items = tuple(
        item.model_copy(update={"ingested_at": AS_OF + timedelta(minutes=30)}) for item in basic_fills()
    )
    hindsight = policy(
        analysis_mode="hindsight_reconstructed",
        availability_cutoff=later,
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(items), retrieved_at=AS_OF + timedelta(hours=1)),
        policy=hindsight,
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert report.status == "available"
    assert report.prospective_eligible is False
    assert report.operational_review_priority is None
    assert report.rows[0].hindsight_peer_rank == report.rows[0].population_rank
    assert report.rows[0].hindsight_peer_rank is not None
    assert report.not_training is True
    assert report.effectiveness_unknown is True

    existence_bound = rank_wallet_cohort(
        coverage=coverage(len(items), retrieved_at=AS_OF + timedelta(hours=1)),
        policy=policy(
            analysis_mode="hindsight_reconstructed",
            availability_cutoff=later,
            frozen_at=AS_OF + timedelta(minutes=30),
            focus_event_time=START,
            focus_published_at=None,
            focus_available_at=AS_OF + timedelta(minutes=30),
        ),
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert existence_bound.status == "available"
    assert existence_bound.policy.focus_published_at is None
    assert existence_bound.rows[0].signal_classification in {"high", "elevated", "routine"}
    assessment = existence_bound.to_payload()["signal_assessment"]
    assert "confidence" not in assessment
    assert assessment["signal_strength"] == existence_bound.rows[0].signal_classification
    assert assessment["statistical_support"] == "sufficient"
    assert assessment["coverage_status"] == "complete"
    assert assessment["coverage_confidence"] == "verified_complete"

    operational_missing_publication = rank_wallet_cohort(
        coverage=coverage(len(basic_fills())),
        policy=policy(focus_published_at=None),
        fills=basic_fills(),
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert operational_missing_publication.status == "abstain"
    assert "focus_market_publication_time_unmapped" in operational_missing_publication.abstention_reasons

    late_focus = policy(
        analysis_mode="hindsight_reconstructed",
        availability_cutoff=later,
        frozen_at=AS_OF + timedelta(minutes=30),
        focus_event_time=START,
        focus_published_at=START + timedelta(minutes=1),
        focus_available_at=AS_OF + timedelta(minutes=30),
    )
    later_focus_report = rank_wallet_cohort(
        coverage=coverage(len(items), retrieved_at=AS_OF + timedelta(hours=1)),
        policy=late_focus,
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert later_focus_report.status == "available"
    assert later_focus_report.prospective_eligible is False
    post_decision_focus = rank_wallet_cohort(
        coverage=coverage(len(items), retrieved_at=AS_OF + timedelta(hours=1)),
        policy=policy(
            analysis_mode="hindsight_reconstructed",
            availability_cutoff=later,
            frozen_at=AS_OF + timedelta(minutes=30),
            focus_event_time=AS_OF + timedelta(minutes=5),
            focus_published_at=START,
            focus_available_at=AS_OF + timedelta(minutes=30),
        ),
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert "focus_market_event_after_decision_cutoff" in post_decision_focus.abstention_reasons
    operational_late_focus = rank_wallet_cohort(
        coverage=coverage(len(basic_fills())),
        policy=policy(
            focus_event_time=AS_OF + timedelta(minutes=5),
            focus_published_at=AS_OF + timedelta(minutes=10),
            focus_available_at=AS_OF + timedelta(minutes=30),
        ),
        fills=basic_fills(),
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    assert operational_late_focus.status == "abstain"
    assert "focus_market_not_frozen_before_operational_scoring" in operational_late_focus.abstention_reasons


def test_candidate_is_excluded_from_exact_tie_safe_peer_ecdf() -> None:
    items = basic_fills() + (
        fill(ACTORS[0], 10, size="100"),
        fill(ACTORS[0], 11, size="100"),
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    row = report.rows[0]
    assert row.status == "available"
    fill_count = next(item for item in row.feature_surprises if item.name == "fill_count")
    assert fill_count.peer_count == 3
    assert fill_count.tail_p == Decimal("0.25")
    with localcontext() as context:
        context.prec = 50
        assert fill_count.surprise == -Decimal("0.25").log10()


def test_each_feature_requires_the_full_declared_leave_one_out_peer_minimum() -> None:
    actors = ACTORS[:3]
    items = (
        fill(actors[0], 0),
        fill(actors[1], 1),
        fill(actors[2], 2, side=TradeSide.SELL),
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(min_peer_count=2, nuisance_k=1), fills=items,
        candidate_actor_uids=(actors[0],), nuisance_vectors=nuisance(actors),
    )
    feature = next(
        item for item in report.rows[0].feature_surprises
        if item.name == "long_shot_buy_notional_share"
    )
    assert feature.peer_count == 1
    assert feature.status == "insufficient_peer_support"


def test_constant_features_are_tie_safe_and_display_order_does_not_create_rank() -> None:
    items = tuple(fill(actor, 0) for actor in ACTORS)
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(), fills=reversed(items),
        candidate_actor_uids=reversed(ACTORS), nuisance_vectors=reversed(nuisance()),
    )
    assert [row.actor_uid for row in report.rows] == list(ACTORS)
    assert {row.statistical_rank for row in report.rows} == {1}
    assert [row.display_order for row in report.rows] == [1, 2, 3, 4]
    for row in report.rows:
        fill_count = next(item for item in row.feature_surprises if item.name == "fill_count")
        assert fill_count.tail_p == Decimal("1")
        assert fill_count.surprise == Decimal("0")


def test_minimum_support_and_exact_nuisance_ood_gate_abstain() -> None:
    items = basic_fills()
    unsupported = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(min_peer_count=5, nuisance_k=1), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    assert unsupported.rows[0].abstention_reasons == ("peer_count_below_minimum",)

    ood = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(nuisance_max_distance=Decimal("1")), fills=items,
        candidate_actor_uids=(ACTORS[-1],), nuisance_vectors=nuisance(far_actor=ACTORS[-1]),
    )
    assert ood.rows[0].abstention_reasons == ("nuisance_support_ood",)
    assert ood.rows[0].nuisance_kth_distance is not None
    assert ood.rows[0].novelty_composite is None


def test_nuisance_context_requires_causal_clocks_and_raw_provenance() -> None:
    items = basic_fills()
    late = list(nuisance())
    late[0] = NuisanceVector(
        ACTORS[0], (("time_to_close_hours", Decimal("1")),), START,
        AS_OF + timedelta(seconds=1), "polymarket:metadata", f"polymarket:raw/{RAW_HASH}",
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=late,
    )
    assert report.rows[0].abstention_reasons == ("nuisance_late_for_operational_ranking", "nuisance_not_available_by_declared_cutoff")
    with pytest.raises(ValueError, match="SHA-256 raw provenance"):
        NuisanceVector(
            ACTORS[0], (("time_to_close_hours", Decimal("1")),), START, START,
            "polymarket:metadata", "polymarket:raw/not-a-hash",
        )


def test_reordering_is_deterministic_but_canonical_fill_mutation_changes_hashes() -> None:
    items = basic_fills()
    kwargs = {
        "coverage": coverage(len(items)),
        "policy": policy(),
        "candidate_actor_uids": ACTORS,
        "nuisance_vectors": nuisance(),
    }
    first = rank_wallet_cohort(fills=items, **kwargs)
    second = rank_wallet_cohort(
        fills=reversed(items),
        coverage=kwargs["coverage"],
        policy=kwargs["policy"],
        candidate_actor_uids=reversed(ACTORS),
        nuisance_vectors=reversed(nuisance()),
    )
    assert first.canonical_bytes() == second.canonical_bytes()

    mutated = items[:-1] + (items[-1].model_copy(update={"price": Decimal("0.75")}),)
    changed = rank_wallet_cohort(fills=mutated, **kwargs)
    assert changed.input_sha256 != first.input_sha256
    assert changed.feature_snapshot_sha256 != first.feature_snapshot_sha256
    assert changed.report_sha256 != first.report_sha256


def test_focus_is_frozen_policy_input_and_missing_focus_abstains() -> None:
    items = basic_fills() + (
        fill(ACTORS[0], 10, market_uid="polymarket:market-2", size="200"),
    )
    first = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    second_policy = policy(focus_market_uid="polymarket:market-2")
    second = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=second_policy, fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    assert first.policy.policy_uid != second.policy.policy_uid
    assert first.policy.to_payload()["high_signal_percentile_min"] == "0.99"
    assert first.policy.to_payload()["elevated_signal_percentile_min"] == "0.95"
    changed_bands = policy(
        high_signal_percentile_min=Decimal("0.98"),
        elevated_signal_percentile_min=Decimal("0.90"),
    )
    assert changed_bands.policy_uid != first.policy.policy_uid
    assert first.rows[0].feature_vector is not None
    assert second.rows[0].feature_vector is not None
    assert first.rows[0].feature_vector.vector_sha256 != second.rows[0].feature_vector.vector_sha256
    assert first.rows[0].feature_vector.value("focus_gross_notional_share") != second.rows[0].feature_vector.value("focus_gross_notional_share")

    missing = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(focus_market_uid=None), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )
    assert "focus_market_not_declared" in missing.abstention_reasons


def test_contract_exposes_no_identity_or_misconduct_probability_claim() -> None:
    items = basic_fills()
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(), fills=items,
        candidate_actor_uids=(ACTORS[0],), nuisance_vectors=nuisance(),
    )

    def keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return set(value) | {key for item in value.values() for key in keys(item)}
        if isinstance(value, list):
            return {key for item in value for key in keys(item)}
        return set()

    payload_keys = keys(report.to_payload())
    assert not ({"fraud_probability", "guilt", "identity", "legal_outcome"} & payload_keys)
    assert report.rows[0].scores_are_probabilities is False
    with pytest.raises(ValueError, match="anomaly features"):
        NuisanceVector(
            ACTORS[0], (("gross_notional", Decimal("1")),), START, START,
            "polymarket:metadata", f"polymarket:raw/{RAW_HASH}",
        )


def test_single_candidate_gets_population_rank_and_reuses_one_sorted_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """201-peer fixture checks mechanics/complexity, not predictive effectiveness."""

    actors = tuple(f"polymarket:0x{index:040x}" for index in range(202))
    candidate = actors[0]
    items = tuple(
        fill(
            actor,
            index,
            size="200" if index == 0 else ("300" if index == 1 else "1"),
            event_time=START + timedelta(seconds=index),
        )
        for index, actor in enumerate(actors)
    )
    nuisance_vectors = tuple(
        NuisanceVector(
            actor, (("time_to_close_hours", Decimal("1")),), START, START,
            "polymarket:metadata", f"polymarket:raw/{RAW_HASH}",
        ) for actor in actors
    )
    profile_calls = 0
    original_profiles = cohort_module._profiles

    def counted_profiles(*args: object, **kwargs: object):
        nonlocal profile_calls
        profile_calls += 1
        return original_profiles(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(cohort_module, "_profiles", counted_profiles)
    report = rank_wallet_cohort(
        coverage=coverage(len(items)),
        policy=policy(min_peer_count=200, nuisance_k=20),
        fills=items,
        candidate_actor_uids=(candidate,),
        nuisance_vectors=nuisance_vectors,
    )
    row = report.rows[0]
    assert row.status == "available"
    assert row.population_size == 202
    assert report.population_actor_count == len(actors)
    assert report.population_actor_set_sha256 == sha256(
        canonical_json_bytes(tuple(sorted(actors)))
    ).hexdigest()
    assert report.candidate_set_sha256 != report.population_actor_set_sha256
    assert row.population_rank == 2
    assert row.operational_review_rank == 2
    assert row.hindsight_peer_rank is None
    # One global profile reused for all leave-one-out population scores.
    assert profile_calls == 1
    assert len(report.peer_profile_sha256) == 64

    expanded = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(min_peer_count=200, nuisance_k=20), fills=items,
        candidate_actor_uids=(candidate, actors[1]), nuisance_vectors=nuisance_vectors,
    )
    same_candidate = next(item for item in expanded.rows if item.actor_uid == candidate)
    assert same_candidate.novelty_composite == row.novelty_composite
    assert same_candidate.population_rank == row.population_rank


def test_all_identical_wallet_scores_share_population_rank_one() -> None:
    """Identical mechanics fixtures verify competition ranking, not effectiveness."""

    actors = ACTORS[:3]
    observed = START + timedelta(minutes=5)
    items = tuple(fill(actor, index, size="10", event_time=observed) for index, actor in enumerate(actors))
    nuisance_vectors = tuple(
        NuisanceVector(
            actor,
            (("time_to_close_hours", Decimal("1")),),
            START,
            START,
            "polymarket:metadata",
            f"polymarket:raw/{RAW_HASH}",
        )
        for actor in actors
    )
    report = rank_wallet_cohort(
        coverage=coverage(len(items)),
        policy=policy(),
        fills=items,
        candidate_actor_uids=actors,
        nuisance_vectors=nuisance_vectors,
    )
    assert all(row.status == "available" for row in report.rows)
    assert {row.population_rank for row in report.rows} == {1}
    assert {row.statistical_rank for row in report.rows} == {1}
    assert {row.signal_classification for row in report.rows} == {"routine"}


def test_signal_assessment_serializes_routine_elevated_abstain_and_unavailable_coherently() -> None:
    """Mechanics fixtures verify output semantics, not signal effectiveness."""

    actors = ACTORS[:3]
    observed = START + timedelta(minutes=5)
    identical = tuple(fill(actor, index, size="10", event_time=observed) for index, actor in enumerate(actors))
    routine_report = rank_wallet_cohort(
        coverage=coverage(len(identical)),
        policy=policy(),
        fills=identical,
        candidate_actor_uids=(actors[0],),
        nuisance_vectors=nuisance(actors),
    ).to_payload()
    routine = routine_report["signal_assessment"]
    assert routine == {
        "status": "available",
        "classification": "routine",
        "review_priority": "routine",
        "signal_strength": "routine",
        "statistical_support": "sufficient",
        "coverage_status": "complete",
        "coverage_confidence": "verified_complete",
        "population_rank": 1,
        "population_size": 3,
        "summary": "Routine peer-relative signal strength with sufficient statistical support in the declared cohort.",
        "decision_owner": "human_reviewer",
    }
    routine_row = routine_report["rows"][0]
    assert routine_row["signal_classification"] == "routine"
    assert routine_row["review_priority"] == "routine"
    assert routine_row["signal_strength"] == "routine"
    assert routine_row["statistical_support"] == "sufficient"

    items = basic_fills()
    elevated = rank_wallet_cohort(
        coverage=coverage(len(items)),
        policy=policy(
            elevated_signal_percentile_min=Decimal("0.80"),
            high_signal_percentile_min=Decimal("0.90"),
        ),
        fills=items,
        candidate_actor_uids=(ACTORS[-1],),
        nuisance_vectors=nuisance(),
    ).to_payload()["signal_assessment"]
    assert elevated["classification"] == "elevated"
    assert elevated["review_priority"] == "elevated"
    assert elevated["signal_strength"] == "elevated"
    assert elevated["summary"].startswith("Elevated peer-relative signal strength")
    assert "confidence" not in elevated

    abstained_report = rank_wallet_cohort(
        coverage=coverage(len(items), retrieved_at=AS_OF + timedelta(minutes=1)),
        policy=policy(),
        fills=items,
        candidate_actor_uids=(ACTORS[0],),
        nuisance_vectors=nuisance(),
    )
    abstained = abstained_report.to_payload()["signal_assessment"]
    assert abstained["status"] == "abstain"
    assert abstained["classification"] == "unavailable"
    assert abstained["review_priority"] == "unavailable"
    assert abstained["signal_strength"] == "unavailable"
    assert abstained["statistical_support"] == "insufficient"
    # The population capture is complete even though it arrived too late for
    # this operational calculation; those facts must remain separate.
    assert abstained["coverage_status"] == "complete"
    assert abstained["coverage_confidence"] == "verified_complete"

    for missing_status in (CoverageStatus.UNKNOWN, CoverageStatus.UNAVAILABLE):
        unavailable = cohort_module.WalletSignalAssessment.derive_values(
            report_status="abstain",
            row_status=None,
            signal_classification="insufficient_data",
            coverage_status=missing_status,
            population_rank=1,
            population_size=100,
        ).to_payload()
        assert unavailable["status"] == "unavailable"
        assert unavailable["classification"] == "unavailable"
        assert unavailable["review_priority"] == "unavailable"
        assert unavailable["signal_strength"] == "unavailable"
        assert unavailable["statistical_support"] == "unavailable"
        assert unavailable["coverage_status"] == missing_status.value
        assert unavailable["coverage_confidence"] == missing_status.value
        assert unavailable["population_rank"] is None
        assert unavailable["population_size"] is None

    partial = cohort_module.WalletSignalAssessment.derive_values(
        report_status="available",
        row_status="available",
        signal_classification="high",
        coverage_status=CoverageStatus.PARTIAL,
        population_rank=1,
        population_size=100,
    ).to_payload()
    assert partial["status"] == "abstain"
    assert partial["classification"] == "unavailable"
    assert partial["review_priority"] == "unavailable"
    assert partial["signal_strength"] == "unavailable"
    assert partial["statistical_support"] == "unavailable"
    assert partial["coverage_status"] == "partial"
    assert partial["coverage_confidence"] == "limited"
    assert partial["population_rank"] is None
    assert partial["population_size"] is None


def test_ten_thousand_wallet_population_uses_one_profile_and_finishes_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Large mechanics fixture guards the non-quadratic cohort scoring path."""

    actors = tuple(f"polymarket:0x{index:040x}" for index in range(10001))
    items = tuple(
        fill(
            actor,
            index,
            size=str(1 + index % 97),
            event_time=START + timedelta(seconds=index % 3000),
        )
        for index, actor in enumerate(actors)
    )
    nuisance_vectors = tuple(
        NuisanceVector(
            actor, (("time_to_close_hours", Decimal("1")),), START, START,
            "polymarket:metadata", f"polymarket:raw/{RAW_HASH}",
        )
        for actor in actors[:21]
    )
    calls = 0
    original_profiles = cohort_module._profiles

    def counted_profiles(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        return original_profiles(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(cohort_module, "_profiles", counted_profiles)
    started = perf_counter()
    report = rank_wallet_cohort(
        coverage=coverage(len(items)), policy=policy(min_peer_count=200, nuisance_k=20), fills=items,
        candidate_actor_uids=(actors[0],), nuisance_vectors=nuisance_vectors,
    )
    elapsed = perf_counter() - started
    assert report.rows[0].population_size == len(actors)
    assert report.population_actor_count == len(actors)
    assert report.population_actor_set_sha256 == sha256(
        canonical_json_bytes(tuple(sorted(actors)))
    ).hexdigest()
    assert calls == 1
    assert elapsed < 15
