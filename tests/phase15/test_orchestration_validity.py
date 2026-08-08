"""Mechanics-only fixtures for Phase 15 causal assembly and evaluation gates.

These minimal fixtures verify fail-closed contracts.  They are not evidence of
model effectiveness and must not be interpreted as an evaluated corpus.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketleak.domain.enums import CoverageStatus
from marketleak.evaluation.metrics import CalibrationGate, evaluate
from marketleak.evaluation.schemas import EvaluationRow
from marketleak.labels.schemas import LabelTarget, LabelValue
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.labels import (
    EvidenceStrength,
    HumanDisposition,
    MultiAxisAdjudication,
    ObservableMechanism,
)
from marketleak.multimodal.orchestration import (
    AsOfAssembly,
    BaselinePlan,
    ExcludedFeatureInput,
    FeatureAssemblyInput,
    NAIVE_BASELINE_FEATURE_SPEC_HASH,
    assess_readiness,
    build_as_of_assembly,
    train_baseline_candidate,
)
from marketleak.multimodal.schemas import (
    EventFact,
    MarketStateSlice,
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    OnChainSettlementFact,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
CUTOFF = T0 + timedelta(days=10)


def _lineage(uid: str, observed_at: datetime) -> tuple[Provenance, SourceReliability]:
    source_uid = f"source:{uid}"
    return (
        Provenance(
            source_uid=source_uid,
            raw_artifact_uid=f"raw:{uid}",
            parser_version="phase15-validity-fixture",
            content_hash="a" * 64,
            retrieved_at=observed_at,
            source_url="https://example.test/fixture",
        ),
        SourceReliability(
            source_uid=source_uid,
            source_class=SourceClass.OFFICIAL_VENUE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("1"),
            assessed_at=observed_at,
            rationale="mechanics-only official-source fixture",
        ),
    )


def _market_fact(index: int, *, day: int, point: int) -> MarketStateSlice:
    window_starts_at = T0 + timedelta(days=day, seconds=150 * point)
    observed_at = T0 + timedelta(days=day, seconds=150 * (point + 1))
    provenance, reliability = _lineage(f"market-{index}-{point}", observed_at)
    return MarketStateSlice(
        event_uid=f"event:market-{index}-{point}",
        event_time=observed_at,
        ingested_at=observed_at + timedelta(seconds=30),
        provenance=provenance,
        reliability=reliability,
        missingness=(
            ModalityMissingness(
                modality=Modality.MARKET_STATE,
                status=MissingnessStatus.OBSERVED,
            ),
        ),
        market_uid=f"market:{index}",
        outcome_uid=f"outcome:{index}",
        window_starts_at=window_starts_at,
        window_ends_at=observed_at,
        last_trade_price=Decimal("0.40") if point == 0 else Decimal("0.55"),
        trade_notional=Decimal("4") if point == 0 else Decimal("6"),
        fill_count=1,
    )


def _market_pair(index: int, *, day: int) -> tuple[MarketStateSlice, MarketStateSlice]:
    return (_market_fact(index, day=day, point=0), _market_fact(index, day=day, point=1))


def _actor_fact(index: int, *, day: int, actor_uid: str) -> OnChainSettlementFact:
    observed_at = T0 + timedelta(days=day, minutes=4)
    provenance, reliability = _lineage(f"chain-{index}", observed_at)
    return OnChainSettlementFact(
        event_uid=f"event:chain-{index}",
        event_time=observed_at,
        ingested_at=observed_at + timedelta(seconds=30),
        provenance=provenance,
        reliability=reliability,
        missingness=(
            ModalityMissingness(
                modality=Modality.ONCHAIN_SETTLEMENT,
                status=MissingnessStatus.OBSERVED,
            ),
        ),
        chain="fixture-chain",
        chain_id=1,
        transaction_hash=f"0x{index:064x}",
        log_index=0,
        block_number=index,
        block_timestamp=observed_at,
        contract_address="0x0000000000000000000000000000000000000001",
        market_uid=f"market:{index}",
        outcome_uid=f"outcome:{index}",
        observed_wallet_uid=actor_uid,
        quantity=Decimal("1"),
    )


def _adjudication(fact: EventFact, index: int) -> MultiAxisAdjudication:
    return MultiAxisAdjudication(
        label_uid=f"label:{index}",
        event_uid=fact.event_uid,
        observable_mechanism=ObservableMechanism.UNEXPLAINED_ACTIVITY,
        evidence_strength=EvidenceStrength.LIMITED,
        disposition=HumanDisposition.ESCALATE_FOR_REVIEW,
        evidence_uids=(fact.event_uid,),
        rationale="mechanics-only human-label fixture",
        adjudicator="fixture-reviewer",
        adjudicated_at=fact.available_at,
        training_eligible=True,
    )


def _bound_feature(
    fact: EventFact,
    *,
    index: int,
    snapshot_uid: str,
    target: LabelTarget = LabelTarget.ACTIVITY_A,
    label: LabelValue = LabelValue.POSITIVE,
    feature_spec_hash: str | None = NAIVE_BASELINE_FEATURE_SPEC_HASH,
    source_facts: tuple[EventFact, ...] | None = None,
    features: dict[str, Decimal] | None = None,
    source_fact_uids: tuple[str, ...] | None = None,
    raw_artifact_uids: tuple[str, ...] | None = None,
    bound_snapshot_uid: str | None = None,
    bound_as_of: datetime | None = CUTOFF,
    feature_observed_at: datetime | None = None,
    feature_available_at: datetime | None = None,
) -> FeatureAssemblyInput:
    declared_facts = (fact,) if source_facts is None else source_facts
    derived_observed_at = max(item.event_time for item in declared_facts)
    derived_available_at = max(item.available_at for item in declared_facts)
    market_uid = getattr(fact, "market_uid", None) or f"market:{index}"
    outcome_uid = getattr(fact, "outcome_uid", None) or f"outcome:{index}"
    return FeatureAssemblyInput(
        feature_uid=f"feature:{index}",
        event_uid=fact.event_uid,
        event_cluster_uid=f"cluster:{index}",
        market_uid=market_uid,
        outcome_uid=outcome_uid,
        platform="fixture-platform",
        category="fixture-category",
        features=features or {"price_change": Decimal("0.15"), "volume": Decimal("10")},
        feature_observed_at=derived_observed_at if feature_observed_at is None else feature_observed_at,
        feature_available_at=derived_available_at if feature_available_at is None else feature_available_at,
        source_fact_uids=tuple(item.event_uid for item in declared_facts)
        if source_fact_uids is None
        else source_fact_uids,
        raw_artifact_uids=tuple(item.provenance.raw_artifact_uid for item in declared_facts)
        if raw_artifact_uids is None
        else raw_artifact_uids,
        feature_spec_hash=feature_spec_hash,
        bound_snapshot_uid=snapshot_uid if bound_snapshot_uid is None else bound_snapshot_uid,
        bound_as_of=bound_as_of,
        coverage_status=CoverageStatus.COMPLETE,
        context_complete=True,
        adjudication=_adjudication(fact, index),
        baseline_target=target,
        baseline_label=label,
    )


def _assembly_for(
    rows: tuple[tuple[tuple[EventFact, ...], LabelValue], ...],
    *,
    target=LabelTarget.ACTIVITY_A,
):
    facts = tuple(fact for source_facts, _ in rows for fact in source_facts)
    store = EventMemoryStore(facts)
    snapshot = store.snapshot_as_of(CUTOFF)
    features = tuple(
        _bound_feature(
            next(fact for fact in reversed(source_facts) if isinstance(fact, MarketStateSlice)),
            index=index,
            snapshot_uid=snapshot.manifest.snapshot_uid,
            source_facts=source_facts,
            target=target,
            label=label,
        )
        for index, (source_facts, label) in enumerate(rows, 1)
    )
    return build_as_of_assembly(store, features, as_of=CUTOFF)


def _small_plan(target: LabelTarget = LabelTarget.ACTIVITY_A) -> BaselinePlan:
    return BaselinePlan(
        target=target,
        train_end=T0 + timedelta(days=2),
        validation_end=T0 + timedelta(days=4),
        analyst_capacity=2,
        validation_min_labeled=2,
        validation_min_positive=1,
        validation_min_negative=1,
        test_min_labeled=2,
        test_min_positive=1,
        test_min_negative=1,
    )


def test_unbound_feature_metadata_fails_closed_with_explicit_reasons():
    pair = _market_pair(1, day=1)
    fact = pair[-1]
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    cases = {
        "feature_observation_time_missing": {"feature_observed_at": None},
        "feature_availability_time_missing": {"feature_available_at": None},
        "source_fact_lineage_missing": {"source_fact_uids": ()},
        "raw_artifact_lineage_missing": {"raw_artifact_uids": ()},
        "feature_spec_hash_missing": {"feature_spec_hash": None},
        "feature_snapshot_binding_missing": {"bound_snapshot_uid": None},
        "feature_cutoff_binding_missing": {"bound_as_of": None},
        "feature_snapshot_mismatch": {"bound_snapshot_uid": "snapshot:wrong"},
        "feature_cutoff_mismatch": {"bound_as_of": CUTOFF - timedelta(seconds=1)},
        "raw_artifact_lineage_mismatch": {"raw_artifact_uids": ("raw:wrong",)},
    }

    for expected_reason, overrides in cases.items():
        base = _bound_feature(fact, index=1, snapshot_uid=snapshot_uid, source_facts=pair)
        item = base.model_copy(update=overrides)
        assembly = build_as_of_assembly(store, (item,), as_of=CUTOFF)
        assert assembly.rows == ()
        assert assembly.excluded[0].reason == expected_reason


def test_future_feature_clocks_are_excluded_even_when_source_fact_is_past():
    pair = _market_pair(1, day=1)
    fact = pair[-1]
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        fact,
        index=1,
        snapshot_uid=snapshot_uid,
        source_facts=pair,
        feature_observed_at=CUTOFF + timedelta(minutes=1),
        feature_available_at=CUTOFF + timedelta(minutes=2),
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "feature_observed_after_as_of"


def test_unknown_feature_spec_is_excluded_even_when_declarations_are_self_consistent():
    pair = _market_pair(1, day=1)
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        pair[-1],
        index=1,
        snapshot_uid=snapshot_uid,
        source_facts=pair,
        feature_spec_hash="c" * 64,
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "feature_spec_hash_unknown"


def test_distinct_rejected_payloads_produce_distinct_input_and_run_hashes():
    pair = _market_pair(1, day=1)
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    base = _bound_feature(
        pair[-1], index=1, snapshot_uid=snapshot_uid, source_facts=pair
    )
    variants = (
        base.model_copy(update={"feature_spec_hash": "c" * 64}),
        base.model_copy(update={"feature_spec_hash": "d" * 64}),
        base.model_copy(
            update={
                "features": {
                    "price_change": Decimal("999999"),
                    "volume": Decimal("-777"),
                }
            }
        ),
        base.model_copy(
            update={
                "feature_observed_at": CUTOFF + timedelta(seconds=1),
                "feature_available_at": CUTOFF + timedelta(seconds=2),
            }
        ),
    )

    assemblies = tuple(
        build_as_of_assembly(store, (variant,), as_of=CUTOFF) for variant in variants
    )

    assert all(assembly.rows == () and len(assembly.excluded) == 1 for assembly in assemblies)
    assert len({assembly.input_hash for assembly in assemblies}) == len(assemblies)
    assert len({assembly.run_uid for assembly in assemblies}) == len(assemblies)
    assert assemblies[0].excluded[0].feature.feature_spec_hash == "c" * 64
    assert assemblies[1].excluded[0].feature.feature_spec_hash == "d" * 64


def test_arbitrary_caller_numbers_cannot_replace_values_derived_from_frozen_facts():
    pair = _market_pair(1, day=1)
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    hostile = _bound_feature(
        pair[-1],
        index=1,
        snapshot_uid=snapshot_uid,
        source_facts=pair,
        features={"price_change": Decimal("999999"), "volume": Decimal("-777")},
    )

    assembly = build_as_of_assembly(store, (hostile,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "feature_values_mismatch"


def test_one_market_price_point_cannot_produce_required_price_change():
    original = _market_fact(1, day=1, point=1)
    fact = original.model_copy(
        update={"window_starts_at": original.window_ends_at - timedelta(minutes=5)}
    )
    store = EventMemoryStore((fact,))
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(fact, index=1, snapshot_uid=snapshot_uid)

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "required_price_change_unavailable"


def test_valid_market_slices_cover_one_exact_contiguous_five_minute_window():
    pair = _market_pair(1, day=1)
    assembly = _assembly_for(((pair, LabelValue.POSITIVE),))

    assert len(assembly.rows) == 1
    row = assembly.rows[0]
    assert row.window_ends_at - row.window_starts_at == timedelta(minutes=5)
    assert row.window_starts_at == pair[0].window_starts_at
    assert row.window_ends_at == pair[-1].window_ends_at
    assert row.derived_features == {
        "price_change": Decimal("0.15"),
        "volume": Decimal("10"),
    }


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("overlap", "market_slice_overlap"),
        ("gap", "market_slice_gap"),
    ),
)
def test_overlapping_or_gapped_slices_cannot_double_count_or_subset_volume(
    mutation: str, expected_reason: str
):
    first, primary = _market_pair(1, day=1)
    offset = timedelta(seconds=-1 if mutation == "overlap" else 1)
    primary = primary.model_copy(update={"window_starts_at": primary.window_starts_at + offset})
    sources = (first, primary)
    store = EventMemoryStore(sources)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        primary, index=1, snapshot_uid=snapshot_uid, source_facts=sources
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == expected_reason


def test_incomplete_market_slice_subset_cannot_claim_full_window_features():
    primary = _market_pair(1, day=1)[-1]
    store = EventMemoryStore((primary,))
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(primary, index=1, snapshot_uid=snapshot_uid)

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "market_window_coverage_incomplete"


def test_arbitrary_eight_day_price_horizon_is_outside_fixed_window():
    stale = _market_fact(1, day=1, point=0)
    primary = _market_fact(2, day=9, point=1).model_copy(
        update={"market_uid": "market:1", "outcome_uid": "outcome:1"}
    )
    sources = (stale, primary)
    store = EventMemoryStore(sources)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        primary, index=1, snapshot_uid=snapshot_uid, source_facts=sources
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "market_slice_outside_fixed_window"


def test_same_market_different_outcome_facts_cannot_fabricate_a_return():
    yes_pair = _market_pair(1, day=1)
    unrelated = _market_fact(2, day=1, point=1).model_copy(
        update={"market_uid": "market:1", "outcome_uid": "outcome:no"}
    )
    sources = (*yes_pair, unrelated)
    store = EventMemoryStore(sources)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        yes_pair[-1],
        index=1,
        snapshot_uid=snapshot_uid,
        source_facts=sources,
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "market_fact_identity_mismatch"


def test_primary_event_identity_must_match_feature_market_and_outcome():
    pair = _market_pair(1, day=1)
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        pair[-1], index=1, snapshot_uid=snapshot_uid, source_facts=pair
    ).model_copy(update={"outcome_uid": "outcome:no"})

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == "event_fact_identity_mismatch"


@pytest.mark.parametrize(
    ("actor_updates", "expected_reason"),
    (
        ({"market_uid": "market:other"}, "actor_fact_identity_mismatch"),
        ({"outcome_uid": "outcome:other"}, "actor_fact_identity_mismatch"),
        ({"observed_wallet_uid": None}, "actor_fact_identifier_missing"),
    ),
)
def test_unrelated_or_unidentified_actor_facts_cannot_enter_row_lineage(
    actor_updates: dict[str, object], expected_reason: str
):
    pair = _market_pair(1, day=1)
    actor = _actor_fact(1, day=1, actor_uid="wallet:observed").model_copy(
        update=actor_updates
    )
    sources = (*pair, actor)
    store = EventMemoryStore(sources)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        pair[-1], index=1, snapshot_uid=snapshot_uid, source_facts=sources
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == expected_reason


@pytest.mark.parametrize(
    ("late_kind", "expected_reason"),
    (
        ("event", "actor_fact_outside_feature_window"),
        ("availability", "actor_fact_available_after_feature"),
    ),
)
def test_later_actor_fact_cannot_enter_an_earlier_feature(
    late_kind: str, expected_reason: str
):
    pair = _market_pair(1, day=1)
    actor = _actor_fact(1, day=1, actor_uid="wallet:observed")
    if late_kind == "event":
        late_event = pair[-1].window_ends_at + timedelta(seconds=1)
        actor = actor.model_copy(
            update={
                "event_time": late_event,
                "block_timestamp": late_event,
                "ingested_at": late_event + timedelta(seconds=1),
            }
        )
    else:
        actor = actor.model_copy(
            update={"ingested_at": pair[-1].available_at + timedelta(seconds=1)}
        )
    sources = (*pair, actor)
    store = EventMemoryStore(sources)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(
        pair[-1], index=1, snapshot_uid=snapshot_uid, source_facts=sources
    )

    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)

    assert assembly.rows == ()
    assert assembly.excluded[0].reason == expected_reason


def test_provenance_bound_actor_is_propagated_and_cross_boundary_rows_are_dropped():
    actor_uid = "wallet:pseudonymous-fixture"
    train_sources = (*_market_pair(1, day=1), _actor_fact(1, day=1, actor_uid=actor_uid))
    test_sources = (*_market_pair(2, day=5), _actor_fact(2, day=5, actor_uid=actor_uid))
    assembly = _assembly_for(
        ((train_sources, LabelValue.POSITIVE), (test_sources, LabelValue.NEGATIVE)),
        target=LabelTarget.ACTOR_EVIDENCE_C,
    )

    readiness = assess_readiness(assembly, plan=_small_plan(LabelTarget.ACTOR_EVIDENCE_C))

    assert all(row.actor_uids == (actor_uid,) for row in assembly.rows)
    assert readiness.dropped_cross_boundary_rows == 2
    assert "actor_identifiers_missing_for_actor_target" not in readiness.reason_codes
    assert readiness.status == "not_ready"


def test_assembled_rows_cannot_inject_an_actor_not_present_in_source_provenance():
    sources = (*_market_pair(1, day=1), _actor_fact(1, day=1, actor_uid="wallet:observed"))
    assembly = _assembly_for(((sources, LabelValue.POSITIVE),), target=LabelTarget.ACTOR_EVIDENCE_C)
    tampered = assembly.rows[0].model_copy(update={"actor_uids": ("wallet:invented",)})

    with pytest.raises(ValueError, match="actor_uids must be derived"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=(tampered,),
            excluded=assembly.excluded,
        )


def test_direct_assembly_recomputes_values_and_rejects_duplicate_row_uids():
    assembly = _assembly_for(((_market_pair(1, day=1), LabelValue.POSITIVE),))
    tampered = assembly.rows[0].model_copy(
        update={"derived_features": {"price_change": Decimal("999999"), "volume": Decimal("-777")}}
    )

    with pytest.raises(ValueError, match="derived_features do not match"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=(tampered,),
            excluded=assembly.excluded,
        )
    with pytest.raises(ValueError, match="feature_uids must be unique"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=(assembly.rows[0], assembly.rows[0]),
            excluded=assembly.excluded,
        )


def test_direct_assembly_rejects_snapshot_with_a_different_cutoff():
    assembly = _assembly_for(((_market_pair(1, day=1), LabelValue.POSITIVE),))

    with pytest.raises(ValueError, match="snapshot cutoff must exactly match"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of - timedelta(seconds=1),
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=assembly.rows,
            excluded=assembly.excluded,
        )


@pytest.mark.parametrize(
    "feature_updates",
    (
        {"baseline_label": LabelValue.NEGATIVE},
        {"coverage_status": CoverageStatus.PARTIAL},
        {"context_complete": False},
    ),
)
def test_direct_assembly_hash_binds_labels_coverage_and_context(
    feature_updates: dict[str, object],
):
    assembly = _assembly_for(((_market_pair(1, day=1), LabelValue.POSITIVE),))
    tampered_feature = assembly.rows[0].feature.model_copy(update=feature_updates)
    tampered_row = assembly.rows[0].model_copy(update={"feature": tampered_feature})

    with pytest.raises(ValueError, match="input_hash does not match"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=(tampered_row,),
            excluded=assembly.excluded,
        )


def test_direct_assembly_hash_binds_exclusions_and_run_identity():
    fact = _market_fact(1, day=1, point=1)
    store = EventMemoryStore((fact,))
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    feature = _bound_feature(fact, index=1, snapshot_uid=snapshot_uid)
    assembly = build_as_of_assembly(store, (feature,), as_of=CUTOFF)
    tampered_excluded = assembly.excluded[0].model_copy(update={"reason": "changed_reason"})

    with pytest.raises(ValueError, match="input_hash does not match"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=assembly.rows,
            excluded=(tampered_excluded,),
        )
    with pytest.raises(ValueError, match="input_hash does not match"):
        AsOfAssembly(
            run_uid=assembly.run_uid,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash="0" * 64,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=assembly.rows,
            excluded=assembly.excluded,
        )
    with pytest.raises(ValueError, match="run_uid does not match"):
        AsOfAssembly(
            run_uid="assembly:" + "f" * 64,
            as_of=assembly.as_of,
            event_snapshot=assembly.event_snapshot,
            input_hash=assembly.input_hash,
            feature_spec_hash=assembly.feature_spec_hash,
            rows=assembly.rows,
            excluded=assembly.excluded,
        )

    with pytest.raises(ValueError, match="excluded feature IDs must match"):
        ExcludedFeatureInput(
            feature_uid="feature:other",
            event_uid=assembly.excluded[0].event_uid,
            feature=assembly.excluded[0].feature,
            reason=assembly.excluded[0].reason,
        )


def test_builder_hashes_are_deterministic_across_input_order_and_exact_duplicates():
    rows = (
        (_market_pair(1, day=1), LabelValue.POSITIVE),
        (_market_pair(2, day=2), LabelValue.NEGATIVE),
    )
    facts = tuple(fact for sources, _ in rows for fact in sources)
    store = EventMemoryStore(reversed(facts))
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    inputs = tuple(
        _bound_feature(
            sources[-1],
            index=index,
            snapshot_uid=snapshot_uid,
            source_facts=sources,
            label=label,
        )
        for index, (sources, label) in enumerate(rows, 1)
    )

    first = build_as_of_assembly(store, inputs, as_of=CUTOFF)
    second = build_as_of_assembly(store, (inputs[1], inputs[0], inputs[1]), as_of=CUTOFF)

    assert first == second
    assert first.input_hash == second.input_hash
    assert first.run_uid == second.run_uid


def test_actor_target_refuses_rows_without_provenance_bound_actor_identifiers():
    assembly = _assembly_for(
        ((_market_pair(1, day=1), LabelValue.POSITIVE),),
        target=LabelTarget.ACTOR_EVIDENCE_C,
    )

    readiness = assess_readiness(assembly, plan=_small_plan(LabelTarget.ACTOR_EVIDENCE_C))

    assert "actor_identifiers_missing_for_actor_target" in readiness.reason_codes
    assert readiness.training_allowed is False


def test_single_test_label_never_emits_candidate_evaluated():
    assembly = _assembly_for(
        (
            (_market_pair(1, day=1), LabelValue.POSITIVE),
            (_market_pair(2, day=2), LabelValue.POSITIVE),
            (_market_pair(3, day=3), LabelValue.NEGATIVE),
            (_market_pair(4, day=5), LabelValue.POSITIVE),
        )
    )

    result = train_baseline_candidate(assembly, plan=_small_plan())

    assert result.status == "not_ready"
    assert result.readiness.test_rows == 1
    assert result.readiness.test_labeled_rows == 1
    assert "test_label_support_gate_not_met" in result.readiness.reason_codes
    assert result.evaluation == ()


def test_sufficient_predeclared_fixture_support_propagates_effectiveness_and_counts():
    assembly = _assembly_for(
        (
            (_market_pair(1, day=1), LabelValue.POSITIVE),
            (_market_pair(2, day=2), LabelValue.POSITIVE),
            (_market_pair(3, day=3), LabelValue.NEGATIVE),
            (_market_pair(4, day=5), LabelValue.POSITIVE),
            (_market_pair(5, day=6), LabelValue.NEGATIVE),
        )
    )

    result = train_baseline_candidate(assembly, plan=_small_plan())

    assert result.status == "candidate_evaluated"
    assert result.readiness.validation_labeled_rows == 2
    assert result.readiness.test_labeled_rows == 2
    assert len(result.evaluation) == 2
    assert all(row.derived_features == {"price_change": Decimal("0.15"), "volume": Decimal("10")} for row in assembly.rows)
    for summary in result.evaluation:
        assert summary.test_rows == 2
        assert summary.labeled_test_rows == 2
        assert summary.positive_test_rows == 1
        assert summary.negative_test_rows == 1
        assert summary.unknown_test_rows == 0
        assert summary.unmapped_test_rows == 0
        assert summary.effectiveness_status == "estimated_from_available_labels"
        assert "not proof" in summary.effectiveness_reason


def test_repeated_identical_inputs_cannot_inflate_support_counts():
    rows = (
        (_market_pair(1, day=1), LabelValue.POSITIVE),
        (_market_pair(2, day=2), LabelValue.POSITIVE),
        (_market_pair(3, day=3), LabelValue.NEGATIVE),
        (_market_pair(4, day=5), LabelValue.POSITIVE),
        (_market_pair(5, day=6), LabelValue.NEGATIVE),
    )
    facts = tuple(fact for sources, _ in rows for fact in sources)
    store = EventMemoryStore(facts)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    unique_inputs = tuple(
        _bound_feature(
            sources[-1],
            index=index,
            snapshot_uid=snapshot_uid,
            source_facts=sources,
            label=label,
        )
        for index, (sources, label) in enumerate(rows, 1)
    )

    assembly = build_as_of_assembly(store, unique_inputs * 201, as_of=CUTOFF)
    readiness = assess_readiness(assembly, plan=BaselinePlan(
        target=LabelTarget.ACTIVITY_A,
        train_end=T0 + timedelta(days=2),
        validation_end=T0 + timedelta(days=4),
    ))

    assert len(assembly.rows) == 5
    assert readiness.validation_labeled_rows == 2
    assert readiness.test_labeled_rows == 2
    assert readiness.status == "not_ready"


def test_conflicting_duplicate_feature_uid_is_rejected_before_assembly():
    pair = _market_pair(1, day=1)
    store = EventMemoryStore(pair)
    snapshot_uid = store.snapshot_as_of(CUTOFF).manifest.snapshot_uid
    original = _bound_feature(pair[-1], index=1, snapshot_uid=snapshot_uid, source_facts=pair)
    conflict = original.model_copy(update={"baseline_label": LabelValue.NEGATIVE})

    with pytest.raises(ValueError, match="conflicting duplicate feature_uid"):
        build_as_of_assembly(store, (original, conflict), as_of=CUTOFF)


def test_evaluation_support_and_counts_use_unique_row_uids():
    row = EvaluationRow(
        row_uid="evaluation:one",
        target=LabelTarget.ACTIVITY_A,
        label=LabelValue.POSITIVE,
        score=Decimal("0.5"),
        event_time=T0,
        market_uid="market:unique",
        event_cluster_uid="cluster:unique",
        platform="fixture-platform",
        category="fixture-category",
    )
    gate = CalibrationGate(min_labeled=2, min_positive=1, min_negative=1)

    reasons = gate.reasons((row,) * 201)
    report = evaluate(
        (row,) * 201,
        target=LabelTarget.ACTIVITY_A,
        analyst_capacity=1,
        calibration_gate=gate,
        bootstrap_samples=2,
    )

    assert "labeled=1 below 2" in reasons
    assert report.total_rows == 1
    assert report.labeled_rows == 1
    conflicting = row.model_copy(update={"label": LabelValue.NEGATIVE})
    with pytest.raises(ValueError, match="conflicting duplicate evaluation row_uid"):
        evaluate(
            (row, conflicting),
            target=LabelTarget.ACTIVITY_A,
            analyst_capacity=1,
        )


def test_plan_cannot_predeclare_a_one_row_label_support_gate():
    with pytest.raises(ValueError, match="greater than or equal to 2"):
        BaselinePlan(
            target=LabelTarget.ACTIVITY_A,
            train_end=T0 + timedelta(days=2),
            validation_end=T0 + timedelta(days=4),
            test_min_labeled=1,
        )
