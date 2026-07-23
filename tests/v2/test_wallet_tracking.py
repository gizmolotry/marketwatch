"""Mechanical wallet-watch contract fixtures; never effectiveness evidence."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

import pytest

from marketleak.domain import (
    ActorVisibility,
    CoverageStatus,
    ObservationKind,
    Outcome,
    PriceObservation,
    TradeFill,
    TradeSide,
)
from marketleak.wallet_tracking import (
    BackgroundCohortMarker,
    BehaviorMetricMetadata,
    EnrollmentDecisionStatus,
    FollowUpStatus,
    WalletMarketScope,
    WalletSourceScope,
    WalletTrackingRequest,
    advance_follow_up_state,
    build_wallet_behavior_output,
    build_wallet_history_snapshot,
    enroll_wallet_watches,
    initial_follow_up_state,
)
from marketleak.ingestion.normalize import canonical_json_bytes


NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)


def source_scope() -> WalletSourceScope:
    return WalletSourceScope(
        scope_uid="scope:polymarket-public-data",
        platform="polymarket",
        source_uids=("polymarket:data-api",),
        market_scope=WalletMarketScope.ALL_PLATFORM_MARKETS,
    )


def cohort(scope: WalletSourceScope | None = None) -> BackgroundCohortMarker:
    selected = scope or source_scope()
    return BackgroundCohortMarker(
        cohort_uid="cohort:frozen-background-v1",
        selection_policy_uid="policy:background-v1",
        scope_uid=selected.scope_uid,
        ascertained_at=NOW - timedelta(days=2),
        frozen_at=NOW - timedelta(days=1),
    )


def request(
    *,
    now: datetime = NOW,
    top_k: int = 2,
    max_retries: int = 1,
    cooldown: timedelta = timedelta(days=1),
) -> WalletTrackingRequest:
    scope = source_scope()
    return WalletTrackingRequest(
        request_uid=f"request:{int(now.timestamp())}",
        incident_uid=f"incident:{int(now.timestamp())}",
        market_uid="polymarket:market-1",
        outcome_uid="polymarket:yes",
        incident_starts_at=now - timedelta(minutes=10),
        incident_ends_at=now - timedelta(minutes=5),
        trigger_event_time=now - timedelta(minutes=5),
        trigger_available_at=now - timedelta(minutes=4),
        as_of=now,
        trigger_source_uid="system:detector-v2",
        trigger_raw_artifact_uids=(f"raw:trigger-{int(now.timestamp())}",),
        source_scope=scope,
        background_cohort=cohort(scope),
        pre_window=timedelta(days=30),
        post_window=timedelta(days=1),
        top_k=top_k,
        follow_up_interval=timedelta(hours=1),
        retry_delay=timedelta(minutes=5),
        max_retries=max_retries,
        cooldown=cooldown,
    )


def fill(
    uid: str,
    *,
    actor_uid: str | None,
    event_time: datetime,
    ingested_at: datetime | None = None,
    price: str = "0.50",
    size: str = "10",
    visibility: ActorVisibility = ActorVisibility.PUBLIC_WALLET,
    market_uid: str = "polymarket:market-1",
    source_uid: str = "polymarket:data-api",
) -> TradeFill:
    return TradeFill(
        fill_uid=f"polymarket:{uid}",
        market_uid=market_uid,
        outcome_uid="polymarket:yes",
        platform="polymarket",
        price=Decimal(price),
        size=Decimal(size),
        side=TradeSide.BUY,
        actor_visibility=visibility,
        actor_uid=actor_uid,
        transaction_uid=f"polymarket:tx-{uid}",
        event_time=event_time,
        ingested_at=ingested_at or event_time + timedelta(seconds=1),
        source_uid=source_uid,
        raw_artifact_uid=f"raw:{uid}",
        parser_version="fixture-v1",
    )


def enrolled_registration(*, max_retries: int = 1, cooldown: timedelta = timedelta(days=1)):
    enrollment_request = request(top_k=1, max_retries=max_retries, cooldown=cooldown)
    result = enroll_wallet_watches(
        enrollment_request,
        [
            fill(
                "trigger-fill",
                actor_uid="polymarket:0xaaa",
                event_time=NOW - timedelta(minutes=7),
            )
        ],
    )
    return result.registrations[0]


def test_enrollment_uses_only_genuine_causal_public_wallet_fills_and_stable_top_k() -> None:
    enrollment_request = request(top_k=1)
    price_only = PriceObservation(
        observation_uid="polymarket:price-only",
        market_uid="polymarket:market-1",
        outcome_uid="polymarket:yes",
        platform="polymarket",
        price=Decimal("0.90"),
        kind=ObservationKind.LAST_TRADE,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        event_time=NOW - timedelta(minutes=7),
        ingested_at=NOW - timedelta(minutes=6),
        source_uid="polymarket:data-api",
        raw_artifact_uid="raw:price-only",
        parser_version="fixture-v1",
    )
    result = enroll_wallet_watches(
        enrollment_request,
        [
            price_only,
            fill("b", actor_uid="polymarket:0xbbb", event_time=NOW - timedelta(minutes=7)),
            fill("a", actor_uid="polymarket:0xaaa", event_time=NOW - timedelta(minutes=7)),
            fill(
                "wrong-source",
                actor_uid="polymarket:0xccc",
                event_time=NOW - timedelta(minutes=7),
                size="1000",
                source_uid="polymarket:unapproved-source",
            ),
        ],
    )

    assert [item.actor_uid for item in result.registrations] == ["polymarket:0xaaa"]
    assert result.registrations[0].attributable_notional == Decimal("5.00")
    assert result.registrations[0].fill_lineage[0].fill_uid == "polymarket:a"
    assert result.decisions[0].rank == 1
    assert result.decisions[0].status == EnrollmentDecisionStatus.ENROLLED


def test_no_actor_or_late_actor_has_explicit_no_watch_reasons() -> None:
    enrollment_request = request()
    result = enroll_wallet_watches(
        enrollment_request,
        [
            fill(
                "unattributed",
                actor_uid=None,
                visibility=ActorVisibility.NOT_AVAILABLE,
                event_time=NOW - timedelta(minutes=8),
            ),
            fill(
                "late-public",
                actor_uid="polymarket:0xaaa",
                event_time=NOW - timedelta(minutes=7),
                ingested_at=NOW + timedelta(minutes=1),
            ),
        ],
    )

    assert result.registrations == ()
    assert result.late_fill_count == 1
    assert result.actor_unavailable_fill_count == 1
    assert result.no_watch_reasons == (
        "fills_not_available_by_trigger_as_of",
        "actor_not_available_on_incident_fills",
    )


def test_enrollment_excludes_a_different_outcome_in_the_same_market() -> None:
    wrong_outcome = fill(
        "wrong-outcome",
        actor_uid="polymarket:0xaaa",
        event_time=NOW - timedelta(minutes=7),
    ).model_copy(update={"outcome_uid": "polymarket:no"})
    result = enroll_wallet_watches(request(top_k=1), [wrong_outcome])

    assert result.registrations == ()
    assert result.incident_fill_count == 0
    assert result.no_watch_reasons == ("no_trade_fills_in_incident_window",)


def test_duplicate_and_cooldown_do_not_mutate_or_replace_existing_watch() -> None:
    existing = enrolled_registration(cooldown=timedelta(days=1))
    active_time = NOW + timedelta(hours=1)
    duplicate_result = enroll_wallet_watches(
        request(now=active_time, top_k=1),
        [fill("duplicate", actor_uid=existing.actor_uid, event_time=active_time - timedelta(minutes=7))],
        existing_registrations=[existing],
    )
    assert duplicate_result.registrations == ()
    assert duplicate_result.decisions[0].status == EnrollmentDecisionStatus.DUPLICATE_ACTIVE_WATCH
    assert duplicate_result.decisions[0].registration_uid == existing.registration_uid

    cooldown_time = existing.expires_at + timedelta(hours=1)
    cooldown_result = enroll_wallet_watches(
        request(now=cooldown_time, top_k=1),
        [fill("cooldown", actor_uid=existing.actor_uid, event_time=cooldown_time - timedelta(minutes=7))],
        existing_registrations=[existing],
    )
    assert cooldown_result.registrations == ()
    assert cooldown_result.decisions[0].status == EnrollmentDecisionStatus.COOLDOWN_ACTIVE
    assert existing.enrolled_at == NOW


def test_late_backfill_is_follow_up_context_and_cannot_support_original_trigger() -> None:
    registration = enrolled_registration()
    causal = fill(
        "causal-history",
        actor_uid=registration.actor_uid,
        event_time=NOW - timedelta(days=1),
        ingested_at=NOW - timedelta(hours=1),
    )
    late_backfill = fill(
        "late-backfill",
        actor_uid=registration.actor_uid,
        event_time=NOW - timedelta(days=2),
        ingested_at=NOW + timedelta(hours=1),
    )
    later_trade = fill(
        "later-trade",
        actor_uid=registration.actor_uid,
        event_time=NOW + timedelta(hours=1),
        ingested_at=NOW + timedelta(hours=1, minutes=1),
        market_uid="polymarket:market-2",
    )
    snapshot_as_of = NOW + timedelta(hours=2)
    snapshot = build_wallet_history_snapshot(
        registration,
        [causal, late_backfill, later_trade],
        snapshot_uid="wallet-history:snapshot-1",
        as_of=snapshot_as_of,
        available_at=snapshot_as_of,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=snapshot_as_of,
    )

    assert snapshot.trigger_eligible_fill_uids == ("polymarket:causal-history",)
    assert snapshot.follow_up_only_fill_uids == (
        "polymarket:late-backfill",
        "polymarket:later-trade",
    )
    assert snapshot.supports_original_trigger is False
    assert snapshot.complete is True


def test_incomplete_history_requires_continuation_or_missingness() -> None:
    registration = enrolled_registration()
    with pytest.raises(ValueError, match="requires continuation"):
        build_wallet_history_snapshot(
            registration,
            [],
            snapshot_uid="wallet-history:invalid",
            as_of=NOW,
            available_at=NOW,
            coverage_status=CoverageStatus.PARTIAL,
        )
    partial = build_wallet_history_snapshot(
        registration,
        [],
        snapshot_uid="wallet-history:partial",
        as_of=NOW,
        available_at=NOW,
        coverage_status=CoverageStatus.PARTIAL,
        continuation_token="page:next",
    )
    assert partial.complete is False


def test_follow_up_state_is_immutable_and_enforces_retry_and_expiry() -> None:
    registration = enrolled_registration(max_retries=1)
    initial = initial_follow_up_state(registration)
    first_attempt = NOW + timedelta(minutes=5)
    retry = advance_follow_up_state(
        registration,
        initial,
        attempted_at=first_attempt,
        available_at=first_attempt,
        retryable_error="source_timeout",
    )
    assert retry.status == FollowUpStatus.RETRY_SCHEDULED
    assert retry.retry_count == 1
    exhausted = advance_follow_up_state(
        registration,
        retry,
        attempted_at=first_attempt + timedelta(minutes=5),
        available_at=first_attempt + timedelta(minutes=5),
        retryable_error="source_timeout",
    )
    assert exhausted.status == FollowUpStatus.RETRIES_EXHAUSTED
    assert exhausted.next_due_at is None
    assert initial.retry_count == 0

    expired = advance_follow_up_state(
        registration,
        initial,
        attempted_at=registration.expires_at,
        available_at=registration.expires_at,
        retryable_error="source_timeout",
    )
    assert expired.status == FollowUpStatus.EXPIRED


def test_terminal_complete_state_copies_snapshot_coverage_metadata() -> None:
    registration = enrolled_registration()
    initial = initial_follow_up_state(registration)
    complete = build_wallet_history_snapshot(
        registration,
        [],
        snapshot_uid="wallet-history:terminal-complete",
        as_of=registration.expires_at,
        available_at=registration.expires_at,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=registration.expires_at,
    )
    terminal = advance_follow_up_state(
        registration,
        initial,
        attempted_at=registration.expires_at,
        available_at=registration.expires_at,
        snapshot=complete,
    )

    assert terminal.status == FollowUpStatus.COMPLETE
    assert terminal.coverage_status == CoverageStatus.COMPLETE
    assert terminal.continuation_token is None
    assert terminal.missing_reasons == ()
    assert terminal.last_snapshot_uid == complete.snapshot_uid


def test_behavior_output_reuses_causal_features_and_has_no_fraud_probability() -> None:
    registration = enrolled_registration()
    history_fill = fill(
        "history",
        actor_uid=registration.actor_uid,
        event_time=NOW - timedelta(days=1),
        ingested_at=NOW - timedelta(hours=1),
    )
    snapshot = build_wallet_history_snapshot(
        registration,
        [history_fill],
        snapshot_uid="wallet-history:behavior",
        as_of=NOW,
        available_at=NOW,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=NOW,
    )
    metrics = (
        BehaviorMetricMetadata("behavior_novelty", Decimal("2.5"), "method:novelty-v1", "available"),
        BehaviorMetricMetadata("case_resemblance", Decimal("0.7"), "method:retrieval-v1", "available"),
        BehaviorMetricMetadata("review_priority", Decimal("4"), "policy:priority-v1", "available"),
    )
    output = build_wallet_behavior_output(snapshot, [history_fill], metrics=metrics)

    assert output.actor_features is not None
    assert output.actor_features.fill_count == 1
    assert output.background_cohort.cohort_uid == "cohort:frozen-background-v1"
    assert all(metric.is_probability is False for metric in output.metrics)
    assert not hasattr(output, "fraud_probability")
    assert output.effectiveness_unknown is True


def test_behavior_output_rejects_same_fill_uid_with_different_canonical_content() -> None:
    registration = enrolled_registration()
    history_fill = fill(
        "hash-bound-history",
        actor_uid=registration.actor_uid,
        event_time=NOW - timedelta(days=1),
        ingested_at=NOW - timedelta(hours=1),
    )
    snapshot = build_wallet_history_snapshot(
        registration,
        [history_fill],
        snapshot_uid="wallet-history:hash-bound",
        as_of=NOW,
        available_at=NOW,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=NOW,
    )
    assert snapshot.fill_lineage[0].canonical_sha256 == sha256(
        canonical_json_bytes(history_fill)
    ).hexdigest()
    altered = history_fill.model_copy(update={"price": Decimal("0.75")})

    with pytest.raises(ValueError, match="does not match its snapshot canonical hash"):
        build_wallet_behavior_output(snapshot, [altered])


def test_late_ingested_resolution_cannot_create_realized_performance() -> None:
    registration = enrolled_registration()
    history_fill = fill(
        "resolution-history",
        actor_uid=registration.actor_uid,
        event_time=NOW - timedelta(days=2),
        ingested_at=NOW - timedelta(days=2) + timedelta(seconds=1),
    )
    snapshot = build_wallet_history_snapshot(
        registration,
        [history_fill],
        snapshot_uid="wallet-history:resolution-cutoff",
        as_of=NOW,
        available_at=NOW,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=NOW,
    )
    late_resolution = Outcome(
        outcome_uid="polymarket:yes",
        market_uid="polymarket:market-1",
        platform="polymarket",
        source_outcome_id="yes",
        label="Yes",
        resolved_value=Decimal("1"),
        resolved_at=NOW - timedelta(hours=1),
        event_time=NOW - timedelta(hours=1),
        ingested_at=NOW + timedelta(hours=1),
        source_uid="polymarket:data-api",
        raw_artifact_uid="raw:late-resolution",
        parser_version="fixture-v1",
    )
    output = build_wallet_behavior_output(snapshot, [history_fill], outcomes=[late_resolution])

    assert output.actor_features is not None
    assert output.actor_features.resolved_fill_count == 0
    assert output.actor_features.realized_performance is None


def test_background_cohort_must_be_frozen_before_trigger() -> None:
    scope = source_scope()
    late_cohort = BackgroundCohortMarker(
        cohort_uid="cohort:late",
        selection_policy_uid="policy:late",
        scope_uid=scope.scope_uid,
        ascertained_at=NOW - timedelta(hours=1),
        frozen_at=NOW,
    )
    base = request()
    with pytest.raises(ValueError, match="frozen before the trigger"):
        WalletTrackingRequest(
            request_uid="request:late-cohort",
            incident_uid=base.incident_uid,
            market_uid=base.market_uid,
            outcome_uid=base.outcome_uid,
            incident_starts_at=base.incident_starts_at,
            incident_ends_at=base.incident_ends_at,
            trigger_event_time=base.trigger_event_time,
            trigger_available_at=base.trigger_available_at,
            as_of=base.as_of,
            trigger_source_uid=base.trigger_source_uid,
            trigger_raw_artifact_uids=base.trigger_raw_artifact_uids,
            source_scope=scope,
            background_cohort=late_cohort,
            pre_window=base.pre_window,
            post_window=base.post_window,
            top_k=base.top_k,
            follow_up_interval=base.follow_up_interval,
            retry_delay=base.retry_delay,
            max_retries=base.max_retries,
            cooldown=base.cooldown,
        )
