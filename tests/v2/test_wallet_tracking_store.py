"""Mechanical persistence fixtures; these are never effectiveness evidence."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
from threading import Barrier

import pytest

from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill, TradeSide
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.wallet_tracking import (
    BackgroundCohortMarker,
    FollowUpStatus,
    WalletMarketScope,
    WalletSourceScope,
    WalletTrackingRequest,
    build_wallet_history_snapshot,
    enroll_wallet_watches,
    initial_follow_up_state,
)
from marketleak.wallet_tracking_store import (
    StoreWriteStatus,
    WalletFillConflictError,
    WalletFillIndex,
    WalletRecordConflictError,
    WalletStoreError,
    WalletTrackingStore,
)


NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
ACTOR_A = "polymarket:0xaaa"
ACTOR_B = "polymarket:0xbbb"


def _scope() -> WalletSourceScope:
    return WalletSourceScope(
        scope_uid="scope:polymarket-public-wallets",
        platform="polymarket",
        source_uids=("polymarket:data-api",),
        market_scope=WalletMarketScope.ALL_PLATFORM_MARKETS,
    )


def _cohort(scope: WalletSourceScope) -> BackgroundCohortMarker:
    return BackgroundCohortMarker(
        cohort_uid="cohort:mechanical-fixture-v1",
        selection_policy_uid="policy:mechanical-fixture-v1",
        scope_uid=scope.scope_uid,
        ascertained_at=NOW - timedelta(days=2),
        frozen_at=NOW - timedelta(days=1),
    )


def _fill(
    uid: str,
    *,
    actor_uid: str = ACTOR_A,
    event_time: datetime = NOW - timedelta(minutes=7),
    ingested_at: datetime | None = None,
) -> TradeFill:
    return TradeFill(
        fill_uid=f"polymarket:{uid}",
        market_uid="polymarket:market-1",
        outcome_uid="polymarket:yes",
        platform="polymarket",
        price=Decimal("0.55"),
        size=Decimal("10"),
        side=TradeSide.BUY,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid=actor_uid,
        transaction_uid=f"polymarket:tx-{uid}",
        event_time=event_time,
        ingested_at=ingested_at or event_time + timedelta(seconds=1),
        source_uid="polymarket:data-api",
        raw_artifact_uid=f"raw:{uid}",
        parser_version="mechanical-fixture-v1",
    )


def _registration():
    scope = _scope()
    request = WalletTrackingRequest(
        request_uid="request:persistence-fixture",
        incident_uid="incident:persistence-fixture",
        market_uid="polymarket:market-1",
        outcome_uid="polymarket:yes",
        incident_starts_at=NOW - timedelta(minutes=10),
        incident_ends_at=NOW - timedelta(minutes=5),
        trigger_event_time=NOW - timedelta(minutes=5),
        trigger_available_at=NOW - timedelta(minutes=4),
        as_of=NOW,
        trigger_source_uid="system:detector-v2",
        trigger_raw_artifact_uids=("raw:trigger-fixture",),
        source_scope=scope,
        background_cohort=_cohort(scope),
        pre_window=timedelta(days=30),
        post_window=timedelta(days=1),
        top_k=1,
        follow_up_interval=timedelta(hours=1),
        retry_delay=timedelta(minutes=5),
        max_retries=2,
        cooldown=timedelta(days=1),
    )
    return enroll_wallet_watches(request, [_fill("trigger")]).registrations[0]


def _canonical_fill_file(root: Path, fill: TradeFill, suffix: str = "") -> Path:
    path = root / f"{fill.fill_uid.replace(':', '-')}{suffix}.json"
    path.write_bytes(canonical_json_bytes(fill))
    return path


def _complete_snapshot(registration, uid: str):
    return build_wallet_history_snapshot(
        registration,
        [_fill("trigger")],
        snapshot_uid=uid,
        as_of=registration.expires_at,
        available_at=registration.expires_at,
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=registration.expires_at,
    )


def test_ledger_replays_registration_snapshot_and_state_after_restart(tmp_path: Path) -> None:
    registration = _registration()
    initial = initial_follow_up_state(registration)
    snapshot = build_wallet_history_snapshot(
        registration,
        [
            _fill("trigger"),
            _fill(
                "follow-up",
                event_time=NOW + timedelta(minutes=30),
                ingested_at=NOW + timedelta(minutes=31),
            ),
        ],
        snapshot_uid="snapshot:restart-fixture",
        as_of=NOW + timedelta(hours=2),
        available_at=NOW + timedelta(hours=2),
        coverage_status=CoverageStatus.COMPLETE,
        complete_through=NOW + timedelta(hours=2),
    )

    store = WalletTrackingStore(tmp_path / "ledger")
    store.append_registration(registration)
    store.append_history_snapshot(snapshot)
    store.append_follow_up_state(initial)

    restarted = WalletTrackingStore(tmp_path / "ledger")
    assert restarted.registrations() == (registration,)
    assert restarted.history_snapshots(registration.registration_uid) == (snapshot,)
    assert restarted.follow_up_states(registration.registration_uid) == (initial,)
    assert [item.sequence for item in restarted.entries()] == [1, 2, 3]


def test_exact_ledger_replay_is_idempotent_and_does_not_append(tmp_path: Path) -> None:
    registration = _registration()
    store = WalletTrackingStore(tmp_path)
    inserted = store.append_registration(registration)
    size_after_insert = store.ledger_path.stat().st_size

    replayed = store.append_registration(registration)

    assert inserted.status == StoreWriteStatus.INSERTED
    assert replayed.status == StoreWriteStatus.DUPLICATE
    assert replayed.entry.entry_hash == inserted.entry.entry_hash
    assert store.ledger_path.stat().st_size == size_after_insert
    assert len(store.entries()) == 1


def test_two_store_instances_coordinate_concurrent_hash_chain_appends(tmp_path: Path) -> None:
    root = tmp_path / "shared-ledger"
    first = WalletTrackingStore(root)
    second = WalletTrackingStore(root)
    registration_a = _registration()
    registration_b = replace(
        registration_a,
        registration_uid="wallet-watch:concurrent-b",
        request_uid="request:concurrent-b",
        actor_uid=ACTOR_B,
    )
    start = Barrier(3)

    def append_after_barrier(store: WalletTrackingStore, registration):
        start.wait(timeout=5)
        return store.append_registration(registration)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(append_after_barrier, first, registration_a)
        future_b = executor.submit(append_after_barrier, second, registration_b)
        start.wait(timeout=5)
        results = (future_a.result(timeout=10), future_b.result(timeout=10))

    restarted = WalletTrackingStore(root)
    entries = restarted.entries()
    assert {result.status for result in results} == {StoreWriteStatus.INSERTED}
    assert [entry.sequence for entry in entries] == [1, 2]
    assert entries[0].previous_hash == "0" * 64
    assert entries[1].previous_hash == entries[0].entry_hash
    assert {item.registration_uid for item in restarted.registrations()} == {
        registration_a.registration_uid,
        registration_b.registration_uid,
    }


def test_stale_store_instance_refreshes_before_sequential_append(tmp_path: Path) -> None:
    root = tmp_path / "shared-ledger"
    first = WalletTrackingStore(root)
    stale = WalletTrackingStore(root)
    registration_a = _registration()
    registration_b = replace(
        registration_a,
        registration_uid="wallet-watch:stale-b",
        request_uid="request:stale-b",
        actor_uid=ACTOR_B,
    )

    first.append_registration(registration_a)
    stale.append_registration(registration_b)

    restarted = WalletTrackingStore(root)
    assert [entry.sequence for entry in restarted.entries()] == [1, 2]
    assert restarted.entries()[1].previous_hash == restarted.entries()[0].entry_hash


def test_conflicting_same_uid_is_quarantined_and_original_remains_authoritative(tmp_path: Path) -> None:
    registration = _registration()
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)
    conflict = replace(registration, attributable_notional=registration.attributable_notional + Decimal("1"))

    with pytest.raises(WalletRecordConflictError) as captured:
        store.append_registration(conflict)

    assert captured.value.quarantine_path.exists()
    assert store.registrations() == (registration,)
    assert len(store.entries()) == 1
    assert list((tmp_path / "quarantine" / "uid-conflicts").glob("*.json")) == [
        captured.value.quarantine_path
    ]


def test_due_lookup_survives_restart_and_ignores_terminal_watch(tmp_path: Path) -> None:
    registration = _registration()
    initial = initial_follow_up_state(registration)
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)
    store.append_follow_up_state(initial)

    restarted = WalletTrackingStore(tmp_path)
    assert restarted.due_watches(registration.next_follow_up_at - timedelta(microseconds=1)) == ()
    due = restarted.due_watches(registration.next_follow_up_at)
    assert len(due) == 1
    assert due[0].registration == registration
    assert due[0].state == initial

    snapshot = _complete_snapshot(registration, "snapshot:terminal-fixture")
    restarted.append_history_snapshot(snapshot)
    terminal = replace(
        initial,
        state_uid="wallet-watch-state:terminal-fixture",
        status=FollowUpStatus.COMPLETE,
        as_of=registration.expires_at,
        available_at=registration.expires_at,
        next_due_at=None,
        coverage_status=CoverageStatus.COMPLETE,
        missing_reasons=(),
        last_snapshot_uid=snapshot.snapshot_uid,
    )
    restarted.append_follow_up_state(terminal)
    assert WalletTrackingStore(tmp_path).due_watches(registration.expires_at + timedelta(days=1)) == ()

    # An immutable older record remains an exact duplicate after later states.
    replayed = restarted.append_follow_up_state(initial)
    assert replayed.status == StoreWriteStatus.DUPLICATE
    assert len(restarted.follow_up_states(registration.registration_uid)) == 2


def test_terminal_follow_up_state_cannot_be_reactivated(tmp_path: Path) -> None:
    registration = _registration()
    initial = initial_follow_up_state(registration)
    snapshot = _complete_snapshot(registration, "snapshot:terminal-irreversible-fixture")
    terminal = replace(
        initial,
        state_uid="wallet-watch-state:terminal-irreversible-fixture",
        status=FollowUpStatus.COMPLETE,
        as_of=registration.expires_at,
        available_at=registration.expires_at,
        next_due_at=None,
        coverage_status=CoverageStatus.COMPLETE,
        missing_reasons=(),
        last_snapshot_uid=snapshot.snapshot_uid,
    )
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)
    store.append_follow_up_state(initial)
    store.append_history_snapshot(snapshot)
    store.append_follow_up_state(terminal)
    reactivated_at = terminal.as_of
    reactivated = replace(
        terminal,
        state_uid="wallet-watch-state:invalid-reactivation-fixture",
        status=FollowUpStatus.SCHEDULED,
        as_of=reactivated_at,
        available_at=reactivated_at,
        next_due_at=reactivated_at,
        coverage_status=CoverageStatus.UNKNOWN,
        missing_reasons=("invalid_reactivation_fixture",),
    )

    with pytest.raises(WalletStoreError, match="irreversible"):
        store.append_follow_up_state(reactivated)

    restarted = WalletTrackingStore(tmp_path)
    assert restarted.latest_follow_up_state(registration.registration_uid) == terminal
    assert restarted.due_watches(registration.expires_at) == ()


def test_complete_state_requires_persisted_expiry_covering_snapshot(tmp_path: Path) -> None:
    registration = _registration()
    initial = initial_follow_up_state(registration)
    unsupported_complete = replace(
        initial,
        state_uid="wallet-watch-state:unsupported-complete-fixture",
        status=FollowUpStatus.COMPLETE,
        as_of=registration.next_follow_up_at,
        available_at=registration.next_follow_up_at,
        next_due_at=None,
        coverage_status=CoverageStatus.COMPLETE,
        missing_reasons=(),
    )
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)
    store.append_follow_up_state(initial)

    with pytest.raises(WalletStoreError, match="complete through watch expiry"):
        store.append_follow_up_state(unsupported_complete)

    assert store.latest_follow_up_state(registration.registration_uid) == initial
    assert len(store.due_watches(registration.next_follow_up_at)) == 1


def test_terminal_states_require_their_expiry_or_retry_condition(tmp_path: Path) -> None:
    registration = _registration()
    initial = initial_follow_up_state(registration)
    premature_expiry = replace(
        initial,
        state_uid="wallet-watch-state:premature-expiry-fixture",
        status=FollowUpStatus.EXPIRED,
        as_of=registration.next_follow_up_at,
        available_at=registration.next_follow_up_at,
        next_due_at=None,
    )
    unexhausted = replace(
        initial,
        state_uid="wallet-watch-state:unexhausted-fixture",
        status=FollowUpStatus.RETRIES_EXHAUSTED,
        as_of=registration.next_follow_up_at,
        available_at=registration.next_follow_up_at,
        next_due_at=None,
        retry_count=registration.max_retries,
    )
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)
    store.append_follow_up_state(initial)

    with pytest.raises(WalletStoreError, match="expiry clock"):
        store.append_follow_up_state(premature_expiry)
    with pytest.raises(WalletStoreError, match="retry budget"):
        store.append_follow_up_state(unexhausted)

    assert store.latest_follow_up_state(registration.registration_uid) == initial
    assert len(store.due_watches(registration.next_follow_up_at)) == 1


def test_history_snapshot_cannot_widen_registered_collection_window(tmp_path: Path) -> None:
    registration = _registration()
    snapshot = _complete_snapshot(registration, "snapshot:bounded-window-fixture")
    widened = replace(
        snapshot,
        window_starts_at=registration.history_window_starts_at - timedelta(days=1),
    )
    store = WalletTrackingStore(tmp_path)
    store.append_registration(registration)

    with pytest.raises(WalletStoreError, match="bounded history window"):
        store.append_history_snapshot(widened)

    assert store.history_snapshots(registration.registration_uid) == ()


def test_actor_index_is_restart_durable_and_exact_replay_is_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "wallet-fills.sqlite3"
    fill = _fill("indexed")
    normalized_path = _canonical_fill_file(tmp_path, fill)
    index = WalletFillIndex(database_path)

    inserted = index.index_fill(fill, normalized_path)
    replayed = WalletFillIndex(database_path).index_fill(fill, normalized_path)

    assert inserted.status == StoreWriteStatus.INSERTED
    assert replayed.status == StoreWriteStatus.DUPLICATE
    assert replayed.reference == inserted.reference
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM wallet_fill_index").fetchone()[0] == 1
        columns = {row[1] for row in connection.execute("PRAGMA table_info(wallet_fill_index)")}
    assert not {"price", "size", "side", "payload", "maker", "taker"} & columns


def test_actor_index_conflicting_fill_uid_is_recorded_and_fails_closed(tmp_path: Path) -> None:
    index = WalletFillIndex(tmp_path / "wallet-fills.sqlite3")
    original = _fill("conflict", actor_uid=ACTOR_A)
    conflicting = _fill("conflict", actor_uid=ACTOR_B)
    original_path = _canonical_fill_file(tmp_path, original, "-original")
    conflicting_path = _canonical_fill_file(tmp_path, conflicting, "-incoming")
    index.index_fill(original, original_path)

    with pytest.raises(WalletFillConflictError):
        index.index_fill(conflicting, conflicting_path)

    assert index.conflict_count(original.fill_uid) == 1
    rows = index.query_actor(
        ACTOR_A,
        start=NOW - timedelta(days=1),
        end=NOW,
        as_of=NOW,
    )
    assert [item.actor_uid for item in rows] == [ACTOR_A]
    assert index.query_actor(
        ACTOR_B,
        start=NOW - timedelta(days=1),
        end=NOW,
        as_of=NOW,
    ) == ()


def test_actor_as_of_range_query_excludes_late_availability_and_other_actors(tmp_path: Path) -> None:
    index = WalletFillIndex(tmp_path / "wallet-fills.sqlite3")
    before = _fill("before", event_time=NOW - timedelta(hours=3))
    in_range = _fill("in-range", event_time=NOW - timedelta(hours=1))
    after = _fill("after", event_time=NOW + timedelta(minutes=1))
    late = _fill(
        "late",
        event_time=NOW - timedelta(minutes=30),
        ingested_at=NOW + timedelta(minutes=1),
    )
    other_actor = _fill("other-actor", actor_uid=ACTOR_B, event_time=NOW - timedelta(minutes=15))
    for fill in (before, in_range, after, late, other_actor):
        index.index_fill(fill, _canonical_fill_file(tmp_path, fill))

    references = index.query_actor(
        ACTOR_A,
        start=NOW - timedelta(hours=2),
        end=NOW + timedelta(hours=1),
        as_of=NOW,
    )

    assert [item.fill_uid for item in references] == [in_range.fill_uid]
    assert references[0].normalized_path.read_bytes() == canonical_json_bytes(in_range)
    assert all(item.actor_uid == ACTOR_A for item in references)
