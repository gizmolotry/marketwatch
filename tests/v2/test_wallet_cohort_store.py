"""Mechanical fixtures for the exact-fact cohort index, not effectiveness evidence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
from pathlib import Path
import sqlite3

import pytest

from marketleak.actors import build_actor_features
from marketleak.domain import ActorVisibility, TradeFill, TradeSide
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.wallet_cohort_store import (
    CanonicalFileError,
    FillConflictError,
    IndexedFileCorruptionError,
    CohortStoreError,
    WalletCohortStore,
)


NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)


def _fill(
    uid: str,
    *,
    actor: str = "polymarket:wallet/a",
    market: str = "polymarket:market/one",
    outcome: str = "polymarket:outcome/yes",
    event_time: datetime | None = None,
    ingested_at: datetime | None = None,
    price: str = "0.123456789123456789",
    size: str = "1000000000000000000.000000001",
    side: TradeSide = TradeSide.BUY,
    transaction_uid: str | None = None,
) -> TradeFill:
    occurred = event_time or NOW - timedelta(hours=2)
    available = ingested_at or occurred + timedelta(seconds=1)
    return TradeFill(
        fill_uid=f"polymarket:fill/{uid}",
        market_uid=market,
        outcome_uid=outcome,
        platform="polymarket",
        price=Decimal(price),
        size=Decimal(size),
        side=side,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid=actor,
        transaction_uid=transaction_uid or f"polymarket:tx/{uid}",
        event_time=occurred,
        ingested_at=available,
        source_uid="polymarket:source/data-api-trades",
        raw_artifact_uid=f"polymarket:raw/{uid}",
        parser_version="mechanical-fixture-v1",
    )


def _write(root: Path, fill: TradeFill) -> Path:
    path = root / f"{sha256(fill.fill_uid.encode('utf-8')).hexdigest()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(fill))
    return path


def _build(tmp_path: Path, fills: list[TradeFill]) -> tuple[WalletCohortStore, Path]:
    root = tmp_path / "canonical"
    for fill in fills:
        _write(root, fill)
    return WalletCohortStore.rebuild(tmp_path / "cohort.sqlite3", [root], batch_size=2), root


def test_rebuild_is_idempotent_and_exact_duplicate_files_are_counted(tmp_path: Path) -> None:
    fill = _fill("one")
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write(first, fill)
    _write(second, fill)

    store = WalletCohortStore.rebuild(tmp_path / "cohort.sqlite3", [second, first])
    manifest = store.manifest()

    assert manifest.source_file_count == 2
    assert manifest.indexed_fill_count == 1
    assert manifest.duplicate_file_count == 1
    assert len(tuple(store.iter_fills(as_of=NOW))) == 1


def test_same_uid_with_different_canonical_bytes_fails_closed(tmp_path: Path) -> None:
    original = _fill("conflict", price="0.4")
    conflict = original.model_copy(update={"price": Decimal("0.6")})
    first = _write(tmp_path / "first", original)
    second = _write(tmp_path / "second", conflict)
    store = WalletCohortStore(tmp_path / "index.sqlite3")

    store.add_files([first])
    with pytest.raises(FillConflictError) as error:
        store.add_files([second])

    assert error.value.fill_uid == original.fill_uid
    assert len(tuple(store.iter_fills(as_of=NOW))) == 1


def test_decimal_strings_round_trip_without_sqlite_real_loss(tmp_path: Path) -> None:
    fill = _fill("decimal")
    store, _ = _build(tmp_path, [fill])

    indexed = tuple(store.iter_fills(as_of=NOW))[0]

    assert indexed.price == fill.price
    assert indexed.size == fill.size
    with localcontext() as context:
        context.prec = len(fill.price.as_tuple().digits) + len(fill.size.as_tuple().digits)
        expected_notional = fill.price * fill.size
    assert indexed.notional == expected_notional
    assert isinstance(indexed.notional, Decimal)


def test_query_enforces_event_and_ingestion_clocks(tmp_path: Path) -> None:
    eligible = _fill("eligible")
    late_observed = _fill(
        "late-observed",
        event_time=NOW - timedelta(hours=3),
        ingested_at=NOW + timedelta(seconds=1),
    )
    future_event = _fill(
        "future-event",
        event_time=NOW + timedelta(hours=1),
        ingested_at=NOW + timedelta(hours=1, seconds=1),
    )
    store, _ = _build(tmp_path, [eligible, late_observed, future_event])

    assert [item.fill_uid for item in store.iter_fills(as_of=NOW)] == [eligible.fill_uid]


def test_cursor_pagination_has_no_silent_truncation(tmp_path: Path) -> None:
    fills = [
        _fill(
            f"page-{index}",
            event_time=NOW - timedelta(minutes=10 - index),
            ingested_at=NOW - timedelta(minutes=10 - index) + timedelta(seconds=1),
        )
        for index in range(7)
    ]
    store, _ = _build(tmp_path, fills)

    first = store.page(as_of=NOW, page_size=3)
    second = store.page(as_of=NOW, page_size=3, cursor=first.next_cursor)
    all_items = tuple(store.iter_fills(as_of=NOW, page_size=2))

    assert first.has_more and first.next_cursor is not None
    assert second.has_more and second.next_cursor is not None
    assert len(all_items) == 7
    assert len({item.fill_uid for item in all_items}) == 7


def test_streaming_aggregation_matches_existing_actor_feature_formulas(tmp_path: Path) -> None:
    common = dict(actor="polymarket:wallet/a", transaction_uid="polymarket:tx/shared")
    first = _fill("aggregate-1", price="0.4", size="10", **common)
    # A different UID with the same semantic fill must be de-duplicated exactly
    # as the existing actor feature builder does.
    replay = first.model_copy(update={"fill_uid": "polymarket:fill/aggregate-replay"})
    sell = _fill(
        "aggregate-2",
        actor="polymarket:wallet/a",
        outcome="polymarket:outcome/no",
        price="0.7",
        size="3",
        side=TradeSide.SELL,
    )
    other = _fill("aggregate-3", actor="polymarket:wallet/b", price="0.9", size="2")
    fills = [first, replay, sell, other]
    store, _ = _build(tmp_path, fills)

    expected = build_actor_features(fills, cutoff=NOW)
    actual = store.actor_features(cutoff=NOW)

    assert actual == expected


def test_manifest_snapshot_is_deterministic_across_rebuilds(tmp_path: Path) -> None:
    root = tmp_path / "canonical"
    _write(root, _fill("manifest-b"))
    _write(root, _fill("manifest-a"))

    first = WalletCohortStore.rebuild(tmp_path / "one.sqlite3", [root], batch_size=1).manifest()
    second = WalletCohortStore.rebuild(tmp_path / "two.sqlite3", [root], batch_size=50).manifest()

    assert first == second


def test_noncanonical_mutation_is_rejected_and_post_index_mutation_is_detected(tmp_path: Path) -> None:
    fill = _fill("mutated")
    source = _write(tmp_path / "canonical", fill)
    raw = source.read_bytes()
    source.write_bytes(raw + b"\n")

    with pytest.raises(CanonicalFileError):
        WalletCohortStore.rebuild(tmp_path / "rejected.sqlite3", [tmp_path / "canonical"])

    source.write_bytes(raw)
    store = WalletCohortStore.rebuild(tmp_path / "accepted.sqlite3", [tmp_path / "canonical"])
    source.write_bytes(raw + b" ")
    with pytest.raises(IndexedFileCorruptionError):
        store.verify_source_files()


def test_sealed_store_blocks_rows_and_detects_index_mutation(tmp_path: Path) -> None:
    fill = _fill("sealed")
    store, root = _build(tmp_path, [fill])

    with pytest.raises(CohortStoreError):
        store.add_files([next(root.glob("*.json"))])
    with sqlite3.connect(store.database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("UPDATE fills SET price_decimal='0.1'")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "INSERT INTO conflicts VALUES('x','a','b','c')"
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("UPDATE source_inventory SET source_root='tampered'")
        connection.execute("DROP INDEX fills_market_clocks")

    with pytest.raises(IndexedFileCorruptionError, match="index"):
        store.verify_integrity()


def test_actor_aggregation_applies_declared_scope_and_separate_availability_cutoff(tmp_path: Path) -> None:
    inside = _fill(
        "scope-inside",
        event_time=NOW - timedelta(hours=2),
        ingested_at=NOW - timedelta(hours=1),
        price="0.4",
        size="10",
    )
    late = _fill(
        "scope-late",
        event_time=NOW - timedelta(hours=2),
        ingested_at=NOW + timedelta(seconds=1),
    )
    other_market = _fill(
        "scope-market",
        market="polymarket:market/two",
        event_time=NOW - timedelta(hours=2),
    )
    other_source = _fill("scope-source", event_time=NOW - timedelta(hours=2)).model_copy(
        update={"source_uid": "polymarket:source/other"}
    )
    store, _ = _build(tmp_path, [inside, late, other_market, other_source])

    actual = store.actor_features(
        cutoff=NOW,
        available_as_of=NOW,
        start=NOW - timedelta(hours=3),
        end=NOW - timedelta(minutes=30),
        market_uid=inside.market_uid,
        source_uid=inside.source_uid,
    )

    assert actual == build_actor_features([inside], cutoff=NOW)


def test_semantic_dedup_uses_decimal_equality_not_text_spelling(tmp_path: Path) -> None:
    first = _fill("decimal-key-a", price="0.4", size="10.0", transaction_uid="polymarket:tx/equal")
    equal = first.model_copy(
        update={
            "fill_uid": "polymarket:fill/decimal-key-b",
            "price": Decimal("0.40"),
            "size": Decimal("10.00"),
        }
    )
    store, _ = _build(tmp_path, [first, equal])

    actual = store.actor_features(cutoff=NOW)

    assert actual == build_actor_features([first, equal], cutoff=NOW)
    assert actual[0].fill_count == 1


def test_source_audit_includes_duplicate_input_paths(tmp_path: Path) -> None:
    fill = _fill("duplicate-audit")
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write(first, fill)
    duplicate_path = _write(second, fill)
    store = WalletCohortStore.rebuild(tmp_path / "cohort.sqlite3", [first, second])

    assert store.verify_source_files() == 2
    duplicate_path.unlink()
    with pytest.raises(IndexedFileCorruptionError):
        store.verify_source_files()


def test_reopening_damaged_sealed_database_does_not_repair_schema(tmp_path: Path) -> None:
    store, _ = _build(tmp_path, [_fill("no-repair")])
    with sqlite3.connect(store.database_path) as connection:
        connection.execute("DROP INDEX fills_source_clocks")

    with pytest.raises(IndexedFileCorruptionError):
        WalletCohortStore(store.database_path)
    with sqlite3.connect(store.database_path) as connection:
        names = {row[1] for row in connection.execute("PRAGMA index_list('fills')")}
    assert "fills_source_clocks" not in names


def test_reopening_database_with_deleted_trigger_fails_without_repair(tmp_path: Path) -> None:
    store, _ = _build(tmp_path, [_fill("no-trigger-repair")])
    with sqlite3.connect(store.database_path) as connection:
        connection.execute("DROP TRIGGER sealed_inventory_update")

    with pytest.raises(IndexedFileCorruptionError):
        WalletCohortStore(store.database_path)
    with sqlite3.connect(store.database_path) as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert "sealed_inventory_update" not in names


def test_exact_product_is_independent_of_ambient_decimal_precision(tmp_path: Path) -> None:
    fill = _fill("precision-independent")
    with localcontext() as context:
        context.prec = 6
        store, _ = _build(tmp_path, [fill])
        observed = tuple(store.iter_fills(as_of=NOW))[0].notional
    with localcontext() as context:
        context.prec = len(fill.price.as_tuple().digits) + len(fill.size.as_tuple().digits)
        exact = fill.price * fill.size

    assert observed == exact


def test_tied_clock_cursor_is_total_and_lossless(tmp_path: Path) -> None:
    tied = NOW - timedelta(minutes=1)
    fills = [
        _fill(f"tied-{index}", event_time=tied, ingested_at=tied + timedelta(seconds=1))
        for index in range(5)
    ]
    store, _ = _build(tmp_path, fills)

    items = tuple(store.iter_fills(as_of=NOW, page_size=1))

    assert len(items) == 5
    assert [item.fill_uid for item in items] == sorted(fill.fill_uid for fill in fills)


def test_missing_source_paths_are_wrapped_as_cohort_errors(tmp_path: Path) -> None:
    store = WalletCohortStore(tmp_path / "unsealed.sqlite3")

    with pytest.raises(CanonicalFileError, match="canonical source file"):
        store.add_files([tmp_path / "missing.json"])
    with pytest.raises(CanonicalFileError, match="canonical source root"):
        WalletCohortStore.rebuild(tmp_path / "rebuilt.sqlite3", [tmp_path / "missing-root"])


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("schema_version", 999, "schema version"),
        ("duplicate_file_count", 999, "duplicate file count"),
    ],
)
def test_manifest_version_and_duplicate_totals_are_recomputed(
    tmp_path: Path,
    column: str,
    value: int,
    message: str,
) -> None:
    store, _ = _build(tmp_path, [_fill(f"manifest-{column}")])
    with sqlite3.connect(store.database_path) as connection:
        trigger_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='immutable_manifest_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER immutable_manifest_update")
        connection.execute(f"UPDATE manifest SET {column}=? WHERE singleton=1", (value,))
        connection.execute(trigger_sql)

    with pytest.raises(IndexedFileCorruptionError, match=message):
        store.verify_integrity()
