from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal

from marketleak.domain import TradeSide
from marketleak.ingestion.connectors.polymarket_ws import BestBidAskUpdate, PriceLevelChange
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.ingestion.storage import NormalizedStore


def test_raw_store_is_content_addressed_append_only_and_verifiable(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    payload = b'{"price":0.123456789012345678}'

    first = store.capture(
        payload,
        platform="polymarket",
        source="data-api/trades",
        request={"method": "GET", "url": "https://example.test/trades"},
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    second = store.capture(
        payload,
        platform="polymarket",
        source="data-api/trades",
        request={"method": "GET", "url": "https://example.test/trades", "offset": 1},
        received_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
    )

    assert first.sha256 == hashlib.sha256(payload).hexdigest()
    assert first.object_path == second.object_path
    assert first.receipt_path != second.receipt_path
    assert first.object_path.read_bytes() == payload
    assert store.verify(first)
    assert store.read_receipt(first)["request"]["method"] == "GET"

    first.object_path.write_bytes(b"tampered")
    assert store.verify(first) is False


def test_normalized_store_replay_is_idempotent_and_uid_conflict_is_quarantined(tmp_path):
    store = NormalizedStore(tmp_path)
    original = {
        "fill_uid": "polymarket:fill/one",
        "platform": "polymarket",
        "event_time": datetime(2026, 1, 2, tzinfo=UTC),
        "price": Decimal("0.123456789012345678"),
    }
    first = store.write([original], record_type="trade_fill")
    replay = store.write([original], record_type="trade_fill")
    conflict = store.write(
        [{**original, "price": Decimal("0.123456789012345679")}],
        record_type="trade_fill",
    )

    assert (first.inserted, first.duplicates, first.conflicts) == (1, 0, 0)
    assert (replay.inserted, replay.duplicates, replay.conflicts) == (0, 1, 0)
    assert (conflict.inserted, conflict.duplicates, conflict.conflicts) == (0, 0, 1)
    assert len(conflict.quarantined) == 1
    assert conflict.quarantined[0].quarantine_path.exists()
    assert b"0.123456789012345678" in first.paths[0].read_bytes()


def test_replayed_event_with_new_receipt_lineage_is_duplicate_and_retains_first_record(tmp_path):
    store = NormalizedStore(tmp_path)
    first_record = {
        "schema_version": "2.0.0",
        "fill_uid": "polymarket:fill/replayed",
        "platform": "polymarket",
        "event_time": datetime(2026, 1, 2, tzinfo=UTC),
        "ingested_at": datetime(2026, 1, 2, 0, 1, tzinfo=UTC),
        "source_uid": "polymarket:source/data-api-trades",
        "raw_artifact_uid": "polymarket:raw/first-page",
        "parser_version": "polymarket-public-v2.0.0",
        "market_uid": "polymarket:market/condition",
        "outcome_uid": "polymarket:outcome/yes",
        "price": Decimal("0.61"),
        "size": Decimal("25"),
        "side": "buy",
        "actor_uid": "polymarket:wallet/0xabc",
        "transaction_uid": "polymarket:tx/0x123",
    }
    replayed_record = {
        **first_record,
        "ingested_at": datetime(2026, 1, 2, 1, 30, tzinfo=UTC),
        "raw_artifact_uid": "polymarket:raw/later-page",
    }

    first = store.write([first_record], record_type="trade_fill")
    replay = store.write([replayed_record], record_type="trade_fill")

    assert first.inserted == 1
    assert (replay.duplicates, replay.conflicts, replay.rejected) == (1, 0, 0)
    assert first.paths[0].read_bytes().find(b"polymarket:raw/first-page") >= 0
    assert b"polymarket:raw/later-page" not in first.paths[0].read_bytes()
    assert not store.quarantine_root.exists()


def test_same_uid_changed_business_content_or_event_time_is_quarantined(tmp_path):
    store = NormalizedStore(tmp_path)
    original = {
        "schema_version": "2.0.0",
        "fill_uid": "polymarket:fill/immutable",
        "platform": "polymarket",
        "event_time": datetime(2026, 1, 2, tzinfo=UTC),
        "ingested_at": datetime(2026, 1, 2, 0, 1, tzinfo=UTC),
        "source_uid": "polymarket:source/data-api-trades",
        "raw_artifact_uid": "polymarket:raw/one",
        "parser_version": "polymarket-public-v2.0.0",
        "price": Decimal("0.61"),
        "size": Decimal("25"),
        "side": "buy",
    }
    store.write([original], record_type="trade_fill")

    changed_price = store.write(
        [{**original, "raw_artifact_uid": "polymarket:raw/two", "price": Decimal("0.62")}],
        record_type="trade_fill",
    )
    changed_date = store.write(
        [
            {
                **original,
                "raw_artifact_uid": "polymarket:raw/three",
                "event_time": datetime(2026, 1, 3, tzinfo=UTC),
            }
        ],
        record_type="trade_fill",
    )

    assert (changed_price.conflicts, changed_price.duplicates) == (1, 0)
    assert (changed_date.conflicts, changed_date.duplicates) == (1, 0)
    assert len(list((store.records_root / "trade_fill").rglob("*.json"))) == 1
    assert len(list(store.quarantine_root.rglob("*.json"))) == 2


def test_market_scoped_changes_and_updates_use_their_canonical_ids_and_retry_idempotently(tmp_path):
    store = NormalizedStore(tmp_path)
    event_time = datetime(2026, 7, 13, 22, 30, tzinfo=UTC)
    lineage = {
        "event_time": event_time,
        "ingested_at": event_time,
        "source_uid": "polymarket:source/ws-market",
        "parser_version": "polymarket-market-ws-v1.0.0",
        "market_uid": "polymarket:market/condition",
        "outcome_uid": "polymarket:outcome/yes",
    }
    first_change = PriceLevelChange(
        **lineage,
        raw_artifact_uid="polymarket:raw/change-one",
        change_uid="polymarket:ws-price-change/one",
        price=Decimal("0.48"),
        size=Decimal("30"),
        side=TradeSide.BUY,
        best_bid=Decimal("0.48"),
        best_ask=Decimal("0.52"),
        level_removed=False,
    )
    second_change = PriceLevelChange(
        **lineage,
        raw_artifact_uid="polymarket:raw/change-two",
        change_uid="polymarket:ws-price-change/two",
        price=Decimal("0.49"),
        size=Decimal("20"),
        side=TradeSide.SELL,
        best_bid=Decimal("0.48"),
        best_ask=Decimal("0.52"),
        level_removed=False,
    )
    first_update = BestBidAskUpdate(
        **lineage,
        raw_artifact_uid="polymarket:raw/update-one",
        update_uid="polymarket:ws-best-bid-ask/one",
        best_bid=Decimal("0.48"),
        best_ask=Decimal("0.52"),
        spread=Decimal("0.04"),
    )
    second_update = BestBidAskUpdate(
        **lineage,
        raw_artifact_uid="polymarket:raw/update-two",
        update_uid="polymarket:ws-best-bid-ask/two",
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        spread=Decimal("0.02"),
    )

    first = store.write([first_change, second_change, first_update, second_update])
    replay = store.write(
        [first_change.model_copy(update={"ingested_at": datetime(2026, 7, 13, 22, 31, tzinfo=UTC)})]
    )

    assert (first.inserted, first.duplicates, first.conflicts, first.rejected) == (4, 0, 0, 0)
    assert (replay.inserted, replay.duplicates, replay.conflicts, replay.rejected) == (0, 1, 0, 0)
    assert len(list((store.records_root / "pricelevelchange").rglob("*.json"))) == 2
    assert len(list((store.records_root / "bestbidaskupdate").rglob("*.json"))) == 2
    assert not store.quarantine_root.exists()
