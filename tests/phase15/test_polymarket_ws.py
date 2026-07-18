from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from marketleak.domain import ActorVisibility
from marketleak.ingestion.connectors.polymarket_ws import (
    HEARTBEAT_PAYLOAD,
    MarketChannelParser,
    MarketChannelSubscription,
    PolymarketMarketWsCollector,
    ReconnectPolicy,
)
from marketleak.ingestion.raw_store import RawArtifactStore


NOW = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


class FakeSocket:
    def __init__(self, frames: list[str | bytes]) -> None:
        self.frames = list(frames)
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str | bytes:
        if not self.frames:
            raise AssertionError("test attempted to receive beyond supplied frames")
        return self.frames.pop(0)

    async def close(self) -> None:
        self.closed = True


def subscription() -> MarketChannelSubscription:
    return MarketChannelSubscription(asset_ids=("100", "200"), custom_feature_enabled=True)


def collector(tmp_path, factory, **kwargs) -> PolymarketMarketWsCollector:
    return PolymarketMarketWsCollector(
        socket_factory=factory,
        raw_store=RawArtifactStore(tmp_path / "raw"),
        subscription=subscription(),
        clock=lambda: NOW,
        **kwargs,
    )


BOOK = {
    "event_type": "book",
    "asset_id": "100",
    "market": "0xmarket",
    "bids": [{"price": "0.48", "size": "30"}, {"price": "0.49", "size": "20"}],
    "asks": [{"price": "0.52", "size": "25"}, {"price": "0.53", "size": "60"}],
    "timestamp": "1760000000000",
    "hash": "0xbookhash",
}
TRADE = {
    "event_type": "last_trade_price",
    "asset_id": "100",
    "market": "0xmarket",
    "price": "0.456",
    "side": "BUY",
    "size": "219.217767",
    "timestamp": "1760000000000",
}


def test_public_asset_subscription_and_documented_heartbeat(tmp_path):
    async def scenario():
        socket = FakeSocket([json.dumps(TRADE)])

        async def factory():
            return socket

        value = collector(tmp_path, factory)
        result = await value.collect(max_messages=1)
        await value.send_heartbeat(socket)

        wire = json.loads(socket.sent[0])
        assert wire == {"assets_ids": ["100", "200"], "custom_feature_enabled": True, "type": "market"}
        assert socket.sent[-1] == HEARTBEAT_PAYLOAD
        assert result.connection_attempts == 1
        assert socket.closed is True

    asyncio.run(scenario())


def test_raw_capture_happens_before_parser_and_unknown_events_are_preserved(tmp_path):
    order: list[str] = []

    class RecordingStore(RawArtifactStore):
        def capture(self, *args, **kwargs):
            order.append("raw")
            return super().capture(*args, **kwargs)

    class RecordingParser(MarketChannelParser):
        def parse_raw(self, raw, capture):
            assert order == ["raw"]
            order.append("parse")
            return super().parse_raw(raw, capture)

    async def unused_factory():
        raise AssertionError("process_frame does not connect")

    value = PolymarketMarketWsCollector(
        socket_factory=unused_factory,
        raw_store=RecordingStore(tmp_path / "raw"),
        subscription=subscription(),
        parser=RecordingParser(),
        clock=lambda: NOW,
    )
    parsed = value.process_frame(json.dumps({"event_type": "not_documented", "timestamp": "1760000000000"}))

    assert order == ["raw", "parse"]
    assert len(parsed.unknown_events) == 1
    assert parsed.unknown_events[0].event_type == "not_documented"
    assert parsed.raw_artifact.raw_artifact_uid.startswith("polymarket:raw/")
    assert parsed.raw_artifact.storage_uri


def test_valid_book_and_trade_normalization_keep_public_only_lineage(tmp_path):
    async def unused_factory():
        raise AssertionError("process_frame does not connect")

    value = collector(tmp_path, unused_factory)
    book = value.process_frame(json.dumps(BOOK))
    trade = value.process_frame(json.dumps(TRADE))

    snapshot = book.snapshots[0]
    fill = trade.fills[0]
    assert [str(level.price) for level in snapshot.bids] == ["0.49", "0.48"]
    assert [str(level.price) for level in snapshot.asks] == ["0.52", "0.53"]
    assert snapshot.source_uid == "polymarket:source/ws-market"
    assert snapshot.raw_artifact_uid.startswith("polymarket:raw/")
    assert fill.actor_visibility == ActorVisibility.NOT_AVAILABLE
    assert fill.actor_uid is None
    assert fill.maker is None and fill.taker is None
    assert str(fill.price) == "0.456"
    assert len(trade.observations) == 1


def test_reconnect_policy_is_bounded_and_deterministic(tmp_path):
    async def scenario():
        attempts = 0
        delays: list[float] = []
        socket = FakeSocket([json.dumps(TRADE)])

        async def factory():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ConnectionError("temporary failure")
            return socket

        async def fake_sleep(delay: float) -> None:
            delays.append(delay)

        value = collector(
            tmp_path,
            factory,
            reconnect_policy=ReconnectPolicy(max_attempts=3, initial_delay_seconds=0.1, multiplier=2.0, max_delay_seconds=1.0),
            sleep=fake_sleep,
        )
        result = await value.collect(max_messages=1)

        assert result.connection_attempts == 3
        assert result.reconnect_delays == (0.1, 0.2)
        assert delays == [0.1, 0.2]
        assert len(result.messages) == 1

    asyncio.run(scenario())


def test_subscription_has_no_private_credential_or_user_channel_fields():
    payload = subscription().wire_payload()

    assert set(payload) == {"assets_ids", "type", "custom_feature_enabled"}
    assert payload["type"] == "market"
    assert not ({"auth", "apiKey", "secret", "passphrase", "markets"} & set(payload))
