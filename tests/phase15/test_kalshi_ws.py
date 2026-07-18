"""Contract tests for the raw-first, non-attributing Kalshi WS adapter."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import pytest

from marketleak.ingestion.connectors.kalshi_ws import (
    KalshiReconnectPolicy,
    KalshiWebSocketAuthenticationError,
    KalshiWebSocketCollector,
    KalshiWebSocketConfig,
)
from marketleak.ingestion.raw_store import RawArtifactStore


class FakeSocket:
    def __init__(self, messages: list[str | bytes | Exception]):
        self.messages = list(messages)
        self.sent: list[str] = []
        self.closed = False
        self.pongs: list[bytes] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str | bytes:
        if not self.messages:
            raise ConnectionError("fake stream exhausted")
        next_message = self.messages.pop(0)
        if isinstance(next_message, Exception):
            raise next_message
        return next_message

    async def close(self) -> None:
        self.closed = True

    async def pong(self, payload: bytes) -> None:
        self.pongs.append(payload)


def auth_headers() -> dict[str, str]:
    return {
        "KALSHI-ACCESS-KEY": "server-key-only",
        "KALSHI-ACCESS-SIGNATURE": "server-signature-only",
        "KALSHI-ACCESS-TIMESTAMP": "1720000000000",
    }


def collector(
    tmp_path: Path,
    socket_factory: Any,
    *,
    headers: Mapping[str, str] | None = None,
    reconnect: KalshiReconnectPolicy | None = None,
) -> KalshiWebSocketCollector:
    return KalshiWebSocketCollector(
        config=KalshiWebSocketConfig(
            market_tickers=("KXHIGHNY-26JUL13-T90",),
            auth_headers=auth_headers() if headers is None else headers,
            reconnect=reconnect or KalshiReconnectPolicy(initial_backoff_seconds=0, maximum_backoff_seconds=0),
        ),
        raw_store=RawArtifactStore(tmp_path / "raw"),
        socket_factory=socket_factory,
        clock=lambda: datetime(2026, 7, 13, 18, 0, tzinfo=UTC),
        sleep=lambda _: asyncio.sleep(0),
    )


def test_collects_documented_trade_raw_first_without_actor_invention(tmp_path: Path) -> None:
    socket = FakeSocket(
        [
            json.dumps(
                {
                    "type": "trade",
                    "sid": 11,
                    "msg": {
                        "trade_id": "trade-123",
                        "market_ticker": "KXHIGHNY-26JUL13-T90",
                        "yes_price_dollars": "0.360",
                        "no_price_dollars": "0.640",
                        "count_fp": "136.00",
                        "taker_outcome_side": "no",
                        "taker_book_side": "ask",
                        "ts_ms": 1_669_149_841_000,
                    },
                }
            )
        ]
    )
    factory_calls: list[tuple[str, Mapping[str, str]]] = []

    async def socket_factory(url: str, headers: Mapping[str, str]) -> FakeSocket:
        factory_calls.append((url, dict(headers)))
        return socket

    result = asyncio.run(collector(tmp_path, socket_factory).collect(max_messages=1))

    assert result.complete is True
    assert factory_calls[0][1] == auth_headers()
    subscriptions = [json.loads(payload) for payload in socket.sent]
    assert [item["params"]["channels"] for item in subscriptions] == [
        ["trade"],
        ["ticker"],
        ["market_lifecycle_v2"],
        ["orderbook_delta"],
    ]
    assert all(item["params"]["market_tickers"] == ["KXHIGHNY-26JUL13-T90"] for item in subscriptions)
    assert len(result.batch.raw_artifacts) == 1
    assert result.batch.raw_artifacts[0].content_hash.startswith("sha256:")
    assert len(result.batch.observations) == 1
    assert len(result.batch.fills) == 1
    fill = result.batch.fills[0]
    assert fill.actor_uid is None and fill.maker is None and fill.taker is None
    assert result.events[0].exposed_fields["taker_outcome_side"] == "no"
    assert result.events[0].exposed_fields["taker_book_side"] == "ask"
    stored = RawArtifactStore(tmp_path / "raw")
    raw_hash = result.batch.raw_artifacts[0].content_hash.removeprefix("sha256:")
    assert stored.read(raw_hash) == json.dumps(
        {
            "type": "trade",
            "sid": 11,
            "msg": {
                "trade_id": "trade-123",
                "market_ticker": "KXHIGHNY-26JUL13-T90",
                "yes_price_dollars": "0.360",
                "no_price_dollars": "0.640",
                "count_fp": "136.00",
                "taker_outcome_side": "no",
                "taker_book_side": "ask",
                "ts_ms": 1_669_149_841_000,
            },
        }
    ).encode()
    receipt_path = next((tmp_path / "raw" / "receipts").rglob("*.json"))
    receipt_text = receipt_path.read_text(encoding="utf-8")
    assert "server-key-only" not in receipt_text
    assert "server-signature-only" not in receipt_text


def test_book_side_is_preserved_but_not_invented_as_a_fill_direction(tmp_path: Path) -> None:
    socket = FakeSocket(
        [
            json.dumps(
                {
                    "type": "trade",
                    "msg": {
                        "trade_id": "trade-book-side-only",
                        "market_ticker": "KXHIGHNY-26JUL13-T90",
                        "yes_price_dollars": "0.501",
                        "no_price_dollars": "0.499",
                        "count_fp": "2.00",
                        "taker_book_side": "bid",
                        "ts": 1_669_149_841,
                    },
                }
            )
        ]
    )

    async def socket_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        return socket

    result = asyncio.run(collector(tmp_path, socket_factory).collect(max_messages=1))

    assert len(result.batch.observations) == 1
    assert result.batch.fills == []
    assert result.events[0].exposed_fields["taker_book_side"] == "bid"
    assert "taker_outcome_side" not in result.events[0].exposed_fields


def test_raw_receipt_exists_before_parse_and_unsupported_messages_are_preserved(tmp_path: Path) -> None:
    class RawFirstCollector(KalshiWebSocketCollector):
        async def _parse_captured(self, payload, capture, socket, result):  # type: ignore[no-untyped-def]
            assert capture.object_path.exists()
            assert capture.receipt_path.exists()
            assert any(item.content_hash.endswith(capture.sha256) for item in result.batch.raw_artifacts)
            await super()._parse_captured(payload, capture, socket, result)

    socket = FakeSocket([json.dumps({"type": "unannounced_message", "msg": {"market_ticker": "KXHIGHNY-26JUL13-T90"}})])

    async def socket_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        return socket

    base = collector(tmp_path, socket_factory)
    raw_first = RawFirstCollector(
        config=base.config,
        raw_store=base.raw_store,
        socket_factory=socket_factory,
        clock=base.clock,
        sleep=base.sleep,
    )
    result = asyncio.run(raw_first.collect(max_messages=1))

    assert result.complete is True
    assert result.events == []
    assert len(result.unsupported_messages) == 1
    assert result.unsupported_messages[0].message_type == "unannounced_message"
    assert result.unsupported_messages[0].reason == "unsupported_message_type"
    assert base.raw_store.verify(result.unsupported_messages[0].raw_sha256)


def test_authenticated_aggregate_book_and_lifecycle_are_not_recast_as_actors(tmp_path: Path) -> None:
    socket = FakeSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "msg": {
                        "market_ticker": "KXHIGHNY-26JUL13-T90",
                        "yes_dollars_fp": [["0.8000", "3.00"], ["0.1000", "1.00"]],
                        "no_dollars_fp": [["0.2000", "4.00"]],
                        "ts_ms": 1_669_149_841_000,
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "msg": {
                        "market_ticker": "KXHIGHNY-26JUL13-T90",
                        "price_dollars": "0.960",
                        "delta_fp": "-54.00",
                        "side": "yes",
                        "ts_ms": 1_669_149_841_000,
                    },
                }
            ),
            json.dumps(
                {
                    "type": "market_lifecycle_v2",
                    "msg": {
                        "market_ticker": "KXHIGHNY-26JUL13-T90",
                        "event_type": "settled",
                        "ts": 1_669_149_841,
                    },
                }
            ),
        ]
    )

    async def socket_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        return socket

    result = asyncio.run(collector(tmp_path, socket_factory).collect(max_messages=3))

    assert result.complete is True
    assert len(result.batch.snapshots) == 2
    assert all(snapshot.asks == () for snapshot in result.batch.snapshots)
    assert result.events[1].message_type == "orderbook_delta"
    assert result.events[1].exposed_fields["side"] == "yes"
    assert result.events[2].message_type == "market_lifecycle_v2"
    assert all(capability.actor_visibility == "unavailable" for capability in result.batch.capabilities)


def test_authentication_omission_fails_before_a_socket_is_opened(tmp_path: Path) -> None:
    calls = 0

    async def socket_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        nonlocal calls
        calls += 1
        return FakeSocket([])

    with pytest.raises(KalshiWebSocketAuthenticationError, match="KALSHI-ACCESS-SIGNATURE"):
        asyncio.run(
            collector(
                tmp_path,
                socket_factory,
                headers={
                    "KALSHI-ACCESS-KEY": "configured",
                    "KALSHI-ACCESS-TIMESTAMP": "1720000000000",
                },
            ).collect(max_messages=1)
        )
    assert calls == 0


def test_heartbeat_and_reconnect_are_acknowledged_and_bounded(tmp_path: Path) -> None:
    heartbeat_socket = FakeSocket([json.dumps({"type": "ping"})])

    async def heartbeat_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        return heartbeat_socket

    heartbeat_result = asyncio.run(collector(tmp_path / "heartbeat", heartbeat_factory).collect(max_messages=1))
    assert heartbeat_result.complete is True
    assert heartbeat_result.heartbeats_acknowledged == 1
    assert heartbeat_socket.pongs == [b"heartbeat"]

    attempts = 0

    async def failing_factory(_: str, __: Mapping[str, str]) -> FakeSocket:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("network unavailable")

    result = asyncio.run(
        collector(
            tmp_path / "reconnect",
            failing_factory,
            reconnect=KalshiReconnectPolicy(
                max_reconnect_attempts=2,
                initial_backoff_seconds=0,
                maximum_backoff_seconds=0,
            ),
        ).collect(max_messages=1)
    )
    assert result.complete is False
    assert attempts == 3  # one initial connection plus exactly two reconnects
    assert result.connection_attempts == 3
    assert result.reconnect_attempts == 2
    assert result.transport_failures == ["ConnectionError", "ConnectionError", "ConnectionError"]
