from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from typing import Any

import pytest

from marketleak.ingestion.config.phase15_registry import (
    ApprovedMarketTarget,
    BoundedStreamSettings,
    DocumentedBtcReferenceMapping,
)
from marketleak.ingestion.connectors.documented_reference_adapters import (
    COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF,
    COINBASE_EXCHANGE_BTC_USD_MAPPING_URL,
    COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
    COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION,
    COINBASE_EXCHANGE_BTC_USD_TICKER_URL,
    CoinbaseExchangeBtcUsdTickerAdapter,
    DocumentedReferenceAdapterStatus,
)
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.multimodal.reference_price import DocumentedReferenceSource
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


class FakeSocket:
    def __init__(self, messages: list[str | bytes | Exception], *, send_error: Exception | None = None) -> None:
        self.messages = list(messages)
        self.send_error = send_error
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)
        if self.send_error is not None:
            raise self.send_error

    async def recv(self) -> str | bytes:
        if not self.messages:
            raise ConnectionError("fake stream exhausted")
        item = self.messages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def close(self) -> None:
        self.closed = True


def target() -> ApprovedMarketTarget:
    return ApprovedMarketTarget(
        target_uid="target:kalshi-btc-reference",
        venue="kalshi",
        market_uid="market:kalshi-btc-reference",
        kalshi_tickers=("KXBTCREF",),
        metadata_ref="metadata:kalshi-btc-reference",
        stream_config_ref="stream:kalshi-btc-reference",
        stream_settings=BoundedStreamSettings(max_messages=100, duration_seconds=60, max_reconnects=0),
        reference_mapping_uid="mapping:kalshi-btc-reference",
    )


def mapping(**updates: str) -> DocumentedBtcReferenceMapping:
    data = {
        "mapping_uid": "mapping:kalshi-btc-reference",
        "target_uid": "target:kalshi-btc-reference",
        "market_rule_source_uid": "source:kalshi-market-rule",
        "market_rule_source_url": "https://www.kalshi.com/markets/KXBTCREF",
        "settlement_source_uid": "source:kalshi-settlement-rule",
        "settlement_source_url": "https://www.kalshi.com/rules/KXBTCREF",
        "primary_source_uid": COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
        "primary_source_url": COINBASE_EXCHANGE_BTC_USD_MAPPING_URL,
        "instrument": "BTC-USD",
        "configured_endpoint_ref": COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF,
    }
    data.update(updates)
    return DocumentedBtcReferenceMapping(**data)


def documented(source: DocumentedBtcReferenceMapping, **updates: object) -> DocumentedReferenceSource:
    data: dict[str, object] = {
        "mapping_uid": source.mapping_uid,
        "market_uid": target().market_uid,
        "market_rule_source_uid": source.market_rule_source_uid,
        "settlement_source_uid": source.settlement_source_uid,
        "primary_source_uid": source.primary_source_uid,
        "asset_symbol": "BTC",
        "quote_currency": "USD",
        "settlement_rule_url": source.settlement_source_url,
        "primary_source_url": source.primary_source_url,
        "event_time": T0 - timedelta(seconds=5),
        "first_seen_at": T0 - timedelta(seconds=5),
        "retrieved_at": T0 - timedelta(seconds=5),
        "ingested_at": T0 - timedelta(seconds=5),
        "provenance": Provenance(
            source_uid=source.market_rule_source_uid,
            raw_artifact_uid="kalshi:raw/mapping-document",
            parser_version="market-rule-v1",
            content_hash="a" * 64,
            retrieved_at=T0 - timedelta(seconds=5),
            source_url=source.market_rule_source_url,
        ),
        "reliability": SourceReliability(
            source_uid=source.market_rule_source_uid,
            source_class=SourceClass.OFFICIAL_VENUE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("0.95"),
            assessed_at=T0 - timedelta(seconds=5),
            rationale="documented market rule source",
        ),
    }
    data.update(updates)
    return DocumentedReferenceSource(**data)


def subscription_confirmation(*, channels: list[dict[str, object]] | None = None) -> str:
    return json.dumps(
        {
            "type": "subscriptions",
            "channels": channels
            or [
                {"name": "ticker", "product_ids": ["BTC-USD"]},
                {"name": "heartbeat", "product_ids": ["BTC-USD"]},
            ],
        }
    )


def ticker(**updates: object) -> str:
    data: dict[str, object] = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "60000.25",
        "time": (T0 - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "sequence": 42,
    }
    data.update(updates)
    return json.dumps(data)


def adapter(tmp_path, socket_factory: Any, **kwargs: object) -> CoinbaseExchangeBtcUsdTickerAdapter:
    return CoinbaseExchangeBtcUsdTickerAdapter(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        socket_factory=socket_factory,
        **kwargs,
    )


def collect(tmp_path, socket_factory: Any, **kwargs: object):
    return asyncio.run(
        adapter(tmp_path, socket_factory, **kwargs).collect(
            target=target(),
            mapping=mapping(),
            documented_mapping=documented(mapping()),
            as_of=T0,
            received_at=T0,
        )
    )


def test_fixed_coinbase_adapter_subscribes_exactly_captures_control_and_ticker_before_parse(tmp_path) -> None:
    socket = FakeSocket([subscription_confirmation(), ticker()])
    factory_calls: list[str] = []

    async def socket_factory(url: str) -> FakeSocket:
        factory_calls.append(url)
        return socket

    result = collect(tmp_path, socket_factory)

    assert result.status == DocumentedReferenceAdapterStatus.COLLECTED
    assert result.observation is not None
    assert result.observation.price == Decimal("60000.25")
    assert result.observation.event_time == T0 - timedelta(seconds=1)
    assert factory_calls == [COINBASE_EXCHANGE_BTC_USD_TICKER_URL]
    assert socket.sent == [COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION]
    assert socket.closed is True
    assert [capture.object_path.read_bytes() for capture in result.raw_captures] == [
        subscription_confirmation().encode("utf-8"),
        ticker().encode("utf-8"),
    ]
    assert result.coverage[0].complete is True
    assert result.coverage[0].raw_sha256 == tuple(capture.sha256 for capture in result.raw_captures)
    assert result.coverage[0].filters["subscription"] == json.loads(COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION)


def test_adapter_rejects_unapproved_or_config_mismatched_mapping_without_connecting(tmp_path) -> None:
    mismatched = mapping(configured_endpoint_ref="config:coinbase-other-feed")
    calls: list[str] = []

    async def socket_factory(url: str) -> FakeSocket:
        calls.append(url)
        return FakeSocket([])

    result = asyncio.run(
        adapter(tmp_path, socket_factory).collect(
            target=target(),
            mapping=mismatched,
            documented_mapping=documented(mismatched),
            as_of=T0,
            received_at=T0,
        )
    )

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_MAPPING_MISMATCH
    assert result.raw_captures == ()
    assert result.coverage[0].complete is False
    assert result.coverage[0].filters["coverage_state"] == "unavailable"
    assert calls == []


def test_adapter_returns_explicit_unavailable_coverage_when_connect_fails(tmp_path) -> None:
    async def socket_factory(_: str) -> FakeSocket:
        raise ConnectionError("offline")

    result = collect(tmp_path, socket_factory)

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_CONNECT
    assert result.raw_captures == ()
    assert result.coverage[0].complete is False
    assert result.coverage[0].filters["coverage_state"] == "unavailable"


def test_adapter_reports_subscription_send_failure_without_fallback(tmp_path) -> None:
    socket = FakeSocket([], send_error=ConnectionError("write failed"))
    result = collect(tmp_path, lambda _: socket)

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_SEND
    assert socket.sent == [COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION]
    assert socket.closed is True
    assert result.raw_captures == ()


def test_adapter_captures_error_frame_and_returns_partial_unavailable_coverage(tmp_path) -> None:
    error = json.dumps({"type": "error", "message": "rate limited"})
    result = collect(tmp_path, lambda _: FakeSocket([error]))

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_ERROR
    assert result.primary_source_availability is not None
    assert result.primary_source_availability.status.value == "unavailable"
    assert result.raw_captures[0].object_path.read_bytes() == error.encode("utf-8")
    assert result.coverage[0].filters["coverage_state"] == "partial"


def test_adapter_requires_exact_subscription_confirmation_after_raw_capture(tmp_path) -> None:
    incorrect_ack = subscription_confirmation(channels=[{"name": "ticker", "product_ids": ["BTC-USD"]}])
    result = collect(tmp_path, lambda _: FakeSocket([incorrect_ack]))

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_SUBSCRIBE
    assert len(result.raw_captures) == 1
    assert result.raw_captures[0].object_path.read_bytes() == incorrect_ack.encode("utf-8")
    assert result.coverage[0].filters["coverage_state"] == "partial"


@pytest.mark.parametrize(
    "frame",
    [
        ticker(product_id="ETH-USD"),
        ticker(price="invalid"),
        ticker(time="not-a-time"),
        json.dumps({"type": "ticker", "product_id": "BTC-USD", "price": "1", "time": "2026-07-13T11:59:59Z"}),
        "not-json",
    ],
)
def test_adapter_captures_malformed_or_wrong_ticker_without_fallback(tmp_path, frame) -> None:
    result = collect(tmp_path, lambda _: FakeSocket([subscription_confirmation(), frame]))

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_MALFORMED
    assert result.observation is None
    assert len(result.raw_captures) == 2
    assert result.raw_captures[-1].object_path.read_bytes() == frame.encode("utf-8")
    assert result.coverage[0].complete is False
    assert result.coverage[0].filters["coverage_state"] == "partial"


def test_adapter_rejects_stale_ticker_after_raw_capture(tmp_path) -> None:
    stale = ticker(time=(T0 - timedelta(seconds=61)).isoformat().replace("+00:00", "Z"))
    result = collect(tmp_path, lambda _: FakeSocket([subscription_confirmation(), stale]))

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_STALE
    assert result.primary_source_availability is not None
    assert result.primary_source_availability.status.value == "available"
    assert result.observation is None
    assert len(result.raw_captures) == 2
    assert result.coverage[0].filters["coverage_state"] == "partial"


def test_adapter_reports_explicit_gap_when_bounded_stream_has_no_ticker(tmp_path) -> None:
    result = collect(
        tmp_path,
        lambda _: FakeSocket([subscription_confirmation(), json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "sequence": 44})]),
        max_inbound_frames=2,
    )

    assert result.status == DocumentedReferenceAdapterStatus.UNAVAILABLE_GAP
    assert len(result.raw_captures) == 2
    assert result.primary_source_availability is not None
    assert result.primary_source_availability.status.value == "unavailable"
    assert result.coverage[0].filters["coverage_state"] == "partial"
