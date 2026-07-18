from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from marketleak.domain import ActorVisibility, TradeSide
from marketleak.ingestion.connectors.http import EvidenceHttpClient, HttpResponse
from marketleak.ingestion.connectors.kalshi import KalshiConnector
from marketleak.ingestion.connectors.polymarket import PolymarketConnector
from marketleak.ingestion.raw_store import RawArtifactStore


class StubTransport:
    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.calls = []

    def request(self, method, url, *, params, timeout):
        self.calls.append((method, url, dict(params or {}), timeout))
        status, body, headers = self.bodies.pop(0)
        if isinstance(body, str):
            body = body.encode("utf-8")
        return HttpResponse(status, body, headers, url)


def client(tmp_path, transport):
    return EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        max_attempts=3,
        sleep=lambda _: None,
    )


def test_polymarket_offset_pagination_composite_uid_and_decimal_preservation(tmp_path):
    # A numeric JSON token is parsed directly as Decimal, never through float.
    first_body = b'''[{"proxyWallet":"0x56687bf447db6ffa42ffe2204a05edaa20f55839","side":"BUY","asset":"asset-yes","conditionId":"condition-1","size":12.3400,"price":0.123456789012345678,"timestamp":1767225600,"outcome":"Yes","outcomeIndex":0,"transactionHash":"0xabc"}]'''
    transport = StubTransport([(200, first_body, {}), (200, b"[]", {})])
    connector = PolymarketConnector(client(tmp_path, transport))

    batch = connector.fetch_trades(page_size=1)

    assert len(batch.fills) == 1
    fill = batch.fills[0]
    assert fill.price == Decimal("0.123456789012345678")
    assert fill.size == Decimal("12.3400")
    assert fill.side is TradeSide.BUY
    assert fill.actor_visibility is ActorVisibility.PUBLIC_WALLET
    assert fill.actor_uid == "polymarket:wallet/0x56687bf447db6ffa42ffe2204a05edaa20f55839"
    assert fill.maker is None and fill.taker is None
    assert fill.fill_uid.startswith("polymarket:fill:")
    assert [call[2]["offset"] for call in transport.calls] == [0, 1]
    assert len(batch.raw_artifacts) == 2
    assert batch.complete is True


def test_polymarket_orderbook_keeps_documented_depth_and_sorting(tmp_path):
    body = json.dumps(
        {
            "market": "condition-1",
            "asset_id": "asset-yes",
            "timestamp": "1767225600",
            "hash": "bookhash",
            "bids": [{"price": "0.40", "size": "2"}, {"price": "0.45", "size": "1"}],
            "asks": [{"price": "0.55", "size": "4"}, {"price": "0.50", "size": "3"}],
            "min_order_size": "1",
            "tick_size": "0.01",
            "neg_risk": False,
        }
    )
    transport = StubTransport([(200, body, {})])
    connector = PolymarketConnector(client(tmp_path, transport))

    batch = connector.fetch_orderbook("asset-yes")

    snapshot = batch.snapshots[0]
    assert [level.price for level in snapshot.bids] == [Decimal("0.45"), Decimal("0.40")]
    assert [level.price for level in snapshot.asks] == [Decimal("0.50"), Decimal("0.55")]
    assert transport.calls[0][2] == {"token_id": "asset-yes"}
    assert batch.capabilities[0].depth_visibility == "available"


def test_kalshi_cursor_pagination_preserves_unknown_actor_and_direction(tmp_path):
    page_one = {
        "trades": [
            {
                "trade_id": "t1",
                "ticker": "KXTEST-YES",
                "count_fp": "10.00",
                "yes_price_dollars": "0.5600",
                "no_price_dollars": "0.4400",
                "created_time": "2026-01-01T00:00:00Z",
            }
        ],
        "cursor": "next-token",
    }
    page_two = {
        "trades": [
            {
                "trade_id": "t2",
                "ticker": "KXTEST-YES",
                "count_fp": "3.00",
                "yes_price_dollars": "0.5700",
                "no_price_dollars": "0.4300",
                "created_time": "2026-01-01T00:00:01Z",
                "taker_side": "yes",
            }
        ],
        "cursor": "",
    }
    transport = StubTransport(
        [(200, json.dumps(page_one), {}), (200, json.dumps(page_two), {})]
    )
    connector = KalshiConnector(client(tmp_path, transport))

    batch = connector.fetch_trades(
        ticker="KXTEST-YES",
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 2, tzinfo=UTC),
        page_size=1,
    )

    assert len(batch.observations) == 2
    assert len(batch.fills) == 1  # no fill was invented for t1's unknown direction
    assert all(item.actor_visibility is ActorVisibility.NOT_AVAILABLE for item in batch.observations)
    assert batch.fills[0].actor_visibility is ActorVisibility.NOT_AVAILABLE
    assert batch.fills[0].actor_uid is None
    assert transport.calls[1][2]["cursor"] == "next-token"
    assert transport.calls[0][2]["min_ts"] == 1767225600
    assert batch.capabilities[0].actor_visibility == "unavailable"
    assert batch.capabilities[0].direction_visibility == "partial"


def test_kalshi_orderbook_does_not_synthesize_opposite_side_asks(tmp_path):
    response = {
        "orderbook_fp": {
            "yes_dollars": [["0.4200", "13.00"], ["0.4000", "2.00"]],
            "no_dollars": [["0.5500", "4.00"]],
        }
    }
    transport = StubTransport([(200, json.dumps(response), {})])
    connector = KalshiConnector(client(tmp_path, transport))

    batch = connector.fetch_orderbook("KXTEST-YES", depth=10)

    assert len(batch.snapshots) == 2
    yes, no = batch.snapshots
    assert [level.price for level in yes.bids] == [Decimal("0.4200"), Decimal("0.4000")]
    assert [level.price for level in no.bids] == [Decimal("0.5500")]
    assert yes.asks == () and no.asks == ()
    assert transport.calls[0][2] == {"depth": 10}


def test_http_retries_capture_every_response_before_parsing(tmp_path):
    transport = StubTransport(
        [(429, "rate limited", {"Retry-After": "0"}), (200, '{"ok":true}', {})]
    )
    raw_store = RawArtifactStore(tmp_path / "raw")
    http = EvidenceHttpClient(raw_store, transport=transport, sleep=lambda _: None)

    parsed = http.get_json(
        platform="kalshi", source="test", url="https://example.test", params={}
    )

    assert parsed.payload == {"ok": True}
    assert len(list((tmp_path / "raw" / "receipts").rglob("*.json"))) == 2

