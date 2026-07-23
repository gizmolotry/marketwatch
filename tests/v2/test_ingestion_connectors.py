from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

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
    assert all("takerOnly" not in call[2] for call in transport.calls)
    assert len(batch.raw_artifacts) == 2
    assert batch.complete is True


def test_polymarket_wallet_query_requests_full_history_and_both_trade_roles(tmp_path):
    # Minimal mechanics fixture shaped from the official /trades response example.
    trade = {
        "proxyWallet": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        "side": "BUY",
        "asset": "asset-yes",
        "conditionId": "condition-1",
        "size": 12.34,
        "price": 0.56,
        "timestamp": 1767225600,
        "outcome": "Yes",
        "outcomeIndex": 0,
        "transactionHash": "0xabc",
    }
    transport = StubTransport(
        [(200, json.dumps([trade]), {}), (200, b"[]", {})]
    )
    connector = PolymarketConnector(
        client(tmp_path, transport),
        clock=lambda: datetime(2026, 1, 2, 0, 0, 1, tzinfo=UTC),
    )

    first = connector.fetch_trades(
        user="0x56687BF447DB6FFA42FFE2204A05EDAA20F55839",
        taker_only=False,
        page_size=1,
        max_pages=1,
    )
    assert first.complete is False
    cursor = json.loads(first.continuation)
    assert cursor == {
        "filters": {
            "end": 1767312000,
            "start": 1,
            "takerOnly": False,
            "user": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        },
        "offset": 1,
        "state": "page",
        "version": 1,
    }

    resumed = connector.fetch_trades(
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        taker_only=False,
        page_size=1,
        max_pages=1,
        continuation=first.continuation,
    )

    assert resumed.complete is True
    assert [call[2]["offset"] for call in transport.calls] == [0, 1]
    assert transport.calls[0][2]["start"] == 1
    assert [call[2]["end"] for call in transport.calls] == [1767312000, 1767312000]
    assert transport.calls[0][2]["takerOnly"] is False
    assert transport.calls[0][2]["user"] == "0x56687bf447db6ffa42ffe2204a05edaa20f55839"


def test_polymarket_wallet_query_sends_server_time_bounds_and_records_exact_filters(tmp_path):
    transport = StubTransport([(200, b"[]", {})])
    connector = PolymarketConnector(client(tmp_path, transport))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)

    batch = connector.fetch_trades(
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        start=start,
        end=end,
        taker_only=True,
    )

    expected = {
        "user": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        "start": 1767225600,
        "end": 1767312000,
        "takerOnly": True,
    }
    assert {key: transport.calls[0][2][key] for key in expected} == expected
    assert PolymarketConnector.trade_query_filters(
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        start=start,
        end=end,
        taker_only=True,
    ) == expected
    assert batch.complete is True


def test_polymarket_continuation_is_bound_to_exact_wallet_window(tmp_path):
    trade = {
        "proxyWallet": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        "side": "BUY",
        "asset": "asset-yes",
        "conditionId": "condition-1",
        "size": 1,
        "price": 0.5,
        "timestamp": 1767225600,
        "outcomeIndex": 0,
        "transactionHash": "0xabc",
    }
    transport = StubTransport([(200, json.dumps([trade]), {})])
    connector = PolymarketConnector(client(tmp_path, transport))
    first = connector.fetch_trades(
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        page_size=1,
        max_pages=1,
    )

    with pytest.raises(ValueError, match="does not match"):
        connector.fetch_trades(
            user="0x1111111111111111111111111111111111111111",
            page_size=1,
            max_pages=1,
            continuation=first.continuation,
        )

    assert len(transport.calls) == 1


def test_polymarket_full_page_at_offset_ceiling_is_partial_and_requires_split(tmp_path):
    trade = {
        "proxyWallet": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        "side": "BUY",
        "asset": "asset-yes",
        "conditionId": "condition-1",
        "size": 1,
        "price": 0.5,
        "timestamp": 1767225600,
        "outcomeIndex": 0,
        "transactionHash": "0xabc",
    }
    transport = StubTransport([(200, json.dumps([trade]), {})])
    connector = PolymarketConnector(client(tmp_path, transport))

    batch = connector.fetch_trades(
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        end=datetime(2026, 1, 2, tzinfo=UTC),
        page_size=1,
        max_pages=1,
        continuation="10000",
    )

    cursor = json.loads(batch.continuation)
    assert batch.complete is False
    assert cursor["state"] == "split_required"
    assert cursor["offset"] == 10001
    assert transport.calls[0][2]["offset"] == 10000
    with pytest.raises(ValueError, match="subdivide the start/end window"):
        connector.fetch_trades(
            user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
            end=datetime(2026, 1, 2, tzinfo=UTC),
            page_size=1,
            max_pages=1,
            continuation=batch.continuation,
        )
    assert len(transport.calls) == 1


def test_polymarket_wallet_query_rejects_invalid_scope_before_network(tmp_path):
    transport = StubTransport([])
    connector = PolymarketConnector(client(tmp_path, transport))

    with pytest.raises(ValueError, match="40-hex"):
        connector.fetch_trades(user="not-a-wallet")
    with pytest.raises(ValueError, match="mutually exclusive"):
        connector.fetch_trades(market="condition-1", event_id=1)
    with pytest.raises(ValueError, match="end must be at or after start"):
        connector.fetch_trades(
            user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
            start=datetime(2026, 1, 2, tzinfo=UTC),
            end=datetime(2026, 1, 1, tzinfo=UTC),
        )

    assert transport.calls == []


def test_polymarket_combined_user_scope_never_synthesizes_full_history_start():
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
    end = datetime(2026, 1, 2, tzinfo=UTC)

    market_filters = PolymarketConnector.trade_query_filters(
        user=wallet,
        market="condition-1",
        end=end,
    )
    event_filters = PolymarketConnector.trade_query_filters(
        user=wallet,
        event_id=1,
        end=end,
    )
    narrowed_filters = PolymarketConnector.trade_query_filters(
        user=wallet,
        market="condition-1",
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=end,
    )

    assert "start" not in market_filters
    assert "start" not in event_filters
    assert market_filters["end"] == 1767312000
    assert event_filters["end"] == 1767312000
    assert narrowed_filters["start"] == 1767225600


def test_polymarket_frozen_end_prevents_moving_head_offset_drift(tmp_path):
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    def trade(timestamp, transaction_hash):
        return {
            "proxyWallet": wallet,
            "side": "BUY",
            "asset": "asset-yes",
            "conditionId": "condition-1",
            "size": 1,
            "price": 0.5,
            "timestamp": timestamp,
            "outcomeIndex": 0,
            "transactionHash": transaction_hash,
        }

    class MovingHeadTransport:
        def __init__(self):
            self.calls = []

        def request(self, method, url, *, params, timeout):
            query = dict(params or {})
            self.calls.append((method, url, query, timeout))
            # A newer row appears at the head after page one. Without a frozen
            # end, offset=1 would return tx-2 again and tx-1 would be skipped.
            stable = [trade(1767225602, "0xtx2"), trade(1767225601, "0xtx1")]
            moving = [trade(1767225603, "0xtx3"), *stable]
            visible = stable if query.get("end") == 1767225602 else moving
            offset = query["offset"]
            page = visible[offset : offset + query["limit"]]
            return HttpResponse(200, json.dumps(page).encode("utf-8"), {}, url)

    transport = MovingHeadTransport()
    connector = PolymarketConnector(
        client(tmp_path, transport),
        clock=lambda: datetime(2026, 1, 1, 0, 0, 3, 987654, tzinfo=UTC),
    )

    batch = connector.fetch_trades(user=wallet, page_size=1, max_pages=3)

    assert batch.complete is True
    assert [fill.transaction_uid for fill in batch.fills] == [
        "polymarket:tx/0xtx2",
        "polymarket:tx/0xtx1",
    ]
    assert [call[2]["offset"] for call in transport.calls] == [0, 1, 2]
    assert [call[2]["end"] for call in transport.calls] == [
        1767225602,
        1767225602,
        1767225602,
    ]


def test_polymarket_rejects_subsecond_bounds_before_network(tmp_path):
    transport = StubTransport([])
    connector = PolymarketConnector(client(tmp_path, transport))
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    with pytest.raises(ValueError, match="start must use whole-second"):
        connector.fetch_trades(
            user=wallet,
            start=datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="end must use whole-second"):
        connector.fetch_trades(
            user=wallet,
            end=datetime(2026, 1, 2, 0, 0, 0, 1, tzinfo=UTC),
        )

    assert transport.calls == []


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


def test_http_receipts_allowlist_response_headers_and_drop_credentials(tmp_path):
    transport = StubTransport(
        [
            (
                200,
                '{"ok":true}',
                {
                    "Content-Type": "application/json",
                    "ETag": '"revision-1"',
                    "Retry-After": "3",
                    "Set-Cookie": "session=secret",
                    "Cookie": "request-secret",
                    "Authorization": "Bearer secret",
                    "Proxy-Authorization": "Basic secret",
                    "X-API-Key": "api-secret",
                    "X-Auth-Token": "token-secret",
                    "CF-Ray": "operational-but-not-allowlisted",
                },
            )
        ]
    )
    raw_store = RawArtifactStore(tmp_path / "raw")
    http = EvidenceHttpClient(raw_store, transport=transport, sleep=lambda _: None)

    http.get_json(platform="polymarket", source="test", url="https://example.test")

    receipt_path = next((tmp_path / "raw" / "receipts").rglob("*.json"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["response_metadata"]["headers"] == {
        "content-type": "application/json",
        "etag": '"revision-1"',
        "retry-after": "3",
    }
    serialized = receipt_path.read_text(encoding="utf-8").lower()
    for secret in ("session=secret", "request-secret", "bearer secret", "basic secret", "api-secret", "token-secret"):
        assert secret not in serialized
