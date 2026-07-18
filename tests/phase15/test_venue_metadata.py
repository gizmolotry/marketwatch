from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from marketleak.ingestion.connectors.http import HttpResponse
from marketleak.ingestion.connectors.venue_metadata import (
    ExpectedOutcome,
    KalshiMetadataTarget,
    PolymarketMetadataTarget,
    SettlementSource,
    VenueMetadataCollector,
    VenueMetadataStatus,
)
from marketleak.ingestion.raw_store import RawArtifactStore


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


class FakeTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, object], float]] = []

    def request(self, method: str, url: str, *, params, timeout: float) -> HttpResponse:
        self.calls.append((method, url, dict(params or {}), timeout))
        if not self.responses:
            raise AssertionError("collector attempted an unconfigured endpoint")
        return self.responses.pop(0)


class RecordingStore(RawArtifactStore):
    def __init__(self, root, order: list[str]) -> None:
        super().__init__(root)
        self.order = order

    def capture(self, *args, **kwargs):
        self.order.append("raw")
        return super().capture(*args, **kwargs)


def response(payload: dict[str, object], *, url: str, status: int = 200) -> HttpResponse:
    return HttpResponse(status_code=status, body=json.dumps(payload).encode("utf-8"), headers={}, url=url)


def polymarket_target() -> PolymarketMetadataTarget:
    return PolymarketMetadataTarget(
        gamma_market_id="123",
        condition_id="0xcondition",
        outcomes=(ExpectedOutcome("Yes", "100"), ExpectedOutcome("No", "200")),
        resolution_source="https://polymarket.example/resolution-rule",
    )


def gamma_payload(*, resolution_source: str = "https://polymarket.example/resolution-rule") -> dict[str, object]:
    return {
        "id": "123",
        "conditionId": "0xcondition",
        "question": "Will the documented BTC condition occur?",
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps(["100", "200"]),
        "active": True,
        "closed": False,
        "archived": False,
        "resolutionSource": resolution_source,
        "startDate": (T0 - timedelta(days=1)).isoformat(),
        "endDate": (T0 + timedelta(days=1)).isoformat(),
        "updatedAt": (T0 - timedelta(minutes=1)).isoformat(),
    }


def clob_payload(*, tokens: list[dict[str, str]] | None = None) -> dict[str, object]:
    return {"t": tokens or [{"t": "100", "o": "Yes"}, {"t": "200", "o": "No"}]}


def kalshi_target() -> KalshiMetadataTarget:
    return KalshiMetadataTarget(
        ticker="KXBTC-26JAN01-T100000",
        event_ticker="KXBTC-26JAN01",
        outcomes=(ExpectedOutcome("Yes", "yes"), ExpectedOutcome("No", "no")),
        rules_primary="Settlement uses the captured official source.",
        settlement_sources=(SettlementSource("Official benchmark", "https://benchmark.example/btc"),),
    )


def kalshi_market_payload(*, rules_primary: str = "Settlement uses the captured official source.") -> dict[str, object]:
    return {
        "market": {
            "ticker": "KXBTC-26JAN01-T100000",
            "event_ticker": "KXBTC-26JAN01",
            "market_type": "binary",
            "title": "Will BTC exceed the documented threshold?",
            "status": "active",
            "rules_primary": rules_primary,
            "rules_secondary": "No inferred settlement source is permitted.",
            "open_time": (T0 - timedelta(days=1)).isoformat(),
            "close_time": (T0 + timedelta(days=1)).isoformat(),
            "expected_expiration_time": (T0 + timedelta(days=1, hours=1)).isoformat(),
            "updated_time": (T0 - timedelta(minutes=1)).isoformat(),
        }
    }


def kalshi_event_metadata() -> dict[str, object]:
    return {
        "market_details": [{"market_ticker": "KXBTC-26JAN01-T100000"}],
        "settlement_sources": [{"name": "Official benchmark", "url": "https://benchmark.example/btc"}],
    }


def test_polymarket_raw_capture_precedes_parse_and_token_mapping_is_exact(tmp_path) -> None:
    order: list[str] = []
    target = polymarket_target()
    transport = FakeTransport(
        [
            response(gamma_payload(), url="https://gamma-api.polymarket.com/markets/123"),
            response(clob_payload(), url="https://clob.polymarket.com/clob-markets/0xcondition"),
        ]
    )

    def decoder(body: bytes):
        assert order and order[-1] == "raw"
        order.append("parse")
        return json.loads(body.decode("utf-8"))

    collector = VenueMetadataCollector(
        raw_store=RecordingStore(tmp_path / "raw", order),
        transport=transport,
        decoder=decoder,
    )
    result = collector.collect_polymarket(target=target, as_of=T0, received_at=T0)

    assert result.status == VenueMetadataStatus.COLLECTED
    assert result.metadata is not None
    assert result.metadata.market_uid == "polymarket:market/0xcondition"
    assert [(item.label, item.venue_outcome_id) for item in result.metadata.outcomes] == [("Yes", "100"), ("No", "200")]
    assert result.metadata.rules == (target.resolution_source,)
    assert result.metadata.settlement_sources == ()
    assert order == ["raw", "parse", "raw", "parse"]
    assert all(item.complete for item in result.coverage)
    assert all(capture.receipt_path.exists() for capture in result.raw_captures)
    assert [call[1] for call in transport.calls] == [
        "https://gamma-api.polymarket.com/markets/123",
        "https://clob.polymarket.com/clob-markets/0xcondition",
    ]


def test_polymarket_changed_rule_or_unmatched_clob_tokens_fails_closed_after_capture(tmp_path) -> None:
    target = polymarket_target()
    changed_rule = VenueMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "rules"),
        transport=FakeTransport([response(gamma_payload(resolution_source="https://changed.example/rule"), url="https://gamma-api.polymarket.com/markets/123")]),
    ).collect_polymarket(target=target, as_of=T0, received_at=T0)
    assert changed_rule.status == VenueMetadataStatus.REJECTED_MISMATCH
    assert changed_rule.metadata is None
    assert len(changed_rule.raw_captures) == 1

    bad_tokens = VenueMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "tokens"),
        transport=FakeTransport(
            [
                response(gamma_payload(), url="https://gamma-api.polymarket.com/markets/123"),
                response(clob_payload(tokens=[{"t": "100", "o": "Yes"}, {"t": "999", "o": "No"}]), url="https://clob.polymarket.com/clob-markets/0xcondition"),
            ]
        ),
    ).collect_polymarket(target=target, as_of=T0, received_at=T0)
    assert bad_tokens.status == VenueMetadataStatus.REJECTED_MISMATCH
    assert bad_tokens.metadata is None
    assert len(bad_tokens.raw_captures) == 2
    assert not any(item.complete for item in bad_tokens.coverage)


def test_kalshi_market_and_event_settlement_sources_are_preserved_not_selected(tmp_path) -> None:
    target = kalshi_target()
    transport = FakeTransport(
        [
            response(kalshi_market_payload(), url="https://external-api.kalshi.com/trade-api/v2/markets/KXBTC-26JAN01-T100000"),
            response(kalshi_event_metadata(), url="https://external-api.kalshi.com/trade-api/v2/events/KXBTC-26JAN01/metadata"),
        ]
    )
    result = VenueMetadataCollector(raw_store=RawArtifactStore(tmp_path / "raw"), transport=transport).collect_kalshi(
        target=target,
        as_of=T0,
        received_at=T0,
    )

    assert result.status == VenueMetadataStatus.COLLECTED
    assert result.metadata is not None
    assert result.metadata.market_uid == "kalshi:market/KXBTC-26JAN01-T100000"
    assert result.metadata.venue_event_id == "KXBTC-26JAN01"
    assert result.metadata.settlement_sources == target.settlement_sources
    assert [(item.label, item.venue_outcome_id) for item in result.metadata.outcomes] == [("Yes", "yes"), ("No", "no")]
    assert result.metadata.source_uids == (
        "kalshi:source/market-metadata",
        "kalshi:source/event-metadata",
    )
    assert [call[1] for call in transport.calls] == [
        "https://external-api.kalshi.com/trade-api/v2/markets/KXBTC-26JAN01-T100000",
        "https://external-api.kalshi.com/trade-api/v2/events/KXBTC-26JAN01/metadata",
    ]


def test_kalshi_mismatch_late_and_http_error_remain_unavailable_with_coverage(tmp_path) -> None:
    target = kalshi_target()
    mismatch = VenueMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "mismatch"),
        transport=FakeTransport([response(kalshi_market_payload(rules_primary="changed"), url="https://external-api.kalshi.com/trade-api/v2/markets/KXBTC-26JAN01-T100000")]),
    ).collect_kalshi(target=target, as_of=T0, received_at=T0)
    assert mismatch.status == VenueMetadataStatus.REJECTED_MISMATCH
    assert mismatch.metadata is None

    late = VenueMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "late"),
        transport=FakeTransport(
            [
                response(kalshi_market_payload(), url="https://external-api.kalshi.com/trade-api/v2/markets/KXBTC-26JAN01-T100000"),
                response(kalshi_event_metadata(), url="https://external-api.kalshi.com/trade-api/v2/events/KXBTC-26JAN01/metadata"),
            ]
        ),
    ).collect_kalshi(target=target, as_of=T0, received_at=T0 + timedelta(minutes=1))
    assert late.status == VenueMetadataStatus.UNAVAILABLE_LATE
    assert late.metadata is None
    assert len(late.raw_captures) == 2
    assert not any(item.complete for item in late.coverage)

    failed = VenueMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "http"),
        transport=FakeTransport([HttpResponse(503, b"not-json", {}, "https://gamma-api.polymarket.com/markets/123")]),
    ).collect_polymarket(target=polymarket_target(), as_of=T0, received_at=T0)
    assert failed.status == VenueMetadataStatus.UNAVAILABLE_HTTP
    assert failed.metadata is None
    assert len(failed.raw_captures) == 1
    assert failed.raw_captures[0].receipt_path.exists()


def test_collector_requires_injected_transport() -> None:
    with pytest.raises(TypeError):
        VenueMetadataCollector(raw_store=RawArtifactStore("unused"))  # type: ignore[call-arg]
