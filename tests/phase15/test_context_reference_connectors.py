from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketleak.ingestion.connectors.http import HttpResponse
from marketleak.ingestion.connectors.market_context import (
    CollectionStatus,
    MarketContextCollector,
    MarketContextSourceConfig,
    RawEndpoint,
    SourcePolicy,
)
from marketleak.ingestion.connectors.reference_price import (
    DocumentedReferenceSourceConfig,
    ReferencePriceCollector,
)
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.multimodal.reference_price import ReferenceAdmissionStatus
from marketleak.multimodal.schemas import ReliabilityTier, SourceClass


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
MARKET_UID = "example:market-001"
RULE_SOURCE_UID = "example:venue-market-rules"
PRIMARY_SOURCE_UID = "example:documented-primary-price-feed"


class FakeTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, object], float]] = []

    def request(self, method: str, url: str, *, params, timeout: float) -> HttpResponse:
        self.calls.append((method, url, dict(params or {}), timeout))
        if not self.responses:
            raise AssertionError("collector attempted an unconfigured/default endpoint")
        return self.responses.pop(0)


class RecordingStore(RawArtifactStore):
    def __init__(self, root, order: list[str]) -> None:
        super().__init__(root)
        self.order = order

    def capture(self, *args, **kwargs):
        self.order.append("raw")
        return super().capture(*args, **kwargs)


def response(payload: dict[str, object], *, url: str = "https://example.test/source", status: int = 200) -> HttpResponse:
    return HttpResponse(status, json.dumps(payload).encode("utf-8"), {}, url)


def policy(source_uid: str, source_class: SourceClass) -> SourcePolicy:
    return SourcePolicy(
        source_uid=source_uid,
        source_class=source_class,
        reliability_tier=ReliabilityTier.HIGH,
        reliability_score=Decimal("0.9"),
        reliability_rationale="explicit Phase 15 source policy for this configured endpoint",
    )


def endpoint(
    *,
    source_uid: str,
    source_class: SourceClass,
    dataset: str,
    url: str,
    expected_sha256: str | None = None,
) -> RawEndpoint:
    return RawEndpoint(
        endpoint_url=url,
        platform="example",
        dataset=dataset,
        policy=policy(source_uid, source_class),
        parser_version="phase15-connector-test-v1",
        expected_sha256=expected_sha256,
    )


def context_payload(*, first_seen_at: datetime = T0 + timedelta(minutes=1)) -> dict[str, object]:
    return {
        "context_uid": "example:market-context-001",
        "market_uid": MARKET_UID,
        "question": "Will the documented example event happen?",
        "category": "example-category",
        "outcomes": [
            {"outcome_uid": "example:market-001-yes", "label": "Yes"},
            {"outcome_uid": "example:market-001-no", "label": "No"},
        ],
        "resolution_schedule": {
            "scheduled_open_at": (T0 - timedelta(days=1)).isoformat(),
            "scheduled_close_at": (T0 + timedelta(days=2)).isoformat(),
            "expected_resolution_at": (T0 + timedelta(days=2, hours=1)).isoformat(),
            "settlement_deadline_at": (T0 + timedelta(days=3)).isoformat(),
            "resolution_rule_url": "https://example.test/market-rules",
        },
        "event_time": T0.isoformat(),
        "first_seen_at": first_seen_at.isoformat(),
    }


def reference_config(*, expected_mapping_hash: str | None = None) -> DocumentedReferenceSourceConfig:
    mapping_endpoint = endpoint(
        source_uid=RULE_SOURCE_UID,
        source_class=SourceClass.OFFICIAL_VENUE,
        dataset="documented_settlement_mapping",
        url="https://example.test/market-rules",
        expected_sha256=expected_mapping_hash,
    )
    primary_endpoint = endpoint(
        source_uid=PRIMARY_SOURCE_UID,
        source_class=SourceClass.PRIMARY_SOURCE,
        dataset="documented_reference_price",
        url="https://example.test/documented-benchmark",
    )
    return DocumentedReferenceSourceConfig(
        market_uid=MARKET_UID,
        market_rule_source_uid=RULE_SOURCE_UID,
        settlement_source_uid="example:documented-settlement-benchmark",
        primary_source_uid=PRIMARY_SOURCE_UID,
        asset_symbol="BTC",
        quote_currency="USD",
        settlement_rule_url="https://example.test/market-rules",
        primary_source_url="https://example.test/documented-benchmark",
        mapping_endpoint=mapping_endpoint,
        primary_price_endpoint=primary_endpoint,
    )


def mapping_payload(*, first_seen_at: datetime = T0 + timedelta(minutes=1)) -> dict[str, object]:
    return {
        "mapping_uid": "example:reference-mapping-001",
        "market_uid": MARKET_UID,
        "market_rule_source_uid": RULE_SOURCE_UID,
        "settlement_source_uid": "example:documented-settlement-benchmark",
        "primary_source_uid": PRIMARY_SOURCE_UID,
        "asset_symbol": "BTC",
        "quote_currency": "USD",
        "settlement_rule_url": "https://example.test/market-rules",
        "primary_source_url": "https://example.test/documented-benchmark",
        "event_time": T0.isoformat(),
        "first_seen_at": first_seen_at.isoformat(),
    }


def price_payload(*, first_seen_at: datetime = T0 + timedelta(minutes=2)) -> dict[str, object]:
    return {
        "reference_price_uid": "example:reference-price-001",
        "market_uid": MARKET_UID,
        "price": "100000.25",
        "event_time": first_seen_at.isoformat(),
        "first_seen_at": first_seen_at.isoformat(),
    }


def test_context_raw_receipt_is_persisted_before_decoder_and_normalized(tmp_path) -> None:
    order: list[str] = []
    store = RecordingStore(tmp_path / "raw", order)
    transport = FakeTransport([response(context_payload(), url="https://example.test/context")])

    def decoder(body: bytes):
        assert order == ["raw"]
        assert any(store.receipts.rglob("*.json"))
        order.append("parse")
        return json.loads(body.decode("utf-8"))

    collector = MarketContextCollector(
        raw_store=store,
        transport=transport,
        sources=(
            MarketContextSourceConfig(
                market_uid=MARKET_UID,
                endpoint=endpoint(
                    source_uid=RULE_SOURCE_UID,
                    source_class=SourceClass.OFFICIAL_VENUE,
                    dataset="market_context",
                    url="https://example.test/context",
                ),
            ),
        ),
        decoder=decoder,
    )

    result = collector.collect(market_uid=MARKET_UID, as_of=T0 + timedelta(minutes=3), received_at=T0 + timedelta(minutes=2))

    assert order == ["raw", "parse"]
    assert result.status == CollectionStatus.COLLECTED
    assert result.context is not None
    assert result.context.provenance.content_hash == result.raw_captures[0].sha256
    assert result.raw_captures[0].object_path.exists() and result.raw_captures[0].receipt_path.exists()
    assert result.coverage[0].complete is True
    assert transport.calls == [("GET", "https://example.test/context", {}, 10.0)]


def test_collectors_have_no_default_endpoint_and_require_documented_mapping(tmp_path) -> None:
    transport = FakeTransport([])
    context = MarketContextCollector(raw_store=RawArtifactStore(tmp_path / "context"), transport=transport, sources=())
    context_result = context.collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)
    reference = ReferencePriceCollector(raw_store=RawArtifactStore(tmp_path / "reference"), transport=transport, documented_sources=())
    reference_result = reference.collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)

    assert context_result.status == CollectionStatus.UNAVAILABLE_MISSING_SOURCE
    assert reference_result.status == CollectionStatus.UNAVAILABLE_MISSING_SOURCE
    assert reference_result.admission.status == ReferenceAdmissionStatus.MISSING_DOCUMENTED_SETTLEMENT_SOURCE
    assert transport.calls == []
    with pytest.raises(TypeError):
        RawEndpoint(  # type: ignore[call-arg]
            platform="example",
            dataset="context",
            policy=policy(RULE_SOURCE_UID, SourceClass.OFFICIAL_VENUE),
            parser_version="test",
        )


def test_documented_mapping_then_primary_price_are_raw_lineaged_and_admitted(tmp_path) -> None:
    config = reference_config()
    transport = FakeTransport(
        [
            response(mapping_payload(), url=config.mapping_endpoint.endpoint_url),
            response(price_payload(), url=config.primary_price_endpoint.endpoint_url),
        ]
    )
    collector = ReferencePriceCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        documented_sources=(config,),
    )

    result = collector.collect(market_uid=MARKET_UID, as_of=T0 + timedelta(minutes=3), received_at=T0 + timedelta(minutes=2))

    assert result.status == CollectionStatus.COLLECTED
    assert result.admission.status == ReferenceAdmissionStatus.ADMITTED
    assert result.admission.observation is not None
    assert result.mapping is not None
    assert result.mapping.primary_source_uid == PRIMARY_SOURCE_UID
    assert result.admission.observation.provenance.source_uid == PRIMARY_SOURCE_UID
    assert len(result.raw_captures) == 2
    assert all(record.complete for record in result.coverage)
    assert [call[1] for call in transport.calls] == [
        "https://example.test/market-rules",
        "https://example.test/documented-benchmark",
    ]


def test_late_context_is_explicitly_excluded_after_raw_capture(tmp_path) -> None:
    transport = FakeTransport([response(context_payload(first_seen_at=T0 + timedelta(minutes=5)))])
    collector = MarketContextCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        sources=(
            MarketContextSourceConfig(
                market_uid=MARKET_UID,
                endpoint=endpoint(
                    source_uid=RULE_SOURCE_UID,
                    source_class=SourceClass.OFFICIAL_VENUE,
                    dataset="market_context",
                    url="https://example.test/context",
                ),
            ),
        ),
    )

    result = collector.collect(market_uid=MARKET_UID, as_of=T0 + timedelta(minutes=3), received_at=T0 + timedelta(minutes=6))

    assert result.status == CollectionStatus.UNAVAILABLE_LATE
    assert result.context is None
    assert len(result.raw_captures) == 1
    assert result.coverage[0].complete is False


def test_hash_conflict_is_quarantined_after_raw_receipt_not_parsed(tmp_path) -> None:
    body = json.dumps(context_payload()).encode("utf-8")
    expected = hashlib.sha256(b"different documented body").hexdigest()
    transport = FakeTransport([HttpResponse(200, body, {}, "https://example.test/context")])
    collector = MarketContextCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        sources=(
            MarketContextSourceConfig(
                market_uid=MARKET_UID,
                endpoint=endpoint(
                    source_uid=RULE_SOURCE_UID,
                    source_class=SourceClass.OFFICIAL_VENUE,
                    dataset="market_context",
                    url="https://example.test/context",
                    expected_sha256=expected,
                ),
            ),
        ),
    )

    result = collector.collect(market_uid=MARKET_UID, as_of=T0 + timedelta(minutes=3), received_at=T0 + timedelta(minutes=2))

    assert result.status == CollectionStatus.QUARANTINED_HASH_CONFLICT
    assert result.context is None
    assert result.raw_captures[0].sha256 == hashlib.sha256(body).hexdigest()
    assert result.raw_captures[0].receipt_path.exists()
