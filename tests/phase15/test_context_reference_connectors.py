from __future__ import annotations

import hashlib
import json
from dataclasses import replace
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
        self.security_bounds: list[tuple[int | None, tuple[str, ...] | None]] = []

    def request(
        self, method: str, url: str, *, params, timeout: float,
        max_response_bytes=None, approved_addresses=None,
    ) -> HttpResponse:
        self.calls.append((method, url, dict(params or {}), timeout))
        self.security_bounds.append((max_response_bytes, approved_addresses))
        if not self.responses:
            raise AssertionError("collector attempted an unconfigured/default endpoint")
        response = self.responses.pop(0)
        if not response.peer_address and approved_addresses:
            response = replace(response, peer_address=approved_addresses[0])
        return response


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


def test_configured_context_receipt_redacts_query_parameters_and_response_secrets(tmp_path) -> None:
    secret = "signed-secret-value"
    configured_endpoint = RawEndpoint(
        endpoint_url=f"https://example.test/context?signature={secret}",
        platform="example",
        dataset="market_context",
        policy=policy(RULE_SOURCE_UID, SourceClass.OFFICIAL_VENUE),
        parser_version="phase15-security-test-v1",
        params={"api_key": secret, "market": MARKET_UID},
    )
    transport = FakeTransport(
        [
            HttpResponse(
                200,
                json.dumps(context_payload()).encode("utf-8"),
                {"Content-Type": "application/json", "Set-Cookie": f"session={secret}"},
                configured_endpoint.endpoint_url,
            )
        ]
    )
    store = RawArtifactStore(tmp_path / "raw")
    result = MarketContextCollector(
        raw_store=store,
        transport=transport,
        sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
    ).collect(
        market_uid=MARKET_UID,
        as_of=T0 + timedelta(minutes=3),
        received_at=T0 + timedelta(minutes=2),
    )

    receipt = store.read_receipt(result.raw_captures[0])
    serialized = json.dumps(receipt)
    assert secret not in serialized
    assert receipt["request"]["parameter_names"] == ["api_key", "market"]
    assert receipt["request"]["redacted_parameter_names"] == ["api_key"]
    assert "set-cookie" not in receipt["response_metadata"]["headers"]
    assert secret not in result.context.provenance.source_url
    assert secret not in json.dumps(result.coverage[0].filters)


def test_configured_context_oversize_is_explicit_and_not_captured(tmp_path) -> None:
    configured_endpoint = RawEndpoint(
        endpoint_url="https://example.test/context",
        platform="example",
        dataset="market_context",
        policy=policy(RULE_SOURCE_UID, SourceClass.OFFICIAL_VENUE),
        parser_version="phase15-security-test-v1",
        max_response_bytes=1024,
    )
    transport = FakeTransport([HttpResponse(200, b"", {}, configured_endpoint.endpoint_url, oversized=True)])
    store = RawArtifactStore(tmp_path / "raw")
    result = MarketContextCollector(
        raw_store=store,
        transport=transport,
        sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
    ).collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)

    assert result.status == CollectionStatus.UNAVAILABLE_INCOMPLETE
    assert result.raw_captures == ()
    assert not any(store.receipts.rglob("*.json"))
    assert "exceeded max_response_bytes=1024" in result.reason


def test_context_endpoint_rejects_lexically_unsafe_urls_without_construction_network():
    for url in (
        "http://example.test/context",
        "https://user:secret@example.test/context",
        "https://example.test:8443/context",
        "https://127.0.0.1/context",
        "https://169.254.169.254/context",
    ):
        with pytest.raises(ValueError):
            endpoint(
                source_uid=RULE_SOURCE_UID,
                source_class=SourceClass.OFFICIAL_VENUE,
                dataset="market_context",
                url=url,
            )


def test_context_private_dns_is_rejected_before_transport_and_capture(tmp_path) -> None:
    configured_endpoint = endpoint(
        source_uid=RULE_SOURCE_UID,
        source_class=SourceClass.OFFICIAL_VENUE,
        dataset="market_context",
        url="https://example.test/context",
    )
    transport = FakeTransport([response(context_payload())])
    store = RawArtifactStore(tmp_path / "raw")
    result = MarketContextCollector(
        raw_store=store,
        transport=transport,
        resolver=lambda _host, _port: ("10.0.0.8",),
        sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
    ).collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)

    assert result.status == CollectionStatus.REJECTED_UNSAFE_SOURCE
    assert transport.calls == []
    assert result.raw_captures == ()


def test_context_effective_origin_and_peer_must_match_approved_connection(tmp_path) -> None:
    configured_endpoint = endpoint(
        source_uid=RULE_SOURCE_UID,
        source_class=SourceClass.OFFICIAL_VENUE,
        dataset="market_context",
        url="https://example.test/context",
    )
    for effective_url, peer_address in (
        ("https://attacker.test/context", "93.184.216.34"),
        (configured_endpoint.endpoint_url, "127.0.0.1"),
    ):
        transport = FakeTransport(
            [
                HttpResponse(
                    200,
                    json.dumps(context_payload()).encode(),
                    {},
                    effective_url,
                    peer_address=peer_address,
                )
            ]
        )
        store = RawArtifactStore(tmp_path / effective_url.split("//", 1)[1].replace("/", "-"))
        result = MarketContextCollector(
            raw_store=store,
            transport=transport,
            resolver=lambda _host, _port: ("93.184.216.34",),
            sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
        ).collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)

        assert result.status == CollectionStatus.REJECTED_UNSAFE_SOURCE
        assert result.raw_captures == ()
        assert not any(store.receipts.rglob("*.json"))


def test_context_transport_receives_the_approved_address_and_read_bound(tmp_path) -> None:
    configured_endpoint = endpoint(
        source_uid=RULE_SOURCE_UID,
        source_class=SourceClass.OFFICIAL_VENUE,
        dataset="market_context",
        url="https://example.test/context",
    )
    transport = FakeTransport([response(context_payload())])
    MarketContextCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        resolver=lambda _host, _port: ("93.184.216.34",),
        sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
    ).collect(market_uid=MARKET_UID, as_of=T0 + timedelta(minutes=3), received_at=T0 + timedelta(minutes=2))

    assert transport.security_bounds == [(2_000_000, ("93.184.216.34",))]


def test_context_missing_peer_attestation_is_rejected_before_capture(tmp_path) -> None:
    configured_endpoint = endpoint(
        source_uid=RULE_SOURCE_UID,
        source_class=SourceClass.OFFICIAL_VENUE,
        dataset="market_context",
        url="https://example.test/context",
    )

    class MissingPeerTransport:
        def request(
            self, method, url, *, params, timeout, max_response_bytes, approved_addresses
        ):
            return response(context_payload(), url=url)

    store = RawArtifactStore(tmp_path / "raw")
    result = MarketContextCollector(
        raw_store=store,
        transport=MissingPeerTransport(),
        resolver=lambda _host, _port: ("93.184.216.34",),
        sources=(MarketContextSourceConfig(market_uid=MARKET_UID, endpoint=configured_endpoint),),
    ).collect(market_uid=MARKET_UID, as_of=T0, received_at=T0)
    assert result.status == CollectionStatus.REJECTED_UNSAFE_SOURCE
    assert result.raw_captures == ()
    assert not any(store.receipts.rglob("*.json"))
