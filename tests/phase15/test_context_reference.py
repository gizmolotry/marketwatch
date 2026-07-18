"""Fail-closed point-in-time contracts for market context and reference price."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from marketleak.multimodal.context import (
    MarketContext,
    ScheduledEventState,
    SiblingRelation,
    admit_market_context,
)
from marketleak.multimodal.reference_price import (
    DocumentedReferenceSource,
    PrimarySourceAvailability,
    PrimarySourceStatus,
    ReferenceAdmissionStatus,
    ReferencePriceObservation,
    admit_reference_price,
)
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
MARKET_UID = "example:market-001"
PRIMARY_UID = "example:documented-primary-price-feed"
RULE_UID = "example:venue-market-rules"


def provenance(source_uid: str, raw_uid: str, minute: int = 2) -> Provenance:
    return Provenance(
        source_uid=source_uid,
        raw_artifact_uid=raw_uid,
        parser_version="phase15-test",
        content_hash="a" * 64,
        retrieved_at=T0 + timedelta(minutes=minute),
        source_url="https://example.test/captured-source",
    )


def reliability(source_uid: str, *, source_class: SourceClass) -> SourceReliability:
    return SourceReliability(
        source_uid=source_uid,
        source_class=source_class,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.9"),
        assessed_at=T0,
        rationale="raw-lineaged Phase 15 test source",
    )


def context_payload() -> dict[str, object]:
    return {
        "context_uid": "example:market-context-001",
        "market_uid": MARKET_UID,
        "question": "Will the documented example event happen?",
        "category": "example-category",
        "outcomes": (
            {"outcome_uid": "example:market-001-yes", "label": "Yes"},
            {"outcome_uid": "example:market-001-no", "label": "No"},
        ),
        "sibling_markets": (
            {
                "market_uid": "example:market-002",
                "relation": SiblingRelation.SAME_EVENT,
                "rationale": "Captured venue metadata reports this relation.",
            },
        ),
        "scheduled_events": (
            {
                "scheduled_event_uid": "example:calendar-event-001",
                "title": "A scheduled publication",
                "state": ScheduledEventState.SCHEDULED,
                "scheduled_for": T0 + timedelta(days=1),
                "announced_at": T0,
                "source_url": "https://example.test/calendar",
            },
        ),
        "resolution_schedule": {
            "scheduled_open_at": T0 - timedelta(days=1),
            "scheduled_close_at": T0 + timedelta(days=2),
            "expected_resolution_at": T0 + timedelta(days=2, hours=1),
            "settlement_deadline_at": T0 + timedelta(days=3),
            "resolution_rule_url": "https://example.test/market-rules",
        },
        "event_time": T0,
        "first_seen_at": T0 + timedelta(minutes=1),
        "retrieved_at": T0 + timedelta(minutes=2),
        "ingested_at": T0 + timedelta(minutes=3),
        "provenance": provenance(RULE_UID, "example:raw-context-001"),
        "reliability": reliability(RULE_UID, source_class=SourceClass.OFFICIAL_VENUE),
    }


def mapping_payload() -> dict[str, object]:
    return {
        "mapping_uid": "example:reference-mapping-001",
        "market_uid": MARKET_UID,
        "market_rule_source_uid": RULE_UID,
        "settlement_source_uid": "example:documented-settlement-benchmark",
        "primary_source_uid": PRIMARY_UID,
        "asset_symbol": "BTC",
        "quote_currency": "USD",
        "settlement_rule_url": "https://example.test/market-rules",
        "primary_source_url": "https://example.test/documented-benchmark",
        "event_time": T0,
        "first_seen_at": T0 + timedelta(minutes=1),
        "retrieved_at": T0 + timedelta(minutes=2),
        "ingested_at": T0 + timedelta(minutes=3),
        "provenance": provenance(RULE_UID, "example:raw-mapping-001"),
        "reliability": reliability(RULE_UID, source_class=SourceClass.OFFICIAL_VENUE),
    }


def availability(status: PrimarySourceStatus = PrimarySourceStatus.AVAILABLE) -> PrimarySourceAvailability:
    return PrimarySourceAvailability(
        availability_uid="example:availability-001",
        primary_source_uid=PRIMARY_UID,
        status=status,
        checked_at=T0,
        first_seen_at=T0 + timedelta(minutes=1),
        retrieved_at=T0 + timedelta(minutes=2),
        ingested_at=T0 + timedelta(minutes=3),
        provenance=provenance(PRIMARY_UID, "example:raw-availability-001"),
        reliability=reliability(PRIMARY_UID, source_class=SourceClass.PRIMARY_SOURCE),
        reason=None if status == PrimarySourceStatus.AVAILABLE else "captured source-health response reported no usable feed",
    )


def reference_observation(*, event_minute: int = 4, ingested_minute: int = 5) -> ReferencePriceObservation:
    mapping = DocumentedReferenceSource.model_validate(mapping_payload())
    return ReferencePriceObservation(
        reference_price_uid="example:reference-price-001",
        market_uid=MARKET_UID,
        price=Decimal("100000.25"),
        event_time=T0 + timedelta(minutes=event_minute),
        first_seen_at=T0 + timedelta(minutes=event_minute),
        retrieved_at=T0 + timedelta(minutes=event_minute),
        ingested_at=T0 + timedelta(minutes=ingested_minute),
        source_mapping=mapping,
        primary_source_availability=availability(),
        provenance=provenance(PRIMARY_UID, "example:raw-reference-001", minute=event_minute),
        reliability=reliability(PRIMARY_UID, source_class=SourceClass.PRIMARY_SOURCE),
    )


def test_market_context_is_raw_lineaged_and_point_in_time() -> None:
    context = MarketContext.model_validate(context_payload())

    assert context.provenance.raw_artifact_uid == "example:raw-context-001"
    assert context.scheduled_events[0].scheduled_for > T0  # future calendar fact, known now
    assert admit_market_context(context, as_of=T0 + timedelta(minutes=3)) == context
    assert admit_market_context(context, as_of=T0 + timedelta(minutes=2)) is None
    assert not hasattr(context, "suspicion")


def test_context_rejects_self_sibling_and_invalid_resolution_schedule() -> None:
    self_sibling = context_payload()
    self_sibling["sibling_markets"] = (
        {
            "market_uid": MARKET_UID,
                "relation": SiblingRelation.SAME_EVENT,
                "rationale": "Not allowed.",
            },
        )
    with pytest.raises(ValidationError, match="own sibling"):
        MarketContext.model_validate(self_sibling)

    bad_schedule = context_payload()
    bad_schedule["resolution_schedule"] = {
        "scheduled_close_at": T0 + timedelta(days=2),
        "expected_resolution_at": T0 + timedelta(days=1),
        "resolution_rule_url": "https://example.test/market-rules",
    }
    with pytest.raises(ValidationError, match="expected_resolution_at"):
        MarketContext.model_validate(bad_schedule)


def test_documented_source_is_required_and_generic_btc_price_is_not_substituted() -> None:
    reference = reference_observation()

    admission = admit_reference_price(
        market_uid=MARKET_UID,
        as_of=T0 + timedelta(minutes=6),
        documented_sources=(),
        primary_source_availability=availability(),
        observation=reference,
    )

    assert admission.status == ReferenceAdmissionStatus.MISSING_DOCUMENTED_SETTLEMENT_SOURCE
    assert admission.observation is None
    assert admission.source_mapping is None


def test_reference_requires_primary_source_availability_and_raw_lineage() -> None:
    mapping = DocumentedReferenceSource.model_validate(mapping_payload())
    reference = reference_observation()

    admitted = admit_reference_price(
        market_uid=MARKET_UID,
        as_of=T0 + timedelta(minutes=6),
        documented_sources=[mapping],
        primary_source_availability=availability(),
        observation=reference,
    )

    assert admitted.status == ReferenceAdmissionStatus.ADMITTED
    assert admitted.observation is not None
    assert admitted.observation.provenance.raw_artifact_uid == "example:raw-reference-001"
    assert admitted.observation.provenance.content_hash == "a" * 64

    unavailable = admit_reference_price(
        market_uid=MARKET_UID,
        as_of=T0 + timedelta(minutes=6),
        documented_sources=[mapping],
        primary_source_availability=availability(PrimarySourceStatus.UNAVAILABLE),
        observation=None,
    )
    assert unavailable.status == ReferenceAdmissionStatus.PRIMARY_SOURCE_NOT_AVAILABLE


def test_future_reference_observation_is_rejected_at_earlier_cutoff() -> None:
    mapping = DocumentedReferenceSource.model_validate(mapping_payload())
    future_reference = reference_observation(event_minute=8, ingested_minute=9)

    result = admit_reference_price(
        market_uid=MARKET_UID,
        as_of=T0 + timedelta(minutes=6),
        documented_sources=[mapping],
        primary_source_availability=availability(),
        observation=future_reference,
    )

    assert result.status == ReferenceAdmissionStatus.REFERENCE_NOT_POINT_IN_TIME
    assert result.observation is None


def test_example_configs_are_strict_contract_instances() -> None:
    root = Path(__file__).resolve().parents[2]
    contexts = json.loads((root / "configs" / "phase15" / "market_context.example.json").read_text())
    sources = json.loads((root / "configs" / "phase15" / "reference_sources.example.json").read_text())

    context = MarketContext.model_validate_json(json.dumps(contexts["contexts"][0]))
    mapping = DocumentedReferenceSource.model_validate_json(json.dumps(sources["documented_sources"][0]))
    source_status = PrimarySourceAvailability.model_validate_json(
        json.dumps(sources["primary_source_availability"][0])
    )

    assert context.market_uid == mapping.market_uid == "example:market-001"
    assert source_status.primary_source_uid == mapping.primary_source_uid
