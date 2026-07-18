from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketleak.multimodal.event_store import AppendStatus, EventMemoryStore
from marketleak.multimodal.labels import (
    EvidenceStrength,
    ExternalLegalAudit,
    ExternalLegalOutcome,
    HumanDisposition,
    MultiAxisAdjudication,
    ObservableMechanism,
)
from marketleak.multimodal.schemas import (
    EventMemorySnapshot,
    MarketStateSlice,
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    OnChainSettlementFact,
    Provenance,
    PublicDocumentClaim,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
    snapshot_manifest_for,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
HASH = "a" * 64


def provenance(*, source: str = "source:venue", minutes: int = 0) -> Provenance:
    return Provenance(
        source_uid=source,
        raw_artifact_uid=f"raw:{source.split(':', 1)[1]}-{minutes}",
        parser_version="phase15-test",
        content_hash=HASH,
        retrieved_at=T0 + timedelta(minutes=minutes),
        source_url="https://example.test/raw",
    )


def reliability(*, source: str = "source:venue") -> SourceReliability:
    return SourceReliability(
        source_uid=source,
        source_class=SourceClass.OFFICIAL_VENUE,
        tier=ReliabilityTier.HIGH,
        score=Decimal("1"),
        assessed_at=T0,
        rationale="official raw endpoint",
    )


def observed(modality: Modality) -> tuple[ModalityMissingness, ...]:
    return (ModalityMissingness(modality=modality, status=MissingnessStatus.OBSERVED),)


def market_slice(
    *,
    uid: str = "event:market-1",
    event_minutes: int = 5,
    ingested_minutes: int = 6,
    notional: str = "100",
) -> MarketStateSlice:
    return MarketStateSlice(
        event_uid=uid,
        event_time=T0 + timedelta(minutes=event_minutes),
        ingested_at=T0 + timedelta(minutes=ingested_minutes),
        provenance=provenance(minutes=event_minutes),
        reliability=reliability(),
        missingness=observed(Modality.MARKET_STATE),
        market_uid="market:btc-15m",
        outcome_uid="outcome:btc-up",
        window_starts_at=T0 + timedelta(minutes=event_minutes - 5),
        window_ends_at=T0 + timedelta(minutes=event_minutes),
        last_trade_price=Decimal("0.55"),
        trade_notional=Decimal(notional),
        fill_count=2,
    )


def document_claim(*, uid: str = "event:document-1", first_seen_minutes: int = 8) -> PublicDocumentClaim:
    first_seen = T0 + timedelta(minutes=first_seen_minutes)
    return PublicDocumentClaim(
        event_uid=uid,
        event_time=first_seen,
        ingested_at=first_seen + timedelta(minutes=1),
        provenance=provenance(source="source:primary", minutes=first_seen_minutes),
        reliability=SourceReliability(
            source_uid="source:primary",
            source_class=SourceClass.PRIMARY_SOURCE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("0.9"),
            assessed_at=T0,
            rationale="configured primary feed",
        ),
        missingness=observed(Modality.PUBLIC_EVIDENCE),
        document_uid="document:release-1",
        claim_uid="claim:release-1",
        canonical_url="https://example.test/release",
        document_hash="b" * 64,
        claimed_published_at=T0 + timedelta(minutes=7),
        first_seen_at=first_seen,
        retrieved_at=first_seen,
    )


def chain_fact(*, uid: str = "event:chain-1") -> OnChainSettlementFact:
    event_time = T0 + timedelta(minutes=4)
    return OnChainSettlementFact(
        event_uid=uid,
        event_time=event_time,
        ingested_at=T0 + timedelta(minutes=6),
        provenance=provenance(source="source:polygon", minutes=4),
        reliability=SourceReliability(
            source_uid="source:polygon",
            source_class=SourceClass.BLOCKCHAIN_RPC,
            tier=ReliabilityTier.HIGH,
            score=Decimal("0.95"),
            assessed_at=T0,
            rationale="block header and log retrieved together",
        ),
        missingness=observed(Modality.ONCHAIN_SETTLEMENT),
        chain="polygon",
        chain_id=137,
        transaction_hash="0xabc",
        log_index=1,
        block_number=123,
        block_timestamp=event_time,
        contract_address="0xcontract",
        token_uid="token:yes",
        market_uid="market:btc-15m",
        outcome_uid="outcome:btc-up",
        observed_wallet_uid="wallet:public-1",
        quantity=Decimal("4"),
    )


def test_as_of_excludes_future_events_and_later_seen_public_evidence():
    store = EventMemoryStore()
    market = market_slice()
    future_document = document_claim(first_seen_minutes=8)
    store.append(future_document)
    store.append(market)

    snapshot = store.snapshot_as_of(T0 + timedelta(minutes=7))

    assert [record.event_uid for record in snapshot.records] == ["event:market-1"]
    assert snapshot.manifest.as_of == T0 + timedelta(minutes=7)
    assert future_document.available_at > snapshot.manifest.as_of


def test_snapshot_rejects_document_that_became_available_after_as_of():
    record = document_claim(first_seen_minutes=8)
    manifest = snapshot_manifest_for(as_of=T0 + timedelta(minutes=7), records=(record,))

    with pytest.raises(ValidationError, match="evidence cannot become available"):
        EventMemorySnapshot(manifest=manifest, records=(record,))


def test_same_semantic_event_is_idempotent_but_collision_is_quarantined():
    store = EventMemoryStore()
    original = market_slice()
    retry_payload = original.model_dump()
    retry_payload["ingested_at"] = T0 + timedelta(minutes=20)
    retry = MarketStateSlice.model_validate(retry_payload)
    collision_payload = original.model_dump()
    collision_payload["trade_notional"] = Decimal("101")
    collision = MarketStateSlice.model_validate(collision_payload)

    assert store.append(original).status == AppendStatus.INSERTED
    assert store.append(retry).status == AppendStatus.IDEMPOTENT
    result = store.append(collision)

    assert result.status == AppendStatus.QUARANTINED
    assert result.collision is not None
    assert store.event_count == 1
    assert len(store.quarantined_collisions()) == 1
    assert store.append(collision).collision == result.collision
    assert len(store.quarantined_collisions()) == 1


def test_snapshots_are_deterministic_across_append_order():
    market = market_slice()
    chain = chain_fact()
    first = EventMemoryStore([market, chain]).snapshot_as_of(T0 + timedelta(minutes=10))
    second = EventMemoryStore([chain, market]).snapshot_as_of(T0 + timedelta(minutes=10))

    assert first.manifest == second.manifest
    assert first.records == second.records
    assert [record.event_uid for record in first.records] == ["event:chain-1", "event:market-1"]


def test_label_ontology_rejects_unknown_training_and_model_ground_truth():
    base = dict(
        label_uid="label:1",
        event_uid="event:market-1",
        observable_mechanism=ObservableMechanism.UNKNOWN,
        evidence_strength=EvidenceStrength.LIMITED,
        disposition=HumanDisposition.INSUFFICIENT_EVIDENCE,
        rationale="coverage is incomplete",
        adjudicator="human analyst",
        adjudicated_at=T0,
        training_eligible=True,
    )
    with pytest.raises(ValidationError, match="unknown and unmapped"):
        MultiAxisAdjudication(**base)

    valid = {**base, "training_eligible": False}
    assert MultiAxisAdjudication(**valid).model_generated is False
    with pytest.raises(ValidationError):
        MultiAxisAdjudication(**{**valid, "model_generated": True})
    with pytest.raises(ValidationError):
        MultiAxisAdjudication(**{**valid, "disposition": "confirmed_insider_trading"})


def test_external_legal_outcome_is_audit_only_not_a_training_target():
    audit = ExternalLegalAudit(
        authority="CFTC",
        outcome=ExternalLegalOutcome.EXTERNALLY_ADJUDICATED_MISCONDUCT,
        public_reference="https://example.test/case",
        adjudicated_at=T0,
    )
    with pytest.raises(ValidationError, match="audit-only"):
        MultiAxisAdjudication(
            label_uid="label:legal-1",
            event_uid="event:market-1",
            observable_mechanism=ObservableMechanism.UNEXPLAINED_ACTIVITY,
            evidence_strength=EvidenceStrength.CORROBORATED,
            disposition=HumanDisposition.ESCALATE_FOR_REVIEW,
            rationale="human escalation and an external case record",
            adjudicator="human analyst",
            adjudicated_at=T0,
            training_eligible=True,
            external_legal_audit=audit,
        )
