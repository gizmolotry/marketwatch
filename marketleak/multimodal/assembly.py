"""Deterministic adapters from v2 facts into a Phase 15 event input.

This module is deliberately an assembly boundary, not a decision engine.  It
admits only facts that were available by the requested ``as_of`` time and
keeps modality absence explicit.  It does not derive a liquidity heuristic,
an identity claim, a fraud probability, or a review disposition.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Iterable, Mapping, Sequence

from pydantic import Field, field_validator, model_validator

from marketleak.domain import IntegrityAssessment, TradeFill
from marketleak.domain.common import NonEmptyStr, NonNegativeDecimal, StableUID
from marketleak.evidence.normalize import NormalizedEvidence
from marketleak.multimodal.features import MarketFeatureSnapshot, build_market_feature_snapshot
from marketleak.multimodal.schemas import (
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    OnChainSettlementFact,
    PHASE15_SCHEMA_VERSION,
    Phase15Model,
    Provenance,
    PublicDocumentClaim,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
    canonical_hash,
)


POLYGON_CHAIN_ID = 137
ORDER_FILLED_EVENT = "OrderFilled"


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _admissible(*, event_time: datetime, ingested_at: datetime, as_of: datetime) -> bool:
    return event_time <= as_of and ingested_at <= as_of


class AssemblyError(ValueError):
    """Raised when caller-supplied material cannot safely enter an event."""


class EvidenceSourceContract(Phase15Model):
    """Configured source policy required before normalized evidence is adapted."""

    source_name: NonEmptyStr
    source_uid: StableUID
    source_class: SourceClass
    reliability_tier: ReliabilityTier
    reliability_score: Decimal = Field(strict=True, ge=Decimal("0"), le=Decimal("1"))
    rationale: NonEmptyStr


class ReferencePriceFact(Phase15Model):
    """A market reference price with an explicit documented settlement source.

    A generic BTC/USD feed is intentionally not accepted: callers must state
    the market's documented settlement/reference source and retain raw lineage.
    """

    schema_version: NonEmptyStr = PHASE15_SCHEMA_VERSION
    reference_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    asset_symbol: NonEmptyStr
    quote_currency: NonEmptyStr
    price: NonNegativeDecimal
    observed_at: datetime
    ingested_at: datetime
    settlement_source_uid: StableUID
    documented_settlement_source_url: NonEmptyStr
    provenance: Provenance
    reliability: SourceReliability

    @field_validator("observed_at", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @field_validator("documented_settlement_source_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        if not value.startswith(("https://", "http://")):
            raise ValueError("documented_settlement_source_url must be an http(s) URL")
        return value

    @model_validator(mode="after")
    def validate_reference_contract(self) -> "ReferencePriceFact":
        if self.observed_at > self.ingested_at:
            raise ValueError("reference price cannot be observed after it is ingested")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("reference reliability must refer to provenance source")
        if self.settlement_source_uid != self.provenance.source_uid:
            raise ValueError("settlement source must match reference provenance source")
        return self

    @property
    def available_at(self) -> datetime:
        return self.ingested_at


class RawOrderFilledFact(Phase15Model):
    """The minimum raw verified Polygon `OrderFilled` fact admitted to v2.

    It intentionally has no price field.  Price cannot be reconstructed from a
    Conditional Tokens ``TransferSingle`` log, and accepting one here would
    reintroduce the legacy placeholder-price defect.
    """

    event_type: NonEmptyStr
    chain: NonEmptyStr
    chain_id: int = Field(strict=True, ge=0)
    transaction_hash: NonEmptyStr
    log_index: int = Field(strict=True, ge=0)
    block_number: int = Field(strict=True, ge=0)
    block_timestamp: datetime
    contract_address: NonEmptyStr
    order_hash: NonEmptyStr
    maker_uid: StableUID | None = None
    taker_uid: StableUID | None = None
    token_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    venue_transaction_uid: StableUID
    fill_uid: StableUID
    maker_amount: NonNegativeDecimal
    taker_amount: NonNegativeDecimal
    fee: NonNegativeDecimal | None = None
    provenance: Provenance
    reliability: SourceReliability
    raw_verified: bool = True

    @field_validator("block_timestamp")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="block_timestamp")

    @model_validator(mode="after")
    def validate_order_filled_only(self) -> "RawOrderFilledFact":
        if self.event_type != ORDER_FILLED_EVENT:
            raise ValueError("only raw OrderFilled events are accepted for settlement corroboration")
        if self.chain.casefold() != "polygon" or self.chain_id != POLYGON_CHAIN_ID:
            raise ValueError("only Polygon chain_id=137 OrderFilled facts are accepted")
        if not self.raw_verified:
            raise ValueError("on-chain settlement facts require raw verified provenance")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("on-chain reliability must refer to provenance source")
        return self


class MultimodalEvent(Phase15Model):
    """A causally frozen event input for later retrieval/fusion stages."""

    schema_version: NonEmptyStr = PHASE15_SCHEMA_VERSION
    event_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    as_of: datetime
    decision_time: datetime
    market_features: MarketFeatureSnapshot | None = None
    reference_price: ReferencePriceFact | None = None
    public_evidence: tuple[PublicDocumentClaim, ...] = ()
    onchain_settlements: tuple[OnChainSettlementFact, ...] = ()
    assessment_uid: StableUID | None = None
    source_high_watermarks: dict[NonEmptyStr, datetime]
    modality_availability: tuple[ModalityMissingness, ...]
    not_proof_of_fraud: bool = True

    @field_validator("as_of", "decision_time")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @field_validator("source_high_watermarks")
    @classmethod
    def validate_watermarks(cls, value: dict[str, datetime]) -> dict[str, datetime]:
        return {name: _utc(timestamp, field_name=f"source_high_watermarks[{name}]") for name, timestamp in value.items()}

    @model_validator(mode="after")
    def validate_event(self) -> "MultimodalEvent":
        if self.decision_time != self.as_of:
            raise ValueError("decision_time must equal the frozen as_of cutoff")
        if any(value > self.as_of for value in self.source_high_watermarks.values()):
            raise ValueError("source high-watermarks cannot exceed as_of")
        if len(self.modality_availability) != len(Modality) or {item.modality for item in self.modality_availability} != set(Modality):
            raise ValueError("modality_availability must name every modality exactly once")
        availability = {item.modality: item.status for item in self.modality_availability}
        if (availability[Modality.MARKET_STATE] == MissingnessStatus.OBSERVED) != (self.market_features is not None):
            raise ValueError("market-state availability must match market_features presence")
        if self.market_features is not None:
            if self.market_features.market_uid != self.market_uid or self.market_features.outcome_uid != self.outcome_uid:
                raise ValueError("market feature identifiers must match the event")
            if self.market_features.as_of > self.as_of:
                raise ValueError("market feature snapshot is after event as_of")
        if self.reference_price is not None:
            if self.reference_price.market_uid != self.market_uid or self.reference_price.outcome_uid not in {None, self.outcome_uid}:
                raise ValueError("reference price identifiers must match the event")
            if self.reference_price.available_at > self.as_of or self.reference_price.observed_at > self.as_of:
                raise ValueError("reference price cannot enter before it is available")
        if (availability[Modality.PUBLIC_EVIDENCE] == MissingnessStatus.OBSERVED) != bool(self.public_evidence):
            raise ValueError("public-evidence availability must match public evidence presence")
        if (availability[Modality.ONCHAIN_SETTLEMENT] == MissingnessStatus.OBSERVED) != bool(self.onchain_settlements):
            raise ValueError("on-chain availability must match settlement presence")
        if len({item.event_uid for item in self.public_evidence}) != len(self.public_evidence):
            raise ValueError("public evidence event_uids must be unique")
        if len({item.event_uid for item in self.onchain_settlements}) != len(self.onchain_settlements):
            raise ValueError("on-chain settlement event_uids must be unique")
        for document in self.public_evidence:
            if document.available_at > self.as_of or document.event_time > self.as_of:
                raise ValueError("future public evidence cannot enter an event")
        for settlement in self.onchain_settlements:
            if settlement.available_at > self.as_of or settlement.event_time > self.as_of:
                raise ValueError("future on-chain settlement cannot enter an event")
            if settlement.market_uid != self.market_uid or settlement.outcome_uid != self.outcome_uid:
                raise ValueError("on-chain settlement identifiers must match event")
        if not self.not_proof_of_fraud:
            raise ValueError("multimodal events must preserve the not-proof-of-fraud boundary")
        return self


def adapt_public_evidence(
    evidence: NormalizedEvidence,
    *,
    contract: EvidenceSourceContract,
) -> PublicDocumentClaim:
    """Convert persistent normalized evidence only when it retains raw lineage."""

    if not isinstance(evidence, NormalizedEvidence):
        evidence = NormalizedEvidence.model_validate(evidence)
    if evidence.source != contract.source_name:
        raise AssemblyError("evidence source does not match its configured source contract")
    if not evidence.raw_artifact_uid:
        raise AssemblyError("normalized public evidence without raw_artifact_uid is not admissible")
    if not evidence.url.startswith(("https://", "http://")):
        raise AssemblyError("normalized public evidence requires an http(s) canonical URL")
    reliability = SourceReliability(
        source_uid=contract.source_uid,
        source_class=contract.source_class,
        tier=contract.reliability_tier,
        score=contract.reliability_score,
        assessed_at=evidence.retrieved_at,
        rationale=contract.rationale,
    )
    digest = canonical_hash({"evidence_uid": evidence.evidence_uid, "content_hash": evidence.content_hash})
    return PublicDocumentClaim(
        event_uid=f"event-evidence:{digest}",
        event_time=evidence.first_seen_at,
        ingested_at=evidence.retrieved_at,
        provenance=Provenance(
            source_uid=contract.source_uid,
            raw_artifact_uid=evidence.raw_artifact_uid,
            parser_version=evidence.parser_version,
            content_hash=evidence.content_hash,
            retrieved_at=evidence.retrieved_at,
            source_url=evidence.url,
        ),
        reliability=reliability,
        missingness=(ModalityMissingness(modality=Modality.PUBLIC_EVIDENCE, status=MissingnessStatus.OBSERVED),),
        document_uid=f"document:{canonical_hash({'document_uid': evidence.document_uid})}",
        claim_uid=f"claim:{canonical_hash({'evidence_uid': evidence.evidence_uid})}",
        canonical_url=evidence.url,
        document_hash=evidence.content_hash,
        text=evidence.searchable_text or None,
        claimed_published_at=evidence.claimed_published_at,
        first_seen_at=evidence.first_seen_at,
        retrieved_at=evidence.retrieved_at,
        modified_at=evidence.modified_at,
    )


def adapt_reference_price(
    value: ReferencePriceFact | Mapping[str, object],
    *,
    as_of: datetime,
) -> ReferencePriceFact | None:
    """Admit a documented reference price only when causally available."""

    reference = value if isinstance(value, ReferencePriceFact) else ReferencePriceFact.model_validate(value)
    cutoff = _utc(as_of, field_name="as_of")
    return reference if _admissible(event_time=reference.observed_at, ingested_at=reference.ingested_at, as_of=cutoff) else None


def adapt_order_filled(
    value: RawOrderFilledFact | Mapping[str, object],
    *,
    fill: TradeFill,
    as_of: datetime,
) -> OnChainSettlementFact | None:
    """Adapt an exactly joined raw Polygon OrderFilled fact, or exclude it.

    Legacy ``TransferSingle`` records, generic Bitcoin claims, placeholder
    prices, and unjoined logs are rejected rather than carried as context.
    """

    if not isinstance(fill, TradeFill):
        raise AssemblyError("on-chain corroboration requires a canonical TradeFill")
    if isinstance(value, Mapping):
        event_type = value.get("event_type")
        chain = str(value.get("chain", "")).casefold()
        if event_type != ORDER_FILLED_EVENT:
            raise AssemblyError("legacy TransferSingle or non-OrderFilled logs are not settlement evidence")
        if chain != "polygon" or value.get("chain_id") != POLYGON_CHAIN_ID:
            raise AssemblyError("generic Bitcoin and non-Polygon claims are not settlement evidence")
        if "price" in value or "placeholder_price" in value:
            raise AssemblyError("OrderFilled settlement adapter never accepts a price or placeholder price")
    raw = value if isinstance(value, RawOrderFilledFact) else RawOrderFilledFact.model_validate(value)
    if raw.fill_uid != fill.fill_uid:
        raise AssemblyError("OrderFilled fact must name the exact canonical fill_uid")
    if raw.venue_transaction_uid != fill.transaction_uid:
        raise AssemblyError("OrderFilled fact must name the exact canonical venue transaction UID")
    if raw.market_uid != fill.market_uid or raw.outcome_uid != fill.outcome_uid:
        raise AssemblyError("OrderFilled market/outcome must exactly match the canonical fill")
    cutoff = _utc(as_of, field_name="as_of")
    if not _admissible(event_time=raw.block_timestamp, ingested_at=raw.provenance.retrieved_at, as_of=cutoff):
        return None
    digest = canonical_hash({"transaction_hash": raw.transaction_hash, "log_index": raw.log_index, "fill_uid": raw.fill_uid})
    return OnChainSettlementFact(
        event_uid=f"event-onchain:{digest}",
        event_time=raw.block_timestamp,
        ingested_at=raw.provenance.retrieved_at,
        provenance=raw.provenance,
        reliability=raw.reliability,
        missingness=(ModalityMissingness(modality=Modality.ONCHAIN_SETTLEMENT, status=MissingnessStatus.OBSERVED),),
        chain="polygon",
        chain_id=POLYGON_CHAIN_ID,
        transaction_hash=raw.transaction_hash,
        log_index=raw.log_index,
        block_number=raw.block_number,
        block_timestamp=raw.block_timestamp,
        contract_address=raw.contract_address,
        token_uid=raw.token_uid,
        market_uid=raw.market_uid,
        outcome_uid=raw.outcome_uid,
        observed_wallet_uid=raw.maker_uid or raw.taker_uid,
        # The two OrderFilled amounts are assets on opposite sides of the
        # exchange.  Without an independently verified asset-side decoder we
        # must not guess which one is the outcome-token quantity.
        quantity=None,
    )


def _availability(*, modality: Modality, observed: bool, absent_status: MissingnessStatus, reason: str) -> ModalityMissingness:
    return ModalityMissingness(
        modality=modality,
        status=MissingnessStatus.OBSERVED if observed else absent_status,
        reason=None if observed else reason,
    )


def assemble_multimodal_event(
    *,
    market_uid: str,
    outcome_uid: str | None,
    as_of: datetime,
    observations: Iterable[object] = (),
    fills: Iterable[object] = (),
    books: Iterable[object] = (),
    evidence: Iterable[PublicDocumentClaim] = (),
    onchain_settlements: Iterable[OnChainSettlementFact] = (),
    reference_price: ReferencePriceFact | Mapping[str, object] | None = None,
    assessment: IntegrityAssessment | None = None,
    source_high_watermarks: Mapping[str, datetime] | None = None,
) -> MultimodalEvent:
    """Assemble a deterministic, causally admissible event input.

    Inputs after ``as_of`` are excluded, including facts which occurred earlier
    but were ingested later.  The returned object represents available data and
    gaps; it intentionally makes no decision from liquidity or volume.
    """

    cutoff = _utc(as_of, field_name="as_of")
    features = build_market_feature_snapshot(
        market_uid=market_uid,
        outcome_uid=outcome_uid,
        as_of=cutoff,
        observations=observations,
        fills=fills,
        books=books,
        source_high_watermarks=source_high_watermarks,
    )
    market_features = features if features.market_state.status == MissingnessStatus.OBSERVED else None
    documents = tuple(
        sorted(
            (
                item
                for item in evidence
                if isinstance(item, PublicDocumentClaim) and _admissible(event_time=item.event_time, ingested_at=item.available_at, as_of=cutoff)
            ),
            key=lambda item: (item.available_at, item.event_uid),
        )
    )
    settlements = tuple(
        sorted(
            (
                item
                for item in onchain_settlements
                if isinstance(item, OnChainSettlementFact)
                and item.market_uid == market_uid
                and item.outcome_uid == outcome_uid
                and _admissible(event_time=item.event_time, ingested_at=item.available_at, as_of=cutoff)
            ),
            key=lambda item: (item.available_at, item.event_uid),
        )
    )
    reference = adapt_reference_price(reference_price, as_of=cutoff) if reference_price is not None else None
    assessment_uid = None
    if isinstance(assessment, IntegrityAssessment) and assessment.market_uid == market_uid and _admissible(
        event_time=assessment.event_time, ingested_at=assessment.ingested_at, as_of=cutoff
    ):
        assessment_uid = assessment.assessment_uid

    availability = (
        _availability(
            modality=Modality.MARKET_STATE,
            observed=market_features is not None,
            absent_status=MissingnessStatus.MISSING,
            reason="no causally admissible price, fill, or order-book records in the completed five-minute window",
        ),
        _availability(
            modality=Modality.PUBLIC_EVIDENCE,
            observed=bool(documents),
            absent_status=MissingnessStatus.UNKNOWN,
            reason="no causally admissible public evidence was supplied for this event",
        ),
        _availability(
            modality=Modality.ONCHAIN_SETTLEMENT,
            observed=bool(settlements),
            absent_status=MissingnessStatus.NOT_AVAILABLE,
            reason="no raw verified Polygon OrderFilled settlement exactly joined to a canonical fill was supplied",
        ),
    )
    merged_watermarks = dict(features.source_high_watermarks)
    for item in (*documents, *settlements):
        existing = merged_watermarks.get(item.provenance.source_uid)
        if existing is None or item.event_time > existing:
            merged_watermarks[item.provenance.source_uid] = item.event_time
    if reference is not None:
        existing = merged_watermarks.get(reference.provenance.source_uid)
        if existing is None or reference.observed_at > existing:
            merged_watermarks[reference.provenance.source_uid] = reference.observed_at
    merged_watermarks = {key: value for key, value in merged_watermarks.items() if value <= cutoff}
    identity = {
        "market_uid": market_uid,
        "outcome_uid": outcome_uid,
        "as_of": cutoff,
        "feature_snapshot_uid": features.snapshot_uid,
        "document_uids": tuple(item.event_uid for item in documents),
        "settlement_uids": tuple(item.event_uid for item in settlements),
        "reference_uid": reference.reference_uid if reference is not None else None,
        "assessment_uid": assessment_uid,
    }
    return MultimodalEvent(
        event_uid=f"event-multimodal:{canonical_hash(identity)}",
        market_uid=market_uid,
        outcome_uid=outcome_uid,
        as_of=cutoff,
        decision_time=cutoff,
        market_features=market_features,
        reference_price=reference,
        public_evidence=documents,
        onchain_settlements=settlements,
        assessment_uid=assessment_uid,
        source_high_watermarks=merged_watermarks,
        modality_availability=availability,
    )


__all__ = [
    "AssemblyError",
    "EvidenceSourceContract",
    "MultimodalEvent",
    "ORDER_FILLED_EVENT",
    "POLYGON_CHAIN_ID",
    "RawOrderFilledFact",
    "ReferencePriceFact",
    "adapt_order_filled",
    "adapt_public_evidence",
    "adapt_reference_price",
    "assemble_multimodal_event",
]
