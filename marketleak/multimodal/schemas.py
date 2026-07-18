"""Immutable contracts for Phase 15's point-in-time multimodal event memory.

This module deliberately represents *facts observed by the system*, rather
than conclusions about people.  It is safe for a fact store to say that a
trade, a document, or a settlement event was observed.  It is not safe for a
fact store to manufacture a fraud probability or a legal conclusion.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Annotated, Any, Literal, Union

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import (
    NonEmptyStr,
    NonNegativeDecimal,
    Probability,
    StableUID,
    StrictDomainModel,
)


PHASE15_SCHEMA_VERSION = "15.0.0"
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


def _utc(value: datetime, *, field_name: str = "timestamp") -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "model_dump"):
        return _json_value(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _canonical_json(value: Any) -> bytes:
    """Produce a stable representation for IDs, idempotency, and manifests."""

    return json.dumps(
        _json_value(value),
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


class Modality(str, Enum):
    MARKET_STATE = "market_state"
    PUBLIC_EVIDENCE = "public_evidence"
    ONCHAIN_SETTLEMENT = "onchain_settlement"


class SourceClass(str, Enum):
    OFFICIAL_VENUE = "official_venue"
    PRIMARY_SOURCE = "primary_source"
    BLOCKCHAIN_RPC = "blockchain_rpc"
    BLOCKCHAIN_EXPLORER = "blockchain_explorer"
    SECONDARY_SOURCE = "secondary_source"
    INTERNAL_COLLECTION = "internal_collection"


class ReliabilityTier(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNASSESSED = "unassessed"


class MissingnessStatus(str, Enum):
    OBSERVED = "observed"
    MISSING = "missing"
    NOT_AVAILABLE = "not_available"
    UNKNOWN = "unknown"


class Phase15Model(StrictDomainModel):
    """Strict, frozen base shared by every Phase 15 object."""


class Provenance(Phase15Model):
    """Immutable raw/source lineage required for every observed fact."""

    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    content_hash: Sha256
    retrieved_at: datetime
    source_url: NonEmptyStr | None = None

    @field_validator("retrieved_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="retrieved_at")

    @field_validator("source_url")
    @classmethod
    def require_http_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.startswith(("https://", "http://")):
            raise ValueError("source_url must be an http(s) URL when supplied")
        return value


class SourceReliability(Phase15Model):
    """A documented source assessment, not a model-derived truth claim."""

    source_uid: StableUID
    source_class: SourceClass
    tier: ReliabilityTier
    score: Annotated[Decimal, Field(strict=True, ge=Decimal("0"), le=Decimal("1"))]
    assessed_at: datetime
    rationale: NonEmptyStr

    @field_validator("assessed_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="assessed_at")


class ModalityMissingness(Phase15Model):
    """Explicit availability state for one modality at a fact's time boundary."""

    modality: Modality
    status: MissingnessStatus
    reason: NonEmptyStr | None = None

    @model_validator(mode="after")
    def require_reason_for_absence(self) -> "ModalityMissingness":
        if self.status != MissingnessStatus.OBSERVED and self.reason is None:
            raise ValueError("missing, unavailable, and unknown modality states require a reason")
        return self


class ObservedFact(Phase15Model):
    """Base class for facts admitted to the event memory.

    ``event_time`` answers when the thing occurred; ``ingested_at`` answers
    when our system could use the captured artifact.  A retrospective query
    must honor both rather than leaking later-collected evidence backwards.
    """

    schema_version: NonEmptyStr = PHASE15_SCHEMA_VERSION
    event_uid: StableUID
    modality: Modality
    event_time: datetime
    ingested_at: datetime
    provenance: Provenance
    reliability: SourceReliability
    missingness: tuple[ModalityMissingness, ...]

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_lineage_and_missingness(self) -> "ObservedFact":
        if self.provenance.retrieved_at > self.ingested_at:
            raise ValueError("raw artifact cannot be retrieved after the fact is ingested")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("reliability assessment must refer to the fact provenance source")
        matching = [item for item in self.missingness if item.modality == self.modality]
        if len(matching) != 1 or matching[0].status != MissingnessStatus.OBSERVED:
            raise ValueError("an observed fact requires exactly one observed state for its modality")
        if len({item.modality for item in self.missingness}) != len(self.missingness):
            raise ValueError("missingness may name each modality at most once")
        return self

    @property
    def available_at(self) -> datetime:
        """The earliest time this fact may enter an as-of decision."""

        return self.ingested_at

    def semantic_payload(self) -> dict[str, Any]:
        """Payload used for retry-safe idempotency (ingest clock excluded)."""

        payload = self.model_dump(mode="json")
        payload.pop("ingested_at", None)
        return payload

    @property
    def semantic_hash(self) -> str:
        return canonical_hash(self.semantic_payload())


class MarketStateSlice(ObservedFact):
    """A causally bounded snapshot of market microstructure."""

    modality: Literal[Modality.MARKET_STATE] = Modality.MARKET_STATE
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    window_starts_at: datetime
    window_ends_at: datetime
    last_trade_price: Probability | None = None
    mid_price: Probability | None = None
    best_bid: Probability | None = None
    best_ask: Probability | None = None
    trade_notional: NonNegativeDecimal | None = None
    fill_count: int | None = Field(default=None, strict=True, ge=0)
    bid_depth: NonNegativeDecimal | None = None
    ask_depth: NonNegativeDecimal | None = None

    @field_validator("window_starts_at", "window_ends_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_market_slice(self) -> "MarketStateSlice":
        if self.window_ends_at <= self.window_starts_at:
            raise ValueError("market state window must have positive duration")
        if self.window_ends_at > self.event_time:
            raise ValueError("market state window cannot end after event_time")
        if self.event_time > self.ingested_at:
            raise ValueError("market state cannot occur after it is ingested")
        if (self.best_bid is None) != (self.best_ask is None):
            raise ValueError("best_bid and best_ask must be supplied together")
        if self.best_bid is not None and self.best_bid >= self.best_ask:  # type: ignore[operator]
            raise ValueError("best_bid must be lower than best_ask")
        if not any(
            value is not None
            for value in (
                self.last_trade_price,
                self.mid_price,
                self.trade_notional,
                self.fill_count,
                self.bid_depth,
                self.ask_depth,
            )
        ):
            raise ValueError("market state slice must contain at least one observed measurement")
        return self


class PublicDocumentClaim(ObservedFact):
    """A public document claim with first-seen, rather than retroactive, timing."""

    modality: Literal[Modality.PUBLIC_EVIDENCE] = Modality.PUBLIC_EVIDENCE
    document_uid: StableUID
    claim_uid: StableUID
    canonical_url: NonEmptyStr
    document_hash: Sha256
    text: NonEmptyStr | None = None
    entity_uids: tuple[StableUID, ...] = ()
    claimed_published_at: datetime | None = None
    first_seen_at: datetime
    retrieved_at: datetime
    modified_at: datetime | None = None

    @field_validator("claimed_published_at", "first_seen_at", "retrieved_at", "modified_at")
    @classmethod
    def require_utc_optional(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @field_validator("canonical_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        if not value.startswith(("https://", "http://")):
            raise ValueError("canonical_url must be an http(s) URL")
        return value

    @model_validator(mode="after")
    def validate_public_timeline(self) -> "PublicDocumentClaim":
        if self.event_time != self.first_seen_at:
            raise ValueError("public document event_time must equal first_seen_at")
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("first_seen_at cannot be after retrieval")
        if self.retrieved_at > self.ingested_at:
            raise ValueError("document cannot be retrieved after it is ingested")
        if self.claimed_published_at is not None and self.claimed_published_at > self.first_seen_at:
            raise ValueError("claimed publication cannot be after first system observation")
        if self.modified_at is not None and self.modified_at > self.first_seen_at:
            raise ValueError("store a later document revision instead of using a later modification time")
        return self

    @property
    def available_at(self) -> datetime:
        return self.first_seen_at


class OnChainSettlementFact(ObservedFact):
    """Raw-chain settlement fact.  Wallets are identifiers, never identities."""

    modality: Literal[Modality.ONCHAIN_SETTLEMENT] = Modality.ONCHAIN_SETTLEMENT
    chain: NonEmptyStr
    chain_id: int = Field(strict=True, ge=0)
    transaction_hash: NonEmptyStr
    log_index: int = Field(strict=True, ge=0)
    block_number: int = Field(strict=True, ge=0)
    block_timestamp: datetime
    contract_address: NonEmptyStr
    token_uid: StableUID | None = None
    market_uid: StableUID | None = None
    outcome_uid: StableUID | None = None
    observed_wallet_uid: StableUID | None = None
    quantity: NonNegativeDecimal | None = None

    @field_validator("block_timestamp")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="block_timestamp")

    @model_validator(mode="after")
    def validate_settlement_fact(self) -> "OnChainSettlementFact":
        if self.block_timestamp != self.event_time:
            raise ValueError("on-chain event_time must equal immutable block_timestamp")
        if self.event_time > self.ingested_at:
            raise ValueError("chain settlement cannot occur after it is ingested")
        if self.market_uid is not None and self.outcome_uid is None:
            raise ValueError("market-linked settlement facts require an outcome_uid")
        return self


EventFact = Annotated[
    Union[MarketStateSlice, PublicDocumentClaim, OnChainSettlementFact],
    Field(discriminator="modality"),
]


class SnapshotManifest(Phase15Model):
    """Frozen selection fingerprint for deterministic point-in-time retrieval."""

    schema_version: NonEmptyStr = PHASE15_SCHEMA_VERSION
    snapshot_uid: StableUID
    as_of: datetime
    event_uids: tuple[StableUID, ...]
    record_set_hash: Sha256

    @field_validator("as_of")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")

    @model_validator(mode="after")
    def validate_unique_ids(self) -> "SnapshotManifest":
        if len(set(self.event_uids)) != len(self.event_uids):
            raise ValueError("snapshot event_uids must be unique")
        return self


class EventMemorySnapshot(Phase15Model):
    """An immutable as-of snapshot. Future facts are invalid, never hidden."""

    manifest: SnapshotManifest
    records: tuple[EventFact, ...]

    @model_validator(mode="after")
    def validate_point_in_time_selection(self) -> "EventMemorySnapshot":
        record_uids = tuple(record.event_uid for record in self.records)
        if record_uids != self.manifest.event_uids:
            raise ValueError("snapshot records must exactly match manifest event_uids in deterministic order")
        if any(record.available_at > self.manifest.as_of for record in self.records):
            raise ValueError("evidence cannot become available after snapshot as_of")
        if any(record.event_time > self.manifest.as_of for record in self.records):
            raise ValueError("future event_time cannot enter an as-of snapshot")
        if canonical_hash([record.model_dump(mode="json") for record in self.records]) != self.manifest.record_set_hash:
            raise ValueError("record_set_hash does not match snapshot records")
        return self


def snapshot_manifest_for(*, as_of: datetime, records: tuple[EventFact, ...]) -> SnapshotManifest:
    """Build a deterministic manifest for already sorted, causally admissible records."""

    normalized_as_of = _utc(as_of, field_name="as_of")
    payload = [record.model_dump(mode="json") for record in records]
    record_set_hash = canonical_hash(payload)
    event_uids = tuple(record.event_uid for record in records)
    fingerprint = {
        "schema_version": PHASE15_SCHEMA_VERSION,
        "as_of": normalized_as_of,
        "event_uids": event_uids,
        "record_set_hash": record_set_hash,
    }
    return SnapshotManifest(
        snapshot_uid=f"snapshot:{canonical_hash(fingerprint)}",
        as_of=normalized_as_of,
        event_uids=event_uids,
        record_set_hash=record_set_hash,
    )


__all__ = [
    "EventFact",
    "EventMemorySnapshot",
    "MarketStateSlice",
    "MissingnessStatus",
    "Modality",
    "ModalityMissingness",
    "OnChainSettlementFact",
    "PHASE15_SCHEMA_VERSION",
    "Phase15Model",
    "Provenance",
    "PublicDocumentClaim",
    "ReliabilityTier",
    "SnapshotManifest",
    "SourceClass",
    "SourceReliability",
    "canonical_hash",
    "snapshot_manifest_for",
]
