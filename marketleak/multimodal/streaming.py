"""Bounded, raw-lineage-only wiring from venue streams into Phase 15 event memory.

The coordinator is transport-agnostic.  It receives already collected public
venue frames, accepts only canonical records tied to a captured raw artifact,
and turns admissible market observations into immutable event-memory facts.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from marketleak.domain import (
    ObservationKind,
    OrderBookSnapshot,
    PriceObservation,
    RawArtifact,
    TradeFill,
)
from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.domain.enums import CoverageStatus
from marketleak.ingestion.connectors.kalshi_ws import KalshiWsCollection
from marketleak.ingestion.connectors.polymarket_ws import (
    BestBidAskUpdate,
    CollectionResult as PolymarketWsCollection,
    ParsedMarketMessage,
    PriceLevelChange,
)
from marketleak.multimodal.event_store import AppendStatus, EventMemoryStore
from marketleak.multimodal.schemas import (
    EventMemorySnapshot,
    MarketStateSlice,
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    Phase15Model,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
    canonical_hash,
)


STREAM_SCHEMA_VERSION = "15.0.0-streaming-v1"


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class UnavailableFrame(Phase15Model):
    """Preserved reason that a raw delivery was not admitted as a market fact."""

    frame_uid: StableUID
    platform: Literal["polymarket", "kalshi"]
    raw_artifact_uid: StableUID | None = None
    received_at: datetime
    reason_code: NonEmptyStr

    @field_validator("received_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="received_at")


class StreamCoverage(Phase15Model):
    platform: Literal["polymarket", "kalshi"]
    status: CoverageStatus
    reason_codes: tuple[NonEmptyStr, ...]
    requested_max_messages: int = Field(ge=1)
    requested_duration_seconds: float = Field(gt=0.0)


class StreamRunSnapshot(Phase15Model):
    """Frozen bounded-run view, including explicit partial live-stream coverage."""

    schema_version: NonEmptyStr = STREAM_SCHEMA_VERSION
    run_uid: StableUID
    as_of: datetime
    event_snapshot: EventMemorySnapshot
    source_high_watermarks: dict[NonEmptyStr, datetime]
    coverage: StreamCoverage
    inserted_count: int = Field(ge=0)
    idempotent_count: int = Field(ge=0)
    quarantined_count: int = Field(ge=0)
    unavailable_frames: tuple[UnavailableFrame, ...]

    @field_validator("as_of")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")

    @field_validator("source_high_watermarks")
    @classmethod
    def validate_watermarks(cls, value: dict[str, datetime]) -> dict[str, datetime]:
        return {name: _utc(timestamp, field_name=f"source_high_watermarks[{name}]") for name, timestamp in value.items()}

    @model_validator(mode="after")
    def validate_snapshot_boundary(self) -> "StreamRunSnapshot":
        if any(value > self.as_of for value in self.source_high_watermarks.values()):
            raise ValueError("high watermarks cannot be later than as_of")
        return self


@dataclass(slots=True)
class _Accumulator:
    platform: Literal["polymarket", "kalshi"]
    as_of: datetime
    facts: list[MarketStateSlice] = field(default_factory=list)
    unavailable: list[UnavailableFrame] = field(default_factory=list)
    inserted: int = 0
    idempotent: int = 0
    quarantined: int = 0


class StreamEventCoordinator:
    """Append raw-lineaged stream observations into an existing event memory store."""

    def __init__(self, store: EventMemoryStore) -> None:
        self.store = store

    @staticmethod
    def _raw_index(artifacts: Iterable[RawArtifact]) -> dict[str, RawArtifact]:
        return {artifact.raw_artifact_uid: artifact for artifact in artifacts}

    @staticmethod
    def _record_uid(record: Any) -> str:
        for name in ("fill_uid", "observation_uid", "snapshot_uid", "change_uid", "update_uid"):
            value = getattr(record, name, None)
            if value is not None:
                return str(value)
        raise ValueError("canonical record lacks a stable UID")

    @staticmethod
    def _content_digest(artifact: RawArtifact) -> str:
        prefix = "sha256:"
        if not artifact.content_hash.startswith(prefix):
            raise ValueError("raw artifact does not provide a SHA-256 content digest")
        digest = artifact.content_hash[len(prefix):]
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("raw artifact content hash is invalid")
        return digest

    def _unavailable(
        self,
        accumulator: _Accumulator,
        *,
        raw_artifact_uid: str | None,
        received_at: datetime,
        reason_code: str,
    ) -> None:
        normalized_received = _utc(received_at, field_name="received_at")
        payload = {
            "platform": accumulator.platform,
            "raw_artifact_uid": raw_artifact_uid,
            "received_at": normalized_received,
            "reason_code": reason_code,
        }
        accumulator.unavailable.append(
            UnavailableFrame(
                frame_uid=f"stream:{canonical_hash(payload)}",
                platform=accumulator.platform,
                raw_artifact_uid=raw_artifact_uid,
                received_at=normalized_received,
                reason_code=reason_code,
            )
        )

    def _provenance(
        self,
        *,
        record: Any,
        artifact: RawArtifact,
    ) -> tuple[Provenance, SourceReliability]:
        source_uid = getattr(record, "source_uid", None)
        parser_version = getattr(record, "parser_version", None)
        if source_uid is None or parser_version is None:
            raise ValueError("canonical record lacks source/parser lineage")
        if source_uid != artifact.source_uid:
            raise ValueError("record source lineage does not match captured raw artifact")
        provenance = Provenance(
            source_uid=source_uid,
            raw_artifact_uid=artifact.raw_artifact_uid,
            parser_version=parser_version,
            content_hash=self._content_digest(artifact),
            retrieved_at=artifact.retrieved_at,
            source_url=None,
        )
        reliability = SourceReliability(
            source_uid=source_uid,
            source_class=SourceClass.OFFICIAL_VENUE,
            tier=ReliabilityTier.UNASSESSED,
            score=Decimal("0"),
            assessed_at=artifact.retrieved_at,
            rationale="official raw venue capture; reliability assessment has not been adjudicated",
        )
        return provenance, reliability

    def _market_fact(
        self,
        *,
        record: Any,
        artifact: RawArtifact,
        kind: str,
        last_trade_price: Decimal | None = None,
        best_bid: Decimal | None = None,
        best_ask: Decimal | None = None,
        trade_notional: Decimal | None = None,
        fill_count: int | None = None,
        bid_depth: Decimal | None = None,
        ask_depth: Decimal | None = None,
    ) -> MarketStateSlice:
        event_time = _utc(record.event_time, field_name="record.event_time")
        ingested_at = _utc(record.ingested_at, field_name="record.ingested_at")
        if event_time > ingested_at:
            raise ValueError("event_time_after_ingestion")
        provenance, reliability = self._provenance(record=record, artifact=artifact)
        uid = self._record_uid(record)
        return MarketStateSlice(
            event_uid=f"stream:{canonical_hash((kind, uid, artifact.raw_artifact_uid))}",
            event_time=event_time,
            ingested_at=ingested_at,
            provenance=provenance,
            reliability=reliability,
            missingness=(ModalityMissingness(modality=Modality.MARKET_STATE, status=MissingnessStatus.OBSERVED),),
            market_uid=record.market_uid,
            outcome_uid=record.outcome_uid,
            window_starts_at=event_time - timedelta(microseconds=1),
            window_ends_at=event_time,
            last_trade_price=last_trade_price,
            best_bid=best_bid,
            best_ask=best_ask,
            trade_notional=trade_notional,
            fill_count=fill_count,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
        )

    def _append_fact(self, accumulator: _Accumulator, fact: MarketStateSlice) -> None:
        if fact.event_time > accumulator.as_of or fact.available_at > accumulator.as_of:
            self._unavailable(
                accumulator,
                raw_artifact_uid=fact.provenance.raw_artifact_uid,
                received_at=fact.ingested_at,
                reason_code="future_or_late_available_event",
            )
            return
        outcome = self.store.append(fact)
        if outcome.status == AppendStatus.INSERTED:
            accumulator.inserted += 1
            accumulator.facts.append(fact)
        elif outcome.status == AppendStatus.IDEMPOTENT:
            accumulator.idempotent += 1
            accumulator.facts.append(fact)
        else:
            accumulator.quarantined += 1
            self._unavailable(
                accumulator,
                raw_artifact_uid=fact.provenance.raw_artifact_uid,
                received_at=fact.ingested_at,
                reason_code="event_uid_collision_quarantined",
            )

    def _admit_record(
        self,
        accumulator: _Accumulator,
        record: Any,
        raw_index: dict[str, RawArtifact],
    ) -> None:
        raw_uid = getattr(record, "raw_artifact_uid", None)
        received_at = getattr(record, "ingested_at", accumulator.as_of)
        if raw_uid is None or raw_uid not in raw_index:
            self._unavailable(
                accumulator,
                raw_artifact_uid=raw_uid,
                received_at=received_at,
                reason_code="missing_verified_raw_lineage",
            )
            return
        artifact = raw_index[raw_uid]
        try:
            if isinstance(record, TradeFill):
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=record,
                        artifact=artifact,
                        kind="fill",
                        last_trade_price=record.price,
                        trade_notional=record.price * record.size,
                        fill_count=1,
                    ),
                )
            elif isinstance(record, OrderBookSnapshot):
                if not record.bids or not record.asks:
                    self._unavailable(
                        accumulator,
                        raw_artifact_uid=raw_uid,
                        received_at=received_at,
                        reason_code="orderbook_missing_two_sided_best_prices",
                    )
                    return
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=record,
                        artifact=artifact,
                        kind="orderbook",
                        best_bid=record.bids[0].price,
                        best_ask=record.asks[0].price,
                        bid_depth=sum((level.size for level in record.bids), Decimal("0")),
                        ask_depth=sum((level.size for level in record.asks), Decimal("0")),
                    ),
                )
            elif isinstance(record, PriceObservation):
                if record.kind != ObservationKind.LAST_TRADE:
                    self._unavailable(
                        accumulator,
                        raw_artifact_uid=raw_uid,
                        received_at=received_at,
                        reason_code="single_price_observation_not_convertible",
                    )
                    return
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=record,
                        artifact=artifact,
                        kind="last_trade_observation",
                        last_trade_price=record.price,
                    ),
                )
            elif isinstance(record, PriceLevelChange):
                if record.best_bid is None or record.best_ask is None:
                    self._unavailable(
                        accumulator,
                        raw_artifact_uid=raw_uid,
                        received_at=received_at,
                        reason_code="price_level_change_missing_two_sided_best_prices",
                    )
                    return
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=record,
                        artifact=artifact,
                        kind="price_level_change",
                        best_bid=record.best_bid,
                        best_ask=record.best_ask,
                    ),
                )
            elif isinstance(record, BestBidAskUpdate):
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=record,
                        artifact=artifact,
                        kind="best_bid_ask",
                        best_bid=record.best_bid,
                        best_ask=record.best_ask,
                    ),
                )
            else:
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=raw_uid,
                    received_at=received_at,
                    reason_code="unsupported_canonical_record_type",
                )
        except (TypeError, ValueError):
            self._unavailable(
                accumulator,
                raw_artifact_uid=raw_uid,
                received_at=received_at,
                reason_code="canonical_record_not_admissible",
            )

    def _admit_best_price_pairs(
        self,
        accumulator: _Accumulator,
        observations: Iterable[PriceObservation],
        raw_index: dict[str, RawArtifact],
    ) -> None:
        grouped: dict[tuple[str, str, str, datetime], dict[ObservationKind, PriceObservation]] = defaultdict(dict)
        for observation in observations:
            if observation.kind not in {ObservationKind.BEST_BID, ObservationKind.BEST_ASK}:
                continue
            key = (
                observation.raw_artifact_uid,
                observation.market_uid,
                observation.outcome_uid,
                observation.event_time,
            )
            grouped[key][observation.kind] = observation
        for pair in grouped.values():
            bid = pair.get(ObservationKind.BEST_BID)
            ask = pair.get(ObservationKind.BEST_ASK)
            representative = bid or ask
            assert representative is not None
            if bid is None or ask is None:
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=representative.raw_artifact_uid,
                    received_at=representative.ingested_at,
                    reason_code="best_price_pair_incomplete",
                )
                continue
            artifact = raw_index.get(bid.raw_artifact_uid)
            if artifact is None:
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=bid.raw_artifact_uid,
                    received_at=bid.ingested_at,
                    reason_code="missing_verified_raw_lineage",
                )
                continue
            try:
                self._append_fact(
                    accumulator,
                    self._market_fact(
                        record=bid,
                        artifact=artifact,
                        kind="paired_best_prices",
                        best_bid=bid.price,
                        best_ask=ask.price,
                    ),
                )
            except (TypeError, ValueError):
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=bid.raw_artifact_uid,
                    received_at=bid.ingested_at,
                    reason_code="best_price_pair_not_admissible",
                )

    def _finalize(
        self,
        accumulator: _Accumulator,
        *,
        requested_max_messages: int,
        requested_duration_seconds: float,
        additional_coverage_reason: str | None = None,
    ) -> StreamRunSnapshot:
        high_watermarks: dict[str, datetime] = {}
        for fact in accumulator.facts:
            current = high_watermarks.get(fact.provenance.source_uid)
            if current is None or fact.event_time > current:
                high_watermarks[fact.provenance.source_uid] = fact.event_time
        coverage_reasons = ["bounded_live_stream_not_historical_coverage"]
        if additional_coverage_reason is not None:
            coverage_reasons.append(additional_coverage_reason)
        if accumulator.unavailable:
            coverage_reasons.append("unavailable_or_unsupported_frames_present")
        snapshot = self.store.snapshot_as_of(accumulator.as_of)
        coverage = StreamCoverage(
            platform=accumulator.platform,
            status=CoverageStatus.PARTIAL,
            reason_codes=tuple(coverage_reasons),
            requested_max_messages=requested_max_messages,
            requested_duration_seconds=requested_duration_seconds,
        )
        payload = {
            "platform": accumulator.platform,
            "as_of": accumulator.as_of,
            "snapshot_uid": snapshot.manifest.snapshot_uid,
            "high_watermarks": high_watermarks,
            "coverage": coverage.model_dump(mode="json"),
            "inserted": accumulator.inserted,
            "idempotent": accumulator.idempotent,
            "quarantined": accumulator.quarantined,
            "unavailable": [item.model_dump(mode="json") for item in accumulator.unavailable],
        }
        return StreamRunSnapshot(
            run_uid=f"stream:{canonical_hash(payload)}",
            as_of=accumulator.as_of,
            event_snapshot=snapshot,
            source_high_watermarks=high_watermarks,
            coverage=coverage,
            inserted_count=accumulator.inserted,
            idempotent_count=accumulator.idempotent,
            quarantined_count=accumulator.quarantined,
            unavailable_frames=tuple(accumulator.unavailable),
        )

    @staticmethod
    def _bounded_messages(
        messages: tuple[ParsedMarketMessage, ...],
        *,
        requested_max_messages: int,
    ) -> tuple[tuple[ParsedMarketMessage, ...], tuple[ParsedMarketMessage, ...]]:
        """Return a deterministic prefix and retained over-limit receipts.

        A correctly implemented collector already respects ``max_messages``.
        This second boundary keeps an injected or future collector from making
        the coordinator unbounded if that contract is violated.
        """

        if requested_max_messages < 1:
            raise ValueError("requested_max_messages must be positive")
        return messages[:requested_max_messages], messages[requested_max_messages:]

    def admit_polymarket(
        self,
        collection: PolymarketWsCollection | Iterable[ParsedMarketMessage],
        *,
        as_of: datetime,
        requested_max_messages: int,
        requested_duration_seconds: float,
    ) -> StreamRunSnapshot:
        """Admit only raw-artifact-indexed canonical records from Polymarket frames."""

        cutoff = _utc(as_of, field_name="as_of")
        if requested_duration_seconds <= 0:
            raise ValueError("requested_duration_seconds must be positive")
        incoming = collection.messages if isinstance(collection, PolymarketWsCollection) else tuple(collection)
        messages, over_limit = self._bounded_messages(
            incoming,
            requested_max_messages=requested_max_messages,
        )
        accumulator = _Accumulator(platform="polymarket", as_of=cutoff)
        for message in over_limit:
            self._unavailable(
                accumulator,
                raw_artifact_uid=message.raw_artifact.raw_artifact_uid,
                received_at=message.raw_artifact.retrieved_at,
                reason_code="collector_message_limit_exceeded",
            )
        for message in messages:
            raw_index = self._raw_index((message.raw_artifact,))
            for unknown in message.unknown_events:
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=unknown.raw_artifact_uid,
                    received_at=unknown.ingested_at,
                    reason_code="unknown_or_malformed_market_frame",
                )
            for lifecycle in message.lifecycle_events:
                self._unavailable(
                    accumulator,
                    raw_artifact_uid=lifecycle.raw_artifact_uid,
                    received_at=lifecycle.ingested_at,
                    reason_code="lifecycle_frame_not_market_state",
                )
            direct_observations = tuple(
                observation
                for observation in message.observations
                if observation.kind not in {ObservationKind.BEST_BID, ObservationKind.BEST_ASK}
            )
            for record in (*message.fills, *message.snapshots, *direct_observations, *message.price_changes, *message.best_bid_ask):
                self._admit_record(accumulator, record, raw_index)
            self._admit_best_price_pairs(accumulator, message.observations, raw_index)
        return self._finalize(
            accumulator,
            requested_max_messages=requested_max_messages,
            requested_duration_seconds=requested_duration_seconds,
        )

    def admit_kalshi(
        self,
        collection: KalshiWsCollection,
        *,
        as_of: datetime,
        requested_max_messages: int,
        requested_duration_seconds: float,
    ) -> StreamRunSnapshot:
        """Admit Kalshi's raw-lineaged canonical batch and retain all gaps explicitly."""

        cutoff = _utc(as_of, field_name="as_of")
        if requested_max_messages < 1 or requested_duration_seconds <= 0:
            raise ValueError("requested stream bounds must be positive")
        accumulator = _Accumulator(platform="kalshi", as_of=cutoff)
        raw_index = self._raw_index(collection.batch.raw_artifacts)
        for item in collection.unsupported_messages:
            self._unavailable(
                accumulator,
                raw_artifact_uid=item.raw_artifact_uid,
                received_at=item.received_at,
                reason_code="unsupported_or_malformed_market_frame",
            )
        for _event in collection.events:
            # Lossless envelopes remain available in the raw store; no market
            # state is fabricated for channel types without a canonical record.
            pass
        direct_observations = tuple(
            observation
            for observation in collection.batch.observations
            if observation.kind not in {ObservationKind.BEST_BID, ObservationKind.BEST_ASK}
        )
        for record in (*collection.batch.fills, *collection.batch.snapshots, *direct_observations):
            self._admit_record(accumulator, record, raw_index)
        self._admit_best_price_pairs(accumulator, collection.batch.observations, raw_index)
        return self._finalize(
            accumulator,
            requested_max_messages=requested_max_messages,
            requested_duration_seconds=requested_duration_seconds,
            additional_coverage_reason=(
                "collector_reported_incomplete" if not collection.complete else None
            ),
        )

    async def collect_polymarket_bounded(
        self,
        collector: Any,
        *,
        as_of: datetime,
        max_messages: int,
        duration_seconds: float,
    ) -> StreamRunSnapshot:
        """Bound a transport-injected collector by both count and wall-clock duration."""

        if max_messages < 1 or duration_seconds <= 0:
            raise ValueError("max_messages and duration_seconds must be positive")
        timed_out = False
        try:
            collection = await asyncio.wait_for(collector.collect(max_messages=max_messages), timeout=duration_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            collection = PolymarketWsCollection(messages=(), connection_attempts=0, reconnect_delays=())
        snapshot = self.admit_polymarket(
            collection,
            as_of=as_of,
            requested_max_messages=max_messages,
            requested_duration_seconds=duration_seconds,
        )
        if not timed_out:
            return snapshot
        # Reconstruct (rather than model_copy) so the immutable run manifest
        # remains internally consistent with the coverage record.
        accumulator = _Accumulator(platform="polymarket", as_of=_utc(as_of, field_name="as_of"))
        return self._finalize(
            accumulator,
            requested_max_messages=max_messages,
            requested_duration_seconds=duration_seconds,
            additional_coverage_reason="duration_bound_elapsed",
        )


def redacted_stream_request(
    *,
    platform: Literal["polymarket", "kalshi"],
    token_count: int,
    ticker_count: int,
    max_messages: int,
    duration_seconds: float,
    status: str,
) -> dict[str, Any]:
    """Return only public configuration counts; server-side configuration is never echoed."""

    return {
        "command": "stream-market",
        "status": status,
        "platform": platform,
        "token_count": token_count,
        "ticker_count": ticker_count,
        "max_messages": max_messages,
        "duration_seconds": duration_seconds,
        "transport": "not_started_without_injected_server_transport",
    }


__all__ = [
    "STREAM_SCHEMA_VERSION",
    "StreamCoverage",
    "StreamEventCoordinator",
    "StreamRunSnapshot",
    "UnavailableFrame",
    "redacted_stream_request",
]
