"""Raw-first public Polymarket market-channel collector.

Contract source:
https://docs.polymarket.com/market-data/websocket/market-channel

Only the unauthenticated market channel is represented here.  The collector
subscribes by public token IDs, writes every frame before parsing it, and keeps
unrecognized or malformed messages as preserved raw evidence rather than
inventing fields that the channel does not provide.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
import json
from typing import Any, Literal, Protocol

from pydantic import Field, field_validator, model_validator

from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
    RawArtifact,
    StrictDomainModel,
    TradeFill,
    TradeSide,
)
from marketleak.domain.common import NonEmptyStr, NonNegativeDecimal, Probability, StableUID
from marketleak.ingestion.normalize import (
    decimal_from,
    parse_json_decimal,
    qualified_id,
    require_text,
    stable_uid,
    utc_datetime,
)
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture


MARKET_CHANNEL_ENDPOINT = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PARSER_VERSION = "polymarket-market-ws-v1.0.0"
SOURCE_UID = "polymarket:source/ws-market"
RAW_SOURCE = "ws/market"
HEARTBEAT_PAYLOAD = "PING"
HEARTBEAT_INTERVAL_SECONDS = 10.0


class MarketSocket(Protocol):
    async def send(self, payload: str) -> Any: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> Any: ...


SocketFactory = Callable[[], Awaitable[MarketSocket]]
SleepFn = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]


class FieldAvailability(StrictDomainModel):
    """Explicitly describes data the public market channel does not expose."""

    field_name: NonEmptyStr
    status: Literal["not_available", "missing", "unknown"]
    reason: NonEmptyStr


PUBLIC_MARKET_LIMITATIONS = (
    FieldAvailability(
        field_name="actor_identity",
        status="not_available",
        reason="public market-channel messages do not identify people or accounts",
    ),
    FieldAvailability(
        field_name="order_owner",
        status="not_available",
        reason="public market-channel messages do not expose resting-order ownership",
    ),
)


class PriceLevelChange(StrictDomainModel):
    """Canonical level update, retaining removal semantics and public-only lineage."""

    schema_version: NonEmptyStr = "2.0.0"
    event_time: datetime
    ingested_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    change_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    platform: Literal["polymarket"] = "polymarket"
    price: Probability
    size: NonNegativeDecimal
    side: TradeSide
    best_bid: Probability | None = None
    best_ask: Probability | None = None
    level_removed: bool
    field_availability: tuple[FieldAvailability, ...] = PUBLIC_MARKET_LIMITATIONS

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_book_bounds(self) -> "PriceLevelChange":
        if self.best_bid is not None and self.best_ask is not None and self.best_bid > self.best_ask:
            raise ValueError("best_bid cannot exceed best_ask")
        return self


class BestBidAskUpdate(StrictDomainModel):
    """Canonical best-price event; no owner or account fields are inferred."""

    schema_version: NonEmptyStr = "2.0.0"
    event_time: datetime
    ingested_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    update_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    platform: Literal["polymarket"] = "polymarket"
    best_bid: Probability
    best_ask: Probability
    spread: NonNegativeDecimal | None = None
    field_availability: tuple[FieldAvailability, ...] = PUBLIC_MARKET_LIMITATIONS

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_prices(self) -> "BestBidAskUpdate":
        if self.best_bid > self.best_ask:
            raise ValueError("best_bid cannot exceed best_ask")
        return self


class MarketLifecycleEvent(StrictDomainModel):
    """Public creation/resolution metadata, including explicit missingness fields."""

    schema_version: NonEmptyStr = "2.0.0"
    event_time: datetime
    ingested_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    lifecycle_uid: StableUID
    lifecycle_type: Literal["new_market", "market_resolved"]
    market_uid: StableUID
    question: NonEmptyStr | None = None
    slug: NonEmptyStr | None = None
    outcome_uids: tuple[StableUID, ...] = ()
    outcome_labels: tuple[NonEmptyStr, ...] = ()
    winning_outcome_uid: StableUID | None = None
    winning_outcome_label: NonEmptyStr | None = None
    field_availability: tuple[FieldAvailability, ...] = PUBLIC_MARKET_LIMITATIONS

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_resolution(self) -> "MarketLifecycleEvent":
        if self.lifecycle_type == "market_resolved" and self.winning_outcome_uid is None:
            raise ValueError("market_resolved requires winning_outcome_uid when parsed as canonical")
        if self.winning_outcome_uid is not None and self.winning_outcome_uid not in self.outcome_uids:
            raise ValueError("winning_outcome_uid must be one of outcome_uids")
        return self


class UnknownMarketChannelEvent(StrictDomainModel):
    """Preserved event envelope for unsupported, unknown, or malformed messages."""

    schema_version: NonEmptyStr = "2.0.0"
    event_time: datetime
    ingested_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    unknown_uid: StableUID
    event_type: NonEmptyStr
    payload_hash: NonEmptyStr
    parse_error: NonEmptyStr | None = None
    field_availability: tuple[FieldAvailability, ...] = PUBLIC_MARKET_LIMITATIONS

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ParsedMarketMessage:
    """One raw receipt and all normalized records derivable without private data."""

    raw_artifact: RawArtifact
    event_type: str
    snapshots: tuple[OrderBookSnapshot, ...] = ()
    fills: tuple[TradeFill, ...] = ()
    observations: tuple[PriceObservation, ...] = ()
    price_changes: tuple[PriceLevelChange, ...] = ()
    best_bid_ask: tuple[BestBidAskUpdate, ...] = ()
    lifecycle_events: tuple[MarketLifecycleEvent, ...] = ()
    unknown_events: tuple[UnknownMarketChannelEvent, ...] = ()
    field_availability: tuple[FieldAvailability, ...] = PUBLIC_MARKET_LIMITATIONS

    @property
    def records(self) -> tuple[Any, ...]:
        return (
            *self.snapshots,
            *self.fills,
            *self.observations,
            *self.price_changes,
            *self.best_bid_ask,
            *self.lifecycle_events,
            *self.unknown_events,
        )


@dataclass(frozen=True, slots=True)
class MarketChannelSubscription:
    """Public market-channel subscription; no user-channel fields exist here."""

    asset_ids: tuple[str, ...]
    custom_feature_enabled: bool = True

    def __post_init__(self) -> None:
        normalized = tuple(require_text(asset, "asset_id") for asset in self.asset_ids)
        if not normalized:
            raise ValueError("at least one public asset_id is required")
        if len(set(normalized)) != len(normalized):
            raise ValueError("asset_ids must be unique")
        object.__setattr__(self, "asset_ids", normalized)

    def wire_payload(self) -> dict[str, Any]:
        return {
            "assets_ids": list(self.asset_ids),
            "type": "market",
            "custom_feature_enabled": self.custom_feature_enabled,
        }


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    multiplier: float = 2.0
    max_delay_seconds: float = 20.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if self.initial_delay_seconds < 0 or self.max_delay_seconds < 0 or self.multiplier < 1:
            raise ValueError("reconnect delays must be non-negative and multiplier must be at least one")

    def delay_for_retry(self, retry_number: int) -> float:
        if retry_number < 1 or retry_number >= self.max_attempts:
            raise ValueError("retry_number must be in [1, max_attempts)")
        return min(self.initial_delay_seconds * (self.multiplier ** (retry_number - 1)), self.max_delay_seconds)


@dataclass(frozen=True, slots=True)
class CollectionResult:
    messages: tuple[ParsedMarketMessage, ...]
    connection_attempts: int
    reconnect_delays: tuple[float, ...]


class MarketChannelParser:
    """Normalize documented market-channel events without guessing unsupported fields."""

    def __init__(self, *, parser_version: str = PARSER_VERSION) -> None:
        self.parser_version = require_text(parser_version, "parser_version")

    @staticmethod
    def _raw_uid(capture: RawCapture) -> str:
        return f"polymarket:raw/{capture.sha256}"

    def _lineage(self, capture: RawCapture, event_time: datetime) -> dict[str, Any]:
        return {
            "event_time": event_time,
            "ingested_at": capture.received_at,
            "source_uid": SOURCE_UID,
            "raw_artifact_uid": self._raw_uid(capture),
            "parser_version": self.parser_version,
        }

    def raw_artifact(self, capture: RawCapture) -> RawArtifact:
        return RawArtifactStore.to_domain(capture, source_uid=SOURCE_UID, parser_version=self.parser_version)

    @staticmethod
    def _event_time(payload: Mapping[str, Any], capture: RawCapture) -> datetime:
        value = payload.get("timestamp")
        return capture.received_at if value is None else utc_datetime(value, "timestamp")

    @staticmethod
    def _market_uid(payload: Mapping[str, Any]) -> str:
        return qualified_id("polymarket", f"market/{require_text(payload.get('market'), 'market')}")

    @staticmethod
    def _outcome_uid(asset_id: Any) -> str:
        return qualified_id("polymarket", f"outcome/{require_text(asset_id, 'asset_id')}")

    @staticmethod
    def _decimal(value: Any, field_name: str, *, upper: Decimal | None = None) -> Decimal:
        return decimal_from(value, field_name, minimum=Decimal("0"), maximum=upper)

    def _unknown(
        self,
        *,
        event_type: str,
        payload_hash: str,
        capture: RawCapture,
        parse_error: str | None = None,
    ) -> ParsedMarketMessage:
        lineage = self._lineage(capture, capture.received_at)
        unknown = UnknownMarketChannelEvent(
            **lineage,
            unknown_uid=stable_uid("polymarket", "ws-unknown", (capture.sha256, event_type, parse_error)),
            event_type=event_type or "malformed",
            payload_hash=payload_hash,
            parse_error=parse_error,
        )
        return ParsedMarketMessage(
            raw_artifact=self.raw_artifact(capture),
            event_type=event_type or "malformed",
            unknown_events=(unknown,),
        )

    def parse_payload(self, payload: Mapping[str, Any], capture: RawCapture) -> ParsedMarketMessage:
        """Parse only documented event types after the immutable raw capture exists."""

        event_type = require_text(payload.get("event_type"), "event_type")
        payload_hash = capture.sha256
        try:
            if event_type == "book":
                return self._book(payload, capture)
            if event_type == "price_change":
                return self._price_change(payload, capture)
            if event_type == "last_trade_price":
                return self._last_trade_price(payload, capture)
            if event_type == "best_bid_ask":
                return self._best_bid_ask(payload, capture)
            if event_type in {"new_market", "market_resolved"}:
                return self._lifecycle(payload, capture, event_type)
            return self._unknown(event_type=event_type, payload_hash=payload_hash, capture=capture)
        except (TypeError, ValueError) as exc:
            return self._unknown(
                event_type=event_type,
                payload_hash=payload_hash,
                capture=capture,
                parse_error=str(exc),
            )

    def parse_raw(self, raw: bytes | str, capture: RawCapture) -> ParsedMarketMessage:
        """Parse after capture; invalid JSON becomes an immutable unknown-event record."""

        try:
            payload = parse_json_decimal(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return self._unknown(
                event_type="malformed",
                payload_hash=capture.sha256,
                capture=capture,
                parse_error=f"invalid_json: {exc}",
            )
        if not isinstance(payload, Mapping):
            return self._unknown(
                event_type="malformed",
                payload_hash=capture.sha256,
                capture=capture,
                parse_error="market-channel frame must decode to an object",
            )
        try:
            return self.parse_payload(payload, capture)
        except ValueError as exc:
            return self._unknown(
                event_type=str(payload.get("event_type") or "malformed"),
                payload_hash=capture.sha256,
                capture=capture,
                parse_error=str(exc),
            )

    def _book(self, payload: Mapping[str, Any], capture: RawCapture) -> ParsedMarketMessage:
        event_time = self._event_time(payload, capture)
        market_uid = self._market_uid(payload)
        asset_id = require_text(payload.get("asset_id"), "asset_id")

        def levels(name: str, *, descending: bool) -> tuple[OrderBookLevel, ...]:
            raw_levels = payload.get(name)
            if not isinstance(raw_levels, list):
                raise ValueError(f"{name} must be a list")
            values = []
            for index, level in enumerate(raw_levels):
                if not isinstance(level, Mapping):
                    raise ValueError(f"{name}[{index}] must be an object")
                values.append(
                    OrderBookLevel(
                        price=self._decimal(level.get("price"), f"{name}[{index}].price", upper=Decimal("1")),
                        size=self._decimal(level.get("size"), f"{name}[{index}].size"),
                    )
                )
            return tuple(sorted(values, key=lambda item: item.price, reverse=descending))

        source_hash = require_text(payload.get("hash"), "hash")
        snapshot = OrderBookSnapshot(
            **self._lineage(capture, event_time),
            snapshot_uid=stable_uid("polymarket", "ws-book", (asset_id, source_hash, event_time, capture.sha256)),
            market_uid=market_uid,
            outcome_uid=self._outcome_uid(asset_id),
            platform="polymarket",
            bids=levels("bids", descending=True),
            asks=levels("asks", descending=False),
        )
        return ParsedMarketMessage(raw_artifact=self.raw_artifact(capture), event_type="book", snapshots=(snapshot,))

    def _price_change(self, payload: Mapping[str, Any], capture: RawCapture) -> ParsedMarketMessage:
        event_time = self._event_time(payload, capture)
        market_uid = self._market_uid(payload)
        raw_changes = payload.get("price_changes")
        if not isinstance(raw_changes, list):
            raise ValueError("price_changes must be a list")
        changes: list[PriceLevelChange] = []
        observations: list[PriceObservation] = []
        for index, raw_change in enumerate(raw_changes):
            if not isinstance(raw_change, Mapping):
                raise ValueError(f"price_changes[{index}] must be an object")
            asset_id = require_text(raw_change.get("asset_id"), f"price_changes[{index}].asset_id")
            side_name = require_text(raw_change.get("side"), f"price_changes[{index}].side").upper()
            if side_name not in {"BUY", "SELL"}:
                raise ValueError(f"price_changes[{index}].side must be BUY or SELL")
            price = self._decimal(raw_change.get("price"), f"price_changes[{index}].price", upper=Decimal("1"))
            size = self._decimal(raw_change.get("size"), f"price_changes[{index}].size")
            best_bid = raw_change.get("best_bid")
            best_ask = raw_change.get("best_ask")
            normalized_bid = None if best_bid is None else self._decimal(best_bid, f"price_changes[{index}].best_bid", upper=Decimal("1"))
            normalized_ask = None if best_ask is None else self._decimal(best_ask, f"price_changes[{index}].best_ask", upper=Decimal("1"))
            change = PriceLevelChange(
                **self._lineage(capture, event_time),
                change_uid=stable_uid(
                    "polymarket", "ws-price-change", (capture.sha256, index, asset_id, price, size, side_name)
                ),
                market_uid=market_uid,
                outcome_uid=self._outcome_uid(asset_id),
                price=price,
                size=size,
                side=TradeSide(side_name.lower()),
                best_bid=normalized_bid,
                best_ask=normalized_ask,
                level_removed=size == Decimal("0"),
            )
            changes.append(change)
            observations.append(
                PriceObservation(
                    **self._lineage(capture, event_time),
                    observation_uid=stable_uid("polymarket", "ws-level", (capture.sha256, index, asset_id)),
                    market_uid=market_uid,
                    outcome_uid=self._outcome_uid(asset_id),
                    platform="polymarket",
                    price=price,
                    kind=ObservationKind.PLATFORM_SNAPSHOT,
                    actor_visibility=ActorVisibility.NOT_AVAILABLE,
                )
            )
        return ParsedMarketMessage(
            raw_artifact=self.raw_artifact(capture),
            event_type="price_change",
            observations=tuple(observations),
            price_changes=tuple(changes),
        )

    def _last_trade_price(self, payload: Mapping[str, Any], capture: RawCapture) -> ParsedMarketMessage:
        event_time = self._event_time(payload, capture)
        market = require_text(payload.get("market"), "market")
        asset_id = require_text(payload.get("asset_id"), "asset_id")
        side_name = require_text(payload.get("side"), "side").upper()
        if side_name not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        price = self._decimal(payload.get("price"), "price", upper=Decimal("1"))
        size = self._decimal(payload.get("size"), "size")
        fill = TradeFill(
            **self._lineage(capture, event_time),
            fill_uid=stable_uid("polymarket", "ws-trade", (capture.sha256, market, asset_id, price, size, side_name)),
            market_uid=qualified_id("polymarket", f"market/{market}"),
            outcome_uid=self._outcome_uid(asset_id),
            platform="polymarket",
            price=price,
            size=size,
            side=TradeSide(side_name.lower()),
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
            actor_uid=None,
            maker=None,
            taker=None,
            transaction_uid=None,
        )
        observation = PriceObservation(
            **self._lineage(capture, event_time),
            observation_uid=stable_uid("polymarket", "ws-last-trade", (capture.sha256, asset_id, price, size)),
            market_uid=qualified_id("polymarket", f"market/{market}"),
            outcome_uid=self._outcome_uid(asset_id),
            platform="polymarket",
            price=price,
            kind=ObservationKind.LAST_TRADE,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
        )
        return ParsedMarketMessage(
            raw_artifact=self.raw_artifact(capture),
            event_type="last_trade_price",
            fills=(fill,),
            observations=(observation,),
        )

    def _best_bid_ask(self, payload: Mapping[str, Any], capture: RawCapture) -> ParsedMarketMessage:
        event_time = self._event_time(payload, capture)
        market_uid = self._market_uid(payload)
        asset_id = require_text(payload.get("asset_id"), "asset_id")
        best_bid = self._decimal(payload.get("best_bid"), "best_bid", upper=Decimal("1"))
        best_ask = self._decimal(payload.get("best_ask"), "best_ask", upper=Decimal("1"))
        spread = payload.get("spread")
        update = BestBidAskUpdate(
            **self._lineage(capture, event_time),
            update_uid=stable_uid("polymarket", "ws-best-bid-ask", (capture.sha256, asset_id, best_bid, best_ask)),
            market_uid=market_uid,
            outcome_uid=self._outcome_uid(asset_id),
            best_bid=best_bid,
            best_ask=best_ask,
            spread=None if spread is None else self._decimal(spread, "spread"),
        )
        observations = (
            PriceObservation(
                **self._lineage(capture, event_time),
                observation_uid=stable_uid("polymarket", "ws-best-bid", (capture.sha256, asset_id, best_bid)),
                market_uid=market_uid,
                outcome_uid=self._outcome_uid(asset_id),
                platform="polymarket",
                price=best_bid,
                kind=ObservationKind.BEST_BID,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
            ),
            PriceObservation(
                **self._lineage(capture, event_time),
                observation_uid=stable_uid("polymarket", "ws-best-ask", (capture.sha256, asset_id, best_ask)),
                market_uid=market_uid,
                outcome_uid=self._outcome_uid(asset_id),
                platform="polymarket",
                price=best_ask,
                kind=ObservationKind.BEST_ASK,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
            ),
        )
        return ParsedMarketMessage(
            raw_artifact=self.raw_artifact(capture),
            event_type="best_bid_ask",
            observations=observations,
            best_bid_ask=(update,),
        )

    def _lifecycle(
        self,
        payload: Mapping[str, Any],
        capture: RawCapture,
        event_type: Literal["new_market", "market_resolved"],
    ) -> ParsedMarketMessage:
        event_time = self._event_time(payload, capture)
        market_uid = self._market_uid(payload)
        assets = payload.get("assets_ids")
        outcomes = payload.get("outcomes")
        if not isinstance(assets, list) or not isinstance(outcomes, list) or len(assets) != len(outcomes):
            raise ValueError("lifecycle event requires equally sized assets_ids and outcomes lists")
        outcome_uids = tuple(self._outcome_uid(asset) for asset in assets)
        outcome_labels = tuple(require_text(outcome, "outcomes item") for outcome in outcomes)
        winning_asset = payload.get("winning_asset_id")
        if event_type == "market_resolved" and winning_asset is None:
            raise ValueError("market_resolved requires winning_asset_id")
        winning_outcome_uid = None if winning_asset is None else self._outcome_uid(winning_asset)
        lifecycle = MarketLifecycleEvent(
            **self._lineage(capture, event_time),
            lifecycle_uid=stable_uid("polymarket", f"ws-{event_type}", (capture.sha256, market_uid, event_time)),
            lifecycle_type=event_type,
            market_uid=market_uid,
            question=None if payload.get("question") is None else require_text(payload.get("question"), "question"),
            slug=None if payload.get("slug") is None else require_text(payload.get("slug"), "slug"),
            outcome_uids=outcome_uids,
            outcome_labels=outcome_labels,
            winning_outcome_uid=winning_outcome_uid,
            winning_outcome_label=(
                None if payload.get("winning_outcome") is None else require_text(payload.get("winning_outcome"), "winning_outcome")
            ),
        )
        return ParsedMarketMessage(
            raw_artifact=self.raw_artifact(capture),
            event_type=event_type,
            lifecycle_events=(lifecycle,),
        )


class PolymarketMarketWsCollector:
    """Injectable async collector with literal market-channel heartbeat semantics."""

    def __init__(
        self,
        *,
        socket_factory: SocketFactory,
        raw_store: RawArtifactStore,
        subscription: MarketChannelSubscription,
        parser: MarketChannelParser | None = None,
        reconnect_policy: ReconnectPolicy = ReconnectPolicy(),
        heartbeat_interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS,
        sleep: SleepFn = asyncio.sleep,
        clock: Clock | None = None,
    ) -> None:
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        self.socket_factory = socket_factory
        self.raw_store = raw_store
        self.subscription = subscription
        self.parser = parser or MarketChannelParser()
        self.reconnect_policy = reconnect_policy
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(UTC))

    async def _send_subscription(self, socket: MarketSocket) -> None:
        await socket.send(json.dumps(self.subscription.wire_payload(), separators=(",", ":"), sort_keys=True))

    async def send_heartbeat(self, socket: MarketSocket) -> None:
        """Send the documented public market-channel heartbeat payload."""

        await socket.send(HEARTBEAT_PAYLOAD)

    async def _heartbeat_loop(self, socket: MarketSocket, stopped: asyncio.Event) -> None:
        try:
            while not stopped.is_set():
                await self.sleep(self.heartbeat_interval_seconds)
                if not stopped.is_set():
                    await self.send_heartbeat(socket)
        except asyncio.CancelledError:
            raise

    def process_frame(self, raw: bytes | str, *, received_at: datetime | None = None) -> ParsedMarketMessage:
        """Capture frame bytes and append a receipt before any decoding or parsing."""

        timestamp = utc_datetime(received_at or self.clock(), "received_at")
        capture = self.raw_store.capture(
            raw,
            platform="polymarket",
            source=RAW_SOURCE,
            request={
                "endpoint": MARKET_CHANNEL_ENDPOINT,
                "channel": "market",
                "assets_ids": list(self.subscription.asset_ids),
                "custom_feature_enabled": self.subscription.custom_feature_enabled,
            },
            received_at=timestamp,
        )
        return self.parser.parse_raw(raw, capture)

    async def _consume_connection(self, socket: MarketSocket, *, max_messages: int | None) -> tuple[ParsedMarketMessage, ...]:
        await self._send_subscription(socket)
        stopped = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat_loop(socket, stopped))
        messages: list[ParsedMarketMessage] = []
        try:
            while max_messages is None or len(messages) < max_messages:
                raw = await socket.recv()
                messages.append(self.process_frame(raw))
        finally:
            stopped.set()
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
            await socket.close()
        return tuple(messages)

    async def collect(self, *, max_messages: int | None = None) -> CollectionResult:
        """Collect a bounded number of frames, reconnecting only within the declared policy."""

        if max_messages is not None and max_messages < 0:
            raise ValueError("max_messages must be non-negative or None")
        collected: list[ParsedMarketMessage] = []
        delays: list[float] = []
        attempts = 0
        while max_messages is None or len(collected) < max_messages:
            attempts += 1
            try:
                socket = await self.socket_factory()
                remaining = None if max_messages is None else max_messages - len(collected)
                collected.extend(await self._consume_connection(socket, max_messages=remaining))
                break
            except (ConnectionError, OSError):
                if attempts >= self.reconnect_policy.max_attempts:
                    break
                delay = self.reconnect_policy.delay_for_retry(attempts)
                delays.append(delay)
                await self.sleep(delay)
        return CollectionResult(tuple(collected), attempts, tuple(delays))


__all__ = [
    "BestBidAskUpdate",
    "CollectionResult",
    "FieldAvailability",
    "HEARTBEAT_INTERVAL_SECONDS",
    "HEARTBEAT_PAYLOAD",
    "MARKET_CHANNEL_ENDPOINT",
    "MarketChannelParser",
    "MarketChannelSubscription",
    "MarketLifecycleEvent",
    "MarketSocket",
    "PARSER_VERSION",
    "ParsedMarketMessage",
    "PolymarketMarketWsCollector",
    "PriceLevelChange",
    "PUBLIC_MARKET_LIMITATIONS",
    "ReconnectPolicy",
    "UnknownMarketChannelEvent",
]
