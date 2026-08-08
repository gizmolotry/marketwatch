"""Raw-first Kalshi market-data WebSocket ingestion.

This adapter implements only the documented market-data subscription contract:

* https://docs.kalshi.com/getting_started/quick_start_websockets
* https://docs.kalshi.com/websockets/public-trades
* https://docs.kalshi.com/websockets/market-ticker
* https://docs.kalshi.com/websockets/orderbook-updates

Kalshi requires an authenticated handshake even for public market-data channels.
Credentials are accepted by the injected server-side transport only; they are
never placed in raw receipts, canonical events, logs, or return values.  The
public trade stream does not provide account identities.  Accordingly this
module never manufactures actors, order owners, or a fill direction when the
message has not explicitly supplied an outcome side.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping, Protocol, runtime_checkable
from urllib.parse import urlsplit

from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
    TradeFill,
    TradeSide,
)

from ..coverage import CapabilityMetadata
from ..normalize import decimal_from, qualified_id, require_text, stable_uid, utc_datetime
from ..quality import DataQualityReport
from ..raw_store import RawArtifactStore, RawCapture
from .models import IngestionBatch


KALSHI_WEBSOCKET_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
"""Documented production endpoint; never contacted by this module implicitly."""

_KALSHI_WEBSOCKET_HOST = "external-api-ws.kalshi.com"
_KALSHI_WEBSOCKET_PATH = "/trade-api/ws/v2"

_AUTH_HEADER_NAMES = (
    "KALSHI-ACCESS-KEY",
    "KALSHI-ACCESS-SIGNATURE",
    "KALSHI-ACCESS-TIMESTAMP",
)
_ALLOWED_CHANNELS = frozenset(
    {"trade", "ticker", "market_lifecycle_v2", "orderbook_delta"}
)


class KalshiWebSocketAuthenticationError(ValueError):
    """Raised before connection when the required server-side handshake is absent."""


class KalshiWebSocketConfigurationError(ValueError):
    """Raised when credentials could be routed outside the approved endpoint."""


class KalshiWebSocketProtocolError(ValueError):
    """Raised when a documented market-data payload cannot be normalized safely."""


@runtime_checkable
class KalshiWebSocket(Protocol):
    """Small injectable socket surface; compatible with common async clients."""

    async def send(self, payload: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


SocketFactory = Callable[
    [str, Mapping[str, str]],
    KalshiWebSocket | Awaitable[KalshiWebSocket],
]
Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class KalshiReconnectPolicy:
    """Finite retry policy; the first connection is not a reconnect attempt."""

    max_reconnect_attempts: int = 2
    initial_backoff_seconds: float = 0.25
    maximum_backoff_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.max_reconnect_attempts < 0:
            raise ValueError("max_reconnect_attempts must be non-negative")
        if self.initial_backoff_seconds < 0 or self.maximum_backoff_seconds < 0:
            raise ValueError("reconnect backoff values must be non-negative")
        if self.maximum_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("maximum_backoff_seconds must be >= initial_backoff_seconds")

    def delay_for(self, reconnect_number: int) -> float:
        """Return a bounded exponential delay for reconnect number one onward."""

        if reconnect_number < 1:
            raise ValueError("reconnect_number must be positive")
        return min(
            self.maximum_backoff_seconds,
            self.initial_backoff_seconds * (2 ** (reconnect_number - 1)),
        )


@dataclass(frozen=True, slots=True)
class KalshiWebSocketConfig:
    """Explicit, narrowly-scoped server configuration for one market set."""

    market_tickers: tuple[str, ...]
    auth_headers: Mapping[str, str] = field(default_factory=dict, repr=False, compare=False)
    channels: tuple[str, ...] = (
        "trade",
        "ticker",
        "market_lifecycle_v2",
        "orderbook_delta",
    )
    websocket_url: str = KALSHI_WEBSOCKET_URL
    reconnect: KalshiReconnectPolicy = field(default_factory=KalshiReconnectPolicy)

    def __post_init__(self) -> None:
        tickers = tuple(dict.fromkeys(require_text(item, "market_tickers") for item in self.market_tickers))
        if not tickers:
            raise ValueError("market_tickers must contain at least one explicit Kalshi ticker")
        channels = tuple(dict.fromkeys(require_text(item, "channels") for item in self.channels))
        if not channels:
            raise ValueError("channels must contain at least one documented market-data channel")
        unsupported = set(channels) - _ALLOWED_CHANNELS
        if unsupported:
            raise ValueError(f"unsupported Kalshi WebSocket channels: {sorted(unsupported)}")
        websocket_url = require_text(self.websocket_url, "websocket_url")
        self._validate_websocket_url(websocket_url)
        object.__setattr__(self, "market_tickers", tickers)
        object.__setattr__(self, "channels", channels)
        object.__setattr__(self, "websocket_url", websocket_url)
        # Copy rather than retain a mutable environment/config mapping.  The
        # values intentionally remain private to the connector instance.
        object.__setattr__(self, "auth_headers", dict(self.auth_headers))

    @staticmethod
    def _validate_websocket_url(websocket_url: str) -> None:
        """Fail closed before credentials can be read or handed to a transport."""

        try:
            parsed = urlsplit(websocket_url)
            port = parsed.port
        except ValueError as exc:
            raise KalshiWebSocketConfigurationError(
                "invalid Kalshi WebSocket endpoint"
            ) from exc
        if (
            parsed.scheme != "wss"
            or parsed.hostname != _KALSHI_WEBSOCKET_HOST
            or port is not None
            or parsed.path != _KALSHI_WEBSOCKET_PATH
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or websocket_url != KALSHI_WEBSOCKET_URL
        ):
            raise KalshiWebSocketConfigurationError(
                "Kalshi WebSocket endpoint must exactly match the approved production endpoint"
            )


@dataclass(frozen=True, slots=True)
class KalshiWsMarketDataEvent:
    """A lossless semantic envelope for fields Kalshi actually exposed.

    `exposed_fields` is copied from the message's documented ``msg`` body. It
    preserves such fields as ``taker_outcome_side`` and ``taker_book_side``
    without converting them into an account, order owner, or unsupported
    directional claim.
    """

    message_type: str
    raw_artifact_uid: str
    raw_sha256: str
    received_at: datetime
    event_time: datetime
    event_time_source: str
    market_ticker: str | None
    exposed_fields: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class KalshiWsUnsupportedMessage:
    """A preserved raw delivery whose type/schema was not safe to normalize."""

    raw_artifact_uid: str
    raw_sha256: str
    received_at: datetime
    message_type: str | None
    reason: str


@dataclass(slots=True)
class KalshiWsCollection:
    """Market data plus explicit transport and parsing limitations."""

    batch: IngestionBatch
    events: list[KalshiWsMarketDataEvent] = field(default_factory=list)
    unsupported_messages: list[KalshiWsUnsupportedMessage] = field(default_factory=list)
    connection_attempts: int = 0
    reconnect_attempts: int = 0
    heartbeats_acknowledged: int = 0
    transport_failures: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return self.batch.complete


class KalshiWebSocketCollector:
    """Collect an explicitly configured, bounded sample from Kalshi's WS API.

    The collector is intentionally transport-injected.  That makes credentials
    server-side, permits deterministic tests, and avoids opening a live socket
    merely by importing or constructing the connector.
    """

    PARSER_VERSION = "kalshi-websocket-v1.0.0"
    SOURCE_UID = "kalshi:source/websocket-market-data"

    trade_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="websocket_public_trades",
        actor_visibility="unavailable",
        direction_visibility="partial",
        depth_visibility="unavailable",
        pagination="live stream; explicit market-ticker subscriptions; reconnects can leave gaps",
        official_documentation="https://docs.kalshi.com/websockets/public-trades",
        notes=(
            "Kalshi requires an authenticated WebSocket handshake even for public data",
            "public trade messages expose no account identifiers or order owners",
            "a canonical fill is emitted only for explicit taker_outcome_side or legacy taker_side",
        ),
    )
    ticker_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="websocket_market_ticker",
        actor_visibility="unavailable",
        direction_visibility="unavailable",
        depth_visibility="partial",
        pagination="live stream; explicit market-ticker subscriptions; reconnects can leave gaps",
        official_documentation="https://docs.kalshi.com/websockets/market-ticker",
        notes=("ticker values are market aggregates, not account or order-owner evidence",),
    )
    orderbook_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="websocket_orderbook_delta",
        actor_visibility="unavailable",
        direction_visibility="partial",
        depth_visibility="available",
        pagination="authenticated live aggregate L2 stream; snapshot followed by deltas; reconnects can leave gaps",
        official_documentation="https://docs.kalshi.com/websockets/orderbook-updates",
        notes=(
            "orderbook_delta requires the server-side authenticated handshake",
            "the stream represents aggregate price levels, never an order owner or account",
            "deltas are retained as events and are not converted into synthetic book snapshots",
        ),
    )
    lifecycle_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="websocket_market_lifecycle_v2",
        actor_visibility="unavailable",
        direction_visibility="unavailable",
        depth_visibility="unavailable",
        pagination="live stream; explicit market-ticker subscriptions; reconnects can leave gaps",
        official_documentation="https://docs.kalshi.com/getting_started/market_lifecycle",
        notes=("lifecycle/status updates are retained as platform status metadata, not adjudication evidence",),
    )

    def __init__(
        self,
        *,
        config: KalshiWebSocketConfig,
        raw_store: RawArtifactStore,
        socket_factory: SocketFactory,
        clock: Clock | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.config = config
        self.raw_store = raw_store
        self.socket_factory = socket_factory
        self.clock = clock or (lambda: datetime.now(UTC))
        self.sleep = sleep

    def _authenticated_headers(self) -> dict[str, str]:
        """Validate but never serialize the mandatory handshake credentials."""

        headers: dict[str, str] = {}
        for name in _AUTH_HEADER_NAMES:
            value = self.config.auth_headers.get(name)
            if value is None or not str(value).strip():
                raise KalshiWebSocketAuthenticationError(
                    f"missing required server-side Kalshi authentication header: {name}"
                )
            headers[name] = str(value)
        return headers

    def subscription_messages(self) -> tuple[str, ...]:
        """Return documented subscription commands without embedding secrets."""

        return tuple(
            json.dumps(
                {
                    "id": identifier,
                    "cmd": "subscribe",
                    "params": {
                        "channels": [channel],
                        "market_tickers": list(self.config.market_tickers),
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            for identifier, channel in enumerate(self.config.channels, start=1)
        )

    @classmethod
    def _raw_uid(cls, capture: RawCapture) -> str:
        return f"kalshi:raw/{capture.sha256}"

    @classmethod
    def _capabilities_for(cls, channels: tuple[str, ...]) -> list[CapabilityMetadata]:
        by_channel = {
            "trade": cls.trade_capability,
            "ticker": cls.ticker_capability,
            "orderbook_delta": cls.orderbook_capability,
            "market_lifecycle_v2": cls.lifecycle_capability,
        }
        return [by_channel[channel] for channel in channels]

    async def collect(self, *, max_messages: int) -> KalshiWsCollection:
        """Receive up to ``max_messages`` with finite reconnect attempts.

        A transport failure is surfaced as an incomplete collection rather than
        silently converted into a complete historical interval.  No outgoing
        request, raw receipt, or result object contains the handshake secrets.
        """

        if max_messages < 1:
            raise ValueError("max_messages must be positive")
        # Revalidate at the credential boundary as defense in depth for config
        # objects restored or mutated outside their normal constructor.
        self.config._validate_websocket_url(self.config.websocket_url)
        headers = self._authenticated_headers()
        quality = DataQualityReport(source="kalshi:websocket-market-data")
        result = KalshiWsCollection(
            batch=IngestionBatch(
                capabilities=self._capabilities_for(self.config.channels),
                quality=quality,
                complete=False,
            )
        )
        remaining = max_messages

        for attempt in range(self.config.reconnect.max_reconnect_attempts + 1):
            socket: KalshiWebSocket | None = None
            result.connection_attempts += 1
            try:
                made_socket = self.socket_factory(self.config.websocket_url, headers)
                socket = await made_socket if inspect.isawaitable(made_socket) else made_socket
                if not isinstance(socket, KalshiWebSocket):
                    raise TypeError("socket_factory must return a KalshiWebSocket-compatible object")
                for command in self.subscription_messages():
                    await socket.send(command)
                while remaining:
                    raw = await socket.recv()
                    if raw is None:  # type: ignore[comparison-overlap]
                        raise ConnectionError("Kalshi WebSocket returned an empty delivery")
                    await self._capture_then_parse(raw, socket, result)
                    remaining -= 1
                result.batch.complete = True
                return result
            except Exception as exc:  # transport boundaries intentionally become explicit gaps
                result.transport_failures.append(type(exc).__name__)
                if attempt >= self.config.reconnect.max_reconnect_attempts:
                    return result
                result.reconnect_attempts += 1
                delay = self.config.reconnect.delay_for(result.reconnect_attempts)
                if delay:
                    await self.sleep(delay)
            finally:
                if socket is not None:
                    await self._close_quietly(socket)
        return result  # defensive: loop always returns

    async def _capture_then_parse(
        self,
        raw: str | bytes,
        socket: KalshiWebSocket,
        result: KalshiWsCollection,
    ) -> None:
        """Capture bytes and receipt before attempting JSON/schema parsing."""

        received_at = utc_datetime(self.clock(), "received_at")
        payload = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
        capture = self.raw_store.capture(
            payload,
            platform="kalshi",
            source="websocket/market-data",
            received_at=received_at,
            request={
                "transport": "websocket",
                "endpoint_path": "/trade-api/ws/v2",
                "configured_market_tickers": list(self.config.market_tickers),
            },
            response_metadata={"delivery": "incoming_market_data_message"},
        )
        # Make lineage available to parsers only after the immutable payload and
        # receipt have been written successfully.
        result.batch.raw_artifacts.append(
            RawArtifactStore.to_domain(
                capture,
                source_uid=self.SOURCE_UID,
                parser_version=self.PARSER_VERSION,
            )
        )
        result.batch.quality.received += 1  # quality is constructed above
        await self._parse_captured(payload, capture, socket, result)

    async def _parse_captured(
        self,
        payload: bytes,
        capture: RawCapture,
        socket: KalshiWebSocket,
        result: KalshiWsCollection,
    ) -> None:
        """Parse one already-captured delivery, preserving unsupported input."""

        try:
            envelope = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._unsupported(result, capture, None, "malformed_json")
            result.batch.quality.invalid += 1
            return
        if not isinstance(envelope, Mapping):
            self._unsupported(result, capture, None, "message_envelope_not_object")
            result.batch.quality.invalid += 1
            return
        raw_message_type = envelope.get("type")
        message_type = str(raw_message_type).strip() if raw_message_type is not None else ""
        if not message_type:
            self._unsupported(result, capture, None, "message_type_missing")
            result.batch.quality.invalid += 1
            return

        # WebSocket control Ping/Pong normally stays below the application API.
        # Supporting a JSON ping makes an injected transport testable without
        # claiming it is a market-data event.
        if message_type.lower() == "ping":
            await self._acknowledge_heartbeat(socket)
            result.heartbeats_acknowledged += 1
            result.batch.quality.normalized += 1
            return

        body = envelope.get("msg")
        if not isinstance(body, Mapping):
            self._unsupported(result, capture, message_type, "message_body_missing_or_not_object")
            result.batch.quality.invalid += 1
            return
        try:
            event = self._event_from_body(message_type, body, capture)
            result.events.append(event)
            if message_type == "trade":
                self._normalize_trade(event, capture, result.batch)
            elif message_type == "ticker":
                self._normalize_ticker(event, capture, result.batch)
            elif message_type == "orderbook_snapshot":
                self._normalize_orderbook_snapshot(event, capture, result.batch)
            elif message_type in {"orderbook_delta", "market_lifecycle_v2"}:
                # These are safely represented by the lossless semantic event.
                # A delta is not a snapshot and lifecycle is not an outcome.
                pass
            else:
                result.events.pop()
                self._unsupported(result, capture, message_type, "unsupported_message_type")
                result.batch.quality.invalid += 1
                return
        except (KalshiWebSocketProtocolError, ValueError) as exc:
            if result.events and result.events[-1].raw_sha256 == capture.sha256:
                result.events.pop()
            self._unsupported(result, capture, message_type, self._safe_reason(exc))
            result.batch.quality.invalid += 1
            return
        result.batch.quality.normalized += 1

    @classmethod
    def _event_from_body(
        cls,
        message_type: str,
        body: Mapping[str, Any],
        capture: RawCapture,
    ) -> KalshiWsMarketDataEvent:
        event_time, event_time_source = cls._event_time(body, capture)
        market_ticker_value = body.get("market_ticker")
        market_ticker = (
            require_text(market_ticker_value, "market_ticker") if market_ticker_value is not None else None
        )
        # This is an envelope of *received* fields, rather than a new identity
        # schema.  A shallow JSON-like copy stops later caller mutation from
        # changing the event while raw bytes remain the authoritative original.
        return KalshiWsMarketDataEvent(
            message_type=message_type,
            raw_artifact_uid=cls._raw_uid(capture),
            raw_sha256=capture.sha256,
            received_at=capture.received_at,
            event_time=event_time,
            event_time_source=event_time_source,
            market_ticker=market_ticker,
            exposed_fields=cls._copy_exposed_fields(body),
        )

    @staticmethod
    def _copy_exposed_fields(body: Mapping[str, Any]) -> Mapping[str, Any]:
        """Copy JSON-compatible public fields without adding derived fields."""

        # `json` provides a useful strict boundary: values remain only values
        # the wire message carried, and unserializable transport objects fail
        # safe rather than being stringified into a misleading claim.
        try:
            return json.loads(json.dumps(dict(body), ensure_ascii=False))
        except (TypeError, ValueError) as exc:
            raise KalshiWebSocketProtocolError("message body is not JSON-compatible") from exc

    @staticmethod
    def _event_time(body: Mapping[str, Any], capture: RawCapture) -> tuple[datetime, str]:
        for field_name in ("ts_ms", "ts", "time"):
            if body.get(field_name) is not None:
                return utc_datetime(body[field_name], field_name), field_name
        # The absence is explicit in event_time_source; this is not a claim that
        # receipt time equals exchange event time.
        return capture.received_at, "received_at_no_exchange_timestamp"

    def _normalize_trade(
        self,
        event: KalshiWsMarketDataEvent,
        capture: RawCapture,
        batch: IngestionBatch,
    ) -> None:
        body = event.exposed_fields
        ticker = require_text(body.get("market_ticker"), "trade.market_ticker")
        trade_id = require_text(body.get("trade_id"), "trade.trade_id")
        yes_price = decimal_from(
            body.get("yes_price_dollars"),
            "trade.yes_price_dollars",
            minimum=Decimal(0),
            maximum=Decimal(1),
        )
        market_uid = qualified_id("kalshi", f"market/{ticker}")
        yes_outcome_uid = qualified_id("kalshi", f"outcome/{ticker}/yes")
        batch.observations.append(
            PriceObservation(
                event_time=event.event_time,
                ingested_at=capture.received_at,
                source_uid=self.SOURCE_UID,
                raw_artifact_uid=self._raw_uid(capture),
                parser_version=self.PARSER_VERSION,
                observation_uid=qualified_id("kalshi", f"ws-trade-price/{trade_id}/yes"),
                market_uid=market_uid,
                outcome_uid=yes_outcome_uid,
                platform="kalshi",
                price=yes_price,
                kind=ObservationKind.LAST_TRADE,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
            )
        )
        # A book side alone is not converted into a fill direction.  Only the
        # documented explicit outcome-side field (or legacy equivalent) grants
        # enough information for the existing canonical TradeFill contract.
        raw_outcome_side = body.get("taker_outcome_side", body.get("taker_side"))
        if raw_outcome_side is None:
            return
        outcome = str(raw_outcome_side).strip().lower()
        if outcome not in {"yes", "no"}:
            return
        size = decimal_from(body.get("count_fp"), "trade.count_fp", minimum=Decimal(0))
        if outcome == "yes":
            outcome_uid = yes_outcome_uid
            price = yes_price
        else:
            outcome_uid = qualified_id("kalshi", f"outcome/{ticker}/no")
            price = decimal_from(
                body.get("no_price_dollars"),
                "trade.no_price_dollars",
                minimum=Decimal(0),
                maximum=Decimal(1),
            )
        batch.fills.append(
            TradeFill(
                event_time=event.event_time,
                ingested_at=capture.received_at,
                source_uid=self.SOURCE_UID,
                raw_artifact_uid=self._raw_uid(capture),
                parser_version=self.PARSER_VERSION,
                fill_uid=qualified_id("kalshi", f"ws-fill/{trade_id}/{outcome}"),
                market_uid=market_uid,
                outcome_uid=outcome_uid,
                platform="kalshi",
                price=price,
                size=size,
                side=TradeSide.BUY,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
                actor_uid=None,
                maker=None,
                taker=None,
                transaction_uid=None,
            )
        )

    def _normalize_ticker(
        self,
        event: KalshiWsMarketDataEvent,
        capture: RawCapture,
        batch: IngestionBatch,
    ) -> None:
        body = event.exposed_fields
        ticker = require_text(body.get("market_ticker"), "ticker.market_ticker")
        market_uid = qualified_id("kalshi", f"market/{ticker}")
        outcome_uid = qualified_id("kalshi", f"outcome/{ticker}/yes")
        fields = (
            ("price_dollars", ObservationKind.LAST_TRADE),
            ("yes_bid_dollars", ObservationKind.BEST_BID),
            ("yes_ask_dollars", ObservationKind.BEST_ASK),
        )
        for field_name, kind in fields:
            raw_price = body.get(field_name)
            if raw_price is None:
                continue
            batch.observations.append(
                PriceObservation(
                    event_time=event.event_time,
                    ingested_at=capture.received_at,
                    source_uid=self.SOURCE_UID,
                    raw_artifact_uid=self._raw_uid(capture),
                    parser_version=self.PARSER_VERSION,
                    observation_uid=stable_uid(
                        "kalshi", "ws-ticker-observation", (capture.sha256, field_name)
                    ),
                    market_uid=market_uid,
                    outcome_uid=outcome_uid,
                    platform="kalshi",
                    price=decimal_from(
                        raw_price,
                        f"ticker.{field_name}",
                        minimum=Decimal(0),
                        maximum=Decimal(1),
                    ),
                    kind=kind,
                    actor_visibility=ActorVisibility.NOT_AVAILABLE,
                )
            )

    def _normalize_orderbook_snapshot(
        self,
        event: KalshiWsMarketDataEvent,
        capture: RawCapture,
        batch: IngestionBatch,
    ) -> None:
        body = event.exposed_fields
        ticker = require_text(body.get("market_ticker"), "orderbook_snapshot.market_ticker")
        market_uid = qualified_id("kalshi", f"market/{ticker}")
        for outcome, level_field in (("yes", "yes_dollars_fp"), ("no", "no_dollars_fp")):
            raw_levels = body.get(level_field)
            if not isinstance(raw_levels, list):
                raise KalshiWebSocketProtocolError(f"orderbook_snapshot.{level_field} must be a list")
            levels: list[OrderBookLevel] = []
            for index, pair in enumerate(raw_levels):
                if not isinstance(pair, list) or len(pair) != 2:
                    raise KalshiWebSocketProtocolError(
                        f"orderbook_snapshot.{level_field}[{index}] must be [price, count]"
                    )
                levels.append(
                    OrderBookLevel(
                        price=decimal_from(
                            pair[0],
                            f"orderbook_snapshot.{level_field}[{index}].price",
                            minimum=Decimal(0),
                            maximum=Decimal(1),
                        ),
                        size=decimal_from(
                            pair[1],
                            f"orderbook_snapshot.{level_field}[{index}].count",
                            minimum=Decimal(0),
                        ),
                    )
                )
            # Kalshi exposes bids for both outcomes.  Do not synthesize asks by
            # transforming the other outcome's bid; only sort the same exposed
            # levels into the canonical descending-bid contract.
            ordered_levels = tuple(sorted(levels, key=lambda level: level.price, reverse=True))
            batch.snapshots.append(
                OrderBookSnapshot(
                    event_time=event.event_time,
                    ingested_at=capture.received_at,
                    source_uid=self.SOURCE_UID,
                    raw_artifact_uid=self._raw_uid(capture),
                    parser_version=self.PARSER_VERSION,
                    snapshot_uid=stable_uid(
                        "kalshi", "ws-orderbook-snapshot", (capture.sha256, outcome)
                    ),
                    market_uid=market_uid,
                    outcome_uid=qualified_id("kalshi", f"outcome/{ticker}/{outcome}"),
                    platform="kalshi",
                    bids=ordered_levels,
                    asks=(),
                )
            )

    async def _acknowledge_heartbeat(self, socket: KalshiWebSocket) -> None:
        pong = getattr(socket, "pong", None)
        if not callable(pong):
            return
        response = pong(b"heartbeat")
        if inspect.isawaitable(response):
            await response

    @staticmethod
    async def _close_quietly(socket: KalshiWebSocket) -> None:
        try:
            response = socket.close()
            if inspect.isawaitable(response):
                await response
        except Exception:
            # A failed close must not turn a captured/explicitly incomplete run
            # into a false success or leak transport-specific exception content.
            return

    @classmethod
    def _unsupported(
        cls,
        result: KalshiWsCollection,
        capture: RawCapture,
        message_type: str | None,
        reason: str,
    ) -> None:
        result.unsupported_messages.append(
            KalshiWsUnsupportedMessage(
                raw_artifact_uid=cls._raw_uid(capture),
                raw_sha256=capture.sha256,
                received_at=capture.received_at,
                message_type=message_type,
                reason=reason,
            )
        )

    @staticmethod
    def _safe_reason(exc: Exception) -> str:
        """Keep error provenance useful without echoing arbitrary payload text."""

        if isinstance(exc, KalshiWebSocketProtocolError):
            return "unsupported_documented_payload"
        return "normalization_failed"
