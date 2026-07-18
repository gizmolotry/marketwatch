"""Fixed, raw-first WebSocket adapter for one documented reference source.

This module intentionally implements only the Coinbase Exchange ``BTC-USD``
ticker feed.  It is neither a generic WebSocket client nor a symbol-to-price
adapter: the URL, subscription, product, channels, and source identity are
constants.  A caller must separately provide a raw-lineaged settlement mapping
that proves this fixed source is authorized for an approved target.
"""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping, Protocol, runtime_checkable

from marketleak.ingestion.config.phase15_registry import (
    ApprovedMarketTarget,
    DocumentedBtcReferenceMapping,
)
from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture
from marketleak.multimodal.reference_price import (
    DocumentedReferenceSource,
    PrimarySourceAvailability,
    PrimarySourceStatus,
    ReferenceAdmissionStatus,
    ReferencePriceAdmission,
    ReferencePriceObservation,
    admit_reference_price,
)
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability


# These values are deliberately constants, not constructor arguments. Adding
# another feed requires a separate reviewed adapter and registry entry.
COINBASE_EXCHANGE_BTC_USD_SOURCE_UID = "source:coinbase-exchange-btc-usd"
COINBASE_EXCHANGE_BTC_USD_TICKER_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_EXCHANGE_BTC_USD_MAPPING_URL = "https://docs.cdp.coinbase.com/exchange/websocket-feed/overview"
COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF = "config:coinbase-exchange-btc-usd-public-feed-v1"
COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID = "BTC-USD"
COINBASE_EXCHANGE_BTC_USD_PLATFORM = "coinbase_exchange"
COINBASE_EXCHANGE_BTC_USD_DATASET = "btc_usd_ticker"
COINBASE_EXCHANGE_BTC_USD_PARSER_VERSION = "coinbase-exchange-btc-usd-ticker-v2"
COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION = (
    '{"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker","heartbeat"]}'
)


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ticker time must be a non-empty ISO-8601 timestamp")
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("ticker time must be a valid ISO-8601 timestamp") from exc
    return _utc(parsed, field="ticker time")


def _parse_price(value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("ticker price must be a decimal, not a boolean")
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("ticker price must be a decimal") from exc
    if not price.is_finite() or price < 0:
        raise ValueError("ticker price must be a finite non-negative decimal")
    return price


@runtime_checkable
class CoinbaseExchangeWebSocket(Protocol):
    """Minimal injectable async session surface for the documented feed."""

    async def send(self, payload: str) -> Any: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> Any: ...


SocketFactory = Callable[
    [str],
    CoinbaseExchangeWebSocket | Awaitable[CoinbaseExchangeWebSocket],
]


class DocumentedReferenceAdapterStatus(str, Enum):
    """Fail-closed outcomes for the one fixed source adapter."""

    COLLECTED = "collected"
    UNAVAILABLE_MAPPING_MISMATCH = "unavailable_mapping_mismatch"
    UNAVAILABLE_CONNECT = "unavailable_connect"
    UNAVAILABLE_SEND = "unavailable_send"
    UNAVAILABLE_SUBSCRIBE = "unavailable_subscribe"
    UNAVAILABLE_ERROR = "unavailable_error"
    UNAVAILABLE_MALFORMED = "unavailable_malformed"
    UNAVAILABLE_STALE = "unavailable_stale"
    UNAVAILABLE_GAP = "unavailable_gap"
    # Retained as compatibility names for callers that persisted the former
    # HTTP-boundary vocabulary. The WebSocket implementation never emits them.
    UNAVAILABLE_REQUEST = "unavailable_request"
    UNAVAILABLE_HTTP = "unavailable_http"


@dataclass(frozen=True, slots=True)
class DocumentedReferenceAdapterResult:
    """All raw frames and the only possible price-admission result."""

    status: DocumentedReferenceAdapterStatus
    reason: str
    admission: ReferencePriceAdmission
    mapping: DocumentedReferenceSource | None
    primary_source_availability: PrimarySourceAvailability | None
    observation: ReferencePriceObservation | None
    raw_captures: tuple[RawCapture, ...]
    coverage: tuple[CoverageRecord, ...]


class CoinbaseExchangeBtcUsdTickerAdapter:
    """Collect one exact Coinbase Exchange ``BTC-USD`` ticker WebSocket frame.

    The caller injects the WebSocket factory; this module never opens a live
    connection by construction. One session is established, the exact official
    subscription is sent once, and at most ``max_inbound_frames`` frames are
    read. Every received control, data, or error frame is captured *before*
    it is decoded. Any failure becomes a non-admission with incomplete
    coverage; there is no retry, discovery, or fallback source.
    """

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        socket_factory: SocketFactory,
        coverage_ledger: CoverageLedger | None = None,
        max_inbound_frames: int = 8,
        maximum_ticker_age_seconds: float = 60.0,
    ) -> None:
        maximum_age = float(maximum_ticker_age_seconds)
        if not isinstance(max_inbound_frames, int) or isinstance(max_inbound_frames, bool):
            raise ValueError("max_inbound_frames must be an integer")
        if max_inbound_frames < 2 or max_inbound_frames > 100:
            raise ValueError("max_inbound_frames must be in [2, 100]")
        if maximum_age <= 0 or maximum_age > 300:
            raise ValueError("maximum_ticker_age_seconds must be in (0, 300]")
        self.raw_store = raw_store
        self.socket_factory = socket_factory
        self.coverage_ledger = coverage_ledger
        self.max_inbound_frames = max_inbound_frames
        self.maximum_ticker_age = timedelta(seconds=maximum_age)

    async def collect(
        self,
        *,
        target: ApprovedMarketTarget,
        mapping: DocumentedBtcReferenceMapping,
        documented_mapping: DocumentedReferenceSource,
        as_of: datetime,
        received_at: datetime,
    ) -> DocumentedReferenceAdapterResult:
        """Read a bounded, subscription-confirmed ticker or fail closed."""

        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        mismatch = self._mapping_mismatch(target, mapping, documented_mapping)
        if mismatch is not None:
            return self._unavailable(
                status=DocumentedReferenceAdapterStatus.UNAVAILABLE_MAPPING_MISMATCH,
                reason=mismatch,
                target=target,
                registry_mapping=mapping,
                cutoff=cutoff,
                mapping=None,
                availability=None,
                captures=(),
                coverage_state="unavailable",
            )
        if any(
            value > cutoff
            for value in (
                documented_mapping.event_time,
                documented_mapping.first_seen_at,
                documented_mapping.retrieved_at,
                documented_mapping.ingested_at,
            )
        ):
            return self._unavailable(
                status=DocumentedReferenceAdapterStatus.UNAVAILABLE_STALE,
                reason="documented settlement mapping was not available at the requested as_of cutoff",
                target=target,
                registry_mapping=mapping,
                cutoff=cutoff,
                mapping=documented_mapping,
                availability=None,
                captures=(),
                coverage_state="partial",
            )

        session: CoinbaseExchangeWebSocket | None = None
        captures: list[RawCapture] = []
        try:
            try:
                candidate = self.socket_factory(COINBASE_EXCHANGE_BTC_USD_TICKER_URL)
                session = await candidate if inspect.isawaitable(candidate) else candidate
                if not isinstance(session, CoinbaseExchangeWebSocket):
                    raise TypeError("socket_factory must return a CoinbaseExchangeWebSocket-compatible session")
            except Exception as exc:
                return self._unavailable(
                    status=DocumentedReferenceAdapterStatus.UNAVAILABLE_CONNECT,
                    reason=f"fixed Coinbase WebSocket connection failed: {type(exc).__name__}",
                    target=target,
                    registry_mapping=mapping,
                    cutoff=cutoff,
                    mapping=documented_mapping,
                    availability=None,
                    captures=(),
                    coverage_state="unavailable",
                )

            try:
                await session.send(COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION)
            except Exception as exc:
                return self._unavailable(
                    status=DocumentedReferenceAdapterStatus.UNAVAILABLE_SEND,
                    reason=f"fixed Coinbase subscription send failed: {type(exc).__name__}",
                    target=target,
                    registry_mapping=mapping,
                    cutoff=cutoff,
                    mapping=documented_mapping,
                    availability=None,
                    captures=(),
                    coverage_state="unavailable",
                )

            subscribed = False
            for _ in range(self.max_inbound_frames):
                try:
                    frame = await session.recv()
                except Exception as exc:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_GAP,
                        reason=f"fixed Coinbase stream ended before an admissible ticker: {type(exc).__name__}",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._unavailable_from_latest(captures, reason="stream ended before ticker"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )

                capture = self._capture_frame(frame=frame, received_at=timestamp)
                captures.append(capture)
                try:
                    payload = self._decode_frame(frame)
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_MALFORMED,
                        reason=f"fixed Coinbase frame is malformed after raw capture: {type(exc).__name__}",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=False, reason="frame could not be decoded"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )

                message_type = payload.get("type")
                if message_type == "error":
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_ERROR,
                        reason="fixed Coinbase feed sent an error frame",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=False, reason="Coinbase error frame"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )
                if message_type == "subscriptions":
                    if subscribed or not self._subscription_confirmed(payload):
                        return self._unavailable(
                            status=DocumentedReferenceAdapterStatus.UNAVAILABLE_SUBSCRIBE,
                            reason="fixed Coinbase subscription confirmation does not exactly match BTC-USD ticker and heartbeat",
                            target=target,
                            registry_mapping=mapping,
                            cutoff=cutoff,
                            mapping=documented_mapping,
                            availability=self._availability(capture, available=False, reason="subscription confirmation mismatch"),
                            captures=tuple(captures),
                            coverage_state="partial",
                        )
                    subscribed = True
                    continue
                if not subscribed:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_SUBSCRIBE,
                        reason="fixed Coinbase ticker arrived before exact subscription confirmation",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=False, reason="subscription not confirmed"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )
                if message_type == "heartbeat":
                    if payload.get("product_id") != COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID:
                        return self._unavailable(
                            status=DocumentedReferenceAdapterStatus.UNAVAILABLE_MALFORMED,
                            reason="fixed Coinbase heartbeat names an unexpected product",
                            target=target,
                            registry_mapping=mapping,
                            cutoff=cutoff,
                            mapping=documented_mapping,
                            availability=self._availability(capture, available=False, reason="unexpected heartbeat product"),
                            captures=tuple(captures),
                            coverage_state="partial",
                        )
                    continue
                if message_type != "ticker":
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_MALFORMED,
                        reason="fixed Coinbase stream supplied an unsupported post-subscription frame",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=False, reason="unsupported frame type"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )

                try:
                    event_time, price, sequence = self._validated_ticker(payload)
                except (TypeError, ValueError, InvalidOperation) as exc:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_MALFORMED,
                        reason=f"fixed Coinbase ticker payload is malformed: {type(exc).__name__}",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=False, reason="ticker payload did not meet the fixed contract"),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )
                if event_time > timestamp or event_time < timestamp - self.maximum_ticker_age:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_STALE,
                        reason="fixed Coinbase ticker time is future-dated or older than the configured bounded freshness window",
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=self._availability(capture, available=True),
                        captures=tuple(captures),
                        coverage_state="partial",
                    )

                availability = self._availability(capture, available=True)
                observation = self._observation(
                    target=target,
                    mapping=documented_mapping,
                    availability=availability,
                    capture=capture,
                    event_time=event_time,
                    price=price,
                    sequence=sequence,
                )
                admission = admit_reference_price(
                    market_uid=target.market_uid,
                    as_of=cutoff,
                    documented_sources=(documented_mapping,),
                    primary_source_availability=availability,
                    observation=observation,
                )
                if admission.status != ReferenceAdmissionStatus.ADMITTED:
                    return self._unavailable(
                        status=DocumentedReferenceAdapterStatus.UNAVAILABLE_STALE,
                        reason=admission.reason,
                        target=target,
                        registry_mapping=mapping,
                        cutoff=cutoff,
                        mapping=documented_mapping,
                        availability=availability,
                        captures=tuple(captures),
                        coverage_state="partial",
                        admission=admission,
                    )
                coverage = self._coverage(
                    target=target,
                    mapping=mapping,
                    timestamp=timestamp,
                    captures=tuple(captures),
                    complete=True,
                    coverage_state="complete",
                )
                self._append_coverage(coverage)
                return DocumentedReferenceAdapterResult(
                    status=DocumentedReferenceAdapterStatus.COLLECTED,
                    reason="fixed Coinbase BTC-USD ticker was raw-lineaged and point-in-time admissible",
                    admission=admission,
                    mapping=documented_mapping,
                    primary_source_availability=availability,
                    observation=observation,
                    raw_captures=tuple(captures),
                    coverage=(coverage,),
                )

            return self._unavailable(
                status=DocumentedReferenceAdapterStatus.UNAVAILABLE_GAP,
                reason="bounded fixed Coinbase stream ended without an admissible BTC-USD ticker",
                target=target,
                registry_mapping=mapping,
                cutoff=cutoff,
                mapping=documented_mapping,
                availability=self._unavailable_from_latest(captures, reason="bounded stream exhausted before ticker"),
                captures=tuple(captures),
                coverage_state="partial",
            )
        finally:
            if session is not None:
                try:
                    await session.close()
                except Exception:
                    # A close failure cannot turn a previous non-admission into
                    # success or erase raw evidence; it is intentionally quiet.
                    pass

    @staticmethod
    def _mapping_mismatch(
        target: ApprovedMarketTarget,
        mapping: DocumentedBtcReferenceMapping,
        documented_mapping: DocumentedReferenceSource,
    ) -> str | None:
        if target.reference_mapping_uid != mapping.mapping_uid or mapping.target_uid != target.target_uid:
            return "approved target and documented BTC mapping IDs do not form the configured one-to-one pair"
        if (
            mapping.primary_source_uid != COINBASE_EXCHANGE_BTC_USD_SOURCE_UID
            or mapping.primary_source_url != COINBASE_EXCHANGE_BTC_USD_MAPPING_URL
            or mapping.configured_endpoint_ref != COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF
            or mapping.instrument != COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID
        ):
            return "registry mapping does not exactly authorize the fixed Coinbase Exchange BTC-USD source configuration"
        expected = {
            "mapping_uid": mapping.mapping_uid,
            "market_uid": target.market_uid,
            "market_rule_source_uid": mapping.market_rule_source_uid,
            "settlement_source_uid": mapping.settlement_source_uid,
            "primary_source_uid": mapping.primary_source_uid,
            "settlement_rule_url": mapping.settlement_source_url,
            "primary_source_url": mapping.primary_source_url,
        }
        for field, expected_value in expected.items():
            if str(getattr(documented_mapping, field)) != str(expected_value):
                return f"raw-lineaged documented mapping {field} does not exactly match the approved registry mapping"
        if documented_mapping.asset_symbol != "BTC" or documented_mapping.quote_currency != "USD":
            return "raw-lineaged documented mapping does not name the exact BTC/USD settlement instrument"
        return None

    def _capture_frame(self, *, frame: object, received_at: datetime) -> RawCapture:
        if isinstance(frame, str):
            body = frame.encode("utf-8")
        elif isinstance(frame, bytes):
            body = frame
        else:
            # Preserve an unexpected injected transport value as evidence before
            # reporting it malformed. Normal sessions can provide only str/bytes.
            body = repr(frame).encode("utf-8", errors="backslashreplace")
        return self.raw_store.capture(
            body,
            platform=COINBASE_EXCHANGE_BTC_USD_PLATFORM,
            source=COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
            request={
                "transport": "websocket",
                "url": COINBASE_EXCHANGE_BTC_USD_TICKER_URL,
                "subscription": json.loads(COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION),
                "configured_endpoint_ref": COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF,
            },
            received_at=received_at,
            response_metadata={"frame_kind": type(frame).__name__},
        )

    @staticmethod
    def _decode_frame(frame: object) -> Mapping[str, Any]:
        if isinstance(frame, bytes):
            raw = frame.decode("utf-8")
        elif isinstance(frame, str):
            raw = frame
        else:
            raise TypeError("WebSocket frame must be str or bytes")
        decoded = json.loads(raw, parse_float=Decimal)
        if not isinstance(decoded, Mapping):
            raise ValueError("WebSocket frame must contain one JSON object")
        return decoded

    @staticmethod
    def _subscription_confirmed(payload: Mapping[str, Any]) -> bool:
        channels = payload.get("channels")
        if not isinstance(channels, list):
            return False
        actual: dict[str, tuple[str, ...]] = {}
        for channel in channels:
            if not isinstance(channel, Mapping):
                return False
            name = channel.get("name")
            product_ids = channel.get("product_ids")
            if not isinstance(name, str) or not isinstance(product_ids, list):
                return False
            if any(not isinstance(product_id, str) for product_id in product_ids):
                return False
            if name in actual:
                return False
            actual[name] = tuple(product_ids)
        return actual == {
            "ticker": (COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID,),
            "heartbeat": (COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID,),
        }

    @staticmethod
    def _validated_ticker(payload: Mapping[str, Any]) -> tuple[datetime, Decimal, int]:
        required = ("type", "product_id", "price", "time", "sequence")
        missing = tuple(field for field in required if field not in payload)
        if missing:
            raise ValueError(f"ticker payload missing required fields: {', '.join(missing)}")
        if payload["type"] != "ticker":
            raise ValueError("ticker payload type must exactly equal 'ticker'")
        if payload["product_id"] != COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID:
            raise ValueError("ticker product_id must exactly equal BTC-USD")
        sequence = payload["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ValueError("ticker sequence must be a non-negative integer")
        return _parse_timestamp(payload["time"]), _parse_price(payload["price"]), sequence

    @staticmethod
    def _reliability(*, assessed_at: datetime) -> SourceReliability:
        return SourceReliability(
            source_uid=COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
            source_class=SourceClass.PRIMARY_SOURCE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("0.95"),
            assessed_at=assessed_at,
            rationale="explicitly configured Coinbase Exchange BTC-USD primary-source ticker",
        )

    def _provenance(self, capture: RawCapture) -> Provenance:
        return Provenance(
            source_uid=COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
            raw_artifact_uid=f"{COINBASE_EXCHANGE_BTC_USD_PLATFORM}:raw/{capture.sha256}",
            parser_version=COINBASE_EXCHANGE_BTC_USD_PARSER_VERSION,
            content_hash=capture.sha256,
            retrieved_at=capture.received_at,
            # The Phase 15 provenance schema permits only HTTP(S) URLs.  The
            # actual immutable WebSocket endpoint remains in the raw receipt
            # and coverage record; this field names its documented mapping.
            source_url=COINBASE_EXCHANGE_BTC_USD_MAPPING_URL,
        )

    def _availability(self, capture: RawCapture, *, available: bool, reason: str | None = None) -> PrimarySourceAvailability:
        return PrimarySourceAvailability(
            availability_uid=f"{COINBASE_EXCHANGE_BTC_USD_PLATFORM}:availability/{capture.sha256}",
            primary_source_uid=COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
            status=PrimarySourceStatus.AVAILABLE if available else PrimarySourceStatus.UNAVAILABLE,
            checked_at=capture.received_at,
            first_seen_at=capture.received_at,
            retrieved_at=capture.received_at,
            ingested_at=capture.received_at,
            provenance=self._provenance(capture),
            reliability=self._reliability(assessed_at=capture.received_at),
            reason=reason,
        )

    def _unavailable_from_latest(
        self,
        captures: list[RawCapture],
        *,
        reason: str,
    ) -> PrimarySourceAvailability | None:
        if not captures:
            return None
        return self._availability(captures[-1], available=False, reason=reason)

    def _observation(
        self,
        *,
        target: ApprovedMarketTarget,
        mapping: DocumentedReferenceSource,
        availability: PrimarySourceAvailability,
        capture: RawCapture,
        event_time: datetime,
        price: Decimal,
        sequence: int,
    ) -> ReferencePriceObservation:
        return ReferencePriceObservation(
            reference_price_uid=(
                f"{COINBASE_EXCHANGE_BTC_USD_PLATFORM}:reference-price/"
                f"{target.market_uid.split(':', 1)[1]}/{sequence}/{capture.sha256}"
            ),
            market_uid=target.market_uid,
            price=price,
            event_time=event_time,
            first_seen_at=capture.received_at,
            retrieved_at=capture.received_at,
            ingested_at=capture.received_at,
            source_mapping=mapping,
            primary_source_availability=availability,
            provenance=self._provenance(capture),
            reliability=self._reliability(assessed_at=capture.received_at),
        )

    def _unavailable(
        self,
        *,
        status: DocumentedReferenceAdapterStatus,
        reason: str,
        target: ApprovedMarketTarget,
        registry_mapping: DocumentedBtcReferenceMapping,
        cutoff: datetime,
        mapping: DocumentedReferenceSource | None,
        availability: PrimarySourceAvailability | None,
        captures: tuple[RawCapture, ...],
        coverage_state: str,
        admission: ReferencePriceAdmission | None = None,
    ) -> DocumentedReferenceAdapterResult:
        result_admission = admission or self._non_admission(
            target=target,
            cutoff=cutoff,
            mapping=mapping,
            availability=availability,
            status=status,
            reason=reason,
        )
        coverage = self._coverage(
            target=target,
            mapping=registry_mapping,
            timestamp=cutoff,
            captures=captures,
            complete=False,
            coverage_state=coverage_state,
            status=status,
        )
        self._append_coverage(coverage)
        return DocumentedReferenceAdapterResult(
            status=status,
            reason=reason,
            admission=result_admission,
            mapping=mapping,
            primary_source_availability=availability,
            observation=None,
            raw_captures=captures,
            coverage=(coverage,),
        )

    @staticmethod
    def _non_admission(
        *,
        target: ApprovedMarketTarget,
        cutoff: datetime,
        mapping: DocumentedReferenceSource | None,
        availability: PrimarySourceAvailability | None,
        status: DocumentedReferenceAdapterStatus,
        reason: str,
    ) -> ReferencePriceAdmission:
        if status == DocumentedReferenceAdapterStatus.UNAVAILABLE_MAPPING_MISMATCH:
            return ReferencePriceAdmission(
                market_uid=target.market_uid,
                as_of=cutoff,
                status=ReferenceAdmissionStatus.REFERENCE_MAPPING_MISMATCH,
                reason=reason,
                source_mapping=mapping,
            )
        if mapping is not None:
            return admit_reference_price(
                market_uid=target.market_uid,
                as_of=cutoff,
                documented_sources=(mapping,),
                primary_source_availability=availability,
                observation=None,
            )
        return ReferencePriceAdmission(
            market_uid=target.market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.MISSING_DOCUMENTED_SETTLEMENT_SOURCE,
            reason=reason,
        )

    def _coverage(
        self,
        *,
        target: ApprovedMarketTarget,
        mapping: DocumentedBtcReferenceMapping,
        timestamp: datetime,
        captures: tuple[RawCapture, ...],
        complete: bool,
        coverage_state: str,
        status: DocumentedReferenceAdapterStatus | None = None,
    ) -> CoverageRecord:
        return CoverageRecord(
            platform=COINBASE_EXCHANGE_BTC_USD_PLATFORM,
            dataset=COINBASE_EXCHANGE_BTC_USD_DATASET,
            interval_start=timestamp,
            interval_end=timestamp,
            fetched_at=timestamp,
            record_count=1 if complete else 0,
            complete=complete,
            raw_sha256=tuple(capture.sha256 for capture in captures),
            filters={
                "target_uid": target.target_uid,
                "market_uid": target.market_uid,
                "mapping_uid": mapping.mapping_uid,
                "source_uid": COINBASE_EXCHANGE_BTC_USD_SOURCE_UID,
                "source_url": COINBASE_EXCHANGE_BTC_USD_TICKER_URL,
                "configured_endpoint_ref": COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF,
                "product_id": COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID,
                "subscription": json.loads(COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION),
                "coverage_state": coverage_state,
                "adapter_status": None if status is None else status.value,
            },
        )

    def _append_coverage(self, record: CoverageRecord) -> None:
        if self.coverage_ledger is not None:
            self.coverage_ledger.append(record)


__all__ = [
    "COINBASE_EXCHANGE_BTC_USD_DATASET",
    "COINBASE_EXCHANGE_BTC_USD_ENDPOINT_REF",
    "COINBASE_EXCHANGE_BTC_USD_MAPPING_URL",
    "COINBASE_EXCHANGE_BTC_USD_PLATFORM",
    "COINBASE_EXCHANGE_BTC_USD_PRODUCT_ID",
    "COINBASE_EXCHANGE_BTC_USD_SOURCE_UID",
    "COINBASE_EXCHANGE_BTC_USD_SUBSCRIPTION",
    "COINBASE_EXCHANGE_BTC_USD_TICKER_URL",
    "CoinbaseExchangeBtcUsdTickerAdapter",
    "CoinbaseExchangeWebSocket",
    "DocumentedReferenceAdapterResult",
    "DocumentedReferenceAdapterStatus",
    "SocketFactory",
]
