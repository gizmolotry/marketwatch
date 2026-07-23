"""Raw-first Polymarket Gamma market metadata clock collection.

This bounded connector calls the documented official Gamma endpoint
``GET https://gamma-api.polymarket.com/markets/{id}`` exactly once for an
explicit numeric market ID.  The Gamma market references document timestamps
such as ``createdAt`` and ``updatedAt``, but do not document a market-public
listing/publication timestamp.  An event's nullable ``published_at`` is also
not documented with that semantic.  Accordingly this connector *always*
returns an unmapped public-visibility/publication clock and never promotes any
of those source fields.  It does not lift historical abstention.

Official references consulted: Polymarket Gamma API ``Get market by ID`` and
the related ``Get market by slug``, ``List markets``, ``Get event by ID``, and
rate-limit documentation at https://docs.polymarket.com/api-reference/markets/get-market-by-id.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
import hashlib
import json
import re
from typing import Any, Mapping

from marketleak.ingestion.connectors.http import HttpResponse, HttpTransport
from marketleak.ingestion.normalize import canonical_json_bytes, utc_datetime
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_GAMMA_MARKET_SOURCE_UID = "polymarket:source/gamma-market-clock"
PARSER_VERSION = "polymarket-gamma-market-clock-v1.0.0"

_NUMERIC_ID = re.compile(r"^(?:0|[1-9][0-9]*)$")
_CONDITION_ID = re.compile(r"^0x[0-9a-fA-F]{64}$")
_SAFE_RESPONSE_HEADERS = frozenset({
    "age", "cache-control", "content-length", "content-type", "date", "etag",
    "expires", "last-modified", "request-id", "retry-after", "traceparent", "x-request-id",
})
_MARKET_TIMESTAMP_FIELDS = (
    "createdAt", "startDate", "updatedAt", "readyTimestamp", "fundedTimestamp", "acceptingOrdersTimestamp",
)


class PolymarketMarketMetadataStatus(str, Enum):
    """Collection outcomes that keep unavailable clocks unavailable."""

    COLLECTED_PUBLICATION_UNMAPPED = "collected_publication_unmapped"
    UNAVAILABLE_HTTP = "unavailable_http"
    UNAVAILABLE_MALFORMED_BODY = "unavailable_malformed_body"
    REJECTED_MISMATCH = "rejected_mismatch"
    REJECTED_EFFECTIVE_URL = "rejected_effective_url"
    REJECTED_SOURCE_TIMESTAMP = "rejected_source_timestamp"
    REJECTED_AMBIGUOUS_EVENT = "rejected_ambiguous_event"
    REJECTED_NONMONOTONE_CLOCK = "rejected_nonmonotone_clock"


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _numeric_id(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value or not _NUMERIC_ID.fullmatch(value):
        raise ValueError(f"{field} must be a nonempty numeric Gamma market ID string")
    return value


def _nonempty_text(value: Any, *, field: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _condition_id(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _CONDITION_ID.fullmatch(value):
        raise ValueError(f"{field} must be a 0x-prefixed 64-hex-character JSON string")
    return value.lower()


def _safe_response_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    """Persist only a deterministic response-header allowlist in this connector."""

    safe: dict[str, str] = {}
    for name, value in sorted(headers.items(), key=lambda item: str(item[0]).lower()):
        normalized = str(name).strip().lower()
        if normalized in _SAFE_RESPONSE_HEADERS:
            safe[normalized] = str(value)
    return safe


def _query_hash(*, method: str, url: str, params: Mapping[str, Any]) -> str:
    """SHA-256 over the canonical method, endpoint, and request parameters."""

    return hashlib.sha256(canonical_json_bytes({"method": method.upper(), "params": dict(params), "url": url})).hexdigest()


@dataclass(frozen=True, slots=True)
class PolymarketGammaMarketTarget:
    """An explicit Gamma market ID and independently expected condition ID."""

    gamma_market_id: str
    condition_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "gamma_market_id", _numeric_id(self.gamma_market_id, field="gamma_market_id"))
        object.__setattr__(self, "condition_id", _condition_id(self.condition_id, field="condition_id"))


@dataclass(frozen=True, slots=True)
class SourceTimestamp:
    """An exact nullable source field plus its validated UTC interpretation."""

    field: str
    raw_value: str | None
    parsed_at: datetime | None
    source_path: str


@dataclass(frozen=True, slots=True)
class PolymarketGammaMarketClockRecord:
    """Immutable raw-lineaged Gamma timing metadata, not a publication claim."""

    market_id: str
    condition_id: str
    source_uid: str
    endpoint_url: str
    request_params: Mapping[str, Any]
    query_hash: str
    parser_version: str
    raw_sha256: str
    raw_artifact_uid: str
    raw_byte_length: int
    receipt_path: str
    receipt_sha256: str
    created_at: SourceTimestamp
    start_date: SourceTimestamp
    updated_at: SourceTimestamp
    ready_timestamp: SourceTimestamp
    funded_timestamp: SourceTimestamp
    accepting_orders_timestamp: SourceTimestamp
    event_published_at: SourceTimestamp
    public_visibility_at: None
    public_visibility_authority: str
    public_visibility_status: str
    public_visibility_reason: str
    first_observed_at: datetime
    retrieved_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class PolymarketMarketMetadataResult:
    status: PolymarketMarketMetadataStatus
    reason: str
    record: PolymarketGammaMarketClockRecord | None
    raw_capture: RawCapture | None


class PolymarketGammaMarketMetadataCollector:
    """Collect one Gamma market response through an injected, bounded transport."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        transport: HttpTransport,
        timeout_seconds: float = 10.0,
        parser_version: str = PARSER_VERSION,
    ) -> None:
        if not str(parser_version).strip():
            raise ValueError("parser_version is required")
        timeout = float(timeout_seconds)
        if not 0 < timeout <= 60:
            raise ValueError("timeout_seconds must be in (0, 60]")
        self.raw_store = raw_store
        self.transport = transport
        self.timeout_seconds = timeout
        self.parser_version = str(parser_version).strip()

    def collect(
        self,
        *,
        target: PolymarketGammaMarketTarget,
        retrieved_at: datetime,
        ingested_at: datetime,
    ) -> PolymarketMarketMetadataResult:
        try:
            retrieved = _aware_utc(retrieved_at, field="retrieved_at")
            ingested = _aware_utc(ingested_at, field="ingested_at")
        except ValueError as exc:
            return self._failure(PolymarketMarketMetadataStatus.REJECTED_NONMONOTONE_CLOCK, str(exc))
        if retrieved > ingested:
            return self._failure(
                PolymarketMarketMetadataStatus.REJECTED_NONMONOTONE_CLOCK,
                "retrieved_at and ingested_at must be monotone",
            )

        url = f"{POLYMARKET_GAMMA_API}/markets/{target.gamma_market_id}"
        params: dict[str, Any] = {}
        try:
            response: HttpResponse = self.transport.request("GET", url, params=params, timeout=self.timeout_seconds)
        except Exception as exc:
            return self._failure(
                PolymarketMarketMetadataStatus.UNAVAILABLE_HTTP,
                f"Gamma market endpoint could not be reached: {type(exc).__name__}",
            )

        # This is intentionally before status validation and JSON parsing.
        effective_url_matches_request = response.url == url
        capture = self.raw_store.capture(
            response.body,
            platform="polymarket",
            source=POLYMARKET_GAMMA_MARKET_SOURCE_UID,
            request={"method": "GET", "url": url, "params": params},
            received_at=retrieved,
            response_metadata={
                "status_code": response.status_code,
                "url": url if effective_url_matches_request else "<redacted-nonmatching-url>",
                "effective_url_matches_request": effective_url_matches_request,
                "headers": _safe_response_headers(response.headers),
            },
        )
        if not effective_url_matches_request:
            return self._failure(
                PolymarketMarketMetadataStatus.REJECTED_EFFECTIVE_URL,
                "Gamma response effective URL does not exactly match the configured official endpoint",
                capture,
            )
        if not 200 <= response.status_code < 300:
            return self._failure(
                PolymarketMarketMetadataStatus.UNAVAILABLE_HTTP,
                f"Gamma market endpoint returned HTTP {response.status_code}",
                capture,
            )
        try:
            payload = json.loads(response.body.decode("utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError("Gamma market response must be a JSON object")
            record = self._record(
                target=target,
                payload=payload,
                capture=capture,
                endpoint_url=url,
                params=params,
                retrieved_at=retrieved,
                ingested_at=ingested,
            )
        except _Mismatch as exc:
            return self._failure(PolymarketMarketMetadataStatus.REJECTED_MISMATCH, str(exc), capture)
        except _AmbiguousEvent as exc:
            return self._failure(PolymarketMarketMetadataStatus.REJECTED_AMBIGUOUS_EVENT, str(exc), capture)
        except _SourceTimestampError as exc:
            return self._failure(PolymarketMarketMetadataStatus.REJECTED_SOURCE_TIMESTAMP, str(exc), capture)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            return self._failure(
                PolymarketMarketMetadataStatus.UNAVAILABLE_MALFORMED_BODY,
                f"Gamma market response is malformed: {type(exc).__name__}",
                capture,
            )
        return PolymarketMarketMetadataResult(
            PolymarketMarketMetadataStatus.COLLECTED_PUBLICATION_UNMAPPED,
            "official Gamma metadata captured; public market publication/visibility time is unmapped",
            record,
            capture,
        )

    def _record(
        self,
        *,
        target: PolymarketGammaMarketTarget,
        payload: Mapping[str, Any],
        capture: RawCapture,
        endpoint_url: str,
        params: Mapping[str, Any],
        retrieved_at: datetime,
        ingested_at: datetime,
    ) -> PolymarketGammaMarketClockRecord:
        try:
            market_id = _numeric_id(payload.get("id"), field="Gamma id")
            condition_id = _condition_id(payload.get("conditionId"), field="Gamma conditionId")
        except ValueError as exc:
            raise _Mismatch("Gamma response omits a valid target identity") from exc
        if market_id != target.gamma_market_id or condition_id != target.condition_id:
            raise _Mismatch("Gamma market ID or condition ID does not match the explicit target")

        timestamps = {
            field: _source_timestamp(
                payload.get(field),
                field=field,
                source_path=f"$.{field}",
                latest=None if field == "startDate" else retrieved_at,
            )
            for field in _MARKET_TIMESTAMP_FIELDS
        }
        event_published = _event_published_timestamp(payload, latest=retrieved_at)
        receipt_bytes = capture.receipt_path.read_bytes()
        return PolymarketGammaMarketClockRecord(
            market_id=market_id,
            condition_id=condition_id,
            source_uid=POLYMARKET_GAMMA_MARKET_SOURCE_UID,
            endpoint_url=endpoint_url,
            request_params=dict(params),
            query_hash=_query_hash(method="GET", url=endpoint_url, params=params),
            parser_version=self.parser_version,
            raw_sha256=capture.sha256,
            raw_artifact_uid=f"{capture.platform}:raw/{capture.sha256}",
            raw_byte_length=capture.byte_length,
            receipt_path=str(capture.receipt_path),
            receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
            created_at=timestamps["createdAt"],
            start_date=timestamps["startDate"],
            updated_at=timestamps["updatedAt"],
            ready_timestamp=timestamps["readyTimestamp"],
            funded_timestamp=timestamps["fundedTimestamp"],
            accepting_orders_timestamp=timestamps["acceptingOrdersTimestamp"],
            event_published_at=event_published,
            public_visibility_at=None,
            public_visibility_authority="unmapped",
            public_visibility_status="unmapped",
            public_visibility_reason=(
                "Gamma documents no market public listing/publication timestamp; "
                "createdAt, startDate, lifecycle times, and event published_at are not promoted"
            ),
            first_observed_at=capture.received_at,
            retrieved_at=capture.received_at,
            ingested_at=ingested_at,
        )

    @staticmethod
    def _failure(
        status: PolymarketMarketMetadataStatus,
        reason: str,
        capture: RawCapture | None = None,
    ) -> PolymarketMarketMetadataResult:
        return PolymarketMarketMetadataResult(status, reason, None, capture)


class _Mismatch(ValueError):
    pass


class _AmbiguousEvent(ValueError):
    pass


class _SourceTimestampError(ValueError):
    pass


def _source_timestamp(
    value: Any,
    *,
    field: str,
    source_path: str,
    latest: datetime | None,
) -> SourceTimestamp:
    if value is None:
        return SourceTimestamp(field=field, raw_value=None, parsed_at=None, source_path=source_path)
    if not isinstance(value, str) or not value.strip():
        raise _SourceTimestampError(f"{source_path} must be a nonempty JSON string or null")
    try:
        parsed = utc_datetime(value, field)
    except (OverflowError, OSError, ValueError) as exc:
        raise _SourceTimestampError(f"{source_path} is not a valid unambiguous timestamp") from exc
    if latest is not None and parsed > latest:
        raise _SourceTimestampError(f"{source_path} is after the retrieval clock")
    return SourceTimestamp(field=field, raw_value=value, parsed_at=parsed, source_path=source_path)


def _event_published_timestamp(payload: Mapping[str, Any], *, latest: datetime) -> SourceTimestamp:
    events = payload.get("events")
    if events is None:
        return SourceTimestamp("published_at", None, None, "$.events[*].published_at")
    if not isinstance(events, list):
        raise _AmbiguousEvent("Gamma events must be an array when present")
    if len(events) > 1:
        raise _AmbiguousEvent("Gamma response contains multiple events; event published_at is ambiguous")
    if not events:
        return SourceTimestamp("published_at", None, None, "$.events[*].published_at")
    event = events[0]
    if not isinstance(event, Mapping):
        raise _AmbiguousEvent("Gamma event entry must be an object")
    return _source_timestamp(
        event.get("published_at"),
        field="event published_at",
        source_path="$.events[0].published_at",
        latest=latest,
    )


__all__ = [
    "PARSER_VERSION",
    "POLYMARKET_GAMMA_API",
    "POLYMARKET_GAMMA_MARKET_SOURCE_UID",
    "PolymarketGammaMarketClockRecord",
    "PolymarketGammaMarketMetadataCollector",
    "PolymarketGammaMarketMetadataStatus",
    "PolymarketGammaMarketTarget",
    "PolymarketMarketMetadataResult",
    "SourceTimestamp",
]
