"""Configured, raw-first collection of Phase 15 market-context snapshots.

This connector deliberately has no venue defaults.  Callers must declare each
source endpoint, its identity/reliability policy, an expected (optional) raw
digest, and the target market.  The transport is injected, every response is
captured before decoding, and malformed, late, incomplete, or hash-conflicting
responses are returned as explicit non-admissions rather than guessed context.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import json
import ipaddress
import urllib.parse
from typing import Any, Callable, Mapping, Protocol, Sequence

from marketleak.ingestion.connectors.http import (
    HttpResponse,
    HttpTransport,
    Resolver,
    approved_https_destination,
    safe_request_metadata,
    safe_response_metadata,
    safe_url,
    system_resolver,
)
from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture
from marketleak.multimodal.context import MarketContext
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _source_url(value: str, *, field: str) -> str:
    normalized = str(value).strip()
    parsed = urllib.parse.urlsplit(normalized)
    if parsed.scheme.casefold() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{field} must be an explicit HTTPS URL without userinfo")
    if parsed.port not in (None, 443):
        raise ValueError(f"{field} must use the default HTTPS port")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        address = None
    if address is not None and not address.is_global:
        raise ValueError(f"{field} must not target a non-public address")
    return normalized


def _sha256(value: str, *, field: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return normalized


class CollectionStatus(str, Enum):
    """States that never silently turn a failed collection into usable data."""

    COLLECTED = "collected"
    UNAVAILABLE_MISSING_SOURCE = "unavailable_missing_source"
    UNAVAILABLE_AMBIGUOUS_SOURCE = "unavailable_ambiguous_source"
    UNAVAILABLE_HTTP = "unavailable_http"
    UNAVAILABLE_LATE = "unavailable_late"
    UNAVAILABLE_INCOMPLETE = "unavailable_incomplete"
    QUARANTINED_HASH_CONFLICT = "quarantined_hash_conflict"
    REJECTED_UNSAFE_SOURCE = "rejected_unsafe_source"


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Declared source identity/reliability; it is never inferred from a URL."""

    source_uid: str
    source_class: SourceClass
    reliability_tier: ReliabilityTier
    reliability_score: Decimal
    reliability_rationale: str

    def __post_init__(self) -> None:
        if not str(self.source_uid).strip() or ":" not in str(self.source_uid):
            raise ValueError("source_uid must be an explicit stable source UID")
        if not str(self.reliability_rationale).strip():
            raise ValueError("reliability_rationale is required")
        score = Decimal(self.reliability_score)
        if score < Decimal("0") or score > Decimal("1"):
            raise ValueError("reliability_score must be in [0, 1]")
        object.__setattr__(self, "reliability_score", score)

    def reliability(self, *, assessed_at: datetime) -> SourceReliability:
        return SourceReliability(
            source_uid=self.source_uid,
            source_class=self.source_class,
            tier=self.reliability_tier,
            score=self.reliability_score,
            assessed_at=_utc(assessed_at, field="assessed_at"),
            rationale=self.reliability_rationale,
        )


@dataclass(frozen=True, slots=True)
class RawEndpoint:
    """One bounded, explicitly configured GET endpoint and its source policy."""

    endpoint_url: str
    platform: str
    dataset: str
    policy: SourcePolicy
    parser_version: str
    params: Mapping[str, Any] | None = None
    expected_sha256: str | None = None
    timeout_seconds: float = 10.0
    max_response_bytes: int = 2_000_000

    def __post_init__(self) -> None:
        object.__setattr__(self, "endpoint_url", _source_url(self.endpoint_url, field="endpoint_url"))
        if not str(self.platform).strip() or not str(self.dataset).strip() or not str(self.parser_version).strip():
            raise ValueError("platform, dataset, and parser_version are required")
        if self.expected_sha256 is not None:
            object.__setattr__(self, "expected_sha256", _sha256(self.expected_sha256, field="expected_sha256"))
        timeout = float(self.timeout_seconds)
        if timeout <= 0 or timeout > 60:
            raise ValueError("timeout_seconds must be in (0, 60]")
        object.__setattr__(self, "timeout_seconds", timeout)
        maximum = int(self.max_response_bytes)
        if maximum < 1024 or maximum > 10_000_000:
            raise ValueError("max_response_bytes must be in [1024, 10000000]")
        object.__setattr__(self, "max_response_bytes", maximum)


@dataclass(frozen=True, slots=True)
class CapturedPayload:
    """A raw receipt plus decoded JSON, only after receipt persistence succeeds."""

    status: CollectionStatus
    reason: str
    capture: RawCapture | None
    payload: Mapping[str, Any] | None


Decoder = Callable[[bytes], Mapping[str, Any]]


def decode_json_object(body: bytes) -> Mapping[str, Any]:
    """Decode exactly one JSON object; arrays and scalars are incomplete source data."""

    value = json.loads(body.decode("utf-8"), parse_float=Decimal)
    if not isinstance(value, Mapping):
        raise ValueError("configured source response must be a JSON object")
    return value


def capture_configured_json(
    *,
    raw_store: RawArtifactStore,
    transport: HttpTransport,
    endpoint: RawEndpoint,
    received_at: datetime,
    decoder: Decoder = decode_json_object,
    resolver: Resolver = system_resolver,
) -> CapturedPayload:
    """Fetch once, persist raw material, then validate hash/status and decode.

    ``transport`` is required by the caller.  This function never instantiates a
    requests client, retries, searches, or discovers another endpoint.
    """

    timestamp = _utc(received_at, field="received_at")
    try:
        approved_origin, approved_addresses = approved_https_destination(endpoint.endpoint_url, resolver)
        response: HttpResponse = transport.request(
            "GET",
            endpoint.endpoint_url,
            params=dict(endpoint.params or {}),
            timeout=endpoint.timeout_seconds,
            max_response_bytes=endpoint.max_response_bytes,
            approved_addresses=approved_addresses,
        )
    except Exception as exc:
        return CapturedPayload(
            status=(
                CollectionStatus.REJECTED_UNSAFE_SOURCE
                if isinstance(exc, ValueError)
                else CollectionStatus.UNAVAILABLE_HTTP
            ),
            reason=f"configured endpoint could not be reached: {type(exc).__name__}",
            capture=None,
            payload=None,
        )

    try:
        final_origin, _ = approved_https_destination(
            response.url,
            lambda _host, _port: approved_addresses,
        )
    except ValueError:
        return CapturedPayload(
            CollectionStatus.REJECTED_UNSAFE_SOURCE,
            "configured endpoint returned an unsafe effective URL",
            None,
            None,
        )
    if final_origin != approved_origin or (
        not response.peer_address or response.peer_address not in approved_addresses
    ):
        return CapturedPayload(
            CollectionStatus.REJECTED_UNSAFE_SOURCE,
            "configured endpoint connection did not match its approved origin and address",
            None,
            None,
        )
    if response.oversized or len(response.body) > endpoint.max_response_bytes:
        return CapturedPayload(
            status=CollectionStatus.UNAVAILABLE_INCOMPLETE,
            reason=f"configured source response exceeded max_response_bytes={endpoint.max_response_bytes}",
            capture=None,
            payload=None,
        )

    capture = raw_store.capture(
        response.body,
        platform=endpoint.platform,
        source=endpoint.policy.source_uid,
        request=safe_request_metadata(
            method="GET", url=endpoint.endpoint_url, params=endpoint.params
        ),
        received_at=timestamp,
        response_metadata=safe_response_metadata(
            status_code=response.status_code, url=response.url, headers=response.headers
        ),
    )
    if endpoint.expected_sha256 is not None and capture.sha256 != endpoint.expected_sha256:
        return CapturedPayload(
            status=CollectionStatus.QUARANTINED_HASH_CONFLICT,
            reason="captured body hash conflicts with the configured expected SHA-256",
            capture=capture,
            payload=None,
        )
    if not 200 <= response.status_code < 300:
        return CapturedPayload(
            status=CollectionStatus.UNAVAILABLE_HTTP,
            reason=f"configured endpoint returned HTTP {response.status_code}",
            capture=capture,
            payload=None,
        )
    try:
        payload = decoder(response.body)
    except (UnicodeDecodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return CapturedPayload(
            status=CollectionStatus.UNAVAILABLE_INCOMPLETE,
            reason=f"configured source body is incomplete or cannot be decoded: {type(exc).__name__}",
            capture=capture,
            payload=None,
        )
    return CapturedPayload(CollectionStatus.COLLECTED, "raw receipt persisted before parsing", capture, payload)


def provenance_for_capture(*, capture: RawCapture, endpoint: RawEndpoint) -> Provenance:
    return Provenance(
        source_uid=endpoint.policy.source_uid,
        raw_artifact_uid=f"{capture.platform}:raw/{capture.sha256}",
        parser_version=endpoint.parser_version,
        content_hash=capture.sha256,
        retrieved_at=capture.received_at,
        source_url=safe_url(endpoint.endpoint_url),
    )


def coverage_for_capture(
    *,
    endpoint: RawEndpoint,
    received_at: datetime,
    capture: RawCapture | None,
    complete: bool,
) -> CoverageRecord:
    timestamp = _utc(received_at, field="received_at")
    return CoverageRecord(
        platform=endpoint.platform,
        dataset=endpoint.dataset,
        interval_start=timestamp,
        interval_end=timestamp,
        fetched_at=timestamp,
        record_count=1 if complete else 0,
        complete=complete,
        raw_sha256=() if capture is None else (capture.sha256,),
        filters={"endpoint_url": safe_url(endpoint.endpoint_url), "source_uid": endpoint.policy.source_uid},
    )


@dataclass(frozen=True, slots=True)
class MarketContextSourceConfig:
    market_uid: str
    endpoint: RawEndpoint

    def __post_init__(self) -> None:
        if not str(self.market_uid).strip() or ":" not in str(self.market_uid):
            raise ValueError("market_uid must be an explicit stable market UID")


@dataclass(frozen=True, slots=True)
class MarketContextCollectionResult:
    status: CollectionStatus
    reason: str
    context: MarketContext | None
    raw_captures: tuple[RawCapture, ...]
    coverage: tuple[CoverageRecord, ...]


class MarketContextCollector:
    """Collect one configured context snapshot for one market, without source fallback."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        transport: HttpTransport,
        sources: Sequence[MarketContextSourceConfig],
        coverage_ledger: CoverageLedger | None = None,
        decoder: Decoder = decode_json_object,
        resolver: Resolver = system_resolver,
    ) -> None:
        self.raw_store = raw_store
        self.transport = transport
        self.sources = tuple(sources)
        self.coverage_ledger = coverage_ledger
        self.decoder = decoder
        self.resolver = resolver

    def collect(self, *, market_uid: str, as_of: datetime, received_at: datetime) -> MarketContextCollectionResult:
        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        configured = tuple(source for source in self.sources if source.market_uid == market_uid)
        if not configured:
            return MarketContextCollectionResult(
                CollectionStatus.UNAVAILABLE_MISSING_SOURCE,
                "no configured market-context source exists for this market",
                None,
                (),
                (),
            )
        if len(configured) != 1:
            return MarketContextCollectionResult(
                CollectionStatus.UNAVAILABLE_AMBIGUOUS_SOURCE,
                "more than one market-context source is configured; explicit adjudication is required",
                None,
                (),
                (),
            )
        configured_source = configured[0]
        captured = capture_configured_json(
            raw_store=self.raw_store,
            transport=self.transport,
            endpoint=configured_source.endpoint,
            received_at=timestamp,
            decoder=self.decoder,
            resolver=self.resolver,
        )
        coverage = coverage_for_capture(
            endpoint=configured_source.endpoint,
            received_at=timestamp,
            capture=captured.capture,
            complete=False,
        )
        if captured.status != CollectionStatus.COLLECTED or captured.capture is None or captured.payload is None:
            self._append_coverage(coverage)
            return MarketContextCollectionResult(
                captured.status,
                captured.reason,
                None,
                () if captured.capture is None else (captured.capture,),
                (coverage,),
            )
        try:
            context = self._normalize(configured_source, captured.capture, captured.payload)
        except (TypeError, ValueError, KeyError) as exc:
            self._append_coverage(coverage)
            return MarketContextCollectionResult(
                CollectionStatus.UNAVAILABLE_INCOMPLETE,
                f"configured context payload is incomplete or invalid: {type(exc).__name__}",
                None,
                (captured.capture,),
                (coverage,),
            )
        if any(
            value > cutoff
            for value in (context.event_time, context.first_seen_at, context.retrieved_at, context.ingested_at)
        ):
            self._append_coverage(coverage)
            return MarketContextCollectionResult(
                CollectionStatus.UNAVAILABLE_LATE,
                "configured market context was not available at the requested as_of cutoff",
                None,
                (captured.capture,),
                (coverage,),
            )
        complete_coverage = coverage_for_capture(
            endpoint=configured_source.endpoint,
            received_at=timestamp,
            capture=captured.capture,
            complete=True,
        )
        self._append_coverage(complete_coverage)
        return MarketContextCollectionResult(
            CollectionStatus.COLLECTED,
            "configured market context was raw-lineaged and point-in-time admissible",
            context,
            (captured.capture,),
            (complete_coverage,),
        )

    def _append_coverage(self, record: CoverageRecord) -> None:
        if self.coverage_ledger is not None:
            self.coverage_ledger.append(record)

    @staticmethod
    def _normalize(
        config: MarketContextSourceConfig,
        capture: RawCapture,
        payload: Mapping[str, Any],
    ) -> MarketContext:
        required = ("question", "category", "outcomes", "resolution_schedule", "event_time", "first_seen_at")
        missing = tuple(field for field in required if field not in payload)
        if missing:
            raise ValueError(f"missing context fields: {', '.join(missing)}")
        if "market_uid" in payload and str(payload["market_uid"]) != config.market_uid:
            raise ValueError("source payload market_uid conflicts with configured market_uid")
        document: dict[str, Any] = {
            "context_uid": payload.get(
                "context_uid", f"{config.endpoint.platform}:context/{config.market_uid.split(':', 1)[1]}/{capture.sha256}"
            ),
            "market_uid": config.market_uid,
            "question": payload["question"],
            "category": payload["category"],
            "outcomes": payload["outcomes"],
            "sibling_markets": payload.get("sibling_markets", ()),
            "scheduled_events": payload.get("scheduled_events", ()),
            "resolution_schedule": payload["resolution_schedule"],
            "event_time": payload["event_time"],
            "first_seen_at": payload["first_seen_at"],
            "retrieved_at": capture.received_at,
            "ingested_at": capture.received_at,
            "provenance": provenance_for_capture(capture=capture, endpoint=config.endpoint).model_dump(mode="json"),
            "reliability": config.endpoint.policy.reliability(assessed_at=capture.received_at).model_dump(mode="json"),
        }
        return MarketContext.model_validate_json(json.dumps(document, default=str))


__all__ = [
    "CapturedPayload",
    "CollectionStatus",
    "Decoder",
    "MarketContextCollectionResult",
    "MarketContextCollector",
    "MarketContextSourceConfig",
    "RawEndpoint",
    "SourcePolicy",
    "capture_configured_json",
    "coverage_for_capture",
    "decode_json_object",
    "provenance_for_capture",
]
