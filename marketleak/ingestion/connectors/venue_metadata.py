"""Raw-first, bounded official venue metadata collection for Phase 15.

This module captures immutable venue facts only.  It does not create market
context, discover siblings, map a settlement source to a reference-price feed,
or make an inference about a market.  Callers must provide an explicit target
whose immutable outcome/rule/source expectations are checked on every run.

Official endpoint contracts consulted before implementation:
* Polymarket Gamma ``GET /markets/{id}`` and CLOB
  ``GET /clob-markets/{condition_id}``;
* Kalshi ``GET /markets/{ticker}`` and
  ``GET /events/{event_ticker}/metadata``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
import json
import re
from typing import Any, Mapping, Sequence

from marketleak.ingestion.connectors.http import HttpTransport
from marketleak.ingestion.connectors.market_context import (
    CapturedPayload,
    CollectionStatus,
    Decoder,
    RawEndpoint,
    SourcePolicy,
    capture_configured_json,
    coverage_for_capture,
    decode_json_object,
)
from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.normalize import qualified_id, require_text, stable_uid, utc_datetime
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture
from marketleak.multimodal.schemas import ReliabilityTier, SourceClass


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"

POLYMARKET_GAMMA_SOURCE_UID = "polymarket:source/gamma-market"
POLYMARKET_CLOB_SOURCE_UID = "polymarket:source/clob-market-info"
KALSHI_MARKET_SOURCE_UID = "kalshi:source/market-metadata"
KALSHI_EVENT_METADATA_SOURCE_UID = "kalshi:source/event-metadata"
PARSER_VERSION = "phase15-venue-metadata-v1.0.0"

_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._:-]+$")


class VenueMetadataStatus(str, Enum):
    """Explicit collection outcomes; no failure is promoted to usable metadata."""

    COLLECTED = "collected"
    UNAVAILABLE_HTTP = "unavailable_http"
    UNAVAILABLE_INCOMPLETE = "unavailable_incomplete"
    UNAVAILABLE_LATE = "unavailable_late"
    REJECTED_MISMATCH = "rejected_mismatch"
    QUARANTINED_HASH_CONFLICT = "quarantined_hash_conflict"


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _segment(value: str, *, field: str) -> str:
    normalized = require_text(value, field)
    if len(normalized) > 512 or not _SEGMENT_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be one bounded path segment")
    return normalized


def _http_url(value: str, *, field: str) -> str:
    normalized = require_text(value, field)
    if not normalized.startswith(("https://", "http://")):
        raise ValueError(f"{field} must be an http(s) URL")
    return normalized


def _optional_time(value: Any, *, field: str) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    return utc_datetime(value, field)


@dataclass(frozen=True, slots=True)
class ExpectedOutcome:
    """An explicit venue outcome label and its immutable token/side identifier."""

    label: str
    venue_outcome_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "label", require_text(self.label, "label"))
        object.__setattr__(self, "venue_outcome_id", _segment(self.venue_outcome_id, field="venue_outcome_id"))


@dataclass(frozen=True, slots=True)
class SettlementSource:
    """A source as published by Kalshi, preserved without selecting a primary."""

    name: str
    url: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", require_text(self.name, "settlement source name"))
        object.__setattr__(self, "url", _http_url(self.url, field="settlement source url"))


@dataclass(frozen=True, slots=True)
class PolymarketMetadataTarget:
    """A pre-registered Gamma market with its expected CLOB token mapping."""

    gamma_market_id: str
    condition_id: str
    outcomes: tuple[ExpectedOutcome, ...]
    resolution_source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "gamma_market_id", _segment(self.gamma_market_id, field="gamma_market_id"))
        object.__setattr__(self, "condition_id", _segment(self.condition_id, field="condition_id"))
        object.__setattr__(self, "resolution_source", require_text(self.resolution_source, "resolution_source"))
        _validate_expected_outcomes(self.outcomes)


@dataclass(frozen=True, slots=True)
class KalshiMetadataTarget:
    """A pre-registered Kalshi ticker, event, rules, outcomes, and sources."""

    ticker: str
    event_ticker: str
    outcomes: tuple[ExpectedOutcome, ...]
    rules_primary: str
    settlement_sources: tuple[SettlementSource, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _segment(self.ticker, field="ticker"))
        object.__setattr__(self, "event_ticker", _segment(self.event_ticker, field="event_ticker"))
        object.__setattr__(self, "rules_primary", require_text(self.rules_primary, "rules_primary"))
        _validate_expected_outcomes(self.outcomes)
        if {item.venue_outcome_id.casefold() for item in self.outcomes} != {"yes", "no"}:
            raise ValueError("Kalshi binary targets must explicitly map Yes and No outcomes")
        if not self.settlement_sources:
            raise ValueError("settlement_sources must be explicitly configured")
        if len(set(self.settlement_sources)) != len(self.settlement_sources):
            raise ValueError("settlement_sources must be unique")


def _validate_expected_outcomes(outcomes: tuple[ExpectedOutcome, ...]) -> None:
    if not outcomes:
        raise ValueError("outcomes must be explicitly configured")
    labels = tuple(item.label.casefold() for item in outcomes)
    identifiers = tuple(item.venue_outcome_id for item in outcomes)
    if len(set(labels)) != len(labels) or len(set(identifiers)) != len(identifiers):
        raise ValueError("outcomes must have unique labels and venue identifiers")


@dataclass(frozen=True, slots=True)
class VenueOutcome:
    outcome_uid: str
    label: str
    venue_outcome_id: str


@dataclass(frozen=True, slots=True)
class VenueTiming:
    opens_at: datetime | None
    closes_at: datetime
    expected_resolution_at: datetime | None
    settled_at: datetime | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "opens_at", None if self.opens_at is None else _utc(self.opens_at, field="opens_at"))
        object.__setattr__(self, "closes_at", _utc(self.closes_at, field="closes_at"))
        object.__setattr__(self, "expected_resolution_at", None if self.expected_resolution_at is None else _utc(self.expected_resolution_at, field="expected_resolution_at"))
        object.__setattr__(self, "settled_at", None if self.settled_at is None else _utc(self.settled_at, field="settled_at"))
        if self.opens_at is not None and self.closes_at <= self.opens_at:
            raise ValueError("closes_at must be after opens_at")
        if (
            self.opens_at is not None
            and self.expected_resolution_at is not None
            and self.expected_resolution_at < self.opens_at
        ):
            raise ValueError("expected_resolution_at cannot precede opens_at")


@dataclass(frozen=True, slots=True)
class VenueMarketMetadata:
    """Neutral immutable venue metadata with complete raw lineage."""

    metadata_uid: str
    market_uid: str
    platform: str
    venue_market_id: str
    venue_event_id: str | None
    question: str
    status: str
    rules: tuple[str, ...]
    timing: VenueTiming
    outcomes: tuple[VenueOutcome, ...]
    settlement_sources: tuple[SettlementSource, ...]
    event_time: datetime
    first_seen_at: datetime
    retrieved_at: datetime
    ingested_at: datetime
    raw_artifact_uids: tuple[str, ...]
    source_uids: tuple[str, ...]

    def __post_init__(self) -> None:
        for field in ("metadata_uid", "market_uid", "platform", "venue_market_id", "question", "status"):
            object.__setattr__(self, field, require_text(getattr(self, field), field))
        if self.venue_event_id is not None:
            object.__setattr__(self, "venue_event_id", require_text(self.venue_event_id, "venue_event_id"))
        if not self.rules or any(not str(item).strip() for item in self.rules):
            raise ValueError("rules must contain captured non-empty values")
        if not self.outcomes or len({item.outcome_uid for item in self.outcomes}) != len(self.outcomes):
            raise ValueError("outcomes must be present and unique")
        if not self.raw_artifact_uids or not self.source_uids:
            raise ValueError("metadata must retain raw artifacts and source identities")
        for field in ("event_time", "first_seen_at", "retrieved_at", "ingested_at"):
            object.__setattr__(self, field, _utc(getattr(self, field), field=field))
        if not (self.event_time <= self.first_seen_at <= self.retrieved_at <= self.ingested_at):
            raise ValueError("metadata timing must be causal")


@dataclass(frozen=True, slots=True)
class VenueMetadataCollectionResult:
    status: VenueMetadataStatus
    reason: str
    metadata: VenueMarketMetadata | None
    raw_captures: tuple[RawCapture, ...]
    coverage: tuple[CoverageRecord, ...]


class VenueMetadataCollector:
    """Collect only pre-registered venue metadata using an injected transport."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        transport: HttpTransport,
        coverage_ledger: CoverageLedger | None = None,
        decoder: Decoder = decode_json_object,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be a RawArtifactStore")
        self.raw_store = raw_store
        self.transport = transport
        self.coverage_ledger = coverage_ledger
        self.decoder = decoder

    def collect_polymarket(
        self,
        *,
        target: PolymarketMetadataTarget,
        as_of: datetime,
        received_at: datetime,
    ) -> VenueMetadataCollectionResult:
        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        gamma_endpoint = self._endpoint(
            platform="polymarket",
            dataset="gamma_market_metadata",
            source_uid=POLYMARKET_GAMMA_SOURCE_UID,
            url=f"{POLYMARKET_GAMMA_API}/markets/{target.gamma_market_id}",
        )
        gamma = self._capture(gamma_endpoint, timestamp)
        gamma_coverage = self._coverage(gamma_endpoint, gamma, timestamp, complete=False)
        if not _captured(gamma):
            return self._failure(gamma, (gamma_coverage,))
        assert gamma.capture is not None and gamma.payload is not None
        try:
            gamma_values = self._polymarket_gamma_values(target, gamma.payload, gamma.capture)
        except (TypeError, ValueError, KeyError) as exc:
            return self._rejected(
                VenueMetadataStatus.REJECTED_MISMATCH,
                f"Gamma metadata conflicts with the explicit target: {type(exc).__name__}",
                (gamma.capture,),
                (gamma_coverage,),
            )

        clob_endpoint = self._endpoint(
            platform="polymarket",
            dataset="clob_market_token_mapping",
            source_uid=POLYMARKET_CLOB_SOURCE_UID,
            url=f"{POLYMARKET_CLOB_API}/clob-markets/{target.condition_id}",
        )
        clob = self._capture(clob_endpoint, timestamp)
        clob_coverage = self._coverage(clob_endpoint, clob, timestamp, complete=False)
        if not _captured(clob):
            return self._failure(clob, (gamma_coverage, clob_coverage), previous=(gamma.capture,))
        assert clob.capture is not None and clob.payload is not None
        try:
            outcomes = self._polymarket_outcomes(target, gamma_values["outcomes"], clob.payload)
            metadata = self._build_polymarket_metadata(target, gamma_values, outcomes, gamma.capture, clob.capture)
        except (TypeError, ValueError, KeyError) as exc:
            return self._rejected(
                VenueMetadataStatus.REJECTED_MISMATCH,
                f"Gamma/CLOB metadata is missing, ambiguous, or changed: {type(exc).__name__}",
                (gamma.capture, clob.capture),
                (gamma_coverage, clob_coverage),
            )
        if not _admissible(metadata, cutoff):
            return self._rejected(
                VenueMetadataStatus.UNAVAILABLE_LATE,
                "venue metadata was not available at the requested as_of cutoff",
                (gamma.capture, clob.capture),
                (gamma_coverage, clob_coverage),
            )
        complete = (
            self._coverage(gamma_endpoint, gamma, timestamp, complete=True),
            self._coverage(clob_endpoint, clob, timestamp, complete=True),
        )
        return self._collected(metadata, (gamma.capture, clob.capture), complete)

    def collect_kalshi(
        self,
        *,
        target: KalshiMetadataTarget,
        as_of: datetime,
        received_at: datetime,
    ) -> VenueMetadataCollectionResult:
        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        market_endpoint = self._endpoint(
            platform="kalshi",
            dataset="market_metadata",
            source_uid=KALSHI_MARKET_SOURCE_UID,
            url=f"{KALSHI_API}/markets/{target.ticker}",
        )
        market = self._capture(market_endpoint, timestamp)
        market_coverage = self._coverage(market_endpoint, market, timestamp, complete=False)
        if not _captured(market):
            return self._failure(market, (market_coverage,))
        assert market.capture is not None and market.payload is not None
        try:
            market_values = self._kalshi_market_values(target, market.payload, market.capture)
        except (TypeError, ValueError, KeyError) as exc:
            return self._rejected(
                VenueMetadataStatus.REJECTED_MISMATCH,
                f"Kalshi market metadata conflicts with the explicit target: {type(exc).__name__}",
                (market.capture,),
                (market_coverage,),
            )

        event_endpoint = self._endpoint(
            platform="kalshi",
            dataset="event_metadata_settlement_sources",
            source_uid=KALSHI_EVENT_METADATA_SOURCE_UID,
            url=f"{KALSHI_API}/events/{target.event_ticker}/metadata",
        )
        event = self._capture(event_endpoint, timestamp)
        event_coverage = self._coverage(event_endpoint, event, timestamp, complete=False)
        if not _captured(event):
            return self._failure(event, (market_coverage, event_coverage), previous=(market.capture,))
        assert event.capture is not None and event.payload is not None
        try:
            sources = self._kalshi_settlement_sources(target, event.payload)
            metadata = self._build_kalshi_metadata(target, market_values, sources, market.capture, event.capture)
        except (TypeError, ValueError, KeyError) as exc:
            return self._rejected(
                VenueMetadataStatus.REJECTED_MISMATCH,
                f"Kalshi event metadata is missing, ambiguous, or changed: {type(exc).__name__}",
                (market.capture, event.capture),
                (market_coverage, event_coverage),
            )
        if not _admissible(metadata, cutoff):
            return self._rejected(
                VenueMetadataStatus.UNAVAILABLE_LATE,
                "venue metadata was not available at the requested as_of cutoff",
                (market.capture, event.capture),
                (market_coverage, event_coverage),
            )
        complete = (
            self._coverage(market_endpoint, market, timestamp, complete=True),
            self._coverage(event_endpoint, event, timestamp, complete=True),
        )
        return self._collected(metadata, (market.capture, event.capture), complete)

    def _capture(self, endpoint: RawEndpoint, received_at: datetime) -> CapturedPayload:
        return capture_configured_json(
            raw_store=self.raw_store,
            transport=self.transport,
            endpoint=endpoint,
            received_at=received_at,
            decoder=self.decoder,
        )

    @staticmethod
    def _endpoint(*, platform: str, dataset: str, source_uid: str, url: str) -> RawEndpoint:
        return RawEndpoint(
            endpoint_url=url,
            platform=platform,
            dataset=dataset,
            policy=SourcePolicy(
                source_uid=source_uid,
                source_class=SourceClass.OFFICIAL_VENUE,
                reliability_tier=ReliabilityTier.HIGH,
                reliability_score="0.95",
                reliability_rationale="documented official venue metadata endpoint",
            ),
            parser_version=PARSER_VERSION,
            timeout_seconds=10.0,
        )

    def _coverage(
        self,
        endpoint: RawEndpoint,
        captured: CapturedPayload,
        received_at: datetime,
        *,
        complete: bool,
    ) -> CoverageRecord:
        return coverage_for_capture(
            endpoint=endpoint,
            received_at=received_at,
            capture=captured.capture,
            complete=complete,
        )

    def _collected(
        self,
        metadata: VenueMarketMetadata,
        captures: tuple[RawCapture, ...],
        coverage: tuple[CoverageRecord, ...],
    ) -> VenueMetadataCollectionResult:
        self._append(coverage)
        return VenueMetadataCollectionResult(
            VenueMetadataStatus.COLLECTED,
            "official venue metadata was raw-lineaged and point-in-time admissible",
            metadata,
            captures,
            coverage,
        )

    def _failure(
        self,
        captured: CapturedPayload,
        coverage: tuple[CoverageRecord, ...],
        *,
        previous: tuple[RawCapture, ...] = (),
    ) -> VenueMetadataCollectionResult:
        self._append(coverage)
        return VenueMetadataCollectionResult(
            _captured_status(captured.status),
            captured.reason,
            None,
            (*previous, *((captured.capture,) if captured.capture is not None else ())),
            coverage,
        )

    def _rejected(
        self,
        status: VenueMetadataStatus,
        reason: str,
        captures: tuple[RawCapture, ...],
        coverage: tuple[CoverageRecord, ...],
    ) -> VenueMetadataCollectionResult:
        self._append(coverage)
        return VenueMetadataCollectionResult(status, reason, None, captures, coverage)

    def _append(self, coverage: Sequence[CoverageRecord]) -> None:
        if self.coverage_ledger is not None:
            for record in coverage:
                self.coverage_ledger.append(record)

    @staticmethod
    def _polymarket_gamma_values(
        target: PolymarketMetadataTarget,
        payload: Mapping[str, Any],
        capture: RawCapture,
    ) -> dict[str, Any]:
        market_id = _segment(payload.get("id"), field="Gamma market id")
        if market_id != target.gamma_market_id:
            raise ValueError("Gamma market ID does not match target")
        condition_id = _segment(payload.get("conditionId"), field="Gamma conditionId")
        if condition_id != target.condition_id:
            raise ValueError("Gamma conditionId does not match target")
        resolution_source = require_text(payload.get("resolutionSource"), "Gamma resolutionSource")
        if resolution_source != target.resolution_source:
            raise ValueError("Gamma resolutionSource changed or does not match target")
        outcomes = _json_string_array(payload.get("outcomes"), field="Gamma outcomes")
        token_ids = _json_string_array(payload.get("clobTokenIds"), field="Gamma clobTokenIds")
        if len(outcomes) != len(token_ids):
            raise ValueError("Gamma outcomes and CLOB token IDs have different lengths")
        _validate_pairs(outcomes, token_ids, field="Gamma")
        flags = tuple((name, payload.get(name)) for name in ("active", "closed", "archived"))
        if any(not isinstance(value, bool) for _, value in flags):
            raise ValueError("Gamma active/closed/archived flags are required booleans")
        enabled = [name for name, value in flags if value]
        if len(enabled) > 1:
            raise ValueError("Gamma lifecycle status is ambiguous")
        event_time = utc_datetime(payload.get("updatedAt"), "Gamma updatedAt")
        if event_time > capture.received_at:
            raise ValueError("Gamma updatedAt cannot be after raw receipt")
        close_at = _optional_time(payload.get("endDate") or payload.get("endDateIso"), field="Gamma endDate")
        if close_at is None:
            raise ValueError("Gamma endDate is required")
        return {
            "question": require_text(payload.get("question"), "Gamma question"),
            "status": enabled[0] if enabled else "inactive",
            "resolution_source": resolution_source,
            "outcomes": tuple(zip(outcomes, token_ids, strict=True)),
            "event_time": event_time,
            "timing": VenueTiming(
                opens_at=_optional_time(payload.get("startDate") or payload.get("startDateIso"), field="Gamma startDate"),
                closes_at=close_at,
                expected_resolution_at=_optional_time(payload.get("umaEndDate") or payload.get("umaEndDateIso"), field="Gamma umaEndDate"),
                settled_at=_optional_time(payload.get("closedTime"), field="Gamma closedTime"),
            ),
        }

    @staticmethod
    def _polymarket_outcomes(
        target: PolymarketMetadataTarget,
        gamma_pairs: tuple[tuple[str, str], ...],
        payload: Mapping[str, Any],
    ) -> tuple[VenueOutcome, ...]:
        tokens = payload.get("t")
        if not isinstance(tokens, list):
            raise ValueError("CLOB market info tokens are required")
        clob_pairs: list[tuple[str, str]] = []
        for item in tokens:
            if not isinstance(item, Mapping):
                raise ValueError("CLOB token mapping entries must be objects")
            clob_pairs.append((require_text(item.get("o"), "CLOB token outcome"), _segment(item.get("t"), field="CLOB token ID")))
        _validate_pairs(tuple(item[0] for item in clob_pairs), tuple(item[1] for item in clob_pairs), field="CLOB")
        expected = tuple((item.label, item.venue_outcome_id) for item in target.outcomes)
        if _canonical_pairs(gamma_pairs) != _canonical_pairs(expected) or _canonical_pairs(clob_pairs) != _canonical_pairs(expected):
            raise ValueError("CLOB token mapping does not exactly match the registered outcomes")
        return tuple(
            VenueOutcome(
                outcome_uid=qualified_id("polymarket", f"outcome/{token_id}"),
                label=label,
                venue_outcome_id=token_id,
            )
            for label, token_id in expected
        )

    @staticmethod
    def _build_polymarket_metadata(
        target: PolymarketMetadataTarget,
        values: Mapping[str, Any],
        outcomes: tuple[VenueOutcome, ...],
        gamma: RawCapture,
        clob: RawCapture,
    ) -> VenueMarketMetadata:
        received_at = max(gamma.received_at, clob.received_at)
        return VenueMarketMetadata(
            metadata_uid=stable_uid("polymarket", "venue-metadata", (target.gamma_market_id, target.condition_id, gamma.sha256, clob.sha256)),
            market_uid=qualified_id("polymarket", f"market/{target.condition_id}"),
            platform="polymarket",
            venue_market_id=target.gamma_market_id,
            venue_event_id=None,
            question=values["question"],
            status=values["status"],
            rules=(values["resolution_source"],),
            timing=values["timing"],
            outcomes=outcomes,
            settlement_sources=(),
            event_time=values["event_time"],
            first_seen_at=received_at,
            retrieved_at=received_at,
            ingested_at=received_at,
            raw_artifact_uids=(f"polymarket:raw/{gamma.sha256}", f"polymarket:raw/{clob.sha256}"),
            source_uids=(POLYMARKET_GAMMA_SOURCE_UID, POLYMARKET_CLOB_SOURCE_UID),
        )

    @staticmethod
    def _kalshi_market_values(
        target: KalshiMetadataTarget,
        payload: Mapping[str, Any],
        capture: RawCapture,
    ) -> dict[str, Any]:
        market = payload.get("market", payload)
        if not isinstance(market, Mapping):
            raise ValueError("Kalshi market response must contain a market object")
        ticker = _segment(market.get("ticker"), field="Kalshi ticker")
        event_ticker = _segment(market.get("event_ticker"), field="Kalshi event_ticker")
        if ticker != target.ticker or event_ticker != target.event_ticker:
            raise ValueError("Kalshi ticker or event_ticker does not match target")
        if require_text(market.get("market_type"), "Kalshi market_type").casefold() != "binary":
            raise ValueError("Kalshi target must remain a documented binary market")
        rules_primary = require_text(market.get("rules_primary"), "Kalshi rules_primary")
        if rules_primary != target.rules_primary:
            raise ValueError("Kalshi rules_primary changed or does not match target")
        event_time = utc_datetime(market.get("updated_time"), "Kalshi updated_time")
        if event_time > capture.received_at:
            raise ValueError("Kalshi updated_time cannot be after raw receipt")
        close_at = _optional_time(market.get("close_time"), field="Kalshi close_time")
        if close_at is None:
            raise ValueError("Kalshi close_time is required")
        return {
            "question": require_text(market.get("title") or market.get("subtitle"), "Kalshi title"),
            "status": require_text(market.get("status"), "Kalshi status"),
            "rules": tuple(value for value in (rules_primary, _optional_text(market.get("rules_secondary"))) if value is not None),
            "event_time": event_time,
            "timing": VenueTiming(
                opens_at=_optional_time(market.get("open_time"), field="Kalshi open_time"),
                closes_at=close_at,
                expected_resolution_at=_optional_time(market.get("expected_expiration_time"), field="Kalshi expected_expiration_time"),
                settled_at=_optional_time(market.get("settlement_ts"), field="Kalshi settlement_ts"),
            ),
        }

    @staticmethod
    def _kalshi_settlement_sources(
        target: KalshiMetadataTarget,
        payload: Mapping[str, Any],
    ) -> tuple[SettlementSource, ...]:
        details = payload.get("market_details")
        if not isinstance(details, list):
            raise ValueError("Kalshi event metadata market_details are required")
        matching = [item for item in details if isinstance(item, Mapping) and str(item.get("market_ticker", "")).strip() == target.ticker]
        if len(matching) != 1:
            raise ValueError("Kalshi event metadata does not uniquely identify the target ticker")
        raw_sources = payload.get("settlement_sources")
        if not isinstance(raw_sources, list):
            raise ValueError("Kalshi settlement_sources are required")
        sources = tuple(
            SettlementSource(
                name=require_text(item.get("name"), "Kalshi settlement source name"),
                url=_http_url(item.get("url"), field="Kalshi settlement source URL"),
            )
            for item in raw_sources
            if isinstance(item, Mapping)
        )
        if len(sources) != len(raw_sources) or len(set(sources)) != len(sources):
            raise ValueError("Kalshi settlement sources are malformed or ambiguous")
        if set(sources) != set(target.settlement_sources):
            raise ValueError("Kalshi settlement sources changed or do not match target")
        return tuple(sorted(sources, key=lambda item: (item.name, item.url)))

    @staticmethod
    def _build_kalshi_metadata(
        target: KalshiMetadataTarget,
        values: Mapping[str, Any],
        sources: tuple[SettlementSource, ...],
        market: RawCapture,
        event: RawCapture,
    ) -> VenueMarketMetadata:
        received_at = max(market.received_at, event.received_at)
        outcomes = tuple(
            VenueOutcome(
                outcome_uid=qualified_id("kalshi", f"outcome/{target.ticker}/{item.venue_outcome_id.lower()}"),
                label=item.label,
                venue_outcome_id=item.venue_outcome_id,
            )
            for item in target.outcomes
        )
        return VenueMarketMetadata(
            metadata_uid=stable_uid("kalshi", "venue-metadata", (target.ticker, target.event_ticker, market.sha256, event.sha256)),
            market_uid=qualified_id("kalshi", f"market/{target.ticker}"),
            platform="kalshi",
            venue_market_id=target.ticker,
            venue_event_id=target.event_ticker,
            question=values["question"],
            status=values["status"],
            rules=values["rules"],
            timing=values["timing"],
            outcomes=outcomes,
            settlement_sources=sources,
            event_time=values["event_time"],
            first_seen_at=received_at,
            retrieved_at=received_at,
            ingested_at=received_at,
            raw_artifact_uids=(f"kalshi:raw/{market.sha256}", f"kalshi:raw/{event.sha256}"),
            source_uids=(KALSHI_MARKET_SOURCE_UID, KALSHI_EVENT_METADATA_SOURCE_UID),
        )


def _captured(value: CapturedPayload) -> bool:
    return value.status == CollectionStatus.COLLECTED and value.capture is not None and value.payload is not None


def _captured_status(status: CollectionStatus) -> VenueMetadataStatus:
    if status == CollectionStatus.QUARANTINED_HASH_CONFLICT:
        return VenueMetadataStatus.QUARANTINED_HASH_CONFLICT
    if status == CollectionStatus.UNAVAILABLE_HTTP:
        return VenueMetadataStatus.UNAVAILABLE_HTTP
    return VenueMetadataStatus.UNAVAILABLE_INCOMPLETE


def _json_string_array(value: Any, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be the documented JSON-encoded string array")
    parsed = json.loads(value)
    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{field} must decode to a non-empty list")
    return tuple(require_text(item, field) for item in parsed)


def _validate_pairs(labels: Sequence[str], identifiers: Sequence[str], *, field: str) -> None:
    if len(labels) != len(identifiers) or not labels:
        raise ValueError(f"{field} outcome mapping is empty or mismatched")
    normalized_labels = tuple(label.casefold() for label in labels)
    if len(set(normalized_labels)) != len(normalized_labels) or len(set(identifiers)) != len(identifiers):
        raise ValueError(f"{field} outcome mapping is ambiguous")


def _canonical_pairs(values: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(((label.casefold(), identifier) for label, identifier in values)))


def _optional_text(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    return require_text(value, "optional text")


def _admissible(metadata: VenueMarketMetadata, cutoff: datetime) -> bool:
    return all(
        value <= cutoff
        for value in (metadata.event_time, metadata.first_seen_at, metadata.retrieved_at, metadata.ingested_at)
    )


__all__ = [
    "ExpectedOutcome",
    "KALSHI_API",
    "KalshiMetadataTarget",
    "POLYMARKET_CLOB_API",
    "POLYMARKET_GAMMA_API",
    "PolymarketMetadataTarget",
    "SettlementSource",
    "VenueMarketMetadata",
    "VenueMetadataCollectionResult",
    "VenueMetadataCollector",
    "VenueMetadataStatus",
    "VenueOutcome",
    "VenueTiming",
]
