"""Configured, raw-first collection of documented reference-price observations.

No exchange, asset, or endpoint is assumed here.  A caller must provide one
per-market documented settlement mapping together with separate configured
market-rule and primary-price endpoints.  The collector captures both raw
responses before decoding and returns an explicit non-admission whenever a
mapping is absent, ambiguous, late, incomplete, unavailable, or quarantined.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import json
from typing import Any, Mapping, Sequence

from marketleak.ingestion.connectors.http import HttpTransport
from marketleak.ingestion.connectors.market_context import (
    CapturedPayload,
    CollectionStatus,
    Decoder,
    RawEndpoint,
    capture_configured_json,
    coverage_for_capture,
    decode_json_object,
    provenance_for_capture,
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


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _http_url(value: str, *, field: str) -> str:
    normalized = str(value).strip()
    if not normalized.startswith(("https://", "http://")):
        raise ValueError(f"{field} must be an explicit http(s) URL")
    return normalized


@dataclass(frozen=True, slots=True)
class DocumentedReferenceSourceConfig:
    """An explicit per-market settlement mapping and both required endpoints."""

    market_uid: str
    market_rule_source_uid: str
    settlement_source_uid: str
    primary_source_uid: str
    asset_symbol: str
    quote_currency: str
    settlement_rule_url: str
    primary_source_url: str
    mapping_endpoint: RawEndpoint
    primary_price_endpoint: RawEndpoint

    def __post_init__(self) -> None:
        for field in (
            "market_uid",
            "market_rule_source_uid",
            "settlement_source_uid",
            "primary_source_uid",
            "asset_symbol",
            "quote_currency",
        ):
            value = str(getattr(self, field)).strip()
            if not value or ":" not in value and field.endswith("_uid"):
                raise ValueError(f"{field} must be explicit")
        object.__setattr__(self, "settlement_rule_url", _http_url(self.settlement_rule_url, field="settlement_rule_url"))
        object.__setattr__(self, "primary_source_url", _http_url(self.primary_source_url, field="primary_source_url"))
        if self.mapping_endpoint.policy.source_uid != self.market_rule_source_uid:
            raise ValueError("mapping endpoint policy must identify market_rule_source_uid")
        if self.primary_price_endpoint.policy.source_uid != self.primary_source_uid:
            raise ValueError("primary price endpoint policy must identify primary_source_uid")


@dataclass(frozen=True, slots=True)
class ReferencePriceCollectionResult:
    """All source material and explicit admission outcome from one bounded run."""

    status: CollectionStatus
    reason: str
    admission: ReferencePriceAdmission
    mapping: DocumentedReferenceSource | None
    primary_source_availability: PrimarySourceAvailability | None
    raw_captures: tuple[RawCapture, ...]
    coverage: tuple[CoverageRecord, ...]


class ReferencePriceCollector:
    """Collect only a uniquely configured, documented settlement source.

    The constructor has no default source list or transport.  It does not
    discover a price feed from an asset symbol, so it cannot fall back to a
    generic BTC/USD (or any other) exchange endpoint.
    """

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        transport: HttpTransport,
        documented_sources: Sequence[DocumentedReferenceSourceConfig],
        coverage_ledger: CoverageLedger | None = None,
        decoder: Decoder = decode_json_object,
    ) -> None:
        self.raw_store = raw_store
        self.transport = transport
        self.documented_sources = tuple(documented_sources)
        self.coverage_ledger = coverage_ledger
        self.decoder = decoder

    def collect(self, *, market_uid: str, as_of: datetime, received_at: datetime) -> ReferencePriceCollectionResult:
        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        configured = tuple(item for item in self.documented_sources if item.market_uid == market_uid)
        if not configured:
            admission = admit_reference_price(
                market_uid=market_uid,
                as_of=cutoff,
                documented_sources=(),
                primary_source_availability=None,
                observation=None,
            )
            return self._result(
                status=CollectionStatus.UNAVAILABLE_MISSING_SOURCE,
                reason="no documented settlement/reference source is configured for this market",
                admission=admission,
            )
        if len(configured) != 1:
            admission = ReferencePriceAdmission(
                market_uid=market_uid,
                as_of=cutoff,
                status=ReferenceAdmissionStatus.AMBIGUOUS_DOCUMENTED_SETTLEMENT_SOURCE,
                reason="more than one configured documented source exists; explicit adjudication is required",
            )
            return self._result(
                status=CollectionStatus.UNAVAILABLE_AMBIGUOUS_SOURCE,
                reason=admission.reason,
                admission=admission,
            )

        config = configured[0]
        mapping_response = capture_configured_json(
            raw_store=self.raw_store,
            transport=self.transport,
            endpoint=config.mapping_endpoint,
            received_at=timestamp,
            decoder=self.decoder,
        )
        mapping_coverage = coverage_for_capture(
            endpoint=config.mapping_endpoint,
            received_at=timestamp,
            capture=mapping_response.capture,
            complete=False,
        )
        if not self._captured(mapping_response):
            self._append(mapping_coverage)
            admission = ReferencePriceAdmission(
                market_uid=market_uid,
                as_of=cutoff,
                status=ReferenceAdmissionStatus.DOCUMENTED_SOURCE_NOT_YET_AVAILABLE,
                reason="the configured documented settlement mapping could not be captured and verified",
            )
            return self._result(
                status=mapping_response.status,
                reason=mapping_response.reason,
                admission=admission,
                raw_captures=self._captures(mapping_response),
                coverage=(mapping_coverage,),
            )
        assert mapping_response.capture is not None and mapping_response.payload is not None
        try:
            mapping = self._normalize_mapping(config, mapping_response.capture, mapping_response.payload)
        except (TypeError, ValueError, KeyError) as exc:
            self._append(mapping_coverage)
            admission = ReferencePriceAdmission(
                market_uid=market_uid,
                as_of=cutoff,
                status=ReferenceAdmissionStatus.DOCUMENTED_SOURCE_NOT_YET_AVAILABLE,
                reason="the configured mapping body was incomplete or conflicts with its declared source mapping",
            )
            return self._result(
                status=CollectionStatus.UNAVAILABLE_INCOMPLETE,
                reason=f"configured mapping payload is incomplete or invalid: {type(exc).__name__}",
                admission=admission,
                raw_captures=(mapping_response.capture,),
                coverage=(mapping_coverage,),
            )
        if any(value > cutoff for value in (mapping.event_time, mapping.first_seen_at, mapping.retrieved_at, mapping.ingested_at)):
            self._append(mapping_coverage)
            admission = admit_reference_price(
                market_uid=market_uid,
                as_of=cutoff,
                documented_sources=(mapping,),
                primary_source_availability=None,
                observation=None,
            )
            return self._result(
                status=CollectionStatus.UNAVAILABLE_LATE,
                reason="documented settlement mapping was not available at the requested as_of cutoff",
                admission=admission,
                mapping=mapping,
                raw_captures=(mapping_response.capture,),
                coverage=(mapping_coverage,),
            )
        complete_mapping_coverage = coverage_for_capture(
            endpoint=config.mapping_endpoint,
            received_at=timestamp,
            capture=mapping_response.capture,
            complete=True,
        )
        self._append(complete_mapping_coverage)

        price_response = capture_configured_json(
            raw_store=self.raw_store,
            transport=self.transport,
            endpoint=config.primary_price_endpoint,
            received_at=timestamp,
            decoder=self.decoder,
        )
        price_coverage = coverage_for_capture(
            endpoint=config.primary_price_endpoint,
            received_at=timestamp,
            capture=price_response.capture,
            complete=False,
        )
        availability = self._availability_from_response(config, price_response)
        if not self._captured(price_response):
            self._append(price_coverage)
            admission = admit_reference_price(
                market_uid=market_uid,
                as_of=cutoff,
                documented_sources=(mapping,),
                primary_source_availability=availability,
                observation=None,
            )
            return self._result(
                status=price_response.status,
                reason=price_response.reason,
                admission=admission,
                mapping=mapping,
                availability=availability,
                raw_captures=(mapping_response.capture, *self._captures(price_response)),
                coverage=(complete_mapping_coverage, price_coverage),
            )
        assert price_response.capture is not None and price_response.payload is not None and availability is not None
        try:
            observation = self._normalize_observation(config, mapping, availability, price_response.capture, price_response.payload)
        except (TypeError, ValueError, KeyError) as exc:
            self._append(price_coverage)
            unavailable = self._unavailable_availability(config, price_response.capture, reason="primary response was incomplete")
            admission = admit_reference_price(
                market_uid=market_uid,
                as_of=cutoff,
                documented_sources=(mapping,),
                primary_source_availability=unavailable,
                observation=None,
            )
            return self._result(
                status=CollectionStatus.UNAVAILABLE_INCOMPLETE,
                reason=f"configured primary-price payload is incomplete or invalid: {type(exc).__name__}",
                admission=admission,
                mapping=mapping,
                availability=unavailable,
                raw_captures=(mapping_response.capture, price_response.capture),
                coverage=(complete_mapping_coverage, price_coverage),
            )
        admitted = admit_reference_price(
            market_uid=market_uid,
            as_of=cutoff,
            documented_sources=(mapping,),
            primary_source_availability=availability,
            observation=observation,
        )
        complete_price_coverage = coverage_for_capture(
            endpoint=config.primary_price_endpoint,
            received_at=timestamp,
            capture=price_response.capture,
            complete=admitted.status == ReferenceAdmissionStatus.ADMITTED,
        )
        self._append(complete_price_coverage)
        if admitted.status != ReferenceAdmissionStatus.ADMITTED:
            return self._result(
                status=CollectionStatus.UNAVAILABLE_LATE,
                reason=admitted.reason,
                admission=admitted,
                mapping=mapping,
                availability=availability,
                raw_captures=(mapping_response.capture, price_response.capture),
                coverage=(complete_mapping_coverage, complete_price_coverage),
            )
        return self._result(
            status=CollectionStatus.COLLECTED,
            reason="documented primary-source reference was raw-lineaged and point-in-time admissible",
            admission=admitted,
            mapping=mapping,
            availability=availability,
            raw_captures=(mapping_response.capture, price_response.capture),
            coverage=(complete_mapping_coverage, complete_price_coverage),
        )

    @staticmethod
    def _captured(response: CapturedPayload) -> bool:
        return response.status == CollectionStatus.COLLECTED and response.capture is not None and response.payload is not None

    @staticmethod
    def _captures(response: CapturedPayload) -> tuple[RawCapture, ...]:
        return () if response.capture is None else (response.capture,)

    def _append(self, record: CoverageRecord) -> None:
        if self.coverage_ledger is not None:
            self.coverage_ledger.append(record)

    @staticmethod
    def _normalize_mapping(
        config: DocumentedReferenceSourceConfig,
        capture: RawCapture,
        payload: Mapping[str, Any],
    ) -> DocumentedReferenceSource:
        expected = {
            "market_uid": config.market_uid,
            "market_rule_source_uid": config.market_rule_source_uid,
            "settlement_source_uid": config.settlement_source_uid,
            "primary_source_uid": config.primary_source_uid,
            "asset_symbol": config.asset_symbol,
            "quote_currency": config.quote_currency,
            "settlement_rule_url": config.settlement_rule_url,
            "primary_source_url": config.primary_source_url,
        }
        missing = tuple(field for field in (*expected, "event_time", "first_seen_at") if field not in payload)
        if missing:
            raise ValueError(f"missing documented mapping fields: {', '.join(missing)}")
        if any(str(payload[field]) != str(value) for field, value in expected.items()):
            raise ValueError("captured mapping disagrees with explicit documented source configuration")
        document = {
            "mapping_uid": payload.get(
                "mapping_uid", f"{config.mapping_endpoint.platform}:reference-mapping/{config.market_uid.split(':', 1)[1]}/{capture.sha256}"
            ),
            **expected,
            "event_time": payload["event_time"],
            "first_seen_at": payload["first_seen_at"],
            "retrieved_at": capture.received_at,
            "ingested_at": capture.received_at,
            "provenance": provenance_for_capture(capture=capture, endpoint=config.mapping_endpoint).model_dump(mode="json"),
            "reliability": config.mapping_endpoint.policy.reliability(assessed_at=capture.received_at).model_dump(mode="json"),
        }
        return DocumentedReferenceSource.model_validate_json(json.dumps(document, default=str))

    @staticmethod
    def _availability_from_response(
        config: DocumentedReferenceSourceConfig,
        response: CapturedPayload,
    ) -> PrimarySourceAvailability | None:
        if response.capture is None:
            return None
        if response.status != CollectionStatus.COLLECTED:
            return ReferencePriceCollector._unavailable_availability(config, response.capture, reason=response.reason)
        capture = response.capture
        return PrimarySourceAvailability(
            availability_uid=f"{config.primary_price_endpoint.platform}:availability/{capture.sha256}",
            primary_source_uid=config.primary_source_uid,
            status=PrimarySourceStatus.AVAILABLE,
            checked_at=capture.received_at,
            first_seen_at=capture.received_at,
            retrieved_at=capture.received_at,
            ingested_at=capture.received_at,
            provenance=provenance_for_capture(capture=capture, endpoint=config.primary_price_endpoint),
            reliability=config.primary_price_endpoint.policy.reliability(assessed_at=capture.received_at),
        )

    @staticmethod
    def _unavailable_availability(
        config: DocumentedReferenceSourceConfig,
        capture: RawCapture,
        *,
        reason: str,
    ) -> PrimarySourceAvailability:
        return PrimarySourceAvailability(
            availability_uid=f"{config.primary_price_endpoint.platform}:availability/{capture.sha256}",
            primary_source_uid=config.primary_source_uid,
            status=PrimarySourceStatus.UNAVAILABLE,
            checked_at=capture.received_at,
            first_seen_at=capture.received_at,
            retrieved_at=capture.received_at,
            ingested_at=capture.received_at,
            provenance=provenance_for_capture(capture=capture, endpoint=config.primary_price_endpoint),
            reliability=config.primary_price_endpoint.policy.reliability(assessed_at=capture.received_at),
            reason=reason,
        )

    @staticmethod
    def _normalize_observation(
        config: DocumentedReferenceSourceConfig,
        mapping: DocumentedReferenceSource,
        availability: PrimarySourceAvailability,
        capture: RawCapture,
        payload: Mapping[str, Any],
    ) -> ReferencePriceObservation:
        required = ("price", "event_time", "first_seen_at")
        missing = tuple(field for field in required if field not in payload)
        if missing:
            raise ValueError(f"missing primary-price fields: {', '.join(missing)}")
        if "market_uid" in payload and str(payload["market_uid"]) != config.market_uid:
            raise ValueError("primary-price payload market_uid conflicts with configured market_uid")
        document = {
            "reference_price_uid": payload.get(
                "reference_price_uid", f"{config.primary_price_endpoint.platform}:reference-price/{config.market_uid.split(':', 1)[1]}/{capture.sha256}"
            ),
            "market_uid": config.market_uid,
            "outcome_uid": payload.get("outcome_uid"),
            "price": payload["price"],
            "event_time": payload["event_time"],
            "first_seen_at": payload["first_seen_at"],
            "retrieved_at": capture.received_at,
            "ingested_at": capture.received_at,
            "source_mapping": mapping.model_dump(mode="json"),
            "primary_source_availability": availability.model_dump(mode="json"),
            "provenance": provenance_for_capture(capture=capture, endpoint=config.primary_price_endpoint).model_dump(mode="json"),
            "reliability": config.primary_price_endpoint.policy.reliability(assessed_at=capture.received_at).model_dump(mode="json"),
        }
        return ReferencePriceObservation.model_validate_json(json.dumps(document, default=str))

    @staticmethod
    def _result(
        *,
        status: CollectionStatus,
        reason: str,
        admission: ReferencePriceAdmission,
        mapping: DocumentedReferenceSource | None = None,
        availability: PrimarySourceAvailability | None = None,
        raw_captures: tuple[RawCapture, ...] = (),
        coverage: tuple[CoverageRecord, ...] = (),
    ) -> ReferencePriceCollectionResult:
        return ReferencePriceCollectionResult(
            status=status,
            reason=reason,
            admission=admission,
            mapping=mapping,
            primary_source_availability=availability,
            raw_captures=raw_captures,
            coverage=coverage,
        )


__all__ = [
    "DocumentedReferenceSourceConfig",
    "ReferencePriceCollectionResult",
    "ReferencePriceCollector",
]
