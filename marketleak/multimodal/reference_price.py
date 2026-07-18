"""Fail-closed documented settlement/reference-price contracts for Phase 15.

This module intentionally does not know a default Bitcoin, crypto, or FX
exchange.  A reference observation can enter only through a point-in-time
mapping that names the specific market's documented settlement rule and
primary source.  Missing documentation is represented as an explicit status,
not replaced by a plausible-looking price.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Iterable, Literal, Mapping

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, NonNegativeDecimal, StableUID
from marketleak.multimodal.schemas import Phase15Model, Provenance, SourceReliability


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _http_url(value: str, *, field_name: str) -> str:
    if not value.startswith(("https://", "http://")):
        raise ValueError(f"{field_name} must be an http(s) URL")
    return value


class PrimarySourceStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ReferenceAdmissionStatus(str, Enum):
    ADMITTED = "admitted"
    MISSING_DOCUMENTED_SETTLEMENT_SOURCE = "missing_documented_settlement_source"
    AMBIGUOUS_DOCUMENTED_SETTLEMENT_SOURCE = "ambiguous_documented_settlement_source"
    DOCUMENTED_SOURCE_NOT_YET_AVAILABLE = "documented_source_not_yet_available"
    PRIMARY_SOURCE_NOT_AVAILABLE = "primary_source_not_available"
    NO_REFERENCE_PRICE_OBSERVED = "no_reference_price_observed"
    REFERENCE_NOT_POINT_IN_TIME = "reference_not_point_in_time"
    REFERENCE_MAPPING_MISMATCH = "reference_mapping_mismatch"


class PrimarySourceAvailability(Phase15Model):
    """Raw-lineaged availability of the primary source named by a mapping."""

    availability_uid: StableUID
    primary_source_uid: StableUID
    status: PrimarySourceStatus
    checked_at: datetime
    first_seen_at: datetime
    retrieved_at: datetime
    ingested_at: datetime
    provenance: Provenance
    reliability: SourceReliability
    reason: NonEmptyStr | None = None

    @field_validator("checked_at", "first_seen_at", "retrieved_at", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_availability(self) -> "PrimarySourceAvailability":
        if self.status != PrimarySourceStatus.AVAILABLE and self.reason is None:
            raise ValueError("unavailable or unknown primary sources require a reason")
        if self.checked_at > self.first_seen_at:
            raise ValueError("availability checked_at cannot be after first_seen_at")
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("availability first_seen_at cannot be after retrieved_at")
        if self.retrieved_at > self.ingested_at:
            raise ValueError("availability retrieved_at cannot be after ingested_at")
        if self.provenance.retrieved_at != self.retrieved_at:
            raise ValueError("availability provenance.retrieved_at must equal retrieved_at")
        if self.provenance.source_uid != self.primary_source_uid:
            raise ValueError("availability provenance must identify the primary source")
        if self.reliability.source_uid != self.primary_source_uid:
            raise ValueError("availability reliability must identify the primary source")
        return self

    @property
    def available_at(self) -> datetime:
        return self.ingested_at


class DocumentedReferenceSource(Phase15Model):
    """The observed, per-market mapping from settlement rule to primary feed."""

    mapping_uid: StableUID
    market_uid: StableUID
    market_rule_source_uid: StableUID
    settlement_source_uid: StableUID
    primary_source_uid: StableUID
    asset_symbol: NonEmptyStr
    quote_currency: NonEmptyStr
    observation_kind: Literal["ticker_last", "candle_high"] = "ticker_last"
    candle_interval: Literal["1m"] | None = None
    requires_closed_candle: bool = False
    settlement_rule_url: NonEmptyStr
    primary_source_url: NonEmptyStr
    event_time: datetime
    first_seen_at: datetime
    retrieved_at: datetime
    ingested_at: datetime
    provenance: Provenance
    reliability: SourceReliability

    @field_validator("event_time", "first_seen_at", "retrieved_at", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @field_validator("settlement_rule_url", "primary_source_url")
    @classmethod
    def require_http_url(cls, value: str, info) -> str:
        return _http_url(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_mapping(self) -> "DocumentedReferenceSource":
        if self.event_time > self.first_seen_at:
            raise ValueError("documented mapping event_time cannot be after first_seen_at")
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("documented mapping first_seen_at cannot be after retrieved_at")
        if self.retrieved_at > self.ingested_at:
            raise ValueError("documented mapping retrieved_at cannot be after ingested_at")
        if self.provenance.retrieved_at != self.retrieved_at:
            raise ValueError("mapping provenance.retrieved_at must equal retrieved_at")
        if self.provenance.source_uid != self.market_rule_source_uid:
            raise ValueError("mapping provenance must identify the market-rule source")
        if self.reliability.source_uid != self.market_rule_source_uid:
            raise ValueError("mapping reliability must identify the market-rule source")
        if self.observation_kind == "ticker_last":
            if (
                self.asset_symbol != "BTC"
                or self.quote_currency != "USD"
                or self.candle_interval is not None
                or self.requires_closed_candle
            ):
                raise ValueError("ticker_last source mappings require BTC/USD and no candle semantics")
        elif (
            self.asset_symbol != "BTC"
            or self.quote_currency != "USDT"
            or self.candle_interval != "1m"
            or not self.requires_closed_candle
        ):
            raise ValueError(
                "candle_high source mappings require BTC/USDT, a 1m interval, and a closed-candle requirement"
            )
        return self

    @property
    def instrument(self) -> str:
        return f"{self.asset_symbol}/{self.quote_currency}"

    @property
    def available_at(self) -> datetime:
        return self.ingested_at


class ReferencePriceObservation(Phase15Model):
    """One raw-lineaged primary-source price for an explicitly mapped market."""

    reference_price_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    price: NonNegativeDecimal
    observation_kind: Literal["ticker_last", "candle_high"] = "ticker_last"
    source_symbol: NonEmptyStr | None = None
    candle_interval: Literal["1m"] | None = None
    candle_start: datetime | None = None
    candle_end: datetime | None = None
    is_final: bool = False
    event_time: datetime
    first_seen_at: datetime
    retrieved_at: datetime
    ingested_at: datetime
    source_mapping: DocumentedReferenceSource
    primary_source_availability: PrimarySourceAvailability
    provenance: Provenance
    reliability: SourceReliability

    @field_validator(
        "event_time",
        "first_seen_at",
        "retrieved_at",
        "ingested_at",
        "candle_start",
        "candle_end",
    )
    @classmethod
    def require_utc(cls, value: datetime | None, info) -> datetime | None:
        if value is None:
            return None
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_reference_observation(self) -> "ReferencePriceObservation":
        if self.market_uid != self.source_mapping.market_uid:
            raise ValueError("reference price market_uid must match its documented source mapping")
        if self.event_time > self.first_seen_at:
            raise ValueError("reference event_time cannot be after first_seen_at")
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("reference first_seen_at cannot be after retrieved_at")
        if self.retrieved_at > self.ingested_at:
            raise ValueError("reference retrieved_at cannot be after ingested_at")
        if self.provenance.retrieved_at != self.retrieved_at:
            raise ValueError("reference provenance.retrieved_at must equal retrieved_at")
        if self.provenance.source_uid != self.source_mapping.primary_source_uid:
            raise ValueError("reference provenance must identify the documented primary source")
        if self.reliability.source_uid != self.source_mapping.primary_source_uid:
            raise ValueError("reference reliability must identify the documented primary source")
        availability = self.primary_source_availability
        if availability.primary_source_uid != self.source_mapping.primary_source_uid:
            raise ValueError("reference availability must identify the documented primary source")
        if availability.status != PrimarySourceStatus.AVAILABLE:
            raise ValueError("a price observation requires an available primary source")
        if self.source_mapping.available_at > self.first_seen_at:
            raise ValueError("reference mapping must be available before the price is first seen")
        if availability.available_at > self.first_seen_at:
            raise ValueError("primary-source availability must be available before the price is first seen")
        if self.observation_kind != self.source_mapping.observation_kind:
            raise ValueError("reference observation kind must match its documented source mapping")
        if self.observation_kind == "ticker_last":
            if any(value is not None for value in (self.candle_interval, self.candle_start, self.candle_end)) or self.is_final:
                raise ValueError("ticker_last observations must not name candle semantics")
        else:
            if (
                self.source_symbol != "BTCUSDT"
                or self.candle_interval != "1m"
                or self.candle_start is None
                or self.candle_end is None
                or not self.is_final
            ):
                raise ValueError(
                    "candle_high observations require BTCUSDT, 1m start/end, and a final closed candle"
                )
            if self.candle_end <= self.candle_start:
                raise ValueError("candle_high observation candle_end must be after candle_start")
            if self.event_time != self.candle_end:
                raise ValueError("candle_high observation event_time must equal candle_end")
            if self.source_mapping.candle_interval != self.candle_interval or not self.source_mapping.requires_closed_candle:
                raise ValueError("candle_high observation conflicts with its documented source mapping")
        return self

    @property
    def available_at(self) -> datetime:
        return self.ingested_at


class ReferencePriceAdmission(Phase15Model):
    """Result of a fail-closed reference-price selection attempt."""

    market_uid: StableUID
    as_of: datetime
    status: ReferenceAdmissionStatus
    reason: NonEmptyStr
    source_mapping: DocumentedReferenceSource | None = None
    observation: ReferencePriceObservation | None = None

    @field_validator("as_of")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")

    @model_validator(mode="after")
    def validate_admission(self) -> "ReferencePriceAdmission":
        if self.status == ReferenceAdmissionStatus.ADMITTED:
            if self.observation is None or self.source_mapping is None:
                raise ValueError("an admitted reference requires mapping and observation")
            if self.observation.market_uid != self.market_uid:
                raise ValueError("admitted observation market_uid must match admission market")
            if self.observation.source_mapping.mapping_uid != self.source_mapping.mapping_uid:
                raise ValueError("admitted observation must use the admitted source mapping")
        elif self.observation is not None:
            raise ValueError("non-admitted reference results cannot carry a price observation")
        return self


def admit_reference_price(
    *,
    market_uid: str,
    as_of: datetime,
    documented_sources: Iterable[DocumentedReferenceSource | Mapping[str, object]],
    primary_source_availability: PrimarySourceAvailability | Mapping[str, object] | None,
    observation: ReferencePriceObservation | Mapping[str, object] | None,
) -> ReferencePriceAdmission:
    """Select a reference price only through a documented available source.

    The no-source path is intentionally a normal, explicit result.  Callers
    must not swap in a generic BTC/USD or other exchange feed for it.
    """

    cutoff = _utc(as_of, field_name="as_of")
    stable_market_uid = str(market_uid)
    mappings = tuple(
        item if isinstance(item, DocumentedReferenceSource) else DocumentedReferenceSource.model_validate(item)
        for item in documented_sources
    )
    matching = tuple(item for item in mappings if item.market_uid == stable_market_uid)
    causally_available = tuple(
        item
        for item in matching
        if all(
            timestamp <= cutoff
            for timestamp in (item.event_time, item.first_seen_at, item.retrieved_at, item.ingested_at)
        )
    )
    if not matching:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.MISSING_DOCUMENTED_SETTLEMENT_SOURCE,
            reason="no documented settlement/reference source mapping is configured for this market",
        )
    if not causally_available:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.DOCUMENTED_SOURCE_NOT_YET_AVAILABLE,
            reason="a documented mapping exists but was not available at the requested cutoff",
        )
    if len(causally_available) != 1:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.AMBIGUOUS_DOCUMENTED_SETTLEMENT_SOURCE,
            reason="more than one documented mapping is available; explicit adjudication is required",
        )
    mapping = causally_available[0]
    if primary_source_availability is None:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.PRIMARY_SOURCE_NOT_AVAILABLE,
            reason="the documented primary source has no raw-lineaged availability record",
            source_mapping=mapping,
        )
    availability = (
        primary_source_availability
        if isinstance(primary_source_availability, PrimarySourceAvailability)
        else PrimarySourceAvailability.model_validate(primary_source_availability)
    )
    if (
        availability.primary_source_uid != mapping.primary_source_uid
        or availability.status != PrimarySourceStatus.AVAILABLE
        or any(
            timestamp > cutoff
            for timestamp in (
                availability.checked_at,
                availability.first_seen_at,
                availability.retrieved_at,
                availability.ingested_at,
            )
        )
    ):
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.PRIMARY_SOURCE_NOT_AVAILABLE,
            reason="the documented primary source was unavailable, unknown, mismatched, or not point-in-time",
            source_mapping=mapping,
        )
    if observation is None:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.NO_REFERENCE_PRICE_OBSERVED,
            reason="no raw-lineaged reference price was observed from the documented primary source",
            source_mapping=mapping,
        )
    reference = (
        observation
        if isinstance(observation, ReferencePriceObservation)
        else ReferencePriceObservation.model_validate(observation)
    )
    if reference.market_uid != stable_market_uid or reference.source_mapping.mapping_uid != mapping.mapping_uid:
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.REFERENCE_MAPPING_MISMATCH,
            reason="reference observation does not use this market's documented source mapping",
            source_mapping=mapping,
        )
    if any(
        timestamp > cutoff
        for timestamp in (
            reference.event_time,
            reference.first_seen_at,
            reference.retrieved_at,
            reference.ingested_at,
        )
    ):
        return _admission(
            market_uid=stable_market_uid,
            as_of=cutoff,
            status=ReferenceAdmissionStatus.REFERENCE_NOT_POINT_IN_TIME,
            reason="reference observation occurred, was seen, retrieved, or ingested after the requested cutoff",
            source_mapping=mapping,
        )
    return _admission(
        market_uid=stable_market_uid,
        as_of=cutoff,
        status=ReferenceAdmissionStatus.ADMITTED,
        reason="documented primary-source reference is raw-lineaged and point-in-time admissible",
        source_mapping=mapping,
        observation=reference,
    )


def _admission(
    *,
    market_uid: str,
    as_of: datetime,
    status: ReferenceAdmissionStatus,
    reason: str,
    source_mapping: DocumentedReferenceSource | None = None,
    observation: ReferencePriceObservation | None = None,
) -> ReferencePriceAdmission:
    return ReferencePriceAdmission(
        market_uid=market_uid,
        as_of=as_of,
        status=status,
        reason=reason,
        source_mapping=source_mapping,
        observation=observation,
    )


__all__ = [
    "DocumentedReferenceSource",
    "PrimarySourceAvailability",
    "PrimarySourceStatus",
    "ReferenceAdmissionStatus",
    "ReferencePriceAdmission",
    "ReferencePriceObservation",
    "admit_reference_price",
]
