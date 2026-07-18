"""Raw-lineaged, point-in-time market context contracts for Phase 15.

Market context describes what the venue and configured primary sources had
published: a question, outcomes, schedule, and explicitly linked sibling
markets.  It is deliberately not an explanation of a market move and has no
field for suspicion, actor intent, or an alert recommendation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Iterable, Mapping

from pydantic import field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.multimodal.schemas import Phase15Model, Provenance, SourceReliability


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _http_url(value: str, *, field_name: str) -> str:
    if not value.startswith(("https://", "http://")):
        raise ValueError(f"{field_name} must be an http(s) URL")
    return value


class SiblingRelation(str, Enum):
    """A venue/documented relationship, not a causal claim."""

    SAME_EVENT = "same_event"
    SERIES_MEMBER = "series_member"
    MUTUALLY_EXCLUSIVE = "mutually_exclusive"
    VENUE_DECLARED = "venue_declared"


class ScheduledEventState(str, Enum):
    """Published calendar-state labels only; none expresses market impact."""

    SCHEDULED = "scheduled"
    UPDATED = "updated"
    CANCELLED = "cancelled"


class MarketOutcomeContext(Phase15Model):
    """An outcome label as published in the captured market context."""

    outcome_uid: StableUID
    label: NonEmptyStr


class SiblingMarketContext(Phase15Model):
    """One explicitly documented sibling market reference."""

    market_uid: StableUID
    relation: SiblingRelation
    rationale: NonEmptyStr


class ScheduledEventControl(Phase15Model):
    """A calendar control that was known when this context was observed.

    The class intentionally has no effect-size, direction, or suspicion field.
    A later model may use it as context, but must not treat its existence as a
    label for any market movement.
    """

    scheduled_event_uid: StableUID
    title: NonEmptyStr
    state: ScheduledEventState
    scheduled_for: datetime | None = None
    announced_at: datetime
    source_url: NonEmptyStr

    @field_validator("scheduled_for", "announced_at")
    @classmethod
    def require_utc_optional(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @field_validator("source_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        return _http_url(value, field_name="source_url")

    @model_validator(mode="after")
    def validate_schedule_state(self) -> "ScheduledEventControl":
        if self.state != ScheduledEventState.CANCELLED and self.scheduled_for is None:
            raise ValueError("scheduled_for is required unless a calendar event is cancelled")
        return self


class ResolutionSchedule(Phase15Model):
    """Published market timing and rule link, without a predicted resolution."""

    scheduled_open_at: datetime | None = None
    scheduled_close_at: datetime | None = None
    expected_resolution_at: datetime | None = None
    settlement_deadline_at: datetime | None = None
    resolution_rule_url: NonEmptyStr

    @field_validator(
        "scheduled_open_at",
        "scheduled_close_at",
        "expected_resolution_at",
        "settlement_deadline_at",
    )
    @classmethod
    def require_utc_optional(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @field_validator("resolution_rule_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        return _http_url(value, field_name="resolution_rule_url")

    @model_validator(mode="after")
    def validate_resolution_order(self) -> "ResolutionSchedule":
        if (
            self.scheduled_open_at is not None
            and self.scheduled_close_at is not None
            and self.scheduled_close_at <= self.scheduled_open_at
        ):
            raise ValueError("scheduled_close_at must be after scheduled_open_at")
        if (
            self.scheduled_close_at is not None
            and self.expected_resolution_at is not None
            and self.expected_resolution_at < self.scheduled_close_at
        ):
            raise ValueError("expected_resolution_at cannot be before scheduled_close_at")
        if (
            self.expected_resolution_at is not None
            and self.settlement_deadline_at is not None
            and self.settlement_deadline_at < self.expected_resolution_at
        ):
            raise ValueError("settlement_deadline_at cannot be before expected_resolution_at")
        if (
            self.expected_resolution_at is None
            and self.scheduled_close_at is not None
            and self.settlement_deadline_at is not None
            and self.settlement_deadline_at < self.scheduled_close_at
        ):
            raise ValueError("settlement_deadline_at cannot be before scheduled_close_at")
        return self


class MarketContext(Phase15Model):
    """A raw-lineaged market-context snapshot, selectable at a causal cutoff."""

    context_uid: StableUID
    market_uid: StableUID
    question: NonEmptyStr
    category: NonEmptyStr
    outcomes: tuple[MarketOutcomeContext, ...]
    sibling_markets: tuple[SiblingMarketContext, ...] = ()
    scheduled_events: tuple[ScheduledEventControl, ...] = ()
    resolution_schedule: ResolutionSchedule
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

    @model_validator(mode="after")
    def validate_context(self) -> "MarketContext":
        if not self.outcomes:
            raise ValueError("market context requires at least one published outcome")
        outcome_uids = tuple(item.outcome_uid for item in self.outcomes)
        labels = tuple(item.label.casefold() for item in self.outcomes)
        if len(set(outcome_uids)) != len(outcome_uids):
            raise ValueError("market context outcome_uids must be unique")
        if len(set(labels)) != len(labels):
            raise ValueError("market context outcome labels must be unique ignoring case")
        sibling_uids = tuple(item.market_uid for item in self.sibling_markets)
        if self.market_uid in sibling_uids:
            raise ValueError("a market cannot be its own sibling")
        if len(set(sibling_uids)) != len(sibling_uids):
            raise ValueError("sibling market_uids must be unique")
        scheduled_uids = tuple(item.scheduled_event_uid for item in self.scheduled_events)
        if len(set(scheduled_uids)) != len(scheduled_uids):
            raise ValueError("scheduled_event_uids must be unique")
        if any(item.announced_at > self.first_seen_at for item in self.scheduled_events):
            raise ValueError("scheduled event controls must be known by the context first_seen_at")
        if self.event_time > self.first_seen_at:
            raise ValueError("context event_time cannot be after first_seen_at")
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("context first_seen_at cannot be after retrieved_at")
        if self.retrieved_at > self.ingested_at:
            raise ValueError("context retrieved_at cannot be after ingested_at")
        if self.provenance.retrieved_at != self.retrieved_at:
            raise ValueError("context provenance.retrieved_at must equal retrieved_at")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("context reliability must refer to provenance source")
        return self

    @property
    def available_at(self) -> datetime:
        """The earliest system time at which this raw context may be used."""

        return self.ingested_at


def admit_market_context(
    value: MarketContext | Mapping[str, object],
    *,
    as_of: datetime,
) -> MarketContext | None:
    """Return a context only when every availability clock precedes ``as_of``.

    Future scheduled *dates* remain valid controls when they were already
    published.  What is rejected is future context publication, retrieval, or
    ingestion being leaked into a past decision.
    """

    context = value if isinstance(value, MarketContext) else MarketContext.model_validate(value)
    cutoff = _utc(as_of, field_name="as_of")
    if any(
        timestamp > cutoff
        for timestamp in (
            context.event_time,
            context.first_seen_at,
            context.retrieved_at,
            context.ingested_at,
        )
    ):
        return None
    return context


def contexts_as_of(
    values: Iterable[MarketContext | Mapping[str, object]],
    *,
    as_of: datetime,
) -> tuple[MarketContext, ...]:
    """Causally select distinct contexts in deterministic UID order."""

    admitted = [item for value in values if (item := admit_market_context(value, as_of=as_of))]
    by_uid = {item.context_uid: item for item in admitted}
    return tuple(by_uid[uid] for uid in sorted(by_uid))


__all__ = [
    "MarketContext",
    "MarketOutcomeContext",
    "ResolutionSchedule",
    "ScheduledEventControl",
    "ScheduledEventState",
    "SiblingMarketContext",
    "SiblingRelation",
    "admit_market_context",
    "contexts_as_of",
]
