"""Canonical market, price, fill, and order-book contracts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import Field, field_validator, model_validator

from .common import (
    LineageRecord,
    NonEmptyStr,
    NonNegativeDecimal,
    Probability,
    StableUID,
    StrictDomainModel,
    assert_uid_matches_platform,
)
from .enums import ActorVisibility, ObservationKind, TradeSide


class Outcome(LineageRecord):
    outcome_uid: StableUID
    market_uid: StableUID
    platform: NonEmptyStr
    source_outcome_id: NonEmptyStr
    label: NonEmptyStr
    resolved_value: Probability | None = None
    resolved_at: datetime | None = None

    @field_validator("resolved_at")
    @classmethod
    def validate_resolved_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return cls.require_utc(value)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        assert_uid_matches_platform(self.market_uid, self.platform, "market_uid")
        assert_uid_matches_platform(self.outcome_uid, self.platform, "outcome_uid")
        if (self.resolved_value is None) != (self.resolved_at is None):
            raise ValueError("resolved_value and resolved_at must be provided together")
        return self


class Market(LineageRecord):
    market_uid: StableUID
    platform: NonEmptyStr
    source_market_id: NonEmptyStr
    market_slug: NonEmptyStr | None = None
    question: NonEmptyStr
    description: NonEmptyStr | None = None
    outcome_uids: tuple[StableUID, ...] = ()
    opens_at: datetime | None = None
    closes_at: datetime | None = None
    resolved_at: datetime | None = None

    @field_validator("opens_at", "closes_at", "resolved_at")
    @classmethod
    def validate_optional_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else cls.require_utc(value)

    @model_validator(mode="after")
    def validate_identity_and_times(self) -> Self:
        assert_uid_matches_platform(self.market_uid, self.platform, "market_uid")
        for outcome_uid in self.outcome_uids:
            assert_uid_matches_platform(outcome_uid, self.platform, "outcome_uids")
        if self.opens_at is not None and self.closes_at is not None and self.closes_at <= self.opens_at:
            raise ValueError("closes_at must be after opens_at")
        return self


class PriceObservation(LineageRecord):
    observation_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    platform: NonEmptyStr
    price: Probability
    kind: ObservationKind
    actor_visibility: ActorVisibility

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        for name in ("observation_uid", "market_uid", "outcome_uid"):
            assert_uid_matches_platform(getattr(self, name), self.platform, name)
        return self


class ActorRef(StrictDomainModel):
    """An actor reference whose observability is never implicit."""

    visibility: ActorVisibility
    actor_uid: StableUID | None = None

    @model_validator(mode="after")
    def validate_visibility(self) -> Self:
        if self.visibility in {ActorVisibility.PUBLIC_WALLET, ActorVisibility.OWN_ACCOUNT_ONLY}:
            if self.actor_uid is None:
                raise ValueError("actor_uid is required when an actor is visible")
        elif self.visibility == ActorVisibility.NOT_AVAILABLE and self.actor_uid is not None:
            raise ValueError("actor_uid must be absent when actor data is not available")
        return self


class TradeFill(LineageRecord):
    fill_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    platform: NonEmptyStr
    price: Probability
    size: NonNegativeDecimal
    side: TradeSide
    actor_visibility: ActorVisibility
    actor_uid: StableUID | None = None
    maker: ActorRef | None = None
    taker: ActorRef | None = None
    transaction_uid: StableUID | None = None

    @model_validator(mode="after")
    def validate_identity_and_actor(self) -> Self:
        for name in ("fill_uid", "market_uid", "outcome_uid"):
            assert_uid_matches_platform(getattr(self, name), self.platform, name)
        if self.actor_visibility in {ActorVisibility.PUBLIC_WALLET, ActorVisibility.OWN_ACCOUNT_ONLY}:
            if self.actor_uid is None:
                raise ValueError("actor_uid is required when actor_visibility identifies an actor")
        elif self.actor_visibility == ActorVisibility.NOT_AVAILABLE and self.actor_uid is not None:
            raise ValueError("actor_uid must be absent when actor data is not available")
        return self


class OrderBookLevel(StrictDomainModel):
    price: Probability
    size: NonNegativeDecimal
    order_count: int | None = Field(default=None, strict=True, ge=0)


class OrderBookSnapshot(LineageRecord):
    snapshot_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID
    platform: NonEmptyStr
    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        for name in ("snapshot_uid", "market_uid", "outcome_uid"):
            assert_uid_matches_platform(getattr(self, name), self.platform, name)
        if any(self.bids[index].price <= self.bids[index + 1].price for index in range(len(self.bids) - 1)):
            raise ValueError("bids must be strictly descending by price")
        if any(self.asks[index].price >= self.asks[index + 1].price for index in range(len(self.asks) - 1)):
            raise ValueError("asks must be strictly ascending by price")
        if self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            raise ValueError("best bid must be below best ask")
        return self
