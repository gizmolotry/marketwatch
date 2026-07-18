"""Causal market microstructure feature assembly for Phase 15.

The objects in this module are intentionally descriptive.  They preserve which
observations were present in a five minute window and derive continuous market
features from them.  In particular, this module does *not* decide that a
market is thin, normal, suspicious, or worthy of review.  Those are separate
model/evaluation questions with their own evidence and abstention policy.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Iterable, Mapping

from pydantic import Field, field_validator, model_validator

from marketleak.domain import OrderBookSnapshot, PriceObservation, TradeFill
from marketleak.domain.common import NonEmptyStr, NonNegativeDecimal, Probability, StableUID
from marketleak.multimodal.schemas import (
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    Phase15Model,
    canonical_hash,
)


FIVE_MINUTES = timedelta(minutes=5)


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _bucket_end(as_of: datetime) -> datetime:
    """Return the closed end of the latest completed five-minute bucket."""

    cutoff = _utc(as_of, field_name="as_of")
    seconds = int(cutoff.timestamp())
    return datetime.fromtimestamp(seconds - (seconds % 300), tz=UTC)


def _within_window(item: object, *, market_uid: str, outcome_uid: str | None, start: datetime, end: datetime, as_of: datetime) -> bool:
    """Accept only genuine canonical records available at the decision cutoff."""

    if not isinstance(item, (PriceObservation, TradeFill, OrderBookSnapshot)):
        return False
    if item.market_uid != market_uid:
        return False
    if outcome_uid is not None and item.outcome_uid != outcome_uid:
        return False
    # The half-open start boundary prevents one record entering two adjacent
    # snapshots.  ``ingested_at`` is separately bounded by the actual decision
    # time because a later collection receipt is not usable retroactively.
    return start < item.event_time <= end and item.ingested_at <= as_of


def _ordered_unique(records: Iterable[PriceObservation | TradeFill | OrderBookSnapshot], uid_field: str) -> tuple:
    """Deterministically de-duplicate canonical records by their immutable UID."""

    selected: dict[str, object] = {}
    for record in records:
        uid = str(getattr(record, uid_field))
        existing = selected.get(uid)
        if existing is not None and existing != record:
            raise ValueError(f"conflicting canonical records share {uid_field}={uid}")
        selected[uid] = record
    return tuple(sorted(selected.values(), key=lambda item: (item.event_time, getattr(item, uid_field))))


class MarketFeatureSnapshot(Phase15Model):
    """A raw-reference-preserving five-minute market feature payload.

    All numeric fields are observations or derived continuous quantities.  No
    score, threshold, class, or action field is permitted here.
    """

    schema_version: NonEmptyStr = "15.0.0"
    snapshot_uid: StableUID
    market_uid: StableUID
    outcome_uid: StableUID | None = None
    as_of: datetime
    window_starts_at: datetime
    window_ends_at: datetime
    source_high_watermarks: dict[NonEmptyStr, datetime]
    market_state: ModalityMissingness
    price_observation_count: int = Field(strict=True, ge=0)
    fill_count: int = Field(strict=True, ge=0)
    orderbook_snapshot_count: int = Field(strict=True, ge=0)
    price_open: Probability | None = None
    price_close: Probability | None = None
    price_change: Decimal | None = Field(default=None, strict=True)
    trade_notional: NonNegativeDecimal | None = None
    mean_fill_size: NonNegativeDecimal | None = None
    best_bid: Probability | None = None
    best_ask: Probability | None = None
    quoted_spread: NonNegativeDecimal | None = None
    bid_depth: NonNegativeDecimal | None = None
    ask_depth: NonNegativeDecimal | None = None
    depth_imbalance: Decimal | None = Field(default=None, strict=True, ge=Decimal("-1"), le=Decimal("1"))
    observation_uids: tuple[StableUID, ...] = ()
    fill_uids: tuple[StableUID, ...] = ()
    orderbook_snapshot_uids: tuple[StableUID, ...] = ()
    raw_artifact_uids: tuple[StableUID, ...] = ()

    @field_validator("as_of", "window_starts_at", "window_ends_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @field_validator("source_high_watermarks")
    @classmethod
    def validate_watermarks(cls, value: dict[str, datetime]) -> dict[str, datetime]:
        return {name: _utc(timestamp, field_name=f"source_high_watermarks[{name}]") for name, timestamp in value.items()}

    @model_validator(mode="after")
    def validate_snapshot(self) -> "MarketFeatureSnapshot":
        if self.window_ends_at <= self.window_starts_at:
            raise ValueError("feature window must have positive duration")
        if self.window_ends_at > self.as_of:
            raise ValueError("feature window cannot end after as_of")
        if self.window_ends_at - self.window_starts_at != FIVE_MINUTES:
            raise ValueError("market feature windows must be exactly five minutes")
        if self.market_state.modality != Modality.MARKET_STATE:
            raise ValueError("market_state must describe the market_state modality")
        observed = self.price_observation_count + self.fill_count + self.orderbook_snapshot_count
        if (self.market_state.status == MissingnessStatus.OBSERVED) != bool(observed):
            raise ValueError("market_state availability must match observed canonical market records")
        if self.price_change is not None and (self.price_open is None or self.price_close is None):
            raise ValueError("price_change requires both price_open and price_close")
        if (self.best_bid is None) != (self.best_ask is None):
            raise ValueError("best_bid and best_ask must be supplied together")
        if self.best_bid is not None and self.best_ask is not None:
            if self.best_bid >= self.best_ask:
                raise ValueError("best_bid must be below best_ask")
            if self.quoted_spread != self.best_ask - self.best_bid:
                raise ValueError("quoted_spread must equal best_ask - best_bid")
        elif self.quoted_spread is not None:
            raise ValueError("quoted_spread requires a complete top of book")
        if self.depth_imbalance is not None and (self.bid_depth is None or self.ask_depth is None):
            raise ValueError("depth_imbalance requires bid_depth and ask_depth")
        if len(set(self.observation_uids)) != len(self.observation_uids):
            raise ValueError("observation_uids must be unique")
        if len(set(self.fill_uids)) != len(self.fill_uids):
            raise ValueError("fill_uids must be unique")
        if len(set(self.orderbook_snapshot_uids)) != len(self.orderbook_snapshot_uids):
            raise ValueError("orderbook_snapshot_uids must be unique")
        if len(set(self.raw_artifact_uids)) != len(self.raw_artifact_uids):
            raise ValueError("raw_artifact_uids must be unique")
        if self.price_observation_count != len(self.observation_uids):
            raise ValueError("price_observation_count must equal observation_uids length")
        if self.fill_count != len(self.fill_uids):
            raise ValueError("fill_count must equal fill_uids length")
        if self.orderbook_snapshot_count != len(self.orderbook_snapshot_uids):
            raise ValueError("orderbook_snapshot_count must equal orderbook_snapshot_uids length")
        if any(watermark > self.as_of for watermark in self.source_high_watermarks.values()):
            raise ValueError("source high-watermarks cannot exceed as_of")
        return self


def build_market_feature_snapshot(
    *,
    market_uid: str,
    outcome_uid: str | None,
    as_of: datetime,
    observations: Iterable[object] = (),
    fills: Iterable[object] = (),
    books: Iterable[object] = (),
    source_high_watermarks: Mapping[str, datetime] | None = None,
) -> MarketFeatureSnapshot:
    """Build the latest completed causal five-minute market payload.

    Records after the bucket end or received after ``as_of`` are excluded.  A
    missing trade or book stream leaves its corresponding values unavailable;
    no volume/liquidity cut-off is transformed into a rule or decision.
    """

    cutoff = _utc(as_of, field_name="as_of")
    end = _bucket_end(cutoff)
    start = end - FIVE_MINUTES
    observations_selected = _ordered_unique(
        (
            item
            for item in observations
            if _within_window(item, market_uid=market_uid, outcome_uid=outcome_uid, start=start, end=end, as_of=cutoff)
            and isinstance(item, PriceObservation)
        ),
        "observation_uid",
    )
    fills_selected = _ordered_unique(
        (
            item
            for item in fills
            if _within_window(item, market_uid=market_uid, outcome_uid=outcome_uid, start=start, end=end, as_of=cutoff)
            and isinstance(item, TradeFill)
        ),
        "fill_uid",
    )
    books_selected = _ordered_unique(
        (
            item
            for item in books
            if _within_window(item, market_uid=market_uid, outcome_uid=outcome_uid, start=start, end=end, as_of=cutoff)
            and isinstance(item, OrderBookSnapshot)
        ),
        "snapshot_uid",
    )

    price_points = sorted(
        [*( (item.event_time, item.observation_uid, item.price) for item in observations_selected ), *( (item.event_time, item.fill_uid, item.price) for item in fills_selected )],
        key=lambda item: (item[0], item[1]),
    )
    price_open = price_points[0][2] if price_points else None
    price_close = price_points[-1][2] if price_points else None
    price_change = price_close - price_open if price_open is not None and price_close is not None else None
    trade_notional = sum((item.size * item.price for item in fills_selected), Decimal("0")) if fills_selected else None
    mean_fill_size = (sum((item.size for item in fills_selected), Decimal("0")) / Decimal(len(fills_selected))) if fills_selected else None

    latest_book = books_selected[-1] if books_selected else None
    best_bid = latest_book.bids[0].price if latest_book and latest_book.bids else None
    best_ask = latest_book.asks[0].price if latest_book and latest_book.asks else None
    bid_depth = sum((level.size for level in latest_book.bids), Decimal("0")) if latest_book and latest_book.bids else None
    ask_depth = sum((level.size for level in latest_book.asks), Decimal("0")) if latest_book and latest_book.asks else None
    quoted_spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    depth_total = (bid_depth or Decimal("0")) + (ask_depth or Decimal("0"))
    depth_imbalance = (bid_depth - ask_depth) / depth_total if bid_depth is not None and ask_depth is not None and depth_total > 0 else None

    all_records = [*observations_selected, *fills_selected, *books_selected]
    availability = ModalityMissingness(
        modality=Modality.MARKET_STATE,
        status=MissingnessStatus.OBSERVED if all_records else MissingnessStatus.MISSING,
        reason=None if all_records else "no causally admissible price, trade, or order-book records in this bucket",
    )
    watermarks = dict(source_high_watermarks or {})
    for record in all_records:
        existing = watermarks.get(record.source_uid)
        if existing is None or record.event_time > existing:
            watermarks[record.source_uid] = record.event_time
    watermarks = {name: timestamp for name, timestamp in watermarks.items() if _utc(timestamp, field_name=f"source_high_watermarks[{name}]") <= cutoff}

    identity = {
        "market_uid": market_uid,
        "outcome_uid": outcome_uid,
        "as_of": cutoff,
        "window_starts_at": start,
        "window_ends_at": end,
        "observation_uids": tuple(item.observation_uid for item in observations_selected),
        "fill_uids": tuple(item.fill_uid for item in fills_selected),
        "orderbook_snapshot_uids": tuple(item.snapshot_uid for item in books_selected),
    }
    return MarketFeatureSnapshot(
        snapshot_uid=f"feature:{canonical_hash(identity)}",
        market_uid=market_uid,
        outcome_uid=outcome_uid,
        as_of=cutoff,
        window_starts_at=start,
        window_ends_at=end,
        source_high_watermarks=watermarks,
        market_state=availability,
        price_observation_count=len(observations_selected),
        fill_count=len(fills_selected),
        orderbook_snapshot_count=len(books_selected),
        price_open=price_open,
        price_close=price_close,
        price_change=price_change,
        trade_notional=trade_notional,
        mean_fill_size=mean_fill_size,
        best_bid=best_bid,
        best_ask=best_ask,
        quoted_spread=quoted_spread,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        depth_imbalance=depth_imbalance,
        observation_uids=tuple(item.observation_uid for item in observations_selected),
        fill_uids=tuple(item.fill_uid for item in fills_selected),
        orderbook_snapshot_uids=tuple(item.snapshot_uid for item in books_selected),
        raw_artifact_uids=tuple(sorted({item.raw_artifact_uid for item in all_records})),
    )


__all__ = ["FIVE_MINUTES", "MarketFeatureSnapshot", "build_market_feature_snapshot"]
