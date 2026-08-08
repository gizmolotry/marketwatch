"""Causal market microstructure feature assembly for Phase 15.

The objects in this module are intentionally descriptive.  They preserve which
observations were present in a five minute window and derive continuous market
features from them.  In particular, this module does *not* decide that a
market is thin, normal, suspicious, or worthy of review.  Those are separate
model/evaluation questions with their own evidence and abstention policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Iterable, Mapping

from pydantic import Field, field_validator, model_validator

from marketleak.domain import ObservationKind, OrderBookSnapshot, PriceObservation, TradeFill
from marketleak.domain.common import NonEmptyStr, NonNegativeDecimal, Probability, StableUID
from marketleak.multimodal.schemas import (
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    Phase15Model,
    canonical_hash,
)


FIVE_MINUTES = timedelta(minutes=5)


@dataclass(frozen=True)
class _PricePoint:
    """Internal causal point with enough lineage to avoid cross-kind ordering."""

    event_time: datetime
    ingested_at: datetime
    price: Decimal
    source_uid: str
    raw_artifact_uid: str
    record_uids: tuple[str, ...]
    synchronization_key: tuple[str, ...] | None = None


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


class PriceSeriesFeature(Phase15Model):
    """A kind-specific causal price series summary with explicit availability.

    A change is available only across at least two distinct event times.  This
    prevents one observed point (including a fill plus its duplicate
    ``LAST_TRADE`` observation) from being represented as a zero return.
    """

    observation_kind: ObservationKind
    point_count: int = Field(strict=True, ge=0)
    distinct_event_time_count: int = Field(strict=True, ge=0)
    observed: bool
    price_open: Probability | None = None
    price_close: Probability | None = None
    price_change: Decimal | None = Field(default=None, strict=True)
    change_available: bool
    change_missing_reason: NonEmptyStr | None = None
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    latest_ingested_at: datetime | None = None
    source_uids: tuple[StableUID, ...] = ()
    raw_artifact_uids: tuple[StableUID, ...] = ()
    record_uids: tuple[StableUID, ...] = ()

    @field_validator("first_event_time", "last_event_time", "latest_ingested_at")
    @classmethod
    def require_optional_utc(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_series(self) -> "PriceSeriesFeature":
        if self.observed != (self.point_count > 0):
            raise ValueError("series observed mask must match point_count")
        if self.distinct_event_time_count > self.point_count:
            raise ValueError("distinct_event_time_count cannot exceed point_count")
        for name, values in (
            ("source_uids", self.source_uids),
            ("raw_artifact_uids", self.raw_artifact_uids),
            ("record_uids", self.record_uids),
        ):
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must be unique")

        if not self.observed:
            if self.distinct_event_time_count != 0:
                raise ValueError("an unobserved series cannot have event times")
            if any(
                value is not None
                for value in (
                    self.price_open,
                    self.price_close,
                    self.price_change,
                    self.first_event_time,
                    self.last_event_time,
                    self.latest_ingested_at,
                )
            ):
                raise ValueError("an unobserved series cannot carry values or clocks")
            if self.source_uids or self.raw_artifact_uids or self.record_uids:
                raise ValueError("an unobserved series cannot carry lineage")
        else:
            if self.distinct_event_time_count < 1:
                raise ValueError("an observed series requires an event time")
            if self.first_event_time is None or self.last_event_time is None or self.latest_ingested_at is None:
                raise ValueError("an observed series requires causal clocks")
            if self.last_event_time < self.first_event_time:
                raise ValueError("series clocks must be chronological")
            if not self.source_uids or not self.raw_artifact_uids or not self.record_uids:
                raise ValueError("an observed series requires source, raw, and record lineage")
            if (self.price_open is None) != (self.price_close is None):
                raise ValueError("series endpoints must be supplied together")

        if self.change_available != (self.price_change is not None):
            raise ValueError("change_available must match price_change availability")
        if self.change_available:
            if self.change_missing_reason is not None:
                raise ValueError("an available change cannot have a missing reason")
            if self.distinct_event_time_count < 2 or self.first_event_time == self.last_event_time:
                raise ValueError("a change requires two distinct temporal points")
            if self.price_open is None or self.price_close is None:
                raise ValueError("a change requires both endpoints")
            if self.price_change != self.price_close - self.price_open:
                raise ValueError("series price_change must equal close minus open")
        elif self.change_missing_reason is None:
            raise ValueError("an unavailable change requires an explicit reason")
        return self


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
    last_trade_series: PriceSeriesFeature
    midpoint_series: PriceSeriesFeature
    best_bid_series: PriceSeriesFeature
    best_ask_series: PriceSeriesFeature
    primary_price_kind: ObservationKind = ObservationKind.LAST_TRADE
    primary_price_change_missing_reason: NonEmptyStr | None = None
    price_open: Probability | None = None
    price_close: Probability | None = None
    price_change: Decimal | None = Field(default=None, strict=True)
    trade_notional: NonNegativeDecimal | None = None
    mean_fill_size: NonNegativeDecimal | None = None
    best_bid: Probability | None = None
    best_ask: Probability | None = None
    best_bid_observed: bool
    best_ask_observed: bool
    quoted_spread: NonNegativeDecimal | None = None
    quoted_spread_available: bool
    quoted_spread_missing_reason: NonEmptyStr | None = None
    bid_depth: NonNegativeDecimal | None = None
    ask_depth: NonNegativeDecimal | None = None
    bid_depth_observed: bool
    ask_depth_observed: bool
    depth_imbalance: Decimal | None = Field(default=None, strict=True, ge=Decimal("-1"), le=Decimal("1"))
    depth_imbalance_available: bool
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
        expected_kinds = (
            (self.last_trade_series, ObservationKind.LAST_TRADE),
            (self.midpoint_series, ObservationKind.MIDPOINT),
            (self.best_bid_series, ObservationKind.BEST_BID),
            (self.best_ask_series, ObservationKind.BEST_ASK),
        )
        if any(series.observation_kind != kind for series, kind in expected_kinds):
            raise ValueError("price series fields must match their observation-kind semantics")
        if self.primary_price_kind != ObservationKind.LAST_TRADE:
            raise ValueError("the causal primary price series is last_trade")
        if (self.price_open, self.price_close, self.price_change) != (
            self.last_trade_series.price_open,
            self.last_trade_series.price_close,
            self.last_trade_series.price_change,
        ):
            raise ValueError("legacy price fields must alias the last-trade series")
        if self.primary_price_change_missing_reason != self.last_trade_series.change_missing_reason:
            raise ValueError("primary missingness must alias the last-trade series")

        if self.best_bid_observed != (self.best_bid is not None):
            raise ValueError("best_bid_observed must match best_bid availability")
        if self.best_ask_observed != (self.best_ask is not None):
            raise ValueError("best_ask_observed must match best_ask availability")
        if self.quoted_spread_available != (self.quoted_spread is not None):
            raise ValueError("quoted_spread_available must match quoted_spread availability")
        if self.quoted_spread is not None:
            if self.best_bid is None or self.best_ask is None:
                raise ValueError("quoted_spread requires both top-of-book sides")
            if self.best_bid >= self.best_ask:
                raise ValueError("a synchronized best bid must be below its best ask")
            if self.quoted_spread != self.best_ask - self.best_bid:
                raise ValueError("quoted_spread must equal best_ask - best_bid")
            if self.quoted_spread_missing_reason is not None:
                raise ValueError("an available spread cannot have a missing reason")
        elif self.quoted_spread_missing_reason is None:
            raise ValueError("an unavailable spread requires an explicit reason")

        if self.bid_depth_observed != (self.bid_depth is not None):
            raise ValueError("bid_depth_observed must match bid_depth availability")
        if self.ask_depth_observed != (self.ask_depth is not None):
            raise ValueError("ask_depth_observed must match ask_depth availability")
        if self.depth_imbalance_available != (self.depth_imbalance is not None):
            raise ValueError("depth_imbalance_available must match depth_imbalance availability")
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


def _point_from_observation(item: PriceObservation) -> _PricePoint:
    synchronization_key = (
        "observation-frame",
        item.event_time.isoformat(),
        item.ingested_at.isoformat(),
        str(item.source_uid),
        str(item.raw_artifact_uid),
    )
    return _PricePoint(
        event_time=item.event_time,
        ingested_at=item.ingested_at,
        price=item.price,
        source_uid=str(item.source_uid),
        raw_artifact_uid=str(item.raw_artifact_uid),
        record_uids=(str(item.observation_uid),),
        synchronization_key=synchronization_key,
    )


def _point_from_fill(item: TradeFill) -> _PricePoint:
    return _PricePoint(
        event_time=item.event_time,
        ingested_at=item.ingested_at,
        price=item.price,
        source_uid=str(item.source_uid),
        raw_artifact_uid=str(item.raw_artifact_uid),
        record_uids=(str(item.fill_uid),),
    )


def _book_point(item: OrderBookSnapshot, *, side: ObservationKind) -> _PricePoint | None:
    levels = item.bids if side == ObservationKind.BEST_BID else item.asks
    if not levels:
        return None
    return _PricePoint(
        event_time=item.event_time,
        ingested_at=item.ingested_at,
        price=levels[0].price,
        source_uid=str(item.source_uid),
        raw_artifact_uid=str(item.raw_artifact_uid),
        record_uids=(str(item.snapshot_uid),),
        synchronization_key=("orderbook", str(item.snapshot_uid)),
    )


def _select_endpoint(
    points: Iterable[_PricePoint], *, opening: bool
) -> tuple[Decimal | None, tuple[str, ...] | None, bool]:
    """Select an endpoint by causal clocks, never by a semantically opaque UID.

    Event time chooses the temporal endpoint and ingestion time orders multiple
    facts at that event time.  Conflicting prices at identical event and
    ingestion clocks are explicitly ambiguous instead of acquiring an order
    from record UID spelling.
    """

    values = tuple(points)
    if not values:
        return None, None, False
    event_time = (min if opening else max)(item.event_time for item in values)
    at_event = tuple(item for item in values if item.event_time == event_time)
    ingested_at = (min if opening else max)(item.ingested_at for item in at_event)
    candidates = tuple(item for item in at_event if item.ingested_at == ingested_at)
    prices = {item.price for item in candidates}
    if len(prices) != 1:
        return None, None, True
    synchronization_keys = {item.synchronization_key for item in candidates}
    synchronization_key = next(iter(synchronization_keys)) if len(synchronization_keys) == 1 else None
    return next(iter(prices)), synchronization_key, False


def _summarize_series(
    *, kind: ObservationKind, points: Iterable[_PricePoint], absent_reason: str
) -> PriceSeriesFeature:
    values = tuple(points)
    if not values:
        return PriceSeriesFeature(
            observation_kind=kind,
            point_count=0,
            distinct_event_time_count=0,
            observed=False,
            change_available=False,
            change_missing_reason=absent_reason,
        )

    event_times = tuple(sorted({item.event_time for item in values}))
    price_open, _, open_ambiguous = _select_endpoint(values, opening=True)
    price_close, _, close_ambiguous = _select_endpoint(values, opening=False)
    endpoints_ambiguous = open_ambiguous or close_ambiguous
    if endpoints_ambiguous:
        price_open = None
        price_close = None
        price_change = None
        missing_reason = "conflicting prices share an endpoint event and ingestion clock"
    elif len(event_times) < 2:
        price_change = None
        missing_reason = "fewer than two distinct event times are available for a change"
    else:
        assert price_open is not None and price_close is not None
        price_change = price_close - price_open
        missing_reason = None

    return PriceSeriesFeature(
        observation_kind=kind,
        point_count=len(values),
        distinct_event_time_count=len(event_times),
        observed=True,
        price_open=price_open,
        price_close=price_close,
        price_change=price_change,
        change_available=price_change is not None,
        change_missing_reason=missing_reason,
        first_event_time=event_times[0],
        last_event_time=event_times[-1],
        latest_ingested_at=max(item.ingested_at for item in values),
        source_uids=tuple(sorted({item.source_uid for item in values})),
        raw_artifact_uids=tuple(sorted({item.raw_artifact_uid for item in values})),
        record_uids=tuple(sorted({uid for item in values for uid in item.record_uids})),
    )


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

    # LAST_TRADE is the documented primary return series.  Quote observations
    # are never mixed into it.  A connector may emit both a fill and a
    # LAST_TRADE observation for the same delivery; prefer the more specific
    # fill point while retaining both canonical records in snapshot lineage.
    fill_points = tuple(_point_from_fill(item) for item in fills_selected)
    fill_semantic_keys = {
        (point.event_time, point.ingested_at, point.source_uid, point.raw_artifact_uid, point.price)
        for point in fill_points
    }
    last_trade_observation_points = tuple(
        point
        for item in observations_selected
        if item.kind == ObservationKind.LAST_TRADE
        for point in (_point_from_observation(item),)
        if (point.event_time, point.ingested_at, point.source_uid, point.raw_artifact_uid, point.price)
        not in fill_semantic_keys
    )
    last_trade_points = (*fill_points, *last_trade_observation_points)

    bid_observation_points = tuple(
        _point_from_observation(item)
        for item in observations_selected
        if item.kind == ObservationKind.BEST_BID
    )
    ask_observation_points = tuple(
        _point_from_observation(item)
        for item in observations_selected
        if item.kind == ObservationKind.BEST_ASK
    )
    direct_midpoint_points = tuple(
        _point_from_observation(item)
        for item in observations_selected
        if item.kind == ObservationKind.MIDPOINT
    )
    bid_book_points = tuple(
        point
        for item in books_selected
        for point in (_book_point(item, side=ObservationKind.BEST_BID),)
        if point is not None
    )
    ask_book_points = tuple(
        point
        for item in books_selected
        for point in (_book_point(item, side=ObservationKind.BEST_ASK),)
        if point is not None
    )
    bid_points = (*bid_observation_points, *bid_book_points)
    ask_points = (*ask_observation_points, *ask_book_points)

    # Midpoints require an explicitly synchronized two-sided quote.  Matching
    # only on event time would pair stale or independently received sides.
    bids_by_frame: dict[tuple[str, ...], list[_PricePoint]] = {}
    asks_by_frame: dict[tuple[str, ...], list[_PricePoint]] = {}
    for point in bid_points:
        if point.synchronization_key is not None:
            bids_by_frame.setdefault(point.synchronization_key, []).append(point)
    for point in ask_points:
        if point.synchronization_key is not None:
            asks_by_frame.setdefault(point.synchronization_key, []).append(point)
    derived_midpoint_points: list[_PricePoint] = []
    for key in sorted(set(bids_by_frame) & set(asks_by_frame)):
        bids = bids_by_frame[key]
        asks = asks_by_frame[key]
        bid_prices = {point.price for point in bids}
        ask_prices = {point.price for point in asks}
        if len(bid_prices) != 1 or len(ask_prices) != 1:
            continue
        bid = next(iter(bid_prices))
        ask = next(iter(ask_prices))
        if bid >= ask:
            continue
        frame_points = (*bids, *asks)
        derived_midpoint_points.append(
            _PricePoint(
                event_time=frame_points[0].event_time,
                ingested_at=max(point.ingested_at for point in frame_points),
                price=(bid + ask) / Decimal("2"),
                source_uid=frame_points[0].source_uid,
                raw_artifact_uid=frame_points[0].raw_artifact_uid,
                record_uids=tuple(sorted({uid for point in frame_points for uid in point.record_uids})),
                synchronization_key=key,
            )
        )
    midpoint_points = (*direct_midpoint_points, *derived_midpoint_points)

    last_trade_series = _summarize_series(
        kind=ObservationKind.LAST_TRADE,
        points=last_trade_points,
        absent_reason="no causally admissible last-trade observation or fill in this bucket",
    )
    midpoint_series = _summarize_series(
        kind=ObservationKind.MIDPOINT,
        points=midpoint_points,
        absent_reason="no direct midpoint or causally synchronized two-sided quote in this bucket",
    )
    best_bid_series = _summarize_series(
        kind=ObservationKind.BEST_BID,
        points=bid_points,
        absent_reason="no causally admissible best-bid observation in this bucket",
    )
    best_ask_series = _summarize_series(
        kind=ObservationKind.BEST_ASK,
        points=ask_points,
        absent_reason="no causally admissible best-ask observation in this bucket",
    )
    price_open = last_trade_series.price_open
    price_close = last_trade_series.price_close
    price_change = last_trade_series.price_change
    trade_notional = sum((item.size * item.price for item in fills_selected), Decimal("0")) if fills_selected else None
    mean_fill_size = (sum((item.size for item in fills_selected), Decimal("0")) / Decimal(len(fills_selected))) if fills_selected else None

    best_bid, best_bid_frame, _ = _select_endpoint(bid_points, opening=False)
    best_ask, best_ask_frame, _ = _select_endpoint(ask_points, opening=False)
    synchronized_top = (
        best_bid is not None
        and best_ask is not None
        and best_bid_frame is not None
        and best_bid_frame == best_ask_frame
        and best_bid < best_ask
    )
    quoted_spread = best_ask - best_bid if synchronized_top else None
    if quoted_spread is not None:
        spread_missing_reason = None
    elif best_bid is None or best_ask is None:
        spread_missing_reason = "both best-bid and best-ask values are required for a quoted spread"
    else:
        spread_missing_reason = "latest bid and ask were not observed in the same causal quote frame"

    bid_depth_points = tuple(
        _PricePoint(
            event_time=item.event_time,
            ingested_at=item.ingested_at,
            price=sum((level.size for level in item.bids), Decimal("0")),
            source_uid=str(item.source_uid),
            raw_artifact_uid=str(item.raw_artifact_uid),
            record_uids=(str(item.snapshot_uid),),
            synchronization_key=("orderbook", str(item.snapshot_uid)),
        )
        for item in books_selected
        if item.bids
    )
    ask_depth_points = tuple(
        _PricePoint(
            event_time=item.event_time,
            ingested_at=item.ingested_at,
            price=sum((level.size for level in item.asks), Decimal("0")),
            source_uid=str(item.source_uid),
            raw_artifact_uid=str(item.raw_artifact_uid),
            record_uids=(str(item.snapshot_uid),),
            synchronization_key=("orderbook", str(item.snapshot_uid)),
        )
        for item in books_selected
        if item.asks
    )
    bid_depth, bid_depth_frame, _ = _select_endpoint(bid_depth_points, opening=False)
    ask_depth, ask_depth_frame, _ = _select_endpoint(ask_depth_points, opening=False)
    depth_total = (
        bid_depth + ask_depth
        if bid_depth is not None and ask_depth is not None and bid_depth_frame == ask_depth_frame
        else None
    )
    depth_imbalance = (
        (bid_depth - ask_depth) / depth_total
        if depth_total is not None and depth_total > 0 and bid_depth is not None and ask_depth is not None
        else None
    )

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
        last_trade_series=last_trade_series,
        midpoint_series=midpoint_series,
        best_bid_series=best_bid_series,
        best_ask_series=best_ask_series,
        primary_price_kind=ObservationKind.LAST_TRADE,
        primary_price_change_missing_reason=last_trade_series.change_missing_reason,
        price_open=price_open,
        price_close=price_close,
        price_change=price_change,
        trade_notional=trade_notional,
        mean_fill_size=mean_fill_size,
        best_bid=best_bid,
        best_ask=best_ask,
        best_bid_observed=best_bid is not None,
        best_ask_observed=best_ask is not None,
        quoted_spread=quoted_spread,
        quoted_spread_available=quoted_spread is not None,
        quoted_spread_missing_reason=spread_missing_reason,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        bid_depth_observed=bid_depth is not None,
        ask_depth_observed=ask_depth is not None,
        depth_imbalance=depth_imbalance,
        depth_imbalance_available=depth_imbalance is not None,
        observation_uids=tuple(item.observation_uid for item in observations_selected),
        fill_uids=tuple(item.fill_uid for item in fills_selected),
        orderbook_snapshot_uids=tuple(item.snapshot_uid for item in books_selected),
        raw_artifact_uids=tuple(sorted({item.raw_artifact_uid for item in all_records})),
    )


__all__ = ["FIVE_MINUTES", "MarketFeatureSnapshot", "PriceSeriesFeature", "build_market_feature_snapshot"]
