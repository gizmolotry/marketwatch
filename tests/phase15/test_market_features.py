"""Minimal fixtures for market-feature mechanics, never effectiveness evidence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
    TradeFill,
    TradeSide,
)
from marketleak.multimodal.features import build_market_feature_snapshot


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
MARKET_UID = "polymarket:market/mechanics"
OUTCOME_UID = "polymarket:outcome/mechanics-yes"


def mechanics_observation(
    *,
    kind: ObservationKind,
    price: str,
    minute: int,
    uid: str,
    raw_frame: str,
    ingested_second: int = 1,
) -> PriceObservation:
    return PriceObservation(
        observation_uid=f"polymarket:{uid}",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        price=Decimal(price),
        kind=kind,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=minute, seconds=ingested_second),
        source_uid="polymarket:source/ws-market",
        raw_artifact_uid=f"raw:{raw_frame}",
        parser_version="mechanics-fixture",
    )


def mechanics_fill(*, price: str, minute: int, uid: str, raw_frame: str) -> TradeFill:
    return TradeFill(
        fill_uid=f"polymarket:{uid}",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        price=Decimal(price),
        size=Decimal("2"),
        side=TradeSide.BUY,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=minute, seconds=1),
        source_uid="polymarket:source/ws-market",
        raw_artifact_uid=f"raw:{raw_frame}",
        parser_version="mechanics-fixture",
    )


def mechanics_book(
    *,
    minute: int,
    uid: str,
    bids: tuple[tuple[str, str], ...] = (),
    asks: tuple[tuple[str, str], ...] = (),
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        snapshot_uid=f"polymarket:{uid}",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        bids=tuple(OrderBookLevel(price=Decimal(price), size=Decimal(size)) for price, size in bids),
        asks=tuple(OrderBookLevel(price=Decimal(price), size=Decimal(size)) for price, size in asks),
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=minute, seconds=1),
        source_uid="polymarket:source/ws-market",
        raw_artifact_uid=f"raw:{uid}",
        parser_version="mechanics-fixture",
    )


def build(*, observations=(), fills=(), books=()):
    return build_market_feature_snapshot(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=observations,
        fills=fills,
        books=books,
    )


def test_bbo_only_series_do_not_fabricate_a_primary_trade_change_or_follow_uid_order():
    observations = (
        mechanics_observation(
            kind=ObservationKind.BEST_BID,
            price="0.48",
            minute=1,
            uid="z-first-bid",
            raw_frame="bbo-1",
        ),
        mechanics_observation(
            kind=ObservationKind.BEST_ASK,
            price="0.52",
            minute=1,
            uid="a-first-ask",
            raw_frame="bbo-1",
        ),
        mechanics_observation(
            kind=ObservationKind.BEST_BID,
            price="0.52",
            minute=4,
            uid="a-last-bid",
            raw_frame="bbo-2",
        ),
        mechanics_observation(
            kind=ObservationKind.BEST_ASK,
            price="0.56",
            minute=4,
            uid="z-last-ask",
            raw_frame="bbo-2",
        ),
    )

    snapshot = build(observations=reversed(observations))

    assert snapshot.price_open is None
    assert snapshot.price_close is None
    assert snapshot.price_change is None
    assert snapshot.primary_price_kind == ObservationKind.LAST_TRADE
    assert snapshot.last_trade_series.observed is False
    assert snapshot.primary_price_change_missing_reason
    assert snapshot.best_bid_series.price_change == Decimal("0.04")
    assert snapshot.best_ask_series.price_change == Decimal("0.04")
    assert snapshot.midpoint_series.price_open == Decimal("0.50")
    assert snapshot.midpoint_series.price_close == Decimal("0.54")
    assert snapshot.midpoint_series.price_change == Decimal("0.04")
    assert snapshot.quoted_spread == Decimal("0.04")


def test_trade_primary_series_deduplicates_matching_fill_observations_and_ignores_bbo():
    fills = (
        mechanics_fill(price="0.45", minute=1, uid="fill-1", raw_frame="trade-1"),
        mechanics_fill(price="0.55", minute=4, uid="fill-2", raw_frame="trade-2"),
    )
    observations = (
        mechanics_observation(
            kind=ObservationKind.LAST_TRADE,
            price="0.45",
            minute=1,
            uid="trade-observation-1",
            raw_frame="trade-1",
        ),
        mechanics_observation(
            kind=ObservationKind.BEST_BID,
            price="0.10",
            minute=2,
            uid="bbo-bid",
            raw_frame="bbo",
        ),
        mechanics_observation(
            kind=ObservationKind.BEST_ASK,
            price="0.90",
            minute=2,
            uid="bbo-ask",
            raw_frame="bbo",
        ),
        mechanics_observation(
            kind=ObservationKind.LAST_TRADE,
            price="0.55",
            minute=4,
            uid="trade-observation-2",
            raw_frame="trade-2",
        ),
    )

    snapshot = build(observations=observations, fills=fills)

    assert snapshot.last_trade_series.point_count == 2
    assert snapshot.last_trade_series.distinct_event_time_count == 2
    assert snapshot.price_open == Decimal("0.45")
    assert snapshot.price_close == Decimal("0.55")
    assert snapshot.price_change == Decimal("0.10")
    assert snapshot.midpoint_series.price_open == Decimal("0.50")
    assert snapshot.midpoint_series.change_available is False


def test_one_sided_bid_and_ask_books_keep_independent_values_and_masks():
    bid_only = build(books=(mechanics_book(minute=2, uid="bid-book", bids=(("0.49", "2"),)),))
    ask_only = build(books=(mechanics_book(minute=2, uid="ask-book", asks=(("0.53", "3"),)),))

    assert bid_only.best_bid == Decimal("0.49")
    assert bid_only.best_bid_observed is True
    assert bid_only.bid_depth == Decimal("2")
    assert bid_only.bid_depth_observed is True
    assert bid_only.best_ask is None
    assert bid_only.best_ask_observed is False
    assert bid_only.ask_depth is None
    assert bid_only.quoted_spread is None
    assert bid_only.quoted_spread_available is False

    assert ask_only.best_ask == Decimal("0.53")
    assert ask_only.best_ask_observed is True
    assert ask_only.ask_depth == Decimal("3")
    assert ask_only.ask_depth_observed is True
    assert ask_only.best_bid is None
    assert ask_only.best_bid_observed is False
    assert ask_only.bid_depth is None
    assert ask_only.quoted_spread is None


def test_stale_asynchronous_sides_are_not_paired_for_midpoint_spread_or_depth_imbalance():
    snapshot = build(
        observations=(
            mechanics_observation(
                kind=ObservationKind.BEST_BID,
                price="0.49",
                minute=1,
                uid="stale-bid",
                raw_frame="bid-frame",
            ),
            mechanics_observation(
                kind=ObservationKind.BEST_ASK,
                price="0.53",
                minute=4,
                uid="newer-ask",
                raw_frame="ask-frame",
            ),
        ),
        books=(
            mechanics_book(minute=1, uid="bid-depth-frame", bids=(("0.49", "2"),)),
            mechanics_book(minute=4, uid="ask-depth-frame", asks=(("0.53", "3"),)),
        ),
    )

    assert snapshot.best_bid == Decimal("0.49")
    assert snapshot.best_ask == Decimal("0.53")
    assert snapshot.best_bid_observed is True and snapshot.best_ask_observed is True
    assert snapshot.midpoint_series.observed is False
    assert snapshot.quoted_spread is None
    assert "same causal quote frame" in snapshot.quoted_spread_missing_reason
    assert snapshot.bid_depth == Decimal("2")
    assert snapshot.ask_depth == Decimal("3")
    assert snapshot.depth_imbalance is None
    assert snapshot.depth_imbalance_available is False


def test_direct_midpoint_is_admissible_but_never_becomes_a_trade_alias():
    snapshot = build(
        observations=(
            mechanics_observation(
                kind=ObservationKind.MIDPOINT,
                price="0.50",
                minute=1,
                uid="midpoint-1",
                raw_frame="midpoint-1",
            ),
            mechanics_observation(
                kind=ObservationKind.MIDPOINT,
                price="0.54",
                minute=4,
                uid="midpoint-2",
                raw_frame="midpoint-2",
            ),
        )
    )

    assert snapshot.midpoint_series.price_change == Decimal("0.04")
    assert snapshot.price_change is None
    assert snapshot.last_trade_series.observed is False
