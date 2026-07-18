from datetime import datetime, timedelta, timezone
from decimal import Decimal

from marketleak.actors import build_actor_features
from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    Outcome,
    PriceObservation,
    TradeFill,
    TradeSide,
)


NOW = datetime(2026, 1, 10, tzinfo=timezone.utc)


def lineage(event_time):
    return {
        "event_time": event_time,
        "ingested_at": event_time + timedelta(seconds=1),
        "source_uid": "test:source",
        "raw_artifact_uid": f"raw:{int(event_time.timestamp())}",
        "parser_version": "2.0.0",
    }


def fill(uid: str, event_time: datetime, side: TradeSide = TradeSide.BUY) -> TradeFill:
    return TradeFill(
        fill_uid=f"test:{uid}",
        market_uid="test:m1",
        outcome_uid="test:yes",
        platform="test",
        price=Decimal("0.40"),
        size=Decimal("10"),
        side=side,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid="test:wallet-1",
        **lineage(event_time),
    )


def outcome(resolved_at: datetime) -> Outcome:
    return Outcome(
        outcome_uid="test:yes",
        market_uid="test:m1",
        platform="test",
        source_outcome_id="yes",
        label="Yes",
        resolved_value=Decimal("1"),
        resolved_at=resolved_at,
        **lineage(resolved_at),
    )


def test_price_snapshot_cannot_create_actor_features() -> None:
    snapshot = PriceObservation(
        observation_uid="test:obs-1",
        market_uid="test:m1",
        outcome_uid="test:yes",
        platform="test",
        price=Decimal("0.5"),
        kind=ObservationKind.PLATFORM_SNAPSHOT,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        **lineage(NOW - timedelta(hours=1)),
    )
    assert build_actor_features([snapshot], cutoff=NOW) == ()


def test_cutoff_excludes_future_fills_and_future_outcomes() -> None:
    past = fill("fill-1", NOW - timedelta(hours=2))
    future = fill("fill-2", NOW + timedelta(hours=2))
    features = build_actor_features([past, future], outcomes=[outcome(NOW + timedelta(days=1))], cutoff=NOW)
    assert len(features) == 1
    assert features[0].fill_count == 1
    assert features[0].resolved_fill_count == 0
    assert features[0].realized_performance is None


def test_only_already_resolved_performance_is_computed() -> None:
    trade = fill("fill-1", NOW - timedelta(days=2))
    features = build_actor_features([trade], outcomes=[outcome(NOW - timedelta(days=1))], cutoff=NOW)
    assert features[0].resolved_fill_count == 1
    assert features[0].realized_performance == Decimal("6.00")
    assert features[0].positions[0].net_size == Decimal("10")
