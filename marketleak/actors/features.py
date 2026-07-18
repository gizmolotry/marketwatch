"""Causal actor positions and performance features from genuine fills only."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Mapping

from marketleak.domain import ActorVisibility, Outcome, TradeFill, TradeSide


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("actor feature cutoff must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ActorPosition:
    actor_uid: str
    actor_visibility: ActorVisibility
    outcome_uid: str
    net_size: Decimal
    gross_buy_size: Decimal
    gross_sell_size: Decimal
    gross_notional: Decimal
    net_cashflow: Decimal
    fill_count: int


@dataclass(frozen=True, slots=True)
class ActorFeatures:
    actor_uid: str
    actor_visibility: ActorVisibility
    cutoff: datetime
    fill_count: int
    market_count: int
    outcome_count: int
    gross_notional: Decimal
    directional_concentration: Decimal
    positions: tuple[ActorPosition, ...]
    resolved_fill_count: int
    realized_performance: Decimal | None


def _eligible_fills(fills: Iterable[object], cutoff: datetime) -> list[TradeFill]:
    """Reject non-fill records and future fills rather than inferring trades."""

    cutoff = _utc(cutoff)
    unique: dict[tuple[object, ...], TradeFill] = {}
    for item in fills:
        if not isinstance(item, TradeFill) or item.event_time > cutoff:
            continue
        if item.actor_uid is None or item.actor_visibility not in {
            ActorVisibility.PUBLIC_WALLET,
            ActorVisibility.OWN_ACCOUNT_ONLY,
        }:
            continue
        key = (
            item.actor_uid,
            item.market_uid,
            item.outcome_uid,
            item.event_time,
            item.price,
            item.size,
            item.side,
            item.transaction_uid,
        )
        current = unique.get(key)
        if current is None or item.fill_uid < current.fill_uid:
            unique[key] = item
    return sorted(unique.values(), key=lambda item: (item.event_time, item.fill_uid))


def build_actor_positions(fills: Iterable[object], *, cutoff: datetime) -> tuple[ActorPosition, ...]:
    eligible = _eligible_fills(fills, cutoff)
    grouped: dict[tuple[str, ActorVisibility, str], list[TradeFill]] = {}
    for fill in eligible:
        grouped.setdefault((fill.actor_uid or "", fill.actor_visibility, fill.outcome_uid), []).append(fill)
    positions: list[ActorPosition] = []
    for (actor_uid, visibility, outcome_uid), actor_fills in sorted(grouped.items(), key=lambda item: item[0][0:3:2]):
        buys = [item for item in actor_fills if item.side == TradeSide.BUY]
        sells = [item for item in actor_fills if item.side == TradeSide.SELL]
        gross_buy = sum((item.size for item in buys), Decimal("0"))
        gross_sell = sum((item.size for item in sells), Decimal("0"))
        buy_notional = sum((item.size * item.price for item in buys), Decimal("0"))
        sell_notional = sum((item.size * item.price for item in sells), Decimal("0"))
        positions.append(
            ActorPosition(
                actor_uid=actor_uid,
                actor_visibility=visibility,
                outcome_uid=outcome_uid,
                net_size=gross_buy - gross_sell,
                gross_buy_size=gross_buy,
                gross_sell_size=gross_sell,
                gross_notional=buy_notional + sell_notional,
                net_cashflow=sell_notional - buy_notional,
                fill_count=len(actor_fills),
            )
        )
    return tuple(positions)


def build_actor_features(
    fills: Iterable[object],
    *,
    outcomes: Iterable[Outcome] = (),
    cutoff: datetime,
) -> tuple[ActorFeatures, ...]:
    """Build actor features with a hard causal cutoff.

    Performance uses only outcomes whose resolution was already known at the
    cutoff. Open and future-resolved outcomes are excluded, not imputed.
    """

    cutoff = _utc(cutoff)
    eligible = _eligible_fills(fills, cutoff)
    positions = build_actor_positions(eligible, cutoff=cutoff)
    outcome_map: Mapping[str, Outcome] = {
        outcome.outcome_uid: outcome
        for outcome in outcomes
        if outcome.resolved_at is not None
        and outcome.resolved_value is not None
        and outcome.resolved_at <= cutoff
    }
    fills_by_actor: dict[str, list[TradeFill]] = {}
    for fill in eligible:
        fills_by_actor.setdefault(fill.actor_uid or "", []).append(fill)
    positions_by_actor: dict[str, list[ActorPosition]] = {}
    for position in positions:
        positions_by_actor.setdefault(position.actor_uid, []).append(position)

    result: list[ActorFeatures] = []
    for actor_uid, actor_fills in sorted(fills_by_actor.items()):
        actor_positions = positions_by_actor[actor_uid]
        gross_notional = sum((position.gross_notional for position in actor_positions), Decimal("0"))
        directional_notionals: dict[str, Decimal] = {}
        for fill in actor_fills:
            signed = fill.size * fill.price * (Decimal("1") if fill.side == TradeSide.BUY else Decimal("-1"))
            directional_notionals[fill.outcome_uid] = directional_notionals.get(fill.outcome_uid, Decimal("0")) + signed
        if gross_notional > 0:
            directional_concentration = max(
                (abs(value) / gross_notional for value in directional_notionals.values()),
                default=Decimal("0"),
            )
        else:
            directional_concentration = Decimal("0")
        performance = Decimal("0")
        resolved_fill_count = 0
        for fill in actor_fills:
            outcome = outcome_map.get(fill.outcome_uid)
            if outcome is None or outcome.resolved_value is None:
                continue
            resolved_fill_count += 1
            if fill.side == TradeSide.BUY:
                performance += fill.size * (outcome.resolved_value - fill.price)
            else:
                performance += fill.size * (fill.price - outcome.resolved_value)
        result.append(
            ActorFeatures(
                actor_uid=actor_uid,
                actor_visibility=actor_fills[0].actor_visibility,
                cutoff=cutoff,
                fill_count=len(actor_fills),
                market_count=len({item.market_uid for item in actor_fills}),
                outcome_count=len({item.outcome_uid for item in actor_fills}),
                gross_notional=gross_notional,
                directional_concentration=directional_concentration,
                positions=tuple(actor_positions),
                resolved_fill_count=resolved_fill_count,
                realized_performance=performance if resolved_fill_count else None,
            )
        )
    return tuple(result)


class ActorFeatureBuilder:
    def build(
        self,
        fills: Iterable[object],
        *,
        outcomes: Iterable[Outcome] = (),
        cutoff: datetime,
    ) -> tuple[ActorFeatures, ...]:
        return build_actor_features(fills, outcomes=outcomes, cutoff=cutoff)
