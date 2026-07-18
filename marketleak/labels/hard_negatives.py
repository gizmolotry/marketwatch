"""Synthetic scenarios that test false-positive mechanics, never ground truth."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Callable

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .schemas import HardNegativeType, LabelTarget, LabelValue, LabelWindow


class MechanicEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, str_strip_whitespace=True)

    event_uid: str = Field(min_length=1)
    event_cluster_uid: str = Field(min_length=1)
    market_uid: str
    starts_at: datetime
    ends_at: datetime
    evidence_refs: tuple[str, ...] = ()

    @field_validator("starts_at", "ends_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("mechanic timestamps must be timezone-aware")
        return value.astimezone(UTC)


DEFAULT_SAMPLING_WEIGHTS: dict[HardNegativeType, Decimal] = {
    HardNegativeType.SCHEDULED_NEWS: Decimal("1.0"),
    HardNegativeType.LIVE_SPORTS: Decimal("1.5"),
    HardNegativeType.SIBLING_REPRICING: Decimal("1.25"),
    HardNegativeType.LOW_LIQUIDITY_PRINT: Decimal("2.0"),
    HardNegativeType.STALE_CATCH_UP: Decimal("1.5"),
    HardNegativeType.RESOLUTION: Decimal("0.5"),
    HardNegativeType.OUTAGE_RECOVERY: Decimal("2.0"),
    HardNegativeType.MARKET_MAKER_REBALANCE: Decimal("1.5"),
}

PUBLICLY_EXPLAINED = {
    HardNegativeType.SCHEDULED_NEWS,
    HardNegativeType.LIVE_SPORTS,
    HardNegativeType.SIBLING_REPRICING,
    HardNegativeType.RESOLUTION,
}


def _window(event: MechanicEvent, kind: HardNegativeType, weight: Decimal | None = None) -> LabelWindow:
    if event.ends_at <= event.starts_at:
        raise ValueError("mechanic ends_at must be after starts_at")
    b_value = LabelValue.NEGATIVE if kind in PUBLICLY_EXPLAINED else LabelValue.UNKNOWN
    return LabelWindow(
        window_uid=f"labels:synthetic/{kind.value}/{event.event_uid}",
        case_uid="labels:synthetic-hard-negatives-v1",
        starts_at=event.starts_at,
        ends_at=event.ends_at,
        event_cluster_uid=event.event_cluster_uid,
        market_uid=event.market_uid,
        labels={
            LabelTarget.ACTIVITY_A: LabelValue.POSITIVE,
            LabelTarget.PUBLIC_EXPLANATION_B: b_value,
            LabelTarget.ACTOR_EVIDENCE_C: LabelValue.UNKNOWN,
        },
        sampling_weight=weight or DEFAULT_SAMPLING_WEIGHTS[kind],
        hard_negative_type=kind,
        synthetic=True,
        training_eligible=False,
        evidence_refs=event.evidence_refs,
    )


def scheduled_news(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.SCHEDULED_NEWS, weight)


def live_sports(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.LIVE_SPORTS, weight)


def sibling_repricing(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.SIBLING_REPRICING, weight)


def low_liquidity_print(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.LOW_LIQUIDITY_PRINT, weight)


def stale_catch_up(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.STALE_CATCH_UP, weight)


def resolution(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.RESOLUTION, weight)


def outage_recovery(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.OUTAGE_RECOVERY, weight)


def market_maker_rebalance(event: MechanicEvent, weight: Decimal | None = None) -> LabelWindow:
    return _window(event, HardNegativeType.MARKET_MAKER_REBALANCE, weight)


HARD_NEGATIVE_GENERATORS: dict[
    HardNegativeType, Callable[[MechanicEvent, Decimal | None], LabelWindow]
] = {
    HardNegativeType.SCHEDULED_NEWS: scheduled_news,
    HardNegativeType.LIVE_SPORTS: live_sports,
    HardNegativeType.SIBLING_REPRICING: sibling_repricing,
    HardNegativeType.LOW_LIQUIDITY_PRINT: low_liquidity_print,
    HardNegativeType.STALE_CATCH_UP: stale_catch_up,
    HardNegativeType.RESOLUTION: resolution,
    HardNegativeType.OUTAGE_RECOVERY: outage_recovery,
    HardNegativeType.MARKET_MAKER_REBALANCE: market_maker_rebalance,
}


def generate_hard_negatives(
    event: MechanicEvent,
    *,
    weights: dict[HardNegativeType, Decimal] | None = None,
) -> tuple[LabelWindow, ...]:
    return tuple(
        generator(event, (weights or {}).get(kind))
        for kind, generator in HARD_NEGATIVE_GENERATORS.items()
    )
