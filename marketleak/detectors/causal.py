"""Causal, fixed-cadence activity surveillance.

The detector answers only question A: whether observable market activity is
abnormal relative to trailing history. Scores are diagnostics, not fraud
probabilities, and thin or stale data causes abstention.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from math import log
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from marketleak.domain import (
    ActivityStatus,
    ActorVisibility,
    ObservationKind,
    OrderBookSnapshot,
    PriceObservation,
    TradeFill,
    TradeSide,
)

from .models import (
    BucketFeatures,
    DetectionBatch,
    DetectorConfig,
    DetectorSignal,
    Incident,
    MarketControl,
)
from .statistics import benjamini_hochberg, causal_tail_diagnostic


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("detector timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _bucket_end(value: datetime, width: timedelta) -> datetime:
    value = _utc(value)
    width_seconds = int(width.total_seconds())
    epoch_seconds = int(value.timestamp())
    start_seconds = epoch_seconds - epoch_seconds % width_seconds
    return datetime.fromtimestamp(start_seconds + width_seconds, tz=timezone.utc)


def _logit(price: float) -> float:
    clipped = min(max(price, 1e-6), 1.0 - 1e-6)
    return log(clipped / (1.0 - clipped))


def _semantic_observations(observations: Iterable[Any]) -> list[Any]:
    unique: dict[tuple[Any, ...], Any] = {}
    for item in observations:
        event_time = _utc(item.event_time)
        key = (
            item.market_uid,
            item.outcome_uid,
            event_time,
            str(item.price),
            str(item.kind),
        )
        current = unique.get(key)
        if current is None or str(item.observation_uid) < str(current.observation_uid):
            unique[key] = item
    return sorted(
        unique.values(),
        key=lambda item: (
            item.market_uid,
            item.outcome_uid,
            _utc(item.event_time),
            str(item.observation_uid),
        ),
    )


def _semantic_fills(fills: Iterable[Any]) -> list[TradeFill]:
    """Admit only validated TradeFill records; snapshots never become trades."""

    unique: dict[tuple[Any, ...], TradeFill] = {}
    for item in fills:
        if not isinstance(item, TradeFill):
            continue
        key = (
            item.market_uid,
            item.outcome_uid,
            item.event_time,
            item.price,
            item.size,
            item.side,
            item.actor_uid,
            item.transaction_uid,
        )
        current = unique.get(key)
        if current is None or item.fill_uid < current.fill_uid:
            unique[key] = item
    return sorted(unique.values(), key=lambda item: (item.event_time, item.fill_uid))


def _semantic_books(books: Iterable[Any]) -> list[OrderBookSnapshot]:
    unique: dict[tuple[Any, ...], OrderBookSnapshot] = {}
    for item in books:
        if not isinstance(item, OrderBookSnapshot):
            continue
        key = (item.market_uid, item.outcome_uid, item.event_time, item.bids, item.asks)
        current = unique.get(key)
        if current is None or item.snapshot_uid < current.snapshot_uid:
            unique[key] = item
    return sorted(unique.values(), key=lambda item: (item.event_time, item.snapshot_uid))


def _choose_price(items: Sequence[Any]) -> tuple[float, datetime, int] | None:
    if not items:
        return None
    by_kind: dict[ObservationKind, list[Any]] = defaultdict(list)
    for item in items:
        by_kind[item.kind].append(item)

    def latest(kind: ObservationKind) -> Any | None:
        candidates = by_kind.get(kind, [])
        return max(candidates, key=lambda item: (_utc(item.event_time), str(item.observation_uid)), default=None)

    selected = latest(ObservationKind.LAST_TRADE)
    if selected is not None:
        return float(selected.price), _utc(selected.event_time), len(items)
    selected = latest(ObservationKind.MIDPOINT)
    if selected is not None:
        return float(selected.price), _utc(selected.event_time), len(items)
    bid = latest(ObservationKind.BEST_BID)
    ask = latest(ObservationKind.BEST_ASK)
    if bid is not None and ask is not None:
        return (
            (float(bid.price) + float(ask.price)) / 2.0,
            max(_utc(bid.event_time), _utc(ask.event_time)),
            len(items),
        )
    selected = latest(ObservationKind.PLATFORM_SNAPSHOT)
    if selected is not None:
        return float(selected.price), _utc(selected.event_time), len(items)
    return None


def _fill_features(items: Sequence[TradeFill]) -> tuple[float, float, float | None, int | None, float | None]:
    if not items:
        return 0.0, 0.0, None, None, None
    volume = sum(float(item.size) for item in items)
    notional = sum(float(item.size * item.price) for item in items)
    signed_volume = sum(
        float(item.size) * (1.0 if item.side == TradeSide.BUY else -1.0)
        for item in items
    )
    imbalance = signed_volume / volume if volume > 0 else None
    actor_notionals: dict[str, float] = defaultdict(float)
    for item in items:
        if item.actor_uid is not None and item.actor_visibility in {
            ActorVisibility.PUBLIC_WALLET,
            ActorVisibility.OWN_ACCOUNT_ONLY,
        }:
            actor_notionals[item.actor_uid] += float(item.size * item.price)
    if not actor_notionals:
        return volume, notional, imbalance, None, None
    total = sum(actor_notionals.values())
    concentration = sum((value / total) ** 2 for value in actor_notionals.values()) if total > 0 else None
    return volume, notional, imbalance, len(actor_notionals), concentration


def _book_features(book: OrderBookSnapshot | None, notional: float) -> tuple[float | None, float | None, float | None]:
    if book is None or not book.bids or not book.asks:
        return None, None, None
    spread = float(book.asks[0].price - book.bids[0].price)
    depth = sum(float(level.size) for level in (*book.bids[:5], *book.asks[:5]))
    impact = notional / depth if depth > 0 else None
    return spread, depth, impact


class CausalActivityDetector:
    def __init__(self, config: DetectorConfig | None = None) -> None:
        self.config = config or DetectorConfig()

    def detect(
        self,
        observations: Iterable[PriceObservation | Any],
        *,
        fills: Iterable[TradeFill | Any] = (),
        books: Iterable[OrderBookSnapshot | Any] = (),
        controls: Iterable[MarketControl] | Mapping[str, MarketControl] = (),
        as_of: datetime | None = None,
    ) -> DetectionBatch:
        canonical_observations = _semantic_observations(observations)
        canonical_fills = _semantic_fills(fills)
        canonical_books = _semantic_books(books)
        if as_of is not None:
            cutoff = _utc(as_of)
            canonical_observations = [item for item in canonical_observations if _utc(item.event_time) <= cutoff]
            canonical_fills = [item for item in canonical_fills if _utc(item.event_time) <= cutoff]
            canonical_books = [item for item in canonical_books if _utc(item.event_time) <= cutoff]
        control_map = dict(controls) if isinstance(controls, Mapping) else {item.market_uid: item for item in controls}
        buckets = self._build_buckets(canonical_observations, canonical_fills, canonical_books, control_map)
        controlled_buckets = self._apply_sibling_controls(buckets, control_map)
        signals = self._score(controlled_buckets)
        signals_with_incidents, incidents = self._cluster(signals)
        return DetectionBatch(tuple(controlled_buckets), tuple(signals_with_incidents), tuple(incidents))

    def _build_buckets(
        self,
        observations: Sequence[Any],
        fills: Sequence[TradeFill],
        books: Sequence[OrderBookSnapshot],
        controls: Mapping[str, MarketControl],
    ) -> list[BucketFeatures]:
        observation_groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
        for item in observations:
            observation_groups[(item.market_uid, item.outcome_uid)].append(item)
        fill_buckets: dict[tuple[str, str, datetime], list[TradeFill]] = defaultdict(list)
        for item in fills:
            fill_buckets[(item.market_uid, item.outcome_uid, _bucket_end(item.event_time, self.config.bucket_width))].append(item)
        books_by_series: dict[tuple[str, str], list[OrderBookSnapshot]] = defaultdict(list)
        for item in books:
            books_by_series[(item.market_uid, item.outcome_uid)].append(item)

        result: list[BucketFeatures] = []
        for series, series_observations in sorted(observation_groups.items()):
            market_uid, outcome_uid = series
            by_bucket: dict[datetime, list[Any]] = defaultdict(list)
            for item in series_observations:
                by_bucket[_bucket_end(item.event_time, self.config.bucket_width)].append(item)
            first_end = min(by_bucket)
            last_end = max(by_bucket)
            current_end = first_end
            last_price: float | None = None
            last_source_time: datetime | None = None
            prices: list[float | None] = []
            book_index = 0
            current_book: OrderBookSnapshot | None = None
            series_books = books_by_series.get(series, [])
            while current_end <= last_end:
                chosen = _choose_price(by_bucket.get(current_end, ()))
                source_count = 0
                if chosen is not None:
                    last_price, last_source_time, source_count = chosen
                carry_age = (
                    (current_end - last_source_time).total_seconds()
                    if last_source_time is not None
                    else None
                )
                price = last_price
                if carry_age is None or carry_age > self.config.max_carry_age.total_seconds():
                    price = None
                while book_index < len(series_books) and _utc(series_books[book_index].event_time) <= current_end:
                    current_book = series_books[book_index]
                    book_index += 1
                usable_book = current_book
                if usable_book is not None and current_end - _utc(usable_book.event_time) > self.config.max_carry_age:
                    usable_book = None
                current_fills = fill_buckets.get((market_uid, outcome_uid, current_end), ())
                volume, notional, imbalance, actor_count, concentration = _fill_features(current_fills)
                spread, depth, impact = _book_features(usable_book, notional)
                prices.append(price)
                move_5m = None
                move_30m = None
                if price is not None and len(prices) >= 2 and prices[-2] is not None:
                    move_5m = _logit(price) - _logit(prices[-2])
                if price is not None and len(prices) >= 7 and prices[-7] is not None:
                    move_30m = _logit(price) - _logit(prices[-7])
                tags = self._control_tags(market_uid, current_end, depth, controls)
                result.append(
                    BucketFeatures(
                        market_uid=market_uid,
                        outcome_uid=outcome_uid,
                        bucket_time=current_end,
                        price=price,
                        source_event_time=last_source_time if price is not None else None,
                        carry_age_seconds=carry_age,
                        move_5m_logit=move_5m,
                        move_30m_logit=move_30m,
                        volume=volume,
                        notional=notional,
                        aggressor_imbalance=imbalance,
                        spread=spread,
                        depth=depth,
                        impact=impact,
                        unique_actor_count=actor_count,
                        actor_concentration=concentration,
                        control_tags=tags,
                        source_observation_count=source_count,
                        source_fill_count=len(current_fills),
                    )
                )
                current_end += self.config.bucket_width
        return sorted(result, key=lambda item: (item.bucket_time, item.market_uid, item.outcome_uid))

    def _control_tags(
        self,
        market_uid: str,
        bucket_time: datetime,
        depth: float | None,
        controls: Mapping[str, MarketControl],
    ) -> tuple[str, ...]:
        tags: list[str] = []
        control = controls.get(market_uid)
        if control is None:
            return ("control_metadata_unavailable",)
        if control.live_event:
            tags.append("live_event")
        if control.scheduled_announcement_at is not None:
            announcement = _utc(control.scheduled_announcement_at)
            if abs(bucket_time - announcement) <= self.config.scheduled_window:
                tags.append("scheduled_announcement_window")
        if control.closes_at is not None:
            remaining = _utc(control.closes_at) - bucket_time
            if timedelta(0) <= remaining <= self.config.near_close_window:
                tags.append("time_to_close_lt_1h")
        liquidity = control.liquidity if control.liquidity is not None else depth
        if liquidity is None:
            tags.append("liquidity_unknown")
        elif liquidity < self.config.thin_liquidity_threshold:
            tags.append("thin_liquidity")
        return tuple(sorted(tags))

    def _apply_sibling_controls(
        self,
        buckets: Sequence[BucketFeatures],
        controls: Mapping[str, MarketControl],
    ) -> list[BucketFeatures]:
        by_event_time: dict[tuple[str, datetime], list[BucketFeatures]] = defaultdict(list)
        for bucket in buckets:
            control = controls.get(bucket.market_uid)
            if control is not None and control.event_uid is not None and bucket.move_5m_logit is not None:
                by_event_time[(control.event_uid, bucket.bucket_time)].append(bucket)
        result: list[BucketFeatures] = []
        for bucket in buckets:
            control = controls.get(bucket.market_uid)
            if control is None or control.event_uid is None or bucket.move_5m_logit is None:
                result.append(bucket)
                continue
            siblings = [
                item.move_5m_logit
                for item in by_event_time.get((control.event_uid, bucket.bucket_time), ())
                if item.market_uid != bucket.market_uid and item.move_5m_logit is not None
            ]
            if not siblings:
                result.append(bucket)
                continue
            sibling_move = float(median(siblings))
            residual = bucket.move_5m_logit - sibling_move
            tags = list(bucket.control_tags)
            same_direction = bucket.move_5m_logit * sibling_move > 0
            controlled = abs(residual) <= self.config.sibling_control_ratio * max(abs(bucket.move_5m_logit), 1e-12)
            if same_direction and controlled:
                tags.append("sibling_repricing")
            result.append(
                replace(
                    bucket,
                    sibling_residual=residual,
                    sibling_count=len(siblings),
                    control_tags=tuple(sorted(set(tags))),
                )
            )
        return result

    @staticmethod
    def _component_values(bucket: BucketFeatures) -> dict[str, float]:
        values: dict[str, float | None] = {
            "price_5m": bucket.primary_price_move,
            "price_30m": None if bucket.move_30m_logit is None else abs(bucket.move_30m_logit),
            "volume": bucket.volume,
            "notional": bucket.notional,
            "aggressor_imbalance": None if bucket.aggressor_imbalance is None else abs(bucket.aggressor_imbalance),
            "spread": bucket.spread,
            "impact": bucket.impact,
            "actor_concentration": bucket.actor_concentration,
        }
        return {key: value for key, value in values.items() if value is not None}

    def _score(self, buckets: Sequence[BucketFeatures]) -> list[DetectorSignal]:
        series_history: dict[tuple[str, str], list[BucketFeatures]] = defaultdict(list)
        provisional: list[DetectorSignal] = []
        for bucket in sorted(buckets, key=lambda item: (item.bucket_time, item.market_uid, item.outcome_uid)):
            series = (bucket.market_uid, bucket.outcome_uid)
            history = [
                item
                for item in series_history[series]
                if bucket.bucket_time - item.bucket_time <= self.config.baseline_window
            ]
            values = self._component_values(bucket)
            primary = values.get("price_5m")
            reasons: list[str] = []
            diagnostics = {}
            if bucket.price is None or primary is None:
                reasons.append("stale_or_missing_price")
            elif len(history) < self.config.min_baseline_observations:
                reasons.append("insufficient_baseline_observations")
            elif bucket.bucket_time - history[0].bucket_time < self.config.min_baseline_elapsed:
                reasons.append("insufficient_baseline_elapsed")
            else:
                for name, current_value in values.items():
                    component_history = [
                        prior_value
                        for item in history
                        if (prior_value := self._component_values(item).get(name)) is not None
                    ]
                    diagnostic = causal_tail_diagnostic(
                        current_value,
                        component_history,
                        min_observations=self.config.min_baseline_observations,
                        empirical_tail_min_observations=self.config.empirical_tail_min_observations,
                    )
                    if diagnostic is not None:
                        diagnostics[name] = diagnostic
                if "price_5m" not in diagnostics:
                    reasons.append("zero_mad_or_thin_empirical_history")
            if "price_5m" not in diagnostics:
                provisional.append(
                    DetectorSignal(
                        market_uid=bucket.market_uid,
                        outcome_uid=bucket.outcome_uid,
                        bucket_time=bucket.bucket_time,
                        status=ActivityStatus.NOT_SCORABLE,
                        score=None,
                        p_value=None,
                        q_value=None,
                        rank=None,
                        control_tags=bucket.control_tags,
                        reason_codes=tuple(reasons),
                    )
                )
            else:
                primary_diagnostic = diagnostics["price_5m"]
                provisional.append(
                    DetectorSignal(
                        market_uid=bucket.market_uid,
                        outcome_uid=bucket.outcome_uid,
                        bucket_time=bucket.bucket_time,
                        status=ActivityStatus.NORMAL,
                        score=primary_diagnostic.score,
                        p_value=primary_diagnostic.p_value,
                        q_value=None,
                        rank=None,
                        component_scores={name: value.score for name, value in diagnostics.items()},
                        component_p_values={name: value.p_value for name, value in diagnostics.items()},
                        diagnostics=diagnostics,
                        control_tags=bucket.control_tags,
                        reason_codes=(),
                    )
                )
            series_history[series].append(bucket)

        by_time: dict[datetime, list[int]] = defaultdict(list)
        for index, signal in enumerate(provisional):
            if signal.p_value is not None:
                by_time[signal.bucket_time].append(index)
        for indices in by_time.values():
            adjusted = benjamini_hochberg([provisional[index].p_value for index in indices])  # type: ignore[list-item]
            for index, (q_value, rank) in zip(indices, adjusted):
                signal = provisional[index]
                status = ActivityStatus.ABNORMAL if q_value <= self.config.fdr_alpha else ActivityStatus.NORMAL
                provisional[index] = replace(signal, q_value=q_value, rank=rank, status=status)
        return provisional

    def _cluster(self, signals: Sequence[DetectorSignal]) -> tuple[list[DetectorSignal], list[Incident]]:
        abnormal_by_series: dict[tuple[str, str], list[DetectorSignal]] = defaultdict(list)
        for signal in signals:
            if signal.status == ActivityStatus.ABNORMAL:
                abnormal_by_series[(signal.market_uid, signal.outcome_uid)].append(signal)
        signal_incidents: dict[tuple[str, str, datetime], str] = {}
        incidents: list[Incident] = []
        for series, abnormal in sorted(abnormal_by_series.items()):
            abnormal.sort(key=lambda item: item.bucket_time)
            clusters: list[list[DetectorSignal]] = []
            for signal in abnormal:
                if not clusters or signal.bucket_time - clusters[-1][-1].bucket_time > self.config.incident_cooldown:
                    clusters.append([signal])
                else:
                    clusters[-1].append(signal)
            for cluster in clusters:
                market_uid, outcome_uid = series
                started_at, ended_at = cluster[0].bucket_time, cluster[-1].bucket_time
                digest = sha256(f"{market_uid}|{outcome_uid}|{started_at.isoformat()}".encode()).hexdigest()[:20]
                incident_uid = f"incident:{digest}"
                peak = max(cluster, key=lambda item: (item.score or 0.0, -item.bucket_time.timestamp()))
                tags = tuple(sorted({tag for item in cluster for tag in item.control_tags}))
                incidents.append(
                    Incident(
                        incident_uid=incident_uid,
                        market_uid=market_uid,
                        outcome_uid=outcome_uid,
                        started_at=started_at,
                        ended_at=ended_at,
                        peak_time=peak.bucket_time,
                        peak_score=peak.score or 0.0,
                        signal_count=len(cluster),
                        control_tags=tags,
                    )
                )
                for signal in cluster:
                    signal_incidents[(signal.market_uid, signal.outcome_uid, signal.bucket_time)] = incident_uid
        updated = [
            replace(
                signal,
                incident_uid=signal_incidents.get((signal.market_uid, signal.outcome_uid, signal.bucket_time)),
            )
            for signal in signals
        ]
        return updated, sorted(incidents, key=lambda item: (item.started_at, item.market_uid, item.outcome_uid))


@dataclass(frozen=True)
class _LegacyObservation:
    observation_uid: str
    market_uid: str
    outcome_uid: str
    event_time: datetime
    price: float
    kind: ObservationKind = ObservationKind.PLATFORM_SNAPSHOT


def detect_legacy_ticks(data: Any, config: DetectorConfig | None = None) -> DetectionBatch:
    """Evaluate a legacy ticks DataFrame/path without inventing missing data."""

    import pandas as pd

    frame = pd.read_parquet(data) if isinstance(data, (str, bytes)) else data.copy()
    observations = []
    for row in frame.to_dict("records"):
        timestamp = datetime.fromtimestamp(int(row["timestamp"]), tz=timezone.utc)
        market_uid = str(row["market_uid"])
        observations.append(
            _LegacyObservation(
                observation_uid=str(row["tick_uid"]),
                market_uid=market_uid,
                outcome_uid=f"{market_uid}-default",
                event_time=timestamp,
                price=float(row["price"]),
            )
        )
    return CausalActivityDetector(config).detect(observations)


detect_activity = CausalActivityDetector().detect
