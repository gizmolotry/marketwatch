"""Deterministic, label-free public-wallet cohort ranking.

The module consumes only canonical ``TradeFill`` facts that pass both their
event and ingestion clocks.  It describes peer-relative behavioral novelty for
human triage.  It has no outcome, legal, identity, guilt, or training-label
input and produces no misconduct probability.
"""

from __future__ import annotations

import json
import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from hashlib import sha256
from typing import Any, Iterable, Literal, Mapping

from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill, TradeSide
from marketleak.ingestion.normalize import canonical_json_bytes


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_POLYMARKET_COMPLETE_TRADE_SOURCES = frozenset(
    {"polymarket:source/data-api-trades", "polymarket:data-api-trades"}
)
_POLYMARKET_COMPLETE_TRADE_DATASET = "public_market_trades"
_ANOMALY_FEATURE_NAMES = frozenset(
    {
        "fill_count",
        "gross_notional",
        "buy_notional",
        "sell_notional",
        "market_count",
        "outcome_count",
        "buy_notional_share",
        "directional_concentration",
        "dominant_market_share",
        "max_fill_share",
        "active_span_seconds",
        "burst_notional_share",
        "burst_fill_share",
        "long_shot_buy_notional_share",
        "focus_buy_notional",
        "focus_net_notional",
        "focus_gross_notional_share",
        "focus_outcome_buy_notional",
        "focus_outcome_net_notional",
    }
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _text(value: str, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _time(value: datetime) -> str:
    return _utc(value, "datetime").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


@dataclass(frozen=True, slots=True)
class CohortQueryFilter:
    name: str
    value_json: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _text(self.name, "query filter name"))
        try:
            value = json.loads(self.value_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("query filter value_json must be valid JSON") from exc
        if canonical_json_bytes(value).decode("utf-8") != self.value_json:
            raise ValueError("query filter value_json must be canonical JSON")

    @classmethod
    def from_value(cls, name: str, value: Any) -> "CohortQueryFilter":
        return cls(name, canonical_json_bytes(value).decode("utf-8"))

    @property
    def value(self) -> Any:
        return json.loads(self.value_json)

    def to_payload(self) -> dict[str, Any]:
        return {"name": self.name, "value": self.value}


@dataclass(frozen=True, slots=True)
class CohortMarketScopeBinding:
    canonical_market_uid: str
    source_query_market_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_market_uid", _text(self.canonical_market_uid, "canonical_market_uid"))
        object.__setattr__(self, "source_query_market_id", _text(self.source_query_market_id, "source_query_market_id"))

    def to_payload(self) -> dict[str, str]:
        return {
            "canonical_market_uid": self.canonical_market_uid,
            "source_query_market_id": self.source_query_market_id,
        }


def _validate_polymarket_trade_filters(filters: tuple[CohortQueryFilter, ...]) -> None:
    by_name = {item.name: item.value for item in filters}
    allowed = {"market", "start", "end", "takerOnly", "limit", "offset"}
    unexpected = set(by_name) - allowed
    if unexpected:
        raise ValueError(f"Polymarket population query contains cohort-narrowing filters: {sorted(unexpected)}")
    if by_name.get("takerOnly") is not False:
        raise ValueError("Polymarket complete-trade acquisition requires explicit takerOnly=false")
    if "limit" in by_name and (isinstance(by_name["limit"], bool) or not isinstance(by_name["limit"], int) or by_name["limit"] <= 0):
        raise ValueError("Polymarket query limit must be a positive integer")
    if "offset" in by_name and (isinstance(by_name["offset"], bool) or not isinstance(by_name["offset"], int) or by_name["offset"] < 0):
        raise ValueError("Polymarket query offset must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class PopulationCoverageSlice:
    """One bounded, exhausted page-range query in a population union."""

    source_uid: str
    interval_start: datetime
    interval_end: datetime
    retrieved_at: datetime
    complete_through: datetime
    status: CoverageStatus
    record_count: int
    raw_sha256: tuple[str, ...]
    query_filters: tuple[CohortQueryFilter, ...]
    raw_record_count: int | None = None
    duplicate_record_count: int | None = None
    conflict_record_count: int | None = None
    canonical_record_count: int | None = None
    exhausted: bool = True
    continuation: str | None = None
    truncated: bool = False
    slice_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_uid", _text(self.source_uid, "slice source_uid"))
        if self.source_uid.startswith("polymarket:") and self.source_uid not in _POLYMARKET_COMPLETE_TRADE_SOURCES:
            raise ValueError("unsupported Polymarket source for complete-trade population coverage")
        start = _utc(self.interval_start, "slice interval_start")
        end = _utc(self.interval_end, "slice interval_end")
        retrieved = _utc(self.retrieved_at, "slice retrieved_at")
        complete = _utc(self.complete_through, "slice complete_through")
        for name, value in (("interval_start", start), ("interval_end", end), ("retrieved_at", retrieved), ("complete_through", complete)):
            object.__setattr__(self, name, value)
        if start.microsecond or end.microsecond or end <= start:
            raise ValueError("coverage slice must be a positive whole-second interval")
        if self.status != CoverageStatus.COMPLETE or complete < end:
            raise ValueError("every population coverage slice must be complete through its end")
        if retrieved < complete or retrieved < end:
            raise ValueError("coverage slice cannot be retrieved before it is complete")
        if not self.exhausted or self.continuation is not None or self.truncated:
            raise ValueError("every population coverage slice must be exhausted and untruncated")
        canonical_count = self.record_count if self.canonical_record_count is None else self.canonical_record_count
        duplicate_count = 0 if self.duplicate_record_count is None else self.duplicate_record_count
        conflict_count = 0 if self.conflict_record_count is None else self.conflict_record_count
        raw_count = canonical_count + duplicate_count + conflict_count if self.raw_record_count is None else self.raw_record_count
        counts = (self.record_count, raw_count, duplicate_count, conflict_count, canonical_count)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
            raise ValueError("slice coverage counts must be non-negative integers")
        if self.record_count != canonical_count or raw_count != canonical_count + duplicate_count + conflict_count:
            raise ValueError("slice raw count must reconcile to canonical, duplicate, and conflict counts")
        for name, value in (("raw_record_count", raw_count), ("duplicate_record_count", duplicate_count), ("conflict_record_count", conflict_count), ("canonical_record_count", canonical_count)):
            object.__setattr__(self, name, value)
        hashes = tuple(sorted({_text(item, "slice raw SHA-256").lower() for item in self.raw_sha256}))
        if not hashes or any(not _SHA256_RE.fullmatch(item) for item in hashes):
            raise ValueError("coverage slice requires valid raw SHA-256 lineage")
        object.__setattr__(self, "raw_sha256", hashes)
        filters = tuple(sorted(self.query_filters, key=lambda item: item.name))
        object.__setattr__(self, "query_filters", filters)
        by_name = {item.name: item.value for item in filters}
        if len(by_name) != len(filters):
            raise ValueError("coverage slice query filters must be distinct")
        if self.source_uid.startswith("polymarket:"):
            _validate_polymarket_trade_filters(filters)
        for name, expected in (("start", int(start.timestamp())), ("end", int(end.timestamp()))):
            if by_name.get(name) != expected or isinstance(by_name.get(name), bool):
                raise ValueError(f"coverage slice {name} must match its interval epoch")
        payload = self.to_payload(include_hash=False)
        object.__setattr__(self, "slice_sha256", sha256(canonical_json_bytes(payload)).hexdigest())

    def shared_filters(self) -> tuple[CohortQueryFilter, ...]:
        return tuple(item for item in self.query_filters if item.name not in {"start", "end", "limit", "offset"})

    def to_payload(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "source_uid": self.source_uid,
            "interval_start": _time(self.interval_start),
            "interval_end": _time(self.interval_end),
            "retrieved_at": _time(self.retrieved_at),
            "complete_through": _time(self.complete_through),
            "status": self.status.value,
            "record_count": self.record_count,
            "raw_record_count": self.raw_record_count,
            "duplicate_record_count": self.duplicate_record_count,
            "conflict_record_count": self.conflict_record_count,
            "canonical_record_count": self.canonical_record_count,
            "raw_sha256": list(self.raw_sha256),
            "query_filters": [item.to_payload() for item in self.query_filters],
            "exhausted": self.exhausted,
            "continuation": self.continuation,
            "truncated": self.truncated,
        }
        return {"slice_sha256": self.slice_sha256, **payload} if include_hash else payload


@dataclass(frozen=True, slots=True)
class PopulationCoverage:
    """One exact, complete population query with explicit retrieval timing."""

    platform: str
    dataset: str
    source_uid: str
    scope_kind: Literal["market", "event", "market_set"]
    scope_market_uids: tuple[str, ...]
    interval_start: datetime
    interval_end: datetime
    retrieved_at: datetime
    as_of: datetime
    complete_through: datetime
    status: CoverageStatus
    record_count: int
    raw_sha256: tuple[str, ...]
    query_filters: tuple[CohortQueryFilter, ...]
    raw_record_count: int | None = None
    duplicate_record_count: int | None = None
    conflict_record_count: int | None = None
    canonical_record_count: int | None = None
    scope_event_uid: str | None = None
    scope_market_bindings: tuple[CohortMarketScopeBinding, ...] = ()
    slices: tuple[PopulationCoverageSlice, ...] = ()
    exhausted: bool = False
    continuation: str | None = None
    gap_intervals: tuple[tuple[datetime, datetime], ...] = ()
    truncated: bool = False
    query_uid: str = field(init=False)
    coverage_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        platform = _text(self.platform, "platform").lower()
        object.__setattr__(self, "platform", platform)
        for name in ("dataset", "source_uid"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        if not self.source_uid.startswith(f"{platform}:"):
            raise ValueError("population source_uid must match platform")
        if platform == "polymarket":
            if self.source_uid not in _POLYMARKET_COMPLETE_TRADE_SOURCES:
                raise ValueError("unsupported Polymarket source for complete-trade population coverage")
            if self.dataset != _POLYMARKET_COMPLETE_TRADE_DATASET:
                raise ValueError("unsupported Polymarket dataset for complete-trade population coverage")
        markets = tuple(sorted({_text(item, "scope market UID") for item in self.scope_market_uids}))
        if not markets:
            raise ValueError("population coverage requires explicit scope_market_uids")
        object.__setattr__(self, "scope_market_uids", markets)
        start = _utc(self.interval_start, "interval_start")
        end = _utc(self.interval_end, "interval_end")
        retrieved = _utc(self.retrieved_at, "retrieved_at")
        cutoff = _utc(self.as_of, "as_of")
        complete_through = _utc(self.complete_through, "complete_through")
        for name, value in (
            ("interval_start", start),
            ("interval_end", end),
            ("retrieved_at", retrieved),
            ("as_of", cutoff),
            ("complete_through", complete_through),
        ):
            object.__setattr__(self, name, value)
        if start.microsecond or end.microsecond:
            raise ValueError("population query interval must use whole-second precision")
        if end < start or end > cutoff:
            raise ValueError("population interval must be ordered and no later than as_of")
        if complete_through < end:
            raise ValueError("population coverage must be complete through interval_end")
        if retrieved < complete_through or retrieved < end:
            raise ValueError("population coverage cannot be retrieved before it is complete")
        if self.status != CoverageStatus.COMPLETE:
            raise ValueError("population cohort ranking requires complete coverage")
        if self.continuation is not None or self.gap_intervals or self.truncated:
            raise ValueError("population coverage cannot be continued, gapped, or truncated")
        if not self.slices and not self.exhausted:
            raise ValueError("unsliced population coverage requires explicit exhausted proof")
        canonical_count = self.record_count if self.canonical_record_count is None else self.canonical_record_count
        duplicate_count = 0 if self.duplicate_record_count is None else self.duplicate_record_count
        conflict_count = 0 if self.conflict_record_count is None else self.conflict_record_count
        raw_count = canonical_count + duplicate_count + conflict_count if self.raw_record_count is None else self.raw_record_count
        counts = (self.record_count, raw_count, duplicate_count, conflict_count, canonical_count)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
            raise ValueError("population coverage counts must be non-negative integers")
        if self.record_count != canonical_count or raw_count != canonical_count + duplicate_count + conflict_count:
            raise ValueError("population raw count must reconcile to canonical, duplicate, and conflict counts")
        for name, value in (("raw_record_count", raw_count), ("duplicate_record_count", duplicate_count), ("conflict_record_count", conflict_count), ("canonical_record_count", canonical_count)):
            object.__setattr__(self, name, value)
        hashes = tuple(sorted({_text(item, "raw SHA-256").lower() for item in self.raw_sha256}))
        if not hashes or any(not _SHA256_RE.fullmatch(item) for item in hashes):
            raise ValueError("population coverage requires valid raw SHA-256 lineage")
        object.__setattr__(self, "raw_sha256", hashes)
        filters = tuple(sorted(self.query_filters, key=lambda item: item.name))
        if len({item.name for item in filters}) != len(filters):
            raise ValueError("population query filters must be distinct")
        object.__setattr__(self, "query_filters", filters)
        by_name = {item.name: item.value for item in filters}
        if any(name.lower() == "user" for name in by_name):
            raise ValueError("user-only coverage cannot establish a population cohort")
        if not ({"market", "eventId"} & set(by_name)):
            raise ValueError("population coverage requires a market or event query filter")
        if platform == "polymarket":
            _validate_polymarket_trade_filters(filters)
        market_filter = by_name.get("market")
        queried_markets = (
            tuple(sorted({_text(item, "query market UID") for item in market_filter}))
            if isinstance(market_filter, list)
            else ((_text(market_filter, "query market UID"),) if isinstance(market_filter, str) else ())
        )
        bindings = tuple(sorted(self.scope_market_bindings, key=lambda item: item.canonical_market_uid))
        object.__setattr__(self, "scope_market_bindings", bindings)
        if bindings:
            if tuple(item.canonical_market_uid for item in bindings) != markets:
                raise ValueError("market scope bindings must cover every canonical market exactly once")
            if len({item.source_query_market_id for item in bindings}) != len(bindings):
                raise ValueError("source query market IDs must be distinct")
            if platform == "polymarket" and any(
                item.canonical_market_uid != f"polymarket:market/{item.source_query_market_id}"
                for item in bindings
            ):
                raise ValueError("Polymarket market bindings must use the deterministic canonical mapping")
            expected_query_markets = tuple(sorted(item.source_query_market_id for item in bindings))
        else:
            expected_query_markets = markets
        if self.scope_kind in {"market", "market_set"} and queried_markets != expected_query_markets:
            raise ValueError("declared market scope must exactly match the raw population query")
        event_filter = by_name.get("eventId")
        if self.scope_kind == "event":
            if self.scope_event_uid is None or event_filter != self.scope_event_uid:
                raise ValueError("declared event scope must exactly match the population query")
            object.__setattr__(self, "scope_event_uid", _text(self.scope_event_uid, "scope_event_uid"))
        elif self.scope_event_uid is not None:
            raise ValueError("scope_event_uid is only valid for event-scoped coverage")
        for name, expected in (("start", int(start.timestamp())), ("end", int(end.timestamp()))):
            value = by_name.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value != expected:
                raise ValueError(f"population query {name} must exactly match its interval epoch")
        slices = tuple(sorted(self.slices, key=lambda item: item.interval_start))
        object.__setattr__(self, "slices", slices)
        if slices:
            if slices[0].interval_start != start or slices[-1].interval_end != end:
                raise ValueError("coverage slice union must exactly bind the population interval")
            for previous, current in zip(slices, slices[1:], strict=False):
                expected = previous.interval_end + timedelta(seconds=1)
                if current.interval_start < expected:
                    raise ValueError("population coverage slices overlap")
                if current.interval_start > expected:
                    raise ValueError("population coverage slices contain a gap")
            shared = tuple(item for item in filters if item.name not in {"start", "end", "limit", "offset"})
            if any(item.source_uid != self.source_uid for item in slices):
                raise ValueError("population coverage slices must use the same source")
            if any(item.shared_filters() != shared for item in slices):
                raise ValueError("population coverage slices must share market and taker policy")
            for field_name in ("raw_record_count", "duplicate_record_count", "conflict_record_count", "canonical_record_count"):
                if sum(getattr(item, field_name) or 0 for item in slices) != getattr(self, field_name):
                    raise ValueError(f"population coverage slice {field_name} values must reconcile")
            if tuple(sorted({digest for item in slices for digest in item.raw_sha256})) != hashes:
                raise ValueError("population coverage raw lineage must equal its slice union")
            if max(item.retrieved_at for item in slices) != retrieved:
                raise ValueError("population coverage retrieved_at must equal the latest slice retrieval")
        normalized_gaps: list[tuple[datetime, datetime]] = []
        for gap_start, gap_end in self.gap_intervals:
            normalized_gaps.append((_utc(gap_start, "gap_start"), _utc(gap_end, "gap_end")))
        object.__setattr__(self, "gap_intervals", tuple(normalized_gaps))
        identity = {
            "contract": "wallet_population_coverage_v1",
            "platform": platform,
            "dataset": self.dataset,
            "source_uid": self.source_uid,
            "scope_kind": self.scope_kind,
            "scope_market_uids": markets,
            "scope_event_uid": self.scope_event_uid,
            "scope_market_bindings": [item.to_payload() for item in bindings],
            "interval_start": _time(start),
            "interval_end": _time(end),
            "query_filters": [item.to_payload() for item in filters],
            "slices": [item.to_payload() for item in slices],
        }
        query_hash = sha256(canonical_json_bytes(identity)).hexdigest()
        object.__setattr__(self, "query_uid", f"cohort-query:{query_hash}")
        coverage_payload = {
            **identity,
            "retrieved_at": _time(retrieved),
            "as_of": _time(cutoff),
            "complete_through": _time(complete_through),
            "status": self.status.value,
            "record_count": self.record_count,
            "raw_record_count": self.raw_record_count,
            "duplicate_record_count": self.duplicate_record_count,
            "conflict_record_count": self.conflict_record_count,
            "canonical_record_count": self.canonical_record_count,
            "raw_sha256": hashes,
            "exhausted": self.exhausted,
        }
        object.__setattr__(self, "coverage_sha256", sha256(canonical_json_bytes(coverage_payload)).hexdigest())

    def to_payload(self) -> dict[str, Any]:
        return {
            "query_uid": self.query_uid,
            "coverage_sha256": self.coverage_sha256,
            "platform": self.platform,
            "dataset": self.dataset,
            "source_uid": self.source_uid,
            "scope_kind": self.scope_kind,
            "scope_market_uids": list(self.scope_market_uids),
            "scope_event_uid": self.scope_event_uid,
            "scope_market_bindings": [item.to_payload() for item in self.scope_market_bindings],
            "interval_start": _time(self.interval_start),
            "interval_end": _time(self.interval_end),
            "retrieved_at": _time(self.retrieved_at),
            "as_of": _time(self.as_of),
            "complete_through": _time(self.complete_through),
            "status": self.status.value,
            "record_count": self.record_count,
            "raw_record_count": self.raw_record_count,
            "duplicate_record_count": self.duplicate_record_count,
            "conflict_record_count": self.conflict_record_count,
            "canonical_record_count": self.canonical_record_count,
            "raw_sha256": list(self.raw_sha256),
            "query_filters": [item.to_payload() for item in self.query_filters],
            "slices": [item.to_payload() for item in self.slices],
            "continuation": self.continuation,
            "gap_intervals": [[_time(start), _time(end)] for start, end in self.gap_intervals],
            "truncated": self.truncated,
            "exhausted": self.exhausted,
        }


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    name: str
    tail: Literal["high", "two_sided"]
    weight: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.name not in _ANOMALY_FEATURE_NAMES:
            raise ValueError(f"unsupported wallet anomaly feature: {self.name}")
        if self.weight <= 0:
            raise ValueError("feature weight must be positive")


_DEFAULT_FEATURES = tuple(
    FeatureSpec(name, tail)
    for name, tail in (
        ("fill_count", "high"),
        ("gross_notional", "high"),
        ("buy_notional_share", "high"),
        ("directional_concentration", "high"),
        ("dominant_market_share", "high"),
        ("max_fill_share", "high"),
        ("active_span_seconds", "two_sided"),
        ("burst_notional_share", "high"),
        ("burst_fill_share", "high"),
        ("long_shot_buy_notional_share", "high"),
        ("focus_buy_notional", "high"),
        ("focus_net_notional", "high"),
        ("focus_gross_notional_share", "high"),
    )
)


@dataclass(frozen=True, slots=True)
class WalletCohortPolicy:
    as_of: datetime
    frozen_at: datetime
    focus_market_uid: str | None
    focus_outcome_uid: str | None = None
    focus_event_time: datetime | None = None
    focus_published_at: datetime | None = None
    focus_available_at: datetime | None = None
    analysis_mode: Literal["operational", "hindsight_reconstructed"] = "operational"
    availability_cutoff: datetime | None = None
    feature_specs: tuple[FeatureSpec, ...] = _DEFAULT_FEATURES
    min_peer_count: int = 200
    min_supported_features: int = 6
    composite_top_k: int = 5
    surprise_clip: Decimal = Decimal("3")
    burst_window_seconds: int = 3600
    long_shot_price_max: Decimal = Decimal("0.10")
    nuisance_scales: tuple[tuple[str, Decimal], ...] = (("time_to_close_hours", Decimal("1")),)
    nuisance_k: int = 20
    nuisance_max_distance: Decimal = Decimal("4")
    decimal_precision: int = 50
    high_signal_percentile_min: Decimal = Decimal("0.99")
    elevated_signal_percentile_min: Decimal = Decimal("0.95")
    policy_uid: str = field(init=False)

    def __post_init__(self) -> None:
        cutoff = _utc(self.as_of, "policy as_of")
        frozen = _utc(self.frozen_at, "policy frozen_at")
        object.__setattr__(self, "as_of", cutoff)
        object.__setattr__(self, "frozen_at", frozen)
        if self.focus_market_uid is not None:
            object.__setattr__(self, "focus_market_uid", _text(self.focus_market_uid, "focus_market_uid"))
        if self.focus_outcome_uid is not None:
            object.__setattr__(self, "focus_outcome_uid", _text(self.focus_outcome_uid, "focus_outcome_uid"))
        if self.focus_event_time is not None:
            object.__setattr__(self, "focus_event_time", _utc(self.focus_event_time, "focus_event_time"))
        if self.focus_published_at is not None:
            object.__setattr__(self, "focus_published_at", _utc(self.focus_published_at, "focus_published_at"))
        if self.focus_available_at is not None:
            object.__setattr__(self, "focus_available_at", _utc(self.focus_available_at, "focus_available_at"))
        availability = self.availability_cutoff
        if self.analysis_mode == "operational":
            if frozen > cutoff:
                raise ValueError("operational cohort policy must be frozen by as_of")
            if availability is not None and _utc(availability, "availability_cutoff") != cutoff:
                raise ValueError("operational availability_cutoff must equal as_of")
            availability = cutoff
        elif self.analysis_mode == "hindsight_reconstructed":
            if availability is None:
                raise ValueError("hindsight reconstruction requires an explicit availability_cutoff")
            availability = _utc(availability, "availability_cutoff")
            if availability <= cutoff:
                raise ValueError("hindsight availability_cutoff must be later than as_of")
            if frozen > availability:
                raise ValueError("hindsight cohort policy must be frozen by availability_cutoff")
        else:  # pragma: no cover - Literal guard for runtime callers
            raise ValueError("unsupported cohort analysis_mode")
        object.__setattr__(self, "availability_cutoff", availability)
        if len({item.name for item in self.feature_specs}) != len(self.feature_specs):
            raise ValueError("feature_specs must have distinct names")
        if self.min_peer_count < 2:
            raise ValueError("min_peer_count must be at least 2; the production default is 200")
        if not 1 <= self.min_supported_features <= len(self.feature_specs):
            raise ValueError("min_supported_features is outside the feature specification")
        if not 1 <= self.composite_top_k <= len(self.feature_specs):
            raise ValueError("composite_top_k is outside the feature specification")
        if self.surprise_clip <= 0 or self.burst_window_seconds <= 0:
            raise ValueError("surprise clip and burst window must be positive")
        if not Decimal("0") <= self.long_shot_price_max <= Decimal("1"):
            raise ValueError("long-shot price threshold must be within [0, 1]")
        scales = tuple(sorted(((_text(name, "nuisance feature"), scale) for name, scale in self.nuisance_scales)))
        if not scales or len({name for name, _scale in scales}) != len(scales):
            raise ValueError("nuisance scales must be non-empty and distinct")
        if any(name in _ANOMALY_FEATURE_NAMES for name, _scale in scales):
            raise ValueError("nuisance support cannot use anomaly features")
        if any(scale <= 0 for _name, scale in scales):
            raise ValueError("nuisance scales must be positive")
        object.__setattr__(self, "nuisance_scales", scales)
        if self.nuisance_k < 1 or self.nuisance_k > self.min_peer_count:
            raise ValueError("nuisance_k must be positive and no larger than min_peer_count")
        if self.nuisance_max_distance < 0 or not 28 <= self.decimal_precision <= 100:
            raise ValueError("nuisance distance or decimal precision is invalid")
        if not (
            Decimal("0")
            <= self.elevated_signal_percentile_min
            < self.high_signal_percentile_min
            <= Decimal("1")
        ):
            raise ValueError("signal percentile thresholds must be ordered within [0, 1]")
        payload = self.to_payload(include_uid=False)
        object.__setattr__(self, "policy_uid", f"cohort-policy:{sha256(canonical_json_bytes(payload)).hexdigest()}")

    def to_payload(self, *, include_uid: bool = True) -> dict[str, Any]:
        payload = {
            "as_of": _time(self.as_of),
            "frozen_at": _time(self.frozen_at),
            "focus_market_uid": self.focus_market_uid,
            "focus_outcome_uid": self.focus_outcome_uid,
            "focus_event_time": None if self.focus_event_time is None else _time(self.focus_event_time),
            "focus_published_at": None if self.focus_published_at is None else _time(self.focus_published_at),
            "focus_available_at": None if self.focus_available_at is None else _time(self.focus_available_at),
            "analysis_mode": self.analysis_mode,
            "availability_cutoff": _time(self.availability_cutoff or self.as_of),
            "feature_specs": [
                {"name": item.name, "tail": item.tail, "weight": _decimal(item.weight)}
                for item in self.feature_specs
            ],
            "min_peer_count": self.min_peer_count,
            "min_supported_features": self.min_supported_features,
            "composite_top_k": self.composite_top_k,
            "surprise_clip": _decimal(self.surprise_clip),
            "burst_window_seconds": self.burst_window_seconds,
            "long_shot_price_max": _decimal(self.long_shot_price_max),
            "nuisance_scales": [[name, _decimal(scale)] for name, scale in self.nuisance_scales],
            "nuisance_k": self.nuisance_k,
            "nuisance_max_distance": _decimal(self.nuisance_max_distance),
            "decimal_precision": self.decimal_precision,
            "high_signal_percentile_min": _decimal(self.high_signal_percentile_min),
            "elevated_signal_percentile_min": _decimal(self.elevated_signal_percentile_min),
        }
        return {"policy_uid": self.policy_uid, **payload} if include_uid else payload


@dataclass(frozen=True, slots=True)
class NuisanceVector:
    actor_uid: str
    values: tuple[tuple[str, Decimal], ...]
    event_time: datetime
    available_at: datetime
    source_uid: str
    raw_artifact_uid: str
    vector_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_uid", _text(self.actor_uid, "nuisance actor_uid"))
        ordered = tuple(sorted(self.values))
        if len({name for name, _value in ordered}) != len(ordered):
            raise ValueError("nuisance feature names must be distinct")
        if any(name in _ANOMALY_FEATURE_NAMES for name, _value in ordered):
            raise ValueError("nuisance vectors cannot contain anomaly features")
        object.__setattr__(self, "values", ordered)
        object.__setattr__(self, "event_time", _utc(self.event_time, "nuisance event_time"))
        object.__setattr__(self, "available_at", _utc(self.available_at, "nuisance available_at"))
        object.__setattr__(self, "source_uid", _text(self.source_uid, "nuisance source_uid"))
        object.__setattr__(self, "raw_artifact_uid", _text(self.raw_artifact_uid, "nuisance raw_artifact_uid"))
        digest = self.raw_artifact_uid.rsplit("/", 1)[-1].lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ValueError("nuisance vector requires SHA-256 raw provenance")
        object.__setattr__(self, "vector_sha256", sha256(canonical_json_bytes(self.to_payload(include_hash=False))).hexdigest())

    def mapping(self) -> dict[str, Decimal]:
        return dict(self.values)

    def to_payload(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "actor_uid": self.actor_uid,
            "values": [[name, _decimal(value)] for name, value in self.values],
            "event_time": _time(self.event_time),
            "available_at": _time(self.available_at),
            "source_uid": self.source_uid,
            "raw_artifact_uid": self.raw_artifact_uid,
        }
        return {"vector_sha256": self.vector_sha256, **payload} if include_hash else payload


@dataclass(frozen=True, slots=True)
class WalletEventFeatureVector:
    actor_uid: str
    values: tuple[tuple[str, Decimal | None], ...]
    fill_uids: tuple[str, ...]
    canonical_fill_sha256: tuple[str, ...]
    vector_sha256: str

    def value(self, name: str) -> Decimal | None:
        return dict(self.values).get(name)

    def to_payload(self) -> dict[str, Any]:
        return {
            "actor_uid": self.actor_uid,
            "values": [[name, _decimal(value)] for name, value in self.values],
            "fill_uids": list(self.fill_uids),
            "canonical_fill_sha256": list(self.canonical_fill_sha256),
            "vector_sha256": self.vector_sha256,
        }


@dataclass(frozen=True, slots=True)
class FeatureSurprise:
    name: str
    tail: str
    peer_count: int
    tail_p: Decimal | None
    surprise: Decimal | None
    status: Literal["available", "insufficient_peer_support", "target_feature_unavailable"]

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "tail": self.tail,
            "peer_count": self.peer_count,
            "tail_p": _decimal(self.tail_p),
            "surprise": _decimal(self.surprise),
            "status": self.status,
        }


SignalClassification = Literal["high", "elevated", "routine", "insufficient_data"]


@dataclass(frozen=True, slots=True)
class WalletSignalAssessment:
    """One coherent, non-probabilistic assessment for analyst triage.

    ``coverage_confidence`` describes only the population data contract.
    ``signal_strength`` and ``statistical_support`` describe only the
    peer-relative calculation.  Neither field is a fraud confidence.
    """

    status: Literal["available", "abstain", "unavailable"]
    classification: Literal["high", "elevated", "routine", "unavailable"]
    review_priority: Literal["high", "elevated", "routine", "unavailable"]
    signal_strength: Literal["high", "elevated", "routine", "unavailable"]
    statistical_support: Literal["sufficient", "insufficient", "unavailable"]
    coverage_status: Literal["complete", "partial", "unavailable", "unknown"]
    coverage_confidence: Literal["verified_complete", "limited", "unavailable", "unknown"]
    population_rank: int | None
    population_size: int | None
    summary: str
    decision_owner: Literal["human_reviewer"] = "human_reviewer"

    @classmethod
    def derive(
        cls,
        *,
        report_status: Literal["available", "abstain"],
        row: RankedWallet | None,
        coverage_status: CoverageStatus,
    ) -> WalletSignalAssessment:
        return cls.derive_values(
            report_status=report_status,
            row_status=None if row is None else row.status,
            signal_classification="insufficient_data" if row is None else row.signal_classification,
            coverage_status=coverage_status,
            population_rank=None if row is None else row.population_rank,
            population_size=None if row is None else row.population_size,
        )

    @classmethod
    def derive_values(
        cls,
        *,
        report_status: Literal["available", "abstain"],
        row_status: Literal["available", "abstain"] | None,
        signal_classification: SignalClassification,
        coverage_status: CoverageStatus,
        population_rank: int | None,
        population_size: int | None,
    ) -> WalletSignalAssessment:
        coverage_confidence: Literal["verified_complete", "limited", "unavailable", "unknown"] = {
            CoverageStatus.COMPLETE: "verified_complete",
            CoverageStatus.PARTIAL: "limited",
            CoverageStatus.UNAVAILABLE: "unavailable",
            CoverageStatus.UNKNOWN: "unknown",
        }[coverage_status]
        coverage_unavailable = coverage_status in {CoverageStatus.UNAVAILABLE, CoverageStatus.UNKNOWN}
        row_available = (
            report_status == "available"
            and row_status == "available"
            and signal_classification in {"high", "elevated", "routine"}
        )
        if coverage_unavailable:
            return cls(
                status="unavailable",
                classification="unavailable",
                review_priority="unavailable",
                signal_strength="unavailable",
                statistical_support="unavailable",
                coverage_status=coverage_status.value,
                coverage_confidence=coverage_confidence,
                population_rank=None,
                population_size=None,
                summary="Signal assessment unavailable because population coverage is unavailable or unknown.",
            )
        if coverage_status == CoverageStatus.PARTIAL:
            return cls(
                status="abstain",
                classification="unavailable",
                review_priority="unavailable",
                signal_strength="unavailable",
                statistical_support="unavailable",
                coverage_status=coverage_status.value,
                coverage_confidence=coverage_confidence,
                population_rank=None,
                population_size=None,
                summary="Signal assessment abstained because complete population coverage is required for percentile and rank claims.",
            )
        if not row_available:
            return cls(
                status="abstain",
                classification="unavailable",
                review_priority="unavailable",
                signal_strength="unavailable",
                statistical_support="insufficient",
                coverage_status=coverage_status.value,
                coverage_confidence=coverage_confidence,
                population_rank=population_rank,
                population_size=population_size,
                summary="Signal assessment abstained because required inputs or statistical support were insufficient.",
            )
        level = signal_classification
        return cls(
            status="available",
            classification=level,
            review_priority=level,
            signal_strength=level,
            statistical_support="sufficient",
            coverage_status=coverage_status.value,
            coverage_confidence=coverage_confidence,
            population_rank=population_rank,
            population_size=population_size,
            summary=(
                f"{level.capitalize()} peer-relative signal strength with sufficient statistical support "
                "in the declared cohort."
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "classification": self.classification,
            "review_priority": self.review_priority,
            "signal_strength": self.signal_strength,
            "statistical_support": self.statistical_support,
            "coverage_status": self.coverage_status,
            "coverage_confidence": self.coverage_confidence,
            "population_rank": self.population_rank,
            "population_size": self.population_size,
            "summary": self.summary,
            "decision_owner": self.decision_owner,
        }


@dataclass(frozen=True, slots=True)
class RankedWallet:
    actor_uid: str
    status: Literal["available", "abstain"]
    abstention_reasons: tuple[str, ...]
    feature_vector: WalletEventFeatureVector | None
    feature_surprises: tuple[FeatureSurprise, ...]
    novelty_composite: Decimal | None
    cohort_percentile: Decimal | None
    nuisance_kth_distance: Decimal | None
    nuisance_neighbor_uids: tuple[str, ...]
    case_resemblance: None
    case_resemblance_status: Literal["unavailable_not_supplied"]
    statistical_rank: int | None = None
    tie_group: int | None = None
    display_order: int | None = None
    population_rank: int | None = None
    population_size: int | None = None
    operational_review_rank: int | None = None
    hindsight_peer_rank: int | None = None
    scores_are_probabilities: Literal[False] = False
    signal_classification: SignalClassification = "insufficient_data"

    def to_payload(self) -> dict[str, Any]:
        available_signal = self.status == "available" and self.signal_classification != "insufficient_data"
        signal_level = self.signal_classification if available_signal else "unavailable"
        return {
            "actor_uid": self.actor_uid,
            "status": self.status,
            "abstention_reasons": list(self.abstention_reasons),
            "feature_vector": None if self.feature_vector is None else self.feature_vector.to_payload(),
            "feature_surprises": [item.to_payload() for item in self.feature_surprises],
            "novelty_composite": _decimal(self.novelty_composite),
            "cohort_percentile": _decimal(self.cohort_percentile),
            "nuisance_kth_distance": _decimal(self.nuisance_kth_distance),
            "nuisance_neighbor_uids": list(self.nuisance_neighbor_uids),
            "case_resemblance": self.case_resemblance,
            "case_resemblance_status": self.case_resemblance_status,
            "statistical_rank": self.statistical_rank,
            "tie_group": self.tie_group,
            "display_order": self.display_order,
            "population_rank": self.population_rank,
            "population_size": self.population_size,
            "operational_review_rank": self.operational_review_rank,
            "hindsight_peer_rank": self.hindsight_peer_rank,
            "signal_classification": self.signal_classification,
            "review_priority": signal_level,
            "signal_strength": signal_level,
            "statistical_support": "sufficient" if available_signal else "insufficient",
        }


@dataclass(frozen=True, slots=True)
class CohortRankingReport:
    policy: WalletCohortPolicy
    coverage: PopulationCoverage
    status: Literal["available", "abstain"]
    abstention_reasons: tuple[str, ...]
    input_fill_count: int
    input_sha256: str
    feature_snapshot_sha256: str
    peer_profile_sha256: str
    nuisance_snapshot_sha256: str
    candidate_set_sha256: str
    population_actor_count: int
    population_actor_set_sha256: str
    rows: tuple[RankedWallet, ...]
    # Internal governance flags retained for API compatibility; the compact
    # analyst payload carries the single decision boundary below instead of
    # repeating these caveats at every level.
    effectiveness_unknown: Literal[True] = True
    not_training: Literal[True] = True
    human_review_required: Literal[True] = True
    prospective_eligible: bool = False
    operational_review_priority: None = None

    def assessment_for(self, actor_uid: str | None = None) -> WalletSignalAssessment:
        if actor_uid is None:
            row = self.rows[0] if len(self.rows) == 1 else None
        else:
            row = next((item for item in self.rows if item.actor_uid == actor_uid), None)
        return WalletSignalAssessment.derive(
            report_status=self.status,
            row=row,
            coverage_status=self.coverage.status,
        )

    @property
    def report_sha256(self) -> str:
        return sha256(canonical_json_bytes(self._unsigned_payload())).hexdigest()

    def _unsigned_payload(self) -> dict[str, Any]:
        assessment = self.assessment_for()
        return {
            "schema_version": "wallet-cohort-ranking-v2",
            "supersedes_schema_version": "wallet-cohort-ranking-v1",
            "signal_assessment": assessment.to_payload(),
            "policy": self.policy.to_payload(),
            "coverage": self.coverage.to_payload(),
            "status": self.status,
            "abstention_reasons": list(self.abstention_reasons),
            "input_fill_count": self.input_fill_count,
            "input_sha256": self.input_sha256,
            "feature_snapshot_sha256": self.feature_snapshot_sha256,
            "peer_profile_sha256": self.peer_profile_sha256,
            "nuisance_snapshot_sha256": self.nuisance_snapshot_sha256,
            "candidate_set_sha256": self.candidate_set_sha256,
            "population_actor_count": self.population_actor_count,
            "population_actor_set_sha256": self.population_actor_set_sha256,
            "rows": [item.to_payload() for item in self.rows],
            "analysis_mode": self.policy.analysis_mode,
            "prospective_eligible": self.prospective_eligible,
            "operational_review_priority": self.operational_review_priority,
        }

    def to_payload(self) -> dict[str, Any]:
        return {**self._unsigned_payload(), "report_sha256": self.report_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_payload())


def _raw_digest(fill: TradeFill) -> str | None:
    candidate = fill.raw_artifact_uid.rsplit("/", 1)[-1].lower()
    return candidate if _SHA256_RE.fullmatch(candidate) else None


def _feature_vector(actor_uid: str, fills: tuple[TradeFill, ...], policy: WalletCohortPolicy) -> WalletEventFeatureVector:
    notionals = tuple(fill.price * fill.size for fill in fills)
    gross = sum(notionals, Decimal("0"))
    buy = sum((notional for fill, notional in zip(fills, notionals, strict=True) if fill.side == TradeSide.BUY), Decimal("0"))
    sell = gross - buy
    market_notionals: dict[str, Decimal] = {}
    outcome_signed: dict[str, Decimal] = {}
    bucket_notional: dict[int, Decimal] = {}
    bucket_count: dict[int, int] = {}
    focus_buy = Decimal("0")
    focus_net = Decimal("0")
    focus_outcome_buy = Decimal("0")
    focus_outcome_net = Decimal("0")
    longshot_buy = Decimal("0")
    for fill, notional in zip(fills, notionals, strict=True):
        market_notionals[fill.market_uid] = market_notionals.get(fill.market_uid, Decimal("0")) + notional
        signed = notional if fill.side == TradeSide.BUY else -notional
        outcome_signed[fill.outcome_uid] = outcome_signed.get(fill.outcome_uid, Decimal("0")) + signed
        bucket = int(fill.event_time.timestamp()) // policy.burst_window_seconds
        bucket_notional[bucket] = bucket_notional.get(bucket, Decimal("0")) + notional
        bucket_count[bucket] = bucket_count.get(bucket, 0) + 1
        if fill.side == TradeSide.BUY and fill.price <= policy.long_shot_price_max:
            longshot_buy += notional
        if fill.market_uid == policy.focus_market_uid:
            focus_net += signed
            if fill.side == TradeSide.BUY:
                focus_buy += notional
        if policy.focus_outcome_uid is not None and fill.outcome_uid == policy.focus_outcome_uid:
            focus_outcome_net += signed
            if fill.side == TradeSide.BUY:
                focus_outcome_buy += notional
    values: dict[str, Decimal | None] = {
        "fill_count": Decimal(len(fills)),
        "gross_notional": gross,
        "buy_notional": buy,
        "sell_notional": sell,
        "market_count": Decimal(len(market_notionals)),
        "outcome_count": Decimal(len({fill.outcome_uid for fill in fills})),
        "buy_notional_share": buy / gross if gross else None,
        "directional_concentration": max((abs(item) for item in outcome_signed.values()), default=Decimal("0")) / gross if gross else None,
        "dominant_market_share": max(market_notionals.values(), default=Decimal("0")) / gross if gross else None,
        "max_fill_share": max(notionals, default=Decimal("0")) / gross if gross else None,
        "active_span_seconds": Decimal(str((fills[-1].event_time - fills[0].event_time).total_seconds())),
        "burst_notional_share": max(bucket_notional.values(), default=Decimal("0")) / gross if gross else None,
        "burst_fill_share": Decimal(max(bucket_count.values(), default=0)) / Decimal(len(fills)) if fills else None,
        "long_shot_buy_notional_share": longshot_buy / buy if buy else None,
        "focus_buy_notional": focus_buy,
        "focus_net_notional": focus_net,
        "focus_gross_notional_share": market_notionals.get(policy.focus_market_uid or "", Decimal("0")) / gross if gross else None,
        "focus_outcome_buy_notional": focus_outcome_buy if policy.focus_outcome_uid is not None else None,
        "focus_outcome_net_notional": focus_outcome_net if policy.focus_outcome_uid is not None else None,
    }
    hashes = tuple(sha256(canonical_json_bytes(fill)).hexdigest() for fill in fills)
    payload = {
        "actor_uid": actor_uid,
        "values": [[name, _decimal(value)] for name, value in sorted(values.items())],
        "fill_uids": [fill.fill_uid for fill in fills],
        "canonical_fill_sha256": hashes,
        "policy_uid": policy.policy_uid,
    }
    return WalletEventFeatureVector(
        actor_uid=actor_uid,
        values=tuple(sorted(values.items())),
        fill_uids=tuple(fill.fill_uid for fill in fills),
        canonical_fill_sha256=hashes,
        vector_sha256=sha256(canonical_json_bytes(payload)).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class _FeatureProfile:
    name: str
    values: tuple[Decimal, ...]


def _profiles(
    peers: tuple[WalletEventFeatureVector, ...], policy: WalletCohortPolicy
) -> tuple[_FeatureProfile, ...]:
    return tuple(
        _FeatureProfile(
            spec.name,
            tuple(sorted(value for peer in peers if (value := peer.value(spec.name)) is not None)),
        )
        for spec in policy.feature_specs
    )


def _median_excluding(values: tuple[Decimal, ...], excluded: Decimal | None) -> Decimal:
    if excluded is None:
        return _median(values)
    index = bisect_left(values, excluded)
    count = len(values) - 1
    if count % 2:
        wanted = count // 2
        source = wanted if wanted < index else wanted + 1
        return values[source]
    left = count // 2 - 1
    right = count // 2
    left_source = left if left < index else left + 1
    right_source = right if right < index else right + 1
    return (values[left_source] + values[right_source]) / Decimal(2)


def _surprises_from_profiles(
    target: WalletEventFeatureVector,
    profiles: tuple[_FeatureProfile, ...],
    policy: WalletCohortPolicy,
    *,
    leave_out_actor: str | None = None,
) -> tuple[tuple[FeatureSurprise, ...], Decimal | None]:
    results: list[FeatureSurprise] = []
    available: list[tuple[Decimal, Decimal]] = []
    profile_by_name = {profile.name: profile for profile in profiles}
    with localcontext() as context:
        context.prec = policy.decimal_precision
        context.rounding = ROUND_HALF_EVEN
        for spec in policy.feature_specs:
            target_value = target.value(spec.name)
            profile = profile_by_name[spec.name]
            values = profile.values
            excluded = target_value if leave_out_actor == target.actor_uid and target_value is not None else None
            peer_count = len(values) - (1 if excluded is not None else 0)
            if target_value is None:
                results.append(FeatureSurprise(spec.name, spec.tail, peer_count, None, None, "target_feature_unavailable"))
                continue
            required = policy.min_peer_count
            if peer_count < required:
                results.append(FeatureSurprise(spec.name, spec.tail, peer_count, None, None, "insufficient_peer_support"))
                continue
            if spec.tail == "high":
                extreme_count = len(values) - bisect_left(values, target_value)
                if excluded is not None and excluded >= target_value:
                    extreme_count -= 1
            else:
                center = _median_excluding(values, excluded)
                deviation = abs(target_value - center)
                if deviation == 0:
                    extreme_count = peer_count
                else:
                    low = center - deviation
                    high = center + deviation
                    extreme_count = bisect_right(values, low) + len(values) - bisect_left(values, high)
                    if excluded is not None and abs(excluded - center) >= deviation:
                        extreme_count -= 1
            tail_p = (Decimal(1) + Decimal(extreme_count)) / (Decimal(peer_count) + Decimal(1))
            surprise = -tail_p.log10()
            results.append(FeatureSurprise(spec.name, spec.tail, peer_count, tail_p, surprise, "available"))
            available.append((min(surprise, policy.surprise_clip), spec.weight))
        if len(available) < policy.min_supported_features:
            return tuple(results), None
        top = sorted(available, key=lambda item: item[0], reverse=True)[: policy.composite_top_k]
        numerator = sum((value * weight for value, weight in top), Decimal("0"))
        denominator = sum((weight for _value, weight in top), Decimal("0"))
        return tuple(results), numerator / denominator


def _surprises(
    target: WalletEventFeatureVector,
    peers: tuple[WalletEventFeatureVector, ...],
    policy: WalletCohortPolicy,
) -> tuple[tuple[FeatureSurprise, ...], Decimal | None]:
    """Compatibility helper for an exact candidate score over one peer profile."""

    return _surprises_from_profiles(target, _profiles(peers, policy), policy)


def _nuisance_support(
    actor_uid: str,
    peer_uids: tuple[str, ...],
    vectors: Mapping[str, NuisanceVector],
    policy: WalletCohortPolicy,
) -> tuple[Decimal | None, tuple[str, ...], str | None]:
    target = vectors.get(actor_uid)
    required_names = tuple(name for name, _scale in policy.nuisance_scales)
    if target is None or tuple(target.mapping()) != required_names:
        return None, (), "nuisance_context_unavailable"
    target_values = target.mapping()
    distances: list[tuple[Decimal, str]] = []
    with localcontext() as context:
        context.prec = policy.decimal_precision
        for peer_uid in peer_uids:
            peer = vectors.get(peer_uid)
            if peer is None or tuple(peer.mapping()) != required_names:
                continue
            peer_values = peer.mapping()
            squared = sum(
                (((target_values[name] - peer_values[name]) / scale) ** 2 for name, scale in policy.nuisance_scales),
                Decimal("0"),
            )
            distances.append((squared.sqrt(), peer_uid))
    distances.sort(key=lambda item: (item[0], item[1]))
    if len(distances) < policy.nuisance_k:
        return None, tuple(uid for _distance, uid in distances), "nuisance_peer_support_insufficient"
    neighbors = tuple(uid for _distance, uid in distances[: policy.nuisance_k])
    kth = distances[policy.nuisance_k - 1][0]
    if kth > policy.nuisance_max_distance:
        return kth, neighbors, "nuisance_support_ood"
    return kth, neighbors, None


def _abstained_row(
    actor_uid: str,
    reasons: Iterable[str],
    vector: WalletEventFeatureVector | None = None,
    *,
    nuisance_kth_distance: Decimal | None = None,
    nuisance_neighbor_uids: tuple[str, ...] = (),
) -> RankedWallet:
    return RankedWallet(
        actor_uid=actor_uid,
        status="abstain",
        abstention_reasons=tuple(sorted(set(reasons))),
        feature_vector=vector,
        feature_surprises=(),
        novelty_composite=None,
        cohort_percentile=None,
        nuisance_kth_distance=nuisance_kth_distance,
        nuisance_neighbor_uids=nuisance_neighbor_uids,
        case_resemblance=None,
        case_resemblance_status="unavailable_not_supplied",
    )


def _signal_classification(
    percentile: Decimal | None, policy: WalletCohortPolicy
) -> Literal["high", "elevated", "routine", "insufficient_data"]:
    if percentile is None:
        return "insufficient_data"
    if percentile >= policy.high_signal_percentile_min:
        return "high"
    if percentile >= policy.elevated_signal_percentile_min:
        return "elevated"
    return "routine"


def rank_wallet_cohort(
    *,
    coverage: PopulationCoverage,
    policy: WalletCohortPolicy,
    fills: Iterable[object],
    candidate_actor_uids: Iterable[str],
    nuisance_vectors: Iterable[NuisanceVector],
) -> CohortRankingReport:
    """Rank declared candidates against all other causally admitted wallets."""

    candidates = tuple(sorted({_text(item, "candidate actor UID") for item in candidate_actor_uids}))
    candidate_hash = sha256(canonical_json_bytes(candidates)).hexdigest()
    global_reasons: list[str] = []
    if policy.as_of != coverage.as_of:
        global_reasons.append("policy_coverage_cutoff_mismatch")
    if policy.focus_market_uid is None:
        global_reasons.append("focus_market_not_declared")
    elif policy.focus_market_uid not in coverage.scope_market_uids:
        global_reasons.append("focus_market_outside_population_scope")
    availability_cutoff = policy.availability_cutoff or policy.as_of
    if policy.focus_event_time is None:
        global_reasons.append("focus_market_event_time_unmapped")
    # In a hindsight reconstruction, a captured same-market trade proves that
    # the market publicly existed no later than that event.  That documented
    # upper bound is sufficient for a same-market peer comparison even when
    # the exact publication timestamp is absent from the source delivery.  We
    # retain focus_published_at=None and use no such fallback operationally.
    if policy.focus_published_at is None and not (
        policy.analysis_mode == "hindsight_reconstructed"
        and policy.focus_event_time is not None
    ):
        global_reasons.append("focus_market_publication_time_unmapped")
    if policy.focus_available_at is None:
        global_reasons.append("focus_market_availability_unmapped")
    if policy.analysis_mode == "operational":
        if policy.focus_event_time is not None and policy.focus_event_time > policy.as_of:
            global_reasons.append("focus_market_event_after_decision_cutoff")
        if policy.focus_published_at is not None and policy.focus_published_at > policy.as_of:
            global_reasons.append("focus_market_published_after_decision_cutoff")
        if policy.focus_available_at is not None and (
            policy.focus_available_at > policy.as_of or policy.focus_available_at > policy.frozen_at
        ):
            global_reasons.append("focus_market_not_frozen_before_operational_scoring")
    else:
        if policy.focus_event_time is not None and policy.focus_event_time > policy.as_of:
            global_reasons.append("focus_market_event_after_decision_cutoff")
        if policy.focus_published_at is not None and policy.focus_published_at > policy.as_of:
            global_reasons.append("focus_market_published_after_decision_cutoff")
        if policy.focus_available_at is not None and policy.focus_available_at > availability_cutoff:
            global_reasons.append("focus_market_availability_after_hindsight_availability_cutoff")
        if policy.focus_available_at is not None and policy.focus_available_at > policy.frozen_at:
            global_reasons.append("focus_market_not_frozen_before_hindsight_scoring")
    if (
        policy.focus_published_at is not None
        and policy.focus_available_at is not None
        and policy.focus_available_at < policy.focus_published_at
    ):
        global_reasons.append("focus_market_availability_precedes_publication")
    if coverage.retrieved_at > availability_cutoff:
        global_reasons.append("population_coverage_not_available_by_declared_cutoff")
    if policy.analysis_mode == "operational" and coverage.retrieved_at > policy.as_of:
        global_reasons.append("population_coverage_late_for_operational_ranking")

    nuisance: dict[str, NuisanceVector] = {}
    nuisance_invalid: dict[str, set[str]] = {}
    for item in nuisance_vectors:
        if not isinstance(item, NuisanceVector):
            global_reasons.append("non_nuisance_vector_input")
            continue
        reasons = nuisance_invalid.setdefault(item.actor_uid, set())
        if item.event_time > policy.as_of:
            reasons.add("nuisance_event_time_after_decision_cutoff")
        if item.available_at > availability_cutoff:
            reasons.add("nuisance_not_available_by_declared_cutoff")
        if policy.analysis_mode == "operational" and item.available_at > policy.as_of:
            reasons.add("nuisance_late_for_operational_ranking")
        existing = nuisance.get(item.actor_uid)
        if existing is not None and existing.vector_sha256 != item.vector_sha256:
            reasons.add("nuisance_actor_conflict")
        elif existing is None:
            nuisance[item.actor_uid] = item
    for actor_uid, reasons in nuisance_invalid.items():
        if reasons:
            nuisance.pop(actor_uid, None)
    nuisance_hash = sha256(
        canonical_json_bytes([nuisance[uid].to_payload() for uid in sorted(nuisance)])
    ).hexdigest()

    unique: dict[str, TradeFill] = {}
    invalid_reasons: set[str] = set()
    raw_hashes = set(coverage.raw_sha256)
    for item in fills:
        if not isinstance(item, TradeFill):
            invalid_reasons.add("non_fill_population_input")
            continue
        if item.platform != coverage.platform or item.source_uid != coverage.source_uid:
            invalid_reasons.add("population_input_scope_mismatch")
            continue
        if item.market_uid not in coverage.scope_market_uids:
            invalid_reasons.add("population_input_market_outside_scope")
            continue
        if not coverage.interval_start <= item.event_time <= coverage.interval_end:
            invalid_reasons.add("population_input_event_outside_coverage")
            continue
        raw_digest = _raw_digest(item)
        if coverage.slices:
            matching_slices = tuple(
                part for part in coverage.slices
                if part.interval_start <= item.event_time <= part.interval_end
            )
            if len(matching_slices) != 1:
                invalid_reasons.add("population_input_not_bound_to_exactly_one_coverage_slice")
                continue
            if raw_digest not in set(matching_slices[0].raw_sha256):
                invalid_reasons.add("population_input_raw_lineage_not_bound_to_event_slice")
                continue
        if (
            item.event_time > policy.as_of
            or item.ingested_at > availability_cutoff
            or item.event_time > item.ingested_at
            or (policy.analysis_mode == "operational" and item.ingested_at > policy.as_of)
        ):
            invalid_reasons.add("population_input_clock_ineligible")
            continue
        if item.actor_visibility != ActorVisibility.PUBLIC_WALLET or item.actor_uid is None:
            invalid_reasons.add("population_actor_unavailable")
            continue
        if raw_digest not in raw_hashes:
            invalid_reasons.add("population_input_raw_lineage_unbound")
            continue
        existing = unique.get(item.fill_uid)
        if existing is not None and canonical_json_bytes(existing) != canonical_json_bytes(item):
            invalid_reasons.add("population_fill_uid_conflict")
            continue
        if existing is not None:
            continue
        unique.setdefault(item.fill_uid, item)
    admitted = tuple(sorted(unique.values(), key=lambda item: (item.event_time, item.fill_uid)))
    if len(admitted) != coverage.canonical_record_count:
        invalid_reasons.add("population_canonical_record_count_mismatch")
    global_reasons.extend(sorted(invalid_reasons))
    input_hashes = tuple(sha256(canonical_json_bytes(item)).hexdigest() for item in admitted)
    input_hash = sha256(canonical_json_bytes(input_hashes)).hexdigest()

    by_actor: dict[str, list[TradeFill]] = {}
    for fill in admitted:
        by_actor.setdefault(fill.actor_uid or "", []).append(fill)
    vectors = {
        actor_uid: _feature_vector(actor_uid, tuple(actor_fills), policy)
        for actor_uid, actor_fills in sorted(by_actor.items())
    }
    feature_hash = sha256(
        canonical_json_bytes([vectors[actor_uid].to_payload() for actor_uid in sorted(vectors)])
    ).hexdigest()
    population_vectors = tuple(vectors[uid] for uid in sorted(vectors))
    population_actor_uids = tuple(sorted(vectors))
    population_actor_set_hash = sha256(canonical_json_bytes(population_actor_uids)).hexdigest()
    population_profiles = _profiles(population_vectors, policy)
    profile_hash = sha256(canonical_json_bytes(population_profiles)).hexdigest()
    if global_reasons:
        rows = tuple(_abstained_row(actor_uid, global_reasons, vectors.get(actor_uid)) for actor_uid in candidates)
        return CohortRankingReport(
            policy=policy,
            coverage=coverage,
            status="abstain",
            abstention_reasons=tuple(sorted(set(global_reasons))),
            input_fill_count=len(admitted),
            input_sha256=input_hash,
            feature_snapshot_sha256=feature_hash,
            peer_profile_sha256=profile_hash,
            nuisance_snapshot_sha256=nuisance_hash,
            candidate_set_sha256=candidate_hash,
            population_actor_count=len(population_actor_uids),
            population_actor_set_sha256=population_actor_set_hash,
            rows=rows,
        )

    population_scores: dict[str, tuple[tuple[FeatureSurprise, ...], Decimal]] = {}
    if len(population_vectors) - 1 >= policy.min_peer_count:
        for vector in population_vectors:
            surprises, composite = _surprises_from_profiles(
                vector,
                population_profiles,
                policy,
                leave_out_actor=vector.actor_uid,
            )
            if composite is not None:
                population_scores[vector.actor_uid] = (surprises, composite)
    sorted_population_composites = tuple(sorted(composite for _surprises, composite in population_scores.values()))

    rows: list[RankedWallet] = []
    for actor_uid in candidates:
        target = vectors.get(actor_uid)
        if target is None:
            rows.append(_abstained_row(actor_uid, ("candidate_activity_unavailable",)))
            continue
        peer_uids = tuple(uid for uid in sorted(vectors) if uid != actor_uid)
        if len(peer_uids) < policy.min_peer_count:
            rows.append(_abstained_row(actor_uid, ("peer_count_below_minimum",), target))
            continue
        if nuisance_invalid.get(actor_uid):
            rows.append(_abstained_row(actor_uid, nuisance_invalid[actor_uid], target))
            continue
        kth, neighbors, nuisance_reason = _nuisance_support(actor_uid, peer_uids, nuisance, policy)
        if nuisance_reason is not None:
            rows.append(
                _abstained_row(
                    actor_uid,
                    (nuisance_reason,),
                    target,
                    nuisance_kth_distance=kth,
                    nuisance_neighbor_uids=neighbors,
                )
            )
            continue
        scored = population_scores.get(actor_uid)
        if scored is None:
            rows.append(_abstained_row(actor_uid, ("insufficient_supported_features",), target))
            continue
        surprises, composite = scored
        calibration_count = len(sorted_population_composites) - 1
        if calibration_count < policy.min_peer_count:
            rows.append(_abstained_row(actor_uid, ("cohort_calibration_support_insufficient",), target))
            continue
        left = bisect_left(sorted_population_composites, composite)
        right = bisect_right(sorted_population_composites, composite)
        less = left
        equal = right - left - 1
        with localcontext() as context:
            context.prec = policy.decimal_precision
            percentile = (
                Decimal(less) + Decimal(equal) / Decimal(2) + Decimal("0.5")
            ) / (Decimal(calibration_count) + Decimal(1))
        # Descending competition rank: tied scores share the same rank and only
        # scores strictly greater than this tie group precede them.
        population_rank = 1 + len(sorted_population_composites) - right
        rows.append(
            RankedWallet(
                actor_uid=actor_uid,
                status="available",
                abstention_reasons=(),
                feature_vector=target,
                feature_surprises=surprises,
                novelty_composite=composite,
                cohort_percentile=percentile,
                nuisance_kth_distance=kth,
                nuisance_neighbor_uids=neighbors,
                case_resemblance=None,
                case_resemblance_status="unavailable_not_supplied",
                population_rank=population_rank,
                population_size=len(population_scores),
                signal_classification=_signal_classification(percentile, policy),
            )
        )

    available = sorted(
        (item for item in rows if item.status == "available"),
        key=lambda item: (-(item.cohort_percentile or Decimal("0")), -(item.novelty_composite or Decimal("0")), item.actor_uid),
    )
    score_groups: dict[tuple[Decimal | None, Decimal | None], tuple[int, int]] = {}
    previous_score: tuple[Decimal | None, Decimal | None] | None = None
    tie_group = 0
    for order, item in enumerate(available, start=1):
        score = (item.cohort_percentile, item.novelty_composite)
        if score != previous_score:
            tie_group += 1
            score_groups[score] = (order, tie_group)
            previous_score = score
    ranked_by_actor: dict[str, RankedWallet] = {}
    for order, item in enumerate(available, start=1):
        rank, group = score_groups[(item.cohort_percentile, item.novelty_composite)]
        ranked_by_actor[item.actor_uid] = RankedWallet(
            actor_uid=item.actor_uid,
            status=item.status,
            abstention_reasons=item.abstention_reasons,
            feature_vector=item.feature_vector,
            feature_surprises=item.feature_surprises,
            novelty_composite=item.novelty_composite,
            cohort_percentile=item.cohort_percentile,
            nuisance_kth_distance=item.nuisance_kth_distance,
            nuisance_neighbor_uids=item.nuisance_neighbor_uids,
            case_resemblance=None,
            case_resemblance_status="unavailable_not_supplied",
            statistical_rank=rank,
            tie_group=group,
            display_order=order,
            population_rank=item.population_rank,
            population_size=item.population_size,
            operational_review_rank=item.population_rank if policy.analysis_mode == "operational" else None,
            hindsight_peer_rank=item.population_rank if policy.analysis_mode == "hindsight_reconstructed" else None,
            signal_classification=item.signal_classification,
        )
    final_rows = tuple(
        sorted(
            (ranked_by_actor.get(item.actor_uid, item) for item in rows),
            key=lambda item: (item.display_order is None, item.display_order or 0, item.actor_uid),
        )
    )
    report_status: Literal["available", "abstain"] = "available" if any(item.status == "available" for item in final_rows) else "abstain"
    report_reasons = () if report_status == "available" else tuple(sorted({reason for item in final_rows for reason in item.abstention_reasons}))
    return CohortRankingReport(
        policy=policy,
        coverage=coverage,
        status=report_status,
        abstention_reasons=report_reasons,
        input_fill_count=len(admitted),
        input_sha256=input_hash,
        feature_snapshot_sha256=feature_hash,
        peer_profile_sha256=profile_hash,
        nuisance_snapshot_sha256=nuisance_hash,
        candidate_set_sha256=candidate_hash,
        population_actor_count=len(population_actor_uids),
        population_actor_set_sha256=population_actor_set_hash,
        rows=final_rows,
        prospective_eligible=(policy.analysis_mode == "operational" and report_status == "available"),
    )


__all__ = [
    "CohortQueryFilter",
    "CohortMarketScopeBinding",
    "CohortRankingReport",
    "FeatureSpec",
    "FeatureSurprise",
    "NuisanceVector",
    "PopulationCoverage",
    "PopulationCoverageSlice",
    "RankedWallet",
    "WalletCohortPolicy",
    "WalletEventFeatureVector",
    "WalletSignalAssessment",
    "rank_wallet_cohort",
]
