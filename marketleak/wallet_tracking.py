"""Immutable, causal wallet-watch enrollment and longitudinal snapshots.

This module deliberately performs no network collection and no identity
resolution.  It turns an already-observed market incident plus genuine,
canonical ``TradeFill`` records into bounded public-wallet watch registrations.
Later history can describe pseudonymous behavior, but it cannot be promoted
back into evidence that was available at the original trigger cutoff.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Iterable, Literal, Mapping

from marketleak.actors.features import ActorFeatures, build_actor_features
from marketleak.domain import ActorVisibility, CoverageStatus, Outcome, TradeFill
from marketleak.ingestion.normalize import canonical_json_bytes


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _nonempty(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _distinct(values: tuple[str, ...], *, field_name: str, required: bool = False) -> tuple[str, ...]:
    normalized = tuple(_nonempty(item, field_name=field_name) for item in values)
    if required and not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must contain distinct values")
    return normalized


def _uid(prefix: str, *parts: str) -> str:
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


class WalletMarketScope(str, Enum):
    """Declared market boundary for longitudinal collection."""

    ALL_PLATFORM_MARKETS = "all_platform_markets"
    EXPLICIT_MARKETS = "explicit_markets"


@dataclass(frozen=True, slots=True)
class WalletSourceScope:
    """Exact, reviewable sources eligible for one wallet watch."""

    scope_uid: str
    platform: str
    source_uids: tuple[str, ...]
    market_scope: WalletMarketScope
    market_uids: tuple[str, ...] = ()
    record_types: tuple[Literal["trade_fill"], ...] = ("trade_fill",)

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope_uid", _nonempty(self.scope_uid, field_name="scope_uid"))
        platform = _nonempty(self.platform, field_name="platform").lower()
        object.__setattr__(self, "platform", platform)
        object.__setattr__(
            self,
            "source_uids",
            _distinct(self.source_uids, field_name="source_uids", required=True),
        )
        markets = _distinct(self.market_uids, field_name="market_uids")
        object.__setattr__(self, "market_uids", markets)
        if self.market_scope == WalletMarketScope.EXPLICIT_MARKETS and not markets:
            raise ValueError("explicit market scope requires market_uids")
        if self.market_scope == WalletMarketScope.ALL_PLATFORM_MARKETS and markets:
            raise ValueError("all-platform scope cannot also enumerate market_uids")
        if self.record_types != ("trade_fill",):
            raise ValueError("wallet tracking currently accepts only genuine trade_fill records")

    def permits(self, fill: TradeFill) -> bool:
        if fill.platform != self.platform or fill.source_uid not in self.source_uids:
            return False
        return self.market_scope == WalletMarketScope.ALL_PLATFORM_MARKETS or fill.market_uid in self.market_uids


@dataclass(frozen=True, slots=True)
class BackgroundCohortMarker:
    """Marker binding comparisons to a cohort fixed before the trigger."""

    cohort_uid: str
    selection_policy_uid: str
    scope_uid: str
    ascertained_at: datetime
    frozen_at: datetime

    def __post_init__(self) -> None:
        for name in ("cohort_uid", "selection_policy_uid", "scope_uid"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), field_name=name))
        ascertained = _utc(self.ascertained_at, field_name="ascertained_at")
        frozen = _utc(self.frozen_at, field_name="frozen_at")
        object.__setattr__(self, "ascertained_at", ascertained)
        object.__setattr__(self, "frozen_at", frozen)
        if frozen < ascertained:
            raise ValueError("background cohort cannot be frozen before it was ascertained")


@dataclass(frozen=True, slots=True)
class WalletTrackingRequest:
    """One bounded request to enroll wallets observed in an incident window."""

    request_uid: str
    incident_uid: str
    market_uid: str
    outcome_uid: str | None
    incident_starts_at: datetime
    incident_ends_at: datetime
    trigger_event_time: datetime
    trigger_available_at: datetime
    as_of: datetime
    trigger_source_uid: str
    trigger_raw_artifact_uids: tuple[str, ...]
    source_scope: WalletSourceScope
    background_cohort: BackgroundCohortMarker
    pre_window: timedelta
    post_window: timedelta
    top_k: int
    follow_up_interval: timedelta
    retry_delay: timedelta
    max_retries: int
    cooldown: timedelta

    def __post_init__(self) -> None:
        for name in ("request_uid", "incident_uid", "market_uid", "trigger_source_uid"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), field_name=name))
        if self.outcome_uid is not None:
            object.__setattr__(self, "outcome_uid", _nonempty(self.outcome_uid, field_name="outcome_uid"))
        for name in (
            "incident_starts_at",
            "incident_ends_at",
            "trigger_event_time",
            "trigger_available_at",
            "as_of",
        ):
            object.__setattr__(self, name, _utc(getattr(self, name), field_name=name))
        object.__setattr__(
            self,
            "trigger_raw_artifact_uids",
            _distinct(
                self.trigger_raw_artifact_uids,
                field_name="trigger_raw_artifact_uids",
                required=True,
            ),
        )
        if self.incident_ends_at < self.incident_starts_at:
            raise ValueError("incident_ends_at cannot precede incident_starts_at")
        if not (
            self.incident_starts_at
            <= self.trigger_event_time
            <= self.incident_ends_at
            <= self.trigger_available_at
            <= self.as_of
        ):
            raise ValueError("trigger clocks must be causal and no later than as_of")
        if self.as_of > self.incident_ends_at + self.post_window:
            raise ValueError("request as_of is already outside the bounded post window")
        if self.source_scope.platform != self.market_uid.split(":", 1)[0]:
            raise ValueError("market_uid and source scope platform must agree")
        if (
            self.source_scope.market_scope == WalletMarketScope.EXPLICIT_MARKETS
            and self.market_uid not in self.source_scope.market_uids
        ):
            raise ValueError("incident market must be included in explicit source scope")
        if self.background_cohort.scope_uid != self.source_scope.scope_uid:
            raise ValueError("background cohort must be ascertained for the same source scope")
        if self.background_cohort.frozen_at > self.trigger_available_at:
            raise ValueError("background cohort must be frozen before the trigger became available")
        if self.pre_window < timedelta(0) or self.post_window <= timedelta(0):
            raise ValueError("pre_window must be non-negative and post_window must be positive")
        if self.top_k <= 0:
            raise ValueError("top_k must be positive")
        if self.follow_up_interval <= timedelta(0) or self.retry_delay <= timedelta(0):
            raise ValueError("follow-up and retry intervals must be positive")
        if self.max_retries < 0 or self.cooldown < timedelta(0):
            raise ValueError("max_retries and cooldown must be non-negative")


@dataclass(frozen=True, slots=True)
class FillLineage:
    fill_uid: str
    canonical_sha256: str
    event_time: datetime
    available_at: datetime
    source_uid: str
    raw_artifact_uid: str
    transaction_uid: str | None

    def __post_init__(self) -> None:
        for name in ("fill_uid", "source_uid", "raw_artifact_uid"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), field_name=name))
        canonical_hash = _nonempty(self.canonical_sha256, field_name="canonical_sha256").lower()
        if len(canonical_hash) != 64 or any(character not in "0123456789abcdef" for character in canonical_hash):
            raise ValueError("canonical_sha256 must be a lowercase SHA-256 hex digest")
        object.__setattr__(self, "canonical_sha256", canonical_hash)
        if self.transaction_uid is not None:
            object.__setattr__(
                self,
                "transaction_uid",
                _nonempty(self.transaction_uid, field_name="transaction_uid"),
            )
        event_time = _utc(self.event_time, field_name="fill event_time")
        available_at = _utc(self.available_at, field_name="fill available_at")
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        if event_time > available_at:
            raise ValueError("fill event_time cannot be later than fill available_at")


@dataclass(frozen=True, slots=True)
class WatchRegistration:
    """Immutable enrollment; it never implies identity, control, or misconduct."""

    registration_uid: str
    request_uid: str
    incident_uid: str
    actor_uid: str
    actor_visibility: ActorVisibility
    enrolled_at: datetime
    trigger_as_of: datetime
    source_scope: WalletSourceScope
    trigger_source_uid: str
    trigger_raw_artifact_uids: tuple[str, ...]
    fill_lineage: tuple[FillLineage, ...]
    attributable_notional: Decimal
    history_window_starts_at: datetime
    history_window_ends_at: datetime
    next_follow_up_at: datetime
    expires_at: datetime
    cooldown_until: datetime
    follow_up_interval: timedelta
    retry_delay: timedelta
    max_retries: int
    background_cohort: BackgroundCohortMarker
    limitations: tuple[str, ...] = (
        "This registration describes a public pseudonymous account, not a person.",
        "Enrollment is a collection decision, not evidence of wrongdoing.",
    )

    def __post_init__(self) -> None:
        for name in ("registration_uid", "request_uid", "incident_uid", "actor_uid", "trigger_source_uid"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), field_name=name))
        if self.actor_visibility != ActorVisibility.PUBLIC_WALLET:
            raise ValueError("wallet watch registration requires PUBLIC_WALLET visibility")
        for name in (
            "enrolled_at",
            "trigger_as_of",
            "history_window_starts_at",
            "history_window_ends_at",
            "next_follow_up_at",
            "expires_at",
            "cooldown_until",
        ):
            object.__setattr__(self, name, _utc(getattr(self, name), field_name=name))
        object.__setattr__(
            self,
            "trigger_raw_artifact_uids",
            _distinct(self.trigger_raw_artifact_uids, field_name="trigger_raw_artifact_uids", required=True),
        )
        if not self.fill_lineage or len({item.fill_uid for item in self.fill_lineage}) != len(self.fill_lineage):
            raise ValueError("watch registration requires distinct trigger fills")
        if any(item.available_at > self.trigger_as_of for item in self.fill_lineage):
            raise ValueError("trigger fills must have been available by trigger_as_of")
        if self.attributable_notional < 0:
            raise ValueError("attributable_notional cannot be negative")
        if not self.history_window_starts_at <= self.trigger_as_of < self.history_window_ends_at:
            raise ValueError("trigger_as_of must fall inside the bounded history window")
        if not self.enrolled_at <= self.next_follow_up_at <= self.expires_at <= self.cooldown_until:
            raise ValueError("follow-up, expiry, and cooldown clocks are inconsistent")
        if self.background_cohort.scope_uid != self.source_scope.scope_uid:
            raise ValueError("registration cohort and source scope must agree")


class EnrollmentDecisionStatus(str, Enum):
    ENROLLED = "enrolled"
    DUPLICATE_ACTIVE_WATCH = "duplicate_active_watch"
    COOLDOWN_ACTIVE = "cooldown_active"


@dataclass(frozen=True, slots=True)
class EnrollmentDecision:
    actor_uid: str
    rank: int
    attributable_notional: Decimal
    status: EnrollmentDecisionStatus
    reason: str
    registration_uid: str | None = None


@dataclass(frozen=True, slots=True)
class WalletEnrollmentResult:
    request_uid: str
    as_of: datetime
    registrations: tuple[WatchRegistration, ...]
    decisions: tuple[EnrollmentDecision, ...]
    incident_fill_count: int
    eligible_fill_count: int
    late_fill_count: int
    actor_unavailable_fill_count: int
    no_watch_reasons: tuple[str, ...] = ()

    @property
    def enrolled(self) -> bool:
        return bool(self.registrations)


def _fill_lineage(fill: TradeFill) -> FillLineage:
    return FillLineage(
        fill_uid=fill.fill_uid,
        canonical_sha256=sha256(canonical_json_bytes(fill)).hexdigest(),
        event_time=fill.event_time,
        available_at=fill.ingested_at,
        source_uid=fill.source_uid,
        raw_artifact_uid=fill.raw_artifact_uid,
        transaction_uid=fill.transaction_uid,
    )


def enroll_wallet_watches(
    request: WalletTrackingRequest,
    fills: Iterable[object],
    *,
    existing_registrations: Iterable[WatchRegistration] = (),
) -> WalletEnrollmentResult:
    """Enroll top-K public wallets from causal, genuine incident fills only."""

    all_incident: list[TradeFill] = []
    eligible: dict[str, TradeFill] = {}
    late_count = 0
    actor_unavailable_count = 0
    for item in fills:
        if not isinstance(item, TradeFill):
            continue
        if item.market_uid != request.market_uid or item.platform != request.source_scope.platform:
            continue
        if request.outcome_uid is not None and item.outcome_uid != request.outcome_uid:
            continue
        if item.source_uid not in request.source_scope.source_uids:
            continue
        if not request.incident_starts_at <= item.event_time <= request.incident_ends_at:
            continue
        all_incident.append(item)
        if item.event_time > item.ingested_at or item.ingested_at > request.as_of:
            late_count += 1
            continue
        if item.size <= 0:
            continue
        if item.actor_visibility != ActorVisibility.PUBLIC_WALLET or item.actor_uid is None:
            actor_unavailable_count += 1
            continue
        current = eligible.get(item.fill_uid)
        if current is None:
            eligible[item.fill_uid] = item

    eligible_fills = sorted(eligible.values(), key=lambda item: (item.event_time, item.fill_uid))
    by_actor: dict[str, list[TradeFill]] = {}
    for fill in eligible_fills:
        by_actor.setdefault(fill.actor_uid or "", []).append(fill)
    ranked = sorted(
        (
            (
                actor_uid,
                sum((fill.price * fill.size for fill in actor_fills), Decimal("0")),
                actor_fills,
            )
            for actor_uid, actor_fills in by_actor.items()
        ),
        key=lambda item: (-item[1], item[0]),
    )[: request.top_k]

    existing = tuple(existing_registrations)
    decisions: list[EnrollmentDecision] = []
    registrations: list[WatchRegistration] = []
    for rank, (actor_uid, notional, actor_fills) in enumerate(ranked, start=1):
        same_scope = sorted(
            (
                registration
                for registration in existing
                if registration.actor_uid == actor_uid
                and registration.source_scope.scope_uid == request.source_scope.scope_uid
            ),
            key=lambda item: (item.enrolled_at, item.registration_uid),
            reverse=True,
        )
        active = next((item for item in same_scope if request.as_of < item.expires_at), None)
        if active is not None:
            decisions.append(
                EnrollmentDecision(
                    actor_uid,
                    rank,
                    notional,
                    EnrollmentDecisionStatus.DUPLICATE_ACTIVE_WATCH,
                    "an immutable active watch already covers this actor and source scope",
                    active.registration_uid,
                )
            )
            continue
        cooling = next((item for item in same_scope if request.as_of < item.cooldown_until), None)
        if cooling is not None:
            decisions.append(
                EnrollmentDecision(
                    actor_uid,
                    rank,
                    notional,
                    EnrollmentDecisionStatus.COOLDOWN_ACTIVE,
                    "the previous watch expired but its declared cooldown is still active",
                    cooling.registration_uid,
                )
            )
            continue
        fill_lineage = tuple(_fill_lineage(fill) for fill in actor_fills)
        registration_uid = _uid(
            "wallet-watch",
            request.request_uid,
            actor_uid,
            *(item.fill_uid for item in fill_lineage),
        )
        expires_at = request.incident_ends_at + request.post_window
        registration = WatchRegistration(
            registration_uid=registration_uid,
            request_uid=request.request_uid,
            incident_uid=request.incident_uid,
            actor_uid=actor_uid,
            actor_visibility=ActorVisibility.PUBLIC_WALLET,
            enrolled_at=request.as_of,
            trigger_as_of=request.as_of,
            source_scope=request.source_scope,
            trigger_source_uid=request.trigger_source_uid,
            trigger_raw_artifact_uids=request.trigger_raw_artifact_uids,
            fill_lineage=fill_lineage,
            attributable_notional=notional,
            history_window_starts_at=request.incident_starts_at - request.pre_window,
            history_window_ends_at=expires_at,
            next_follow_up_at=min(request.as_of + request.follow_up_interval, expires_at),
            expires_at=expires_at,
            cooldown_until=expires_at + request.cooldown,
            follow_up_interval=request.follow_up_interval,
            retry_delay=request.retry_delay,
            max_retries=request.max_retries,
            background_cohort=request.background_cohort,
        )
        registrations.append(registration)
        decisions.append(
            EnrollmentDecision(
                actor_uid,
                rank,
                notional,
                EnrollmentDecisionStatus.ENROLLED,
                "public-wallet fills were attributable and available by the trigger cutoff",
                registration_uid,
            )
        )

    no_watch_reasons: list[str] = []
    if not eligible_fills:
        if late_count:
            no_watch_reasons.append("fills_not_available_by_trigger_as_of")
        if actor_unavailable_count:
            no_watch_reasons.append("actor_not_available_on_incident_fills")
        if not all_incident:
            no_watch_reasons.append("no_trade_fills_in_incident_window")
    elif not registrations:
        no_watch_reasons.append("top_ranked_candidates_already_watched_or_in_cooldown")
    return WalletEnrollmentResult(
        request_uid=request.request_uid,
        as_of=request.as_of,
        registrations=tuple(registrations),
        decisions=tuple(decisions),
        incident_fill_count=len(all_incident),
        eligible_fill_count=len(eligible_fills),
        late_fill_count=late_count,
        actor_unavailable_fill_count=actor_unavailable_count,
        no_watch_reasons=tuple(no_watch_reasons),
    )


@dataclass(frozen=True, slots=True)
class WalletHistorySnapshot:
    """One causal, immutable longitudinal view of a watched public wallet."""

    snapshot_uid: str
    registration_uid: str
    actor_uid: str
    as_of: datetime
    available_at: datetime
    source_scope: WalletSourceScope
    window_starts_at: datetime
    window_ends_at: datetime
    fill_lineage: tuple[FillLineage, ...]
    trigger_eligible_fill_uids: tuple[str, ...]
    follow_up_only_fill_uids: tuple[str, ...]
    coverage_status: CoverageStatus
    complete_through: datetime | None
    continuation_token: str | None
    missing_reasons: tuple[str, ...]
    late_excluded_count: int
    supports_original_trigger: bool
    background_cohort: BackgroundCohortMarker

    def __post_init__(self) -> None:
        for name in ("snapshot_uid", "registration_uid", "actor_uid"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), field_name=name))
        for name in ("as_of", "available_at", "window_starts_at", "window_ends_at"):
            object.__setattr__(self, name, _utc(getattr(self, name), field_name=name))
        if self.available_at > self.as_of:
            raise ValueError("snapshot available_at cannot be later than as_of")
        if self.window_ends_at <= self.window_starts_at:
            raise ValueError("snapshot history window must have positive duration")
        complete_through = None if self.complete_through is None else _utc(
            self.complete_through,
            field_name="complete_through",
        )
        object.__setattr__(self, "complete_through", complete_through)
        if complete_through is not None and complete_through > self.as_of:
            raise ValueError("complete_through cannot be later than as_of")
        trigger_uids = _distinct(self.trigger_eligible_fill_uids, field_name="trigger_eligible_fill_uids")
        follow_up_uids = _distinct(self.follow_up_only_fill_uids, field_name="follow_up_only_fill_uids")
        object.__setattr__(self, "trigger_eligible_fill_uids", trigger_uids)
        object.__setattr__(self, "follow_up_only_fill_uids", follow_up_uids)
        if set(trigger_uids) & set(follow_up_uids):
            raise ValueError("trigger-eligible and follow-up-only fills must be disjoint")
        if set(trigger_uids) | set(follow_up_uids) != {item.fill_uid for item in self.fill_lineage}:
            raise ValueError("every history fill must have exactly one causal evidence role")
        object.__setattr__(self, "missing_reasons", _distinct(self.missing_reasons, field_name="missing_reasons"))
        if self.coverage_status == CoverageStatus.COMPLETE and (
            self.continuation_token is not None or self.missing_reasons or complete_through is None
        ):
            raise ValueError("complete coverage requires complete_through and no continuation or missingness")
        if self.coverage_status != CoverageStatus.COMPLETE and not (
            self.continuation_token is not None or self.missing_reasons
        ):
            raise ValueError("incomplete coverage requires continuation or explicit missingness")
        if self.supports_original_trigger and (follow_up_uids or self.late_excluded_count):
            raise ValueError("late or follow-up fills cannot support the original trigger")

    @property
    def complete(self) -> bool:
        return (
            self.coverage_status == CoverageStatus.COMPLETE
            and self.continuation_token is None
            and not self.missing_reasons
            and self.complete_through is not None
            and self.complete_through >= min(self.as_of, self.window_ends_at)
        )


def build_wallet_history_snapshot(
    registration: WatchRegistration,
    fills: Iterable[object],
    *,
    snapshot_uid: str,
    as_of: datetime,
    available_at: datetime,
    coverage_status: CoverageStatus,
    complete_through: datetime | None = None,
    continuation_token: str | None = None,
    missing_reasons: tuple[str, ...] = (),
) -> WalletHistorySnapshot:
    """Freeze history while separating trigger-eligible and later knowledge."""

    cutoff = _utc(as_of, field_name="as_of")
    snapshot_available = _utc(available_at, field_name="available_at")
    if snapshot_available > cutoff:
        raise ValueError("snapshot available_at cannot be later than its as_of cutoff")
    complete_cutoff = None if complete_through is None else _utc(complete_through, field_name="complete_through")
    if complete_cutoff is not None and complete_cutoff > cutoff:
        raise ValueError("complete_through cannot be later than as_of")
    normalized_missing = _distinct(missing_reasons, field_name="missing_reasons")
    if coverage_status == CoverageStatus.COMPLETE and (continuation_token or normalized_missing):
        raise ValueError("complete coverage cannot carry continuation or missingness")
    if coverage_status != CoverageStatus.COMPLETE and not (continuation_token or normalized_missing):
        raise ValueError("incomplete coverage requires continuation or an explicit missing reason")

    unique: dict[str, TradeFill] = {}
    late_excluded = 0
    for item in fills:
        if not isinstance(item, TradeFill):
            continue
        if item.actor_uid != registration.actor_uid or item.actor_visibility != ActorVisibility.PUBLIC_WALLET:
            continue
        if not registration.source_scope.permits(item):
            continue
        if not registration.history_window_starts_at <= item.event_time <= registration.history_window_ends_at:
            continue
        if item.event_time > cutoff:
            continue
        if item.event_time > item.ingested_at or item.ingested_at > snapshot_available:
            late_excluded += 1
            continue
        unique.setdefault(item.fill_uid, item)
    selected = sorted(unique.values(), key=lambda item: (item.event_time, item.fill_uid))
    lineage = tuple(_fill_lineage(item) for item in selected)
    trigger_eligible = tuple(
        item.fill_uid
        for item in lineage
        if item.event_time <= registration.trigger_as_of and item.available_at <= registration.trigger_as_of
    )
    follow_up_only = tuple(item.fill_uid for item in lineage if item.fill_uid not in set(trigger_eligible))
    supports_original = (
        snapshot_available <= registration.trigger_as_of
        and not follow_up_only
        and late_excluded == 0
    )
    return WalletHistorySnapshot(
        snapshot_uid=_nonempty(snapshot_uid, field_name="snapshot_uid"),
        registration_uid=registration.registration_uid,
        actor_uid=registration.actor_uid,
        as_of=cutoff,
        available_at=snapshot_available,
        source_scope=registration.source_scope,
        window_starts_at=registration.history_window_starts_at,
        window_ends_at=registration.history_window_ends_at,
        fill_lineage=lineage,
        trigger_eligible_fill_uids=trigger_eligible,
        follow_up_only_fill_uids=follow_up_only,
        coverage_status=coverage_status,
        complete_through=complete_cutoff,
        continuation_token=None if continuation_token is None else _nonempty(continuation_token, field_name="continuation_token"),
        missing_reasons=normalized_missing,
        late_excluded_count=late_excluded,
        supports_original_trigger=supports_original,
        background_cohort=registration.background_cohort,
    )


class FollowUpStatus(str, Enum):
    SCHEDULED = "scheduled"
    CONTINUATION_PENDING = "continuation_pending"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETE = "complete"
    EXPIRED = "expired"
    RETRIES_EXHAUSTED = "retries_exhausted"


@dataclass(frozen=True, slots=True)
class WatchFollowUpState:
    state_uid: str
    registration_uid: str
    status: FollowUpStatus
    as_of: datetime
    available_at: datetime
    next_due_at: datetime | None
    expires_at: datetime
    retry_count: int
    max_retries: int
    continuation_token: str | None
    coverage_status: CoverageStatus
    missing_reasons: tuple[str, ...]
    last_snapshot_uid: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state_uid", _nonempty(self.state_uid, field_name="state_uid"))
        object.__setattr__(
            self,
            "registration_uid",
            _nonempty(self.registration_uid, field_name="registration_uid"),
        )
        for name in ("as_of", "available_at", "expires_at"):
            object.__setattr__(self, name, _utc(getattr(self, name), field_name=name))
        if self.available_at < self.as_of:
            raise ValueError("follow-up available_at cannot precede its as_of attempt clock")
        if self.next_due_at is not None:
            next_due = _utc(self.next_due_at, field_name="next_due_at")
            object.__setattr__(self, "next_due_at", next_due)
            if next_due < self.as_of or next_due > self.expires_at:
                raise ValueError("next_due_at must be between as_of and expiry")
        if self.retry_count < 0 or self.retry_count > self.max_retries + 1:
            raise ValueError("follow-up retry_count is outside the declared retry budget")
        object.__setattr__(self, "missing_reasons", _distinct(self.missing_reasons, field_name="missing_reasons"))
        if self.status in {
            FollowUpStatus.COMPLETE,
            FollowUpStatus.EXPIRED,
            FollowUpStatus.RETRIES_EXHAUSTED,
        } and self.next_due_at is not None:
            raise ValueError("terminal follow-up states cannot have a next due time")


def initial_follow_up_state(registration: WatchRegistration) -> WatchFollowUpState:
    return WatchFollowUpState(
        state_uid=_uid("wallet-watch-state", registration.registration_uid, "0"),
        registration_uid=registration.registration_uid,
        status=FollowUpStatus.SCHEDULED,
        as_of=registration.enrolled_at,
        available_at=registration.enrolled_at,
        next_due_at=registration.next_follow_up_at,
        expires_at=registration.expires_at,
        retry_count=0,
        max_retries=registration.max_retries,
        continuation_token=None,
        coverage_status=CoverageStatus.UNKNOWN,
        missing_reasons=("initial_collection_not_yet_attempted",),
    )


def advance_follow_up_state(
    registration: WatchRegistration,
    current: WatchFollowUpState,
    *,
    attempted_at: datetime,
    available_at: datetime,
    snapshot: WalletHistorySnapshot | None = None,
    retryable_error: str | None = None,
) -> WatchFollowUpState:
    """Return the next immutable schedule state after one collection attempt."""

    attempted = _utc(attempted_at, field_name="attempted_at")
    available = _utc(available_at, field_name="available_at")
    if current.registration_uid != registration.registration_uid:
        raise ValueError("follow-up state does not belong to registration")
    if attempted < current.as_of or available < attempted:
        raise ValueError("follow-up attempt clocks must be monotonic")
    if snapshot is not None and retryable_error is not None:
        raise ValueError("one attempt cannot provide both a snapshot and an error")
    sequence = current.retry_count + 1
    state_uid = _uid("wallet-watch-state", registration.registration_uid, attempted.isoformat(), str(sequence))
    snapshot_covers_expiry = (
        snapshot is not None
        and snapshot.complete
        and snapshot.complete_through is not None
        and snapshot.complete_through >= registration.expires_at
    )
    if attempted >= registration.expires_at:
        terminal_continuation = snapshot.continuation_token if snapshot_covers_expiry and snapshot is not None else current.continuation_token
        terminal_coverage = snapshot.coverage_status if snapshot_covers_expiry and snapshot is not None else current.coverage_status
        return WatchFollowUpState(
            state_uid,
            registration.registration_uid,
            FollowUpStatus.COMPLETE if snapshot_covers_expiry else FollowUpStatus.EXPIRED,
            attempted,
            available,
            None,
            registration.expires_at,
            current.retry_count,
            registration.max_retries,
            terminal_continuation,
            terminal_coverage,
            () if snapshot_covers_expiry else tuple(sorted({*current.missing_reasons, "watch_expired"})),
            snapshot.snapshot_uid if snapshot_covers_expiry and snapshot is not None else current.last_snapshot_uid,
        )
    if retryable_error is not None:
        reason = _nonempty(retryable_error, field_name="retryable_error")
        retry_count = current.retry_count + 1
        exhausted = retry_count > registration.max_retries
        return WatchFollowUpState(
            state_uid,
            registration.registration_uid,
            FollowUpStatus.RETRIES_EXHAUSTED if exhausted else FollowUpStatus.RETRY_SCHEDULED,
            attempted,
            available,
            None if exhausted else min(attempted + registration.retry_delay, registration.expires_at),
            registration.expires_at,
            retry_count,
            registration.max_retries,
            current.continuation_token,
            CoverageStatus.PARTIAL,
            tuple(sorted({*current.missing_reasons, reason})),
            current.last_snapshot_uid,
        )
    if snapshot is None:
        raise ValueError("successful follow-up attempt requires a history snapshot")
    if snapshot.registration_uid != registration.registration_uid or snapshot.available_at > available:
        raise ValueError("history snapshot is not available for this registration and attempt")
    if snapshot.continuation_token is not None:
        status = FollowUpStatus.CONTINUATION_PENDING
        next_due = attempted
    else:
        status = FollowUpStatus.SCHEDULED
        next_due = min(attempted + registration.follow_up_interval, registration.expires_at)
    return WatchFollowUpState(
        state_uid,
        registration.registration_uid,
        status,
        attempted,
        available,
        next_due,
        registration.expires_at,
        current.retry_count,
        registration.max_retries,
        snapshot.continuation_token,
        snapshot.coverage_status,
        snapshot.missing_reasons,
        snapshot.snapshot_uid,
    )


@dataclass(frozen=True, slots=True)
class BehaviorMetricMetadata:
    name: Literal["behavior_novelty", "case_resemblance", "review_priority"]
    value: Decimal | None
    method_uid: str | None
    status: Literal["available", "unavailable", "not_evaluated"]
    is_probability: Literal[False] = False

    def __post_init__(self) -> None:
        if self.value is not None and self.value < 0:
            raise ValueError("behavior metadata values cannot be negative")
        if self.status == "available" and (self.value is None or self.method_uid is None):
            raise ValueError("available behavior metadata requires a value and method_uid")
        if self.status != "available" and self.value is not None:
            raise ValueError("unavailable metadata cannot carry a value")


def unavailable_behavior_metrics() -> tuple[BehaviorMetricMetadata, ...]:
    return tuple(
        BehaviorMetricMetadata(name, None, None, "not_evaluated")
        for name in ("behavior_novelty", "case_resemblance", "review_priority")
    )


@dataclass(frozen=True, slots=True)
class WalletBehaviorOutput:
    registration_uid: str
    actor_uid: str
    as_of: datetime
    available_at: datetime
    actor_features: ActorFeatures | None
    metrics: tuple[BehaviorMetricMetadata, ...]
    coverage_status: CoverageStatus
    missing_reasons: tuple[str, ...]
    background_cohort: BackgroundCohortMarker
    follow_up_only: bool
    effectiveness_unknown: Literal[True] = True
    human_review_required: Literal[True] = True
    limitations: tuple[str, ...] = (
        "Behavior novelty and case resemblance are triage metadata, not fraud probabilities.",
        "Transaction patterns do not establish identity, common control, intent, or legal liability.",
    )


def build_wallet_behavior_output(
    snapshot: WalletHistorySnapshot,
    fills: Iterable[object],
    *,
    outcomes: Iterable[Outcome] = (),
    metrics: tuple[BehaviorMetricMetadata, ...] | None = None,
) -> WalletBehaviorOutput:
    """Reuse causal actor features only for fills admitted to the snapshot."""

    allowed = {item.fill_uid: item for item in snapshot.fill_lineage}
    causal_fills: list[TradeFill] = []
    for item in fills:
        if not isinstance(item, TradeFill) or item.fill_uid not in allowed:
            continue
        actual_hash = sha256(canonical_json_bytes(item)).hexdigest()
        if actual_hash != allowed[item.fill_uid].canonical_sha256:
            raise ValueError(
                f"caller fill {item.fill_uid!r} does not match its snapshot canonical hash"
            )
        if (
            item.actor_uid == snapshot.actor_uid
            and item.event_time <= snapshot.as_of
            and item.ingested_at <= snapshot.available_at
        ):
            causal_fills.append(item)
    causal_outcomes = [
        outcome
        for outcome in outcomes
        if isinstance(outcome, Outcome)
        and outcome.event_time <= snapshot.as_of
        and outcome.ingested_at <= snapshot.available_at
        and outcome.resolved_at is not None
        and outcome.resolved_at <= snapshot.as_of
    ]
    features = build_actor_features(causal_fills, outcomes=causal_outcomes, cutoff=snapshot.as_of)
    actor_features = next((item for item in features if item.actor_uid == snapshot.actor_uid), None)
    selected_metrics = unavailable_behavior_metrics() if metrics is None else metrics
    names = tuple(item.name for item in selected_metrics)
    if set(names) != {"behavior_novelty", "case_resemblance", "review_priority"} or len(names) != 3:
        raise ValueError("behavior output requires exactly the three non-fraud triage metrics")
    return WalletBehaviorOutput(
        registration_uid=snapshot.registration_uid,
        actor_uid=snapshot.actor_uid,
        as_of=snapshot.as_of,
        available_at=snapshot.available_at,
        actor_features=actor_features,
        metrics=selected_metrics,
        coverage_status=snapshot.coverage_status,
        missing_reasons=snapshot.missing_reasons,
        background_cohort=snapshot.background_cohort,
        follow_up_only=not snapshot.supports_original_trigger,
    )


__all__ = [
    "BackgroundCohortMarker",
    "BehaviorMetricMetadata",
    "EnrollmentDecision",
    "EnrollmentDecisionStatus",
    "FillLineage",
    "FollowUpStatus",
    "WalletBehaviorOutput",
    "WalletEnrollmentResult",
    "WalletHistorySnapshot",
    "WalletMarketScope",
    "WalletSourceScope",
    "WalletTrackingRequest",
    "WatchFollowUpState",
    "WatchRegistration",
    "advance_follow_up_state",
    "build_wallet_behavior_output",
    "build_wallet_history_snapshot",
    "enroll_wallet_watches",
    "initial_follow_up_state",
    "unavailable_behavior_metrics",
]
