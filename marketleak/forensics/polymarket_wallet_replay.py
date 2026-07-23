"""Honest post-hoc reconstruction of one public Polymarket wallet query.

This module performs no network work and makes no identity or legal finding. It
keeps the historical event cutoff separate from the reconstruction clock. A
trade can therefore be included in a descriptive reconstruction while still
being explicitly unavailable to a prospective system at the historical cutoff.

Later enforcement material is carried only as audit context. It never enters
the feature input, does not create a training label, and cannot establish that
MarketLeak emitted an alert at the historical time.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Iterable

from marketleak.actors.features import build_actor_features
from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill, TradeSide
from marketleak.ingestion.coverage import CoverageRecord
from marketleak.ingestion.normalize import canonical_json_bytes


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WALLET_UID_RE = re.compile(r"^polymarket:wallet/(0x[0-9a-f]{40})$")


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _text(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _time(value: datetime) -> str:
    return _utc(value, field_name="datetime").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decimal(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class ReplayQueryFilter:
    """One exact HTTP query value stored as canonical JSON, not inferred text."""

    name: str
    value_json: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _text(self.name, field_name="query filter name"))
        try:
            decoded = json.loads(self.value_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("query filter value_json must be valid JSON") from exc
        canonical = canonical_json_bytes(decoded).decode("utf-8")
        if canonical != self.value_json:
            raise ValueError("query filter value_json must use canonical JSON")

    @classmethod
    def from_value(cls, name: str, value: Any) -> "ReplayQueryFilter":
        return cls(name=name, value_json=canonical_json_bytes(value).decode("utf-8"))

    @property
    def value(self) -> Any:
        return json.loads(self.value_json)

    def to_payload(self) -> dict[str, Any]:
        return {"name": self.name, "value": self.value}


@dataclass(frozen=True, slots=True)
class WalletReplayCoverage:
    """Immutable coverage and exact-query receipt for one wallet crawl."""

    platform: str
    dataset: str
    source_uid: str
    queried_actor_uid: str
    interval_start: datetime
    interval_end: datetime
    retrieved_at: datetime
    status: CoverageStatus
    record_count: int
    raw_sha256: tuple[str, ...]
    query_filters: tuple[ReplayQueryFilter, ...]
    continuation: str | None = None
    missing_reasons: tuple[str, ...] = ()
    query_uid: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("dataset", "source_uid", "queried_actor_uid"):
            object.__setattr__(self, name, _text(getattr(self, name), field_name=name))
        platform = _text(self.platform, field_name="platform").lower()
        object.__setattr__(self, "platform", platform)
        start = _utc(self.interval_start, field_name="coverage interval_start")
        end = _utc(self.interval_end, field_name="coverage interval_end")
        retrieved = _utc(self.retrieved_at, field_name="coverage retrieved_at")
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        object.__setattr__(self, "retrieved_at", retrieved)
        if end < start:
            raise ValueError("coverage interval_end cannot precede interval_start")
        if isinstance(self.record_count, bool) or self.record_count < 0:
            raise ValueError("coverage record_count must be a non-negative integer")

        hashes = tuple(str(item).lower() for item in self.raw_sha256)
        if any(not _SHA256_RE.fullmatch(item) for item in hashes):
            raise ValueError("coverage raw_sha256 values must be lowercase SHA-256 digests")
        if len(hashes) != len(set(hashes)):
            raise ValueError("coverage raw_sha256 values must be distinct")
        object.__setattr__(self, "raw_sha256", hashes)

        ordered_filters = tuple(sorted(self.query_filters, key=lambda item: item.name))
        if len({item.name for item in ordered_filters}) != len(ordered_filters):
            raise ValueError("coverage query filter names must be distinct")
        object.__setattr__(self, "query_filters", ordered_filters)
        filters_by_name = {item.name: item.value for item in ordered_filters}
        for name, expected in (
            ("start", int(start.timestamp())),
            ("end", int(end.timestamp())),
        ):
            value = filters_by_name.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"coverage query filter {name} must be a canonical integer epoch")
            if value != expected:
                raise ValueError(f"coverage query filter {name} must exactly match coverage interval_{name}")
        if start.microsecond or end.microsecond:
            raise ValueError("coverage query interval must use whole-second precision")
        query_identity = {
            "contract": "polymarket_wallet_query_v1",
            "platform": platform,
            "dataset": self.dataset,
            "source_uid": self.source_uid,
            "queried_actor_uid": self.queried_actor_uid,
            "interval_start": _time(start),
            "interval_end": _time(end),
            "query_filters": [item.to_payload() for item in ordered_filters],
        }
        query_digest = sha256(canonical_json_bytes(query_identity)).hexdigest()
        object.__setattr__(self, "query_uid", f"polymarket:wallet-replay-query/{query_digest}")
        reasons = tuple(_text(item, field_name="missing reason") for item in self.missing_reasons)
        if len(reasons) != len(set(reasons)):
            raise ValueError("coverage missing_reasons must be distinct")
        object.__setattr__(self, "missing_reasons", reasons)
        if self.continuation is not None:
            object.__setattr__(self, "continuation", _text(self.continuation, field_name="continuation"))
        if self.status == CoverageStatus.COMPLETE:
            if self.continuation is not None or reasons:
                raise ValueError("complete coverage cannot carry a continuation or missing reason")
            if not hashes:
                raise ValueError("complete coverage requires raw payload hashes")
        elif self.continuation is None and not reasons:
            raise ValueError("non-complete coverage requires a continuation or missing reason")

    @classmethod
    def from_coverage_record(
        cls,
        record: CoverageRecord,
        *,
        source_uid: str,
        queried_actor_uid: str,
    ) -> "WalletReplayCoverage":
        """Convert the collector ledger row without strengthening its claim."""

        complete = bool(record.complete)
        return cls(
            platform=record.platform,
            dataset=record.dataset,
            source_uid=source_uid,
            queried_actor_uid=queried_actor_uid,
            interval_start=record.interval_start,
            interval_end=record.interval_end,
            retrieved_at=record.fetched_at,
            status=CoverageStatus.COMPLETE if complete else CoverageStatus.PARTIAL,
            record_count=record.record_count,
            raw_sha256=record.raw_sha256,
            query_filters=tuple(
                ReplayQueryFilter.from_value(name, value)
                for name, value in record.filters.items()
            ),
            continuation=record.continuation,
            missing_reasons=() if complete else ("collector_did_not_claim_complete",),
        )

    def filter_value(self, name: str) -> Any | None:
        return next((item.value for item in self.query_filters if item.name == name), None)

    def to_payload(self) -> dict[str, Any]:
        return {
            "query_uid": self.query_uid,
            "platform": self.platform,
            "dataset": self.dataset,
            "source_uid": self.source_uid,
            "queried_actor_uid": self.queried_actor_uid,
            "interval_start": _time(self.interval_start),
            "interval_end": _time(self.interval_end),
            "retrieved_at": _time(self.retrieved_at),
            "status": self.status.value,
            "record_count": self.record_count,
            "raw_sha256": list(self.raw_sha256),
            "query_filters": [item.to_payload() for item in self.query_filters],
            "continuation": self.continuation,
            "missing_reasons": list(self.missing_reasons),
        }


@dataclass(frozen=True, slots=True)
class PolymarketWalletCase:
    """Neutral historical boundary; no legal conclusion belongs in this object."""

    case_uid: str
    actor_uid: str
    historical_cutoff: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "case_uid", _text(self.case_uid, field_name="case_uid"))
        actor = _text(self.actor_uid, field_name="actor_uid").lower()
        if not _WALLET_UID_RE.fullmatch(actor):
            raise ValueError("actor_uid must be a public Polymarket wallet UID")
        object.__setattr__(self, "actor_uid", actor)
        object.__setattr__(
            self,
            "historical_cutoff",
            _utc(self.historical_cutoff, field_name="historical_cutoff"),
        )


@dataclass(frozen=True, slots=True)
class ExternalCaseAuditContext:
    """Later public material retained for audit, never used as a feature or label."""

    context_uid: str
    mapped_actor_uid: str
    title: str
    source_url: str
    available_at: datetime
    legal_stage: str
    mapping_basis: str
    mapping_strength: str
    summary: str

    def __post_init__(self) -> None:
        for name in (
            "context_uid",
            "mapped_actor_uid",
            "title",
            "source_url",
            "legal_stage",
            "mapping_basis",
            "mapping_strength",
            "summary",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), field_name=name))
        if not self.source_url.startswith("https://"):
            raise ValueError("external audit source_url must be HTTPS")
        object.__setattr__(self, "mapped_actor_uid", self.mapped_actor_uid.lower())
        object.__setattr__(self, "available_at", _utc(self.available_at, field_name="audit available_at"))

    def to_payload(self) -> dict[str, Any]:
        return {
            "context_uid": self.context_uid,
            "mapped_actor_uid": self.mapped_actor_uid,
            "title": self.title,
            "source_url": self.source_url,
            "available_at": _time(self.available_at),
            "legal_stage": self.legal_stage,
            "mapping_basis": self.mapping_basis,
            "mapping_strength": self.mapping_strength,
            "summary": self.summary,
            "analysis_use": "excluded_later_audit_context",
            "training_use": "not_eligible",
        }


@dataclass(frozen=True, slots=True)
class ForensicFillLineage:
    fill_uid: str
    event_time: datetime
    ingested_at: datetime
    source_uid: str
    raw_artifact_uid: str
    transaction_uid: str | None
    market_uid: str
    outcome_uid: str
    canonical_record_sha256: str

    @classmethod
    def from_fill(cls, fill: TradeFill) -> "ForensicFillLineage":
        return cls(
            fill_uid=fill.fill_uid,
            event_time=fill.event_time,
            ingested_at=fill.ingested_at,
            source_uid=fill.source_uid,
            raw_artifact_uid=fill.raw_artifact_uid,
            transaction_uid=fill.transaction_uid,
            market_uid=fill.market_uid,
            outcome_uid=fill.outcome_uid,
            canonical_record_sha256=sha256(canonical_json_bytes(fill)).hexdigest(),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "fill_uid": self.fill_uid,
            "event_time": _time(self.event_time),
            "ingested_at": _time(self.ingested_at),
            "source_uid": self.source_uid,
            "raw_artifact_uid": self.raw_artifact_uid,
            "transaction_uid": self.transaction_uid,
            "market_uid": self.market_uid,
            "outcome_uid": self.outcome_uid,
            "canonical_record_sha256": self.canonical_record_sha256,
        }


@dataclass(frozen=True, slots=True)
class ReplayExcludedCounts:
    non_fill_record: int = 0
    wrong_platform: int = 0
    wrong_actor: int = 0
    wrong_source: int = 0
    outside_query_interval: int = 0
    post_cutoff_event: int = 0
    post_reconstruction_ingestion: int = 0
    unbound_raw_lineage: int = 0
    duplicate_fill_uid: int = 0
    source_unavailable_at_reconstruction: int = 0

    def to_payload(self) -> dict[str, int]:
        return {item.name: getattr(self, item.name) for item in fields(self)}


@dataclass(frozen=True, slots=True)
class ReplayDescriptiveMetrics:
    observed_fill_count: int
    observed_market_count: int
    observed_outcome_count: int
    observed_buy_fill_count: int
    observed_sell_fill_count: int
    observed_gross_notional: Decimal
    directional_concentration: Decimal
    dominant_market_notional_share: Decimal
    first_observed_event_time: datetime | None
    last_observed_event_time: datetime | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "observed_fill_count": self.observed_fill_count,
            "observed_market_count": self.observed_market_count,
            "observed_outcome_count": self.observed_outcome_count,
            "observed_buy_fill_count": self.observed_buy_fill_count,
            "observed_sell_fill_count": self.observed_sell_fill_count,
            "observed_gross_notional": _decimal(self.observed_gross_notional),
            "directional_concentration": _decimal(self.directional_concentration),
            "dominant_market_notional_share": _decimal(self.dominant_market_notional_share),
            "first_observed_event_time": None if self.first_observed_event_time is None else _time(self.first_observed_event_time),
            "last_observed_event_time": None if self.last_observed_event_time is None else _time(self.last_observed_event_time),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveEligibility:
    eligible: bool
    source_was_available_at_cutoff: bool
    prospectively_available_fill_count: int
    reason_codes: tuple[str, ...]
    historical_alert_claim: str = "not_made"
    effectiveness_claim: str = "not_made"
    training_eligible: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "source_was_available_at_cutoff": self.source_was_available_at_cutoff,
            "prospectively_available_fill_count": self.prospectively_available_fill_count,
            "reason_codes": list(self.reason_codes),
            "historical_alert_claim": self.historical_alert_claim,
            "effectiveness_claim": self.effectiveness_claim,
            "training_eligible": self.training_eligible,
        }


_LIMITATIONS = (
    "This is a post-hoc descriptive reconstruction, not evidence that MarketLeak emitted a historical alert.",
    "Observed public-wallet activity does not establish identity, common control, intent, access, or legal liability.",
    "The report contains no fraud probability or validated effectiveness claim.",
    "External case material is later audit context only and was excluded from features and training labels.",
)


@dataclass(frozen=True, slots=True)
class WalletForensicReplayReport:
    schema_version: str
    report_uid: str
    case_uid: str
    actor_uid: str
    historical_cutoff: datetime
    reconstructed_at: datetime
    reconstruction_status: str
    abstention_reasons: tuple[str, ...]
    coverage: WalletReplayCoverage
    prospective: ProspectiveEligibility
    metrics: ReplayDescriptiveMetrics | None
    fill_lineage: tuple[ForensicFillLineage, ...]
    excluded_counts: ReplayExcludedCounts
    analysis_input_sha256: str
    external_audit_context: tuple[ExternalCaseAuditContext, ...]
    replay_mode: str = "hindsight_reconstructed"
    limitations: tuple[str, ...] = _LIMITATIONS

    def __post_init__(self) -> None:
        if self.replay_mode != "hindsight_reconstructed":
            raise ValueError("wallet forensic reports must remain hindsight_reconstructed")

    @property
    def _unsigned_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "report_uid": self.report_uid,
            "case_uid": self.case_uid,
            "actor_uid": self.actor_uid,
            "platform": "polymarket",
            "historical_cutoff": _time(self.historical_cutoff),
            "reconstructed_at": _time(self.reconstructed_at),
            "replay_mode": self.replay_mode,
            "reconstruction_status": self.reconstruction_status,
            "abstention_reasons": list(self.abstention_reasons),
            "coverage": self.coverage.to_payload(),
            "prospective": self.prospective.to_payload(),
            "metrics": None if self.metrics is None else self.metrics.to_payload(),
            "fill_lineage": [item.to_payload() for item in self.fill_lineage],
            "excluded_counts": self.excluded_counts.to_payload(),
            "analysis_input_sha256": self.analysis_input_sha256,
            "external_audit_context": [item.to_payload() for item in self.external_audit_context],
            "limitations": list(self.limitations),
        }

    @property
    def report_sha256(self) -> str:
        return sha256(canonical_json_bytes(self._unsigned_payload)).hexdigest()

    def to_payload(self) -> dict[str, Any]:
        return {**self._unsigned_payload, "report_sha256": self.report_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_payload())


def _raw_digest(fill: TradeFill) -> str | None:
    candidate = fill.raw_artifact_uid.rsplit("/", 1)[-1].lower()
    return candidate if _SHA256_RE.fullmatch(candidate) else None


def _metrics(actor_uid: str, fills: tuple[TradeFill, ...], cutoff: datetime) -> ReplayDescriptiveMetrics:
    if not fills:
        return ReplayDescriptiveMetrics(
            observed_fill_count=0,
            observed_market_count=0,
            observed_outcome_count=0,
            observed_buy_fill_count=0,
            observed_sell_fill_count=0,
            observed_gross_notional=Decimal("0"),
            directional_concentration=Decimal("0"),
            dominant_market_notional_share=Decimal("0"),
            first_observed_event_time=None,
            last_observed_event_time=None,
        )
    features = build_actor_features(fills, cutoff=cutoff)
    actor_feature = next((item for item in features if item.actor_uid == actor_uid), None)
    if actor_feature is None:
        raise ValueError("filtered public-wallet fills did not produce actor features")
    market_notional: dict[str, Decimal] = {}
    for fill in fills:
        market_notional[fill.market_uid] = market_notional.get(fill.market_uid, Decimal("0")) + fill.size * fill.price
    dominant_share = (
        max(market_notional.values(), default=Decimal("0")) / actor_feature.gross_notional
        if actor_feature.gross_notional > 0
        else Decimal("0")
    )
    return ReplayDescriptiveMetrics(
        observed_fill_count=actor_feature.fill_count,
        observed_market_count=actor_feature.market_count,
        observed_outcome_count=actor_feature.outcome_count,
        observed_buy_fill_count=sum(fill.side == TradeSide.BUY for fill in fills),
        observed_sell_fill_count=sum(fill.side == TradeSide.SELL for fill in fills),
        observed_gross_notional=actor_feature.gross_notional,
        directional_concentration=actor_feature.directional_concentration,
        dominant_market_notional_share=dominant_share,
        first_observed_event_time=fills[0].event_time,
        last_observed_event_time=fills[-1].event_time,
    )


def build_polymarket_wallet_forensic_replay(
    case: PolymarketWalletCase,
    *,
    reconstructed_at: datetime,
    coverage: WalletReplayCoverage,
    fills: Iterable[object],
    external_audit_context: Iterable[ExternalCaseAuditContext] = (),
) -> WalletForensicReplayReport:
    """Build a deterministic dual-clock report from already-captured records.

    Outcomes and legal dispositions are deliberately not accepted as feature
    inputs. The only derived values are descriptive fill/position aggregates.
    """

    reconstruction = _utc(reconstructed_at, field_name="reconstructed_at")
    if reconstruction < case.historical_cutoff:
        raise ValueError("reconstructed_at cannot precede historical_cutoff")
    if coverage.platform != "polymarket" or coverage.queried_actor_uid != case.actor_uid:
        raise ValueError("coverage must be bound to the case's exact Polymarket wallet")
    wallet_match = _WALLET_UID_RE.fullmatch(case.actor_uid)
    assert wallet_match is not None
    query_user = coverage.filter_value("user")
    if not isinstance(query_user, str) or query_user.lower() != wallet_match.group(1):
        raise ValueError("coverage query_filters must bind the exact wallet through the user parameter")

    audit_context = tuple(sorted(external_audit_context, key=lambda item: (item.available_at, item.context_uid)))
    for item in audit_context:
        if item.mapped_actor_uid != case.actor_uid:
            raise ValueError("external audit context must map to the case wallet")
        if item.available_at <= case.historical_cutoff:
            raise ValueError("external case mapping must remain later audit context")

    counts = {item.name: 0 for item in fields(ReplayExcludedCounts)}
    raw_hashes = set(coverage.raw_sha256)
    unique: dict[str, TradeFill] = {}
    matching_query_rows = 0
    for item in fills:
        if not isinstance(item, TradeFill):
            counts["non_fill_record"] += 1
            continue
        if item.platform != "polymarket":
            counts["wrong_platform"] += 1
            continue
        if item.actor_visibility != ActorVisibility.PUBLIC_WALLET or item.actor_uid != case.actor_uid:
            counts["wrong_actor"] += 1
            continue
        if item.source_uid != coverage.source_uid:
            counts["wrong_source"] += 1
            continue
        matching_query_rows += 1
        if not coverage.interval_start <= item.event_time <= coverage.interval_end:
            counts["outside_query_interval"] += 1
            continue
        if item.event_time > case.historical_cutoff:
            counts["post_cutoff_event"] += 1
            continue
        if item.ingested_at > reconstruction:
            counts["post_reconstruction_ingestion"] += 1
            continue
        if _raw_digest(item) not in raw_hashes:
            counts["unbound_raw_lineage"] += 1
            continue
        existing = unique.get(item.fill_uid)
        if existing is not None:
            if canonical_json_bytes(existing) != canonical_json_bytes(item):
                raise ValueError(f"conflicting records share fill_uid {item.fill_uid}")
            counts["duplicate_fill_uid"] += 1
            continue
        unique[item.fill_uid] = item

    selected = tuple(sorted(unique.values(), key=lambda item: (item.event_time, item.fill_uid)))
    coverage_available = coverage.retrieved_at <= reconstruction
    if not coverage_available:
        counts["source_unavailable_at_reconstruction"] = len(selected)
        selected = ()

    abstention_reasons: list[str] = []
    if not coverage_available:
        abstention_reasons.append("source_retrieved_after_reconstruction")
    if coverage.status != CoverageStatus.COMPLETE:
        abstention_reasons.append("incomplete_source_coverage")
    if not coverage.interval_start <= case.historical_cutoff <= coverage.interval_end:
        abstention_reasons.append("coverage_does_not_span_historical_cutoff")
    if matching_query_rows != coverage.record_count:
        abstention_reasons.append("coverage_record_count_mismatch")
    if counts["unbound_raw_lineage"]:
        abstention_reasons.append("unbound_raw_lineage")
    if counts["outside_query_interval"]:
        abstention_reasons.append("row_outside_declared_query_interval")

    metrics = None if abstention_reasons else _metrics(case.actor_uid, selected, case.historical_cutoff)
    if abstention_reasons:
        reconstruction_status = "abstain"
    elif selected:
        reconstruction_status = "descriptive_complete"
    else:
        reconstruction_status = "descriptive_complete_no_observed_activity"

    prospective_reasons: list[str] = []
    if coverage.retrieved_at > case.historical_cutoff:
        prospective_reasons.append("source_retrieved_after_historical_cutoff")
    if coverage.status != CoverageStatus.COMPLETE:
        prospective_reasons.append("incomplete_source_coverage")
    if not coverage.interval_start <= case.historical_cutoff <= coverage.interval_end:
        prospective_reasons.append("coverage_does_not_span_historical_cutoff")
    late_fill_count = sum(item.ingested_at > case.historical_cutoff for item in selected)
    if late_fill_count:
        prospective_reasons.append("pre_cutoff_events_ingested_after_historical_cutoff")
    if abstention_reasons and "incomplete_source_coverage" not in prospective_reasons:
        prospective_reasons.append("reconstruction_abstained")
    source_at_cutoff = coverage.retrieved_at <= case.historical_cutoff
    prospectively_available = (
        sum(item.ingested_at <= case.historical_cutoff for item in selected)
        if source_at_cutoff
        else 0
    )
    prospective = ProspectiveEligibility(
        eligible=not prospective_reasons,
        source_was_available_at_cutoff=source_at_cutoff,
        prospectively_available_fill_count=prospectively_available,
        reason_codes=tuple(prospective_reasons),
    )

    lineage = tuple(ForensicFillLineage.from_fill(item) for item in selected)
    excluded = ReplayExcludedCounts(**counts)
    analysis_payload = {
        "schema_version": "1.0.0",
        "case_uid": case.case_uid,
        "actor_uid": case.actor_uid,
        "historical_cutoff": _time(case.historical_cutoff),
        "reconstructed_at": _time(reconstruction),
        "replay_mode": "hindsight_reconstructed",
        "coverage": coverage.to_payload(),
        "selected_fill_lineage": [item.to_payload() for item in lineage],
        "excluded_counts": excluded.to_payload(),
        "reconstruction_status": reconstruction_status,
        "abstention_reasons": abstention_reasons,
    }
    analysis_hash = sha256(canonical_json_bytes(analysis_payload)).hexdigest()
    return WalletForensicReplayReport(
        schema_version="1.0.0",
        report_uid=f"polymarket:wallet-forensic-replay/{analysis_hash}",
        case_uid=case.case_uid,
        actor_uid=case.actor_uid,
        historical_cutoff=case.historical_cutoff,
        reconstructed_at=reconstruction,
        reconstruction_status=reconstruction_status,
        abstention_reasons=tuple(abstention_reasons),
        coverage=coverage,
        prospective=prospective,
        metrics=metrics,
        fill_lineage=lineage,
        excluded_counts=excluded,
        analysis_input_sha256=analysis_hash,
        external_audit_context=audit_context,
    )
