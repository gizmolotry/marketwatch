"""Cautious point-in-time public-information explanation logic."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from marketleak.domain import PublicExplanation, PublicExplanationStatus
from marketleak.evidence.coverage import CoverageAssessment
from marketleak.evidence.matching import MatchResult
from marketleak.evidence.normalize import TimestampParseStatus, utc_datetime


@dataclass(frozen=True)
class ExplanationResult:
    explanation: PublicExplanation
    match_result: MatchResult
    coverage: CoverageAssessment
    pre_observed_lead_hours: float

    @property
    def status(self) -> PublicExplanationStatus:
        return self.explanation.status

    @property
    def evidence_uids(self) -> tuple[str, ...]:
        return self.explanation.evidence_uids


def _derived_uid(prefix: str, seed: str) -> str:
    return f"{prefix}:{hashlib.sha256(seed.encode('utf-8')).hexdigest()}"


def assess_public_explanation(
    *,
    signal_uid: str,
    shock_time: datetime,
    as_of: datetime,
    matches: MatchResult,
    coverage: CoverageAssessment,
    parser_version: str = "public-explanation-v1",
) -> ExplanationResult:
    """Assess B without claiming exhaustive knowledge of public information."""
    shock = utc_datetime(shock_time, field_name="shock_time")
    cutoff = utc_datetime(as_of, field_name="as_of")
    if cutoff < shock:
        raise ValueError("as_of cannot be earlier than shock_time")

    relevant = tuple(
        item
        for item in matches.candidates
        if item.relevant and item.eligible_as_of and item.evidence.first_seen_at <= cutoff
    )
    pre_shock = tuple(item for item in relevant if item.evidence.first_seen_at <= shock)
    ambiguous = tuple(
        item
        for item in relevant
        if item.evidence.first_seen_at > shock
        and item.evidence.publication_timestamp_status == TimestampParseStatus.VALID
        and item.evidence.claimed_published_at is not None
        and item.evidence.claimed_published_at <= shock
    )

    if pre_shock:
        status = PublicExplanationStatus.OBSERVED_PRE_SHOCK
        supporting = pre_shock
        earliest = min(item.evidence.first_seen_at for item in pre_shock)
        summary = "Relevant public information was observed in a monitored source before the activity."
        limitations = ("This establishes observed availability, not that every trader saw the source.",)
    elif ambiguous:
        status = PublicExplanationStatus.TIMING_AMBIGUOUS
        supporting = ambiguous
        earliest = None
        summary = "A relevant document was first observed after the activity but claims an earlier publication time."
        limitations = ("The claimed publication time was not prospectively observed and requires verification.",)
    elif coverage.adequate:
        status = PublicExplanationStatus.NO_MATCH_OBSERVED_PRE_SHOCK
        supporting = relevant
        earliest = None
        summary = "No matching public information was observed before the activity within the monitored sources."
        limitations = ("This is not evidence that no public information existed outside monitored sources.",)
    else:
        status = PublicExplanationStatus.UNKNOWN_COVERAGE
        supporting = relevant
        earliest = None
        summary = "Public-information timing is unknown because monitored-source coverage was incomplete."
        limitations = (coverage.rationale,)

    post_shock_first_seen = [
        item.evidence.first_seen_at for item in relevant if item.evidence.first_seen_at > shock
    ]
    lead_hours = 0.0
    if post_shock_first_seen:
        lead_hours = max(0.0, (min(post_shock_first_seen) - shock).total_seconds() / 3600.0)

    evidence_uids = tuple(item.evidence.evidence_uid for item in supporting)
    coverage_uids = tuple(
        f"coverage:{hashlib.sha256(f'{source}|{coverage.window_start.isoformat()}|{coverage.window_end.isoformat()}'.encode('utf-8')).hexdigest()}"
        for source in coverage.required_sources
    )
    seed = "|".join((signal_uid, shock.isoformat(), cutoff.isoformat(), status.value, *evidence_uids))
    lineage_raw_uid = _derived_uid("raw", seed or signal_uid)
    explanation = PublicExplanation(
        explanation_uid=_derived_uid("explanation", seed),
        signal_uid=signal_uid,
        status=status,
        shock_time=shock,
        earliest_matching_public_time=earliest,
        evidence_uids=evidence_uids,
        coverage_uids=coverage_uids if coverage.adequate else (),
        summary=summary,
        limitations=limitations,
        event_time=shock,
        ingested_at=cutoff,
        source_uid="system:public-evidence",
        raw_artifact_uid=lineage_raw_uid,
        parser_version=parser_version,
    )
    return ExplanationResult(
        explanation=explanation,
        match_result=matches,
        coverage=coverage,
        pre_observed_lead_hours=lead_hours,
    )


__all__ = [
    "ExplanationResult",
    "PublicExplanation",
    "PublicExplanationStatus",
    "assess_public_explanation",
]
