from datetime import datetime, timedelta, timezone

from marketleak.domain import CoverageStatus, PublicExplanationStatus
from marketleak.evidence.coverage import CoverageLedger, SourceCoverageInterval
from marketleak.evidence.explanation import assess_public_explanation
from marketleak.evidence.matching import EvidenceMatcher
from marketleak.evidence.normalize import TimestampParseStatus, normalize_evidence


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _evidence(
    *,
    source_document_id="doc-1",
    first_seen_at=T0 + timedelta(hours=2),
    retrieved_at=None,
    claimed_published_at=None,
    title="Acme merger receives regulatory approval",
    body="The Acme merger was approved by the regulator.",
    metadata=None,
    backfill=False,
):
    return normalize_evidence(
        source="wire",
        source_document_id=source_document_id,
        retrieved_at=retrieved_at or first_seen_at,
        first_seen_at=first_seen_at,
        claimed_published_at=claimed_published_at,
        title=title,
        body=body,
        metadata=metadata,
        backfill=backfill,
    )


def _coverage(*, status=CoverageStatus.COMPLETE, end=T0 + timedelta(hours=4)):
    ledger = CoverageLedger(
        [
            SourceCoverageInterval(
                source="wire",
                started_at=T0,
                ended_at=end,
                status=status,
                connector_version="wire-v1",
            )
        ]
    )
    return ledger.assess(
        required_sources=["wire"],
        window_start=T0,
        window_end=T0 + timedelta(hours=2),
    )


def _assess(evidence, *, shock=T0 + timedelta(hours=2), as_of=T0 + timedelta(hours=4), coverage=None):
    matches = EvidenceMatcher().match(
        market_uid="venue:acme-merger",
        market_text="Will the Acme merger receive regulatory approval?",
        candidates=evidence,
        as_of=as_of,
    )
    return assess_public_explanation(
        signal_uid="signal:acme-merger",
        shock_time=shock,
        as_of=as_of,
        matches=matches,
        coverage=coverage or _coverage(),
    )


def test_backfill_does_not_fabricate_historical_first_seen_time():
    retrieved = T0 + timedelta(days=3)
    observation = normalize_evidence(
        source="archive",
        source_document_id="historical-1",
        retrieved_at=retrieved,
        claimed_published_at=T0,
        title="Historical Acme merger article",
        backfill=True,
    )

    assert observation.first_seen_at == retrieved
    assert observation.claimed_published_at == T0
    assert observation.backfill is True
    assert observation.publication_timestamp_status == TimestampParseStatus.VALID
    assert len(observation.content_hash) == 64


def test_observed_pre_shock_information_is_an_explanation_not_a_leak_signal():
    result = _assess(
        [_evidence(first_seen_at=T0 + timedelta(hours=1))],
        shock=T0 + timedelta(hours=2),
    )

    assert result.status == PublicExplanationStatus.OBSERVED_PRE_SHOCK
    assert result.pre_observed_lead_hours == 0.0
    assert len(result.evidence_uids) == 1
    assert result.explanation.earliest_matching_public_time == T0 + timedelta(hours=1)


def test_post_shock_first_seen_with_pre_shock_claimed_time_is_ambiguous():
    result = _assess(
        [
            _evidence(
                first_seen_at=T0 + timedelta(hours=3),
                claimed_published_at=T0 + timedelta(hours=1),
            )
        ],
        shock=T0 + timedelta(hours=2),
    )

    assert result.status == PublicExplanationStatus.TIMING_AMBIGUOUS
    assert result.pre_observed_lead_hours == 1.0
    assert "requires verification" in result.explanation.limitations[0]


def test_outage_forces_unknown_coverage_instead_of_a_negative_claim():
    result = _assess([], coverage=_coverage(status=CoverageStatus.UNAVAILABLE))

    assert result.status == PublicExplanationStatus.UNKNOWN_COVERAGE
    assert result.explanation.coverage_uids == ()


def test_document_first_seen_after_as_of_is_excluded_from_support():
    future = _evidence(first_seen_at=T0 + timedelta(hours=5))
    result = _assess([future], as_of=T0 + timedelta(hours=4))

    assert result.status == PublicExplanationStatus.NO_MATCH_OBSERVED_PRE_SHOCK
    assert result.evidence_uids == ()
    candidate = result.match_result.candidates[0]
    assert candidate.relevant is True
    assert candidate.eligible_as_of is False

