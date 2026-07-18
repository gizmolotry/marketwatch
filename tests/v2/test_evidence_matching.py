from datetime import datetime, timedelta, timezone

from marketleak.domain import CoverageStatus, PublicExplanationStatus
from marketleak.evidence.coverage import CoverageLedger, SourceCoverageInterval
from marketleak.evidence.explanation import assess_public_explanation
from marketleak.evidence.matching import EvidenceMatcher
from marketleak.evidence.normalize import normalize_evidence


UTC = timezone.utc
NOW = datetime(2026, 2, 1, 12, tzinfo=UTC)


def test_unrelated_exact_query_result_is_preserved_but_not_treated_as_proof():
    unrelated = normalize_evidence(
        source="search",
        source_document_id="unrelated",
        retrieved_at=NOW,
        first_seen_at=NOW,
        title="Regional weather forecast",
        body="Rain is likely in Seattle this evening.",
        metadata={"search_strategy": "exact", "search_query": "Acme merger approval"},
    )
    matches = EvidenceMatcher().match(
        market_uid="venue:acme-merger",
        market_text="Will the Acme merger receive regulatory approval?",
        candidates=[unrelated],
        as_of=NOW + timedelta(hours=1),
        upstream_scores={unrelated.evidence_uid: 0.99},
    )

    assert len(matches.candidates) == 1
    candidate = matches.candidates[0]
    assert candidate.upstream_score == 0.99
    assert candidate.relevant is False
    assert "insufficient_entity_overlap" in candidate.reasons
    assert "exact" not in candidate.reasons

    ledger = CoverageLedger(
        [
            SourceCoverageInterval(
                source="search",
                started_at=NOW - timedelta(hours=2),
                ended_at=NOW + timedelta(hours=1),
                status=CoverageStatus.COMPLETE,
                connector_version="search-v1",
            )
        ]
    )
    coverage = ledger.assess(
        required_sources=["search"],
        window_start=NOW - timedelta(hours=1),
        window_end=NOW,
    )
    result = assess_public_explanation(
        signal_uid="signal:acme-merger",
        shock_time=NOW,
        as_of=NOW + timedelta(hours=1),
        matches=matches,
        coverage=coverage,
    )
    assert result.status == PublicExplanationStatus.NO_MATCH_OBSERVED_PRE_SHOCK
    assert result.evidence_uids == ()


def test_explicit_market_verification_can_establish_relevance_without_overlap():
    evidence = normalize_evidence(
        source="filing",
        source_document_id="filing-7",
        retrieved_at=NOW,
        title="Form 8-K",
        body="The transaction closed.",
        metadata={"verified_market_uids": ["venue:acme-merger"]},
    )
    result = EvidenceMatcher().match(
        market_uid="venue:acme-merger",
        market_text="Will the Acme merger receive regulatory approval?",
        candidates=[evidence],
        as_of=NOW,
    )

    assert result.candidates[0].explicit_verified is True
    assert result.candidates[0].relevant is True
    assert result.candidates[0].reasons == ("explicit_market_verification",)

