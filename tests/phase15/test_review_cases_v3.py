from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path

from fastapi.testclient import TestClient

import marketleak.api as api
from marketleak.multimodal.review_cases import (
    CoverageModalityState,
    FrozenReviewCase,
    FrozenReviewCaseRepository,
    OrdinaryExplanationCheck,
    RedactedEvidenceLedgerEntry,
    ReviewCaseKind,
    ReviewCheckStatus,
    ReviewCoverage,
    ReviewCoverageStatus,
    ReviewDecision,
    ReviewRouting,
    ReviewTrigger,
    review_cases_config_document,
)
from marketleak.multimodal.schemas import canonical_hash
from marketleak.multimodal.serving import ServingBundleRepository


T0 = datetime(2026, 7, 13, 12, tzinfo=UTC)


def review_case(
    *,
    decision: ReviewDecision = ReviewDecision.TRIAGE,
    published_at: datetime = T0 + timedelta(minutes=5),
) -> FrozenReviewCase:
    complete = decision == ReviewDecision.TRIAGE
    coverage_status = ReviewCoverageStatus.COMPLETE if complete else ReviewCoverageStatus.PARTIAL
    return FrozenReviewCase(
        case_uid="review:market-window-1",
        case_kind=ReviewCaseKind.RECORDED_SNAPSHOT,
        published_at=published_at,
        as_of=T0,
        venue="polymarket",
        market_uid="market:window-1",
        question="Recorded market mechanism review window",
        outcome_uid="outcome:window-yes",
        trigger=ReviewTrigger(
            trigger_uid="trigger:window-1",
            event_uid="event:market-window-1",
            window_starts_at=T0 - timedelta(minutes=5),
            window_ends_at=T0 - timedelta(minutes=1),
            observed_at=T0 - timedelta(minutes=1),
            available_at=T0,
            price_open=Decimal("0.40"),
            price_close=Decimal("0.55"),
            price_change=Decimal("0.15"),
            observation_count=3,
            fill_count=1,
            orderbook_snapshot_count=1,
            trade_notional=Decimal("4"),
            raw_artifact_uids=("raw:market-window-1",),
        ),
        ordinary_checks=(
            OrdinaryExplanationCheck(
                check_uid="check:market-mechanics-1",
                kind="market_mechanics",
                status=ReviewCheckStatus.OBSERVED,
                coverage_status=ReviewCoverageStatus.COMPLETE,
                summary="Market depth and spread observations are recorded.",
                evidence_uids=("event:market-window-1",),
            ),
            OrdinaryExplanationCheck(
                check_uid="check:public-evidence-1",
                kind="public_evidence",
                status=ReviewCheckStatus.OBSERVED if complete else ReviewCheckStatus.UNAVAILABLE,
                coverage_status=ReviewCoverageStatus.COMPLETE if complete else ReviewCoverageStatus.PARTIAL,
                summary=(
                    "Point-in-time public evidence coverage is recorded."
                    if complete
                    else "Point-in-time public evidence coverage is incomplete."
                ),
                evidence_uids=("event:public-window-1",) if complete else (),
            ),
        ),
        coverage=ReviewCoverage(
            overall_status=coverage_status,
            modality_states=(
                CoverageModalityState(
                    modality="market_state",
                    status=ReviewCoverageStatus.COMPLETE,
                    reason="Recorded market material is available.",
                ),
                CoverageModalityState(
                    modality="public_evidence",
                    status=ReviewCoverageStatus.COMPLETE if complete else ReviewCoverageStatus.PARTIAL,
                    reason=(
                        "Recorded public evidence coverage is available."
                        if complete
                        else "Recorded public evidence coverage is incomplete."
                    ),
                ),
            ),
            source_high_watermarks={"source:venue": T0, "source:public": T0},
            gap_reasons=() if complete else ("public_evidence_coverage_partial",),
            raw_receipt_count=2 if complete else 1,
            late_excluded_count=0,
        ),
        routing=ReviewRouting(
            decision=decision,
            coverage_status=coverage_status,
            abstention_reasons=() if complete else ("public_evidence_coverage_partial",),
            mechanism_hypotheses=("thin_liquidity_artifact",),
        ),
        evidence_ledger=(
            RedactedEvidenceLedgerEntry(
                evidence_uid="event:market-window-1",
                modality="market_state",
                event_time=T0 - timedelta(minutes=1),
                available_at=T0,
                source_uid="source:venue",
                raw_artifact_uid="raw:market-window-1",
                content_hash="a" * 64,
                reliability_tier="high",
            ),
            *(
                (
                    RedactedEvidenceLedgerEntry(
                        evidence_uid="event:public-window-1",
                        modality="public_evidence",
                        event_time=T0 - timedelta(minutes=2),
                        available_at=T0,
                        source_uid="source:public",
                        raw_artifact_uid="raw:public-window-1",
                        content_hash="b" * 64,
                        reliability_tier="medium",
                    ),
                )
                if complete
                else ()
            ),
        ),
    )


def write_config(path: Path, *cases: FrozenReviewCase) -> FrozenReviewCaseRepository:
    path.write_text(json.dumps(review_cases_config_document(cases)), encoding="utf-8")
    return FrozenReviewCaseRepository(path)


def client(repository: FrozenReviewCaseRepository) -> TestClient:
    serving = ServingBundleRepository("unused-review-case-bundles", clock=lambda: T0 + timedelta(minutes=10))
    api.app.dependency_overrides[api.get_v3_serving_repository] = lambda: serving
    api.app.dependency_overrides[api.get_v3_review_case_repository] = lambda: repository
    return TestClient(api.app)


def clear_overrides() -> None:
    api.app.dependency_overrides.clear()


def test_v3_review_case_endpoints_return_hash_checked_triage_packet(tmp_path: Path) -> None:
    repository = write_config(tmp_path / "review-cases.json", review_case())
    test_client = client(repository)
    try:
        listing = test_client.get("/api/v3/review-cases")
        assert listing.status_code == 200
        body = listing.json()
        assert body["status"] == "available_precomputed_review_cases"
        assert body["read_only"] is True
        assert body["live_fetch"] is False
        assert body["live_inference"] is False
        assert body["case_kind_required"] is True
        assert body["not_proof_of_fraud"] is True
        assert body["effectiveness_unknown"] is True
        packet = body["review_cases"][0]
        assert packet["case_kind"] == "recorded_snapshot"
        assert packet["routing"]["decision"] == "triage"
        assert packet["coverage"]["overall_status"] == "complete"

        selected = test_client.get("/api/v3/review-cases/review:market-window-1").json()
        assert selected["case"]["case_uid"] == "review:market-window-1"
        assert selected["case"]["trigger"]["raw_artifact_uids"] == ["raw:market-window-1"]
    finally:
        clear_overrides()


def test_partial_case_preserves_abstention_without_a_substitute_decision(tmp_path: Path) -> None:
    repository = write_config(
        tmp_path / "review-cases.json",
        review_case(decision=ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE),
    )
    test_client = client(repository)
    try:
        packet = test_client.get("/api/v3/review-cases/review:market-window-1").json()["case"]
        assert packet["coverage"]["overall_status"] == "partial"
        assert packet["routing"]["decision"] == "abstain_insufficient_evidence"
        assert packet["routing"]["abstention_reasons"] == ["public_evidence_coverage_partial"]
        assert packet["ordinary_checks"][1]["status"] == "unavailable"
    finally:
        clear_overrides()


def test_missing_review_case_configuration_is_explicitly_unavailable() -> None:
    test_client = client(FrozenReviewCaseRepository(None))
    try:
        body = test_client.get("/api/v3/review-cases").json()
        assert body["status"] == "review_cases_not_configured"
        assert body["review_cases"] == []
        assert body["live_fetch"] is False
        assert body["live_inference"] is False
    finally:
        clear_overrides()


def test_late_or_tampered_cases_are_excluded_fail_closed(tmp_path: Path) -> None:
    late_repository = write_config(
        tmp_path / "late-review-cases.json",
        review_case(published_at=T0 + timedelta(hours=1)),
    )
    late_client = client(late_repository)
    try:
        late = late_client.get("/api/v3/review-cases").json()
        assert late["status"] == "unavailable_late_configuration"
        assert late["review_cases"] == []
    finally:
        clear_overrides()

    tampered_document = review_cases_config_document((review_case(),))
    tampered_document["review_cases"][0]["question"] = "Modified packet content"  # type: ignore[index]
    tampered_path = tmp_path / "tampered-review-cases.json"
    tampered_path.write_text(json.dumps(tampered_document), encoding="utf-8")
    tampered_client = client(FrozenReviewCaseRepository(tampered_path))
    try:
        tampered = tampered_client.get("/api/v3/review-cases").json()
        assert tampered["status"] == "unavailable"
        assert tampered["review_cases"] == []
    finally:
        clear_overrides()

    document = review_cases_config_document((review_case(),))
    document["review_cases"][0]["evidence_ledger"][0]["available_at"] = "2026-07-13T12:01:00Z"  # type: ignore[index]
    document["review_cases_sha256"] = canonical_hash(document["review_cases"])
    malformed_path = tmp_path / "late-evidence.json"
    malformed_path.write_text(json.dumps(document), encoding="utf-8")
    malformed_client = client(FrozenReviewCaseRepository(malformed_path))
    try:
        malformed = malformed_client.get("/api/v3/review-cases").json()
        assert malformed["status"] == "unavailable"
        assert malformed["review_cases"] == []
    finally:
        clear_overrides()


def test_review_case_routes_do_not_accept_lookup_or_inference_inputs(tmp_path: Path) -> None:
    repository = write_config(tmp_path / "review-cases.json", review_case())
    test_client = client(repository)
    try:
        normal = test_client.get("/api/v3/review-cases")
        attempted_lookup = test_client.get("/api/v3/review-cases?market_uid=market:other&address=not-used")
        path_lookup = test_client.get("/api/v3/review-cases/market:other")

        assert attempted_lookup.status_code == 200
        assert attempted_lookup.json() == normal.json()
        assert path_lookup.status_code == 200
        assert path_lookup.json()["status"] == "unavailable"
        encoded = json.dumps(normal.json()).lower()
        for forbidden in ("fraud_probability", "identity", "wallet", "insider", "live_inference\": true"):
            assert forbidden not in encoded
    finally:
        clear_overrides()
