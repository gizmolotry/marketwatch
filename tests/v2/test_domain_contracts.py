from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketleak.domain import (
    ActivitySignal,
    ActivityStatus,
    ActorEvidence,
    ActorEvidenceStatus,
    ActorVisibility,
    IntegrityAssessment,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
    PublicExplanation,
    PublicExplanationStatus,
    platform_qualified_uid,
)
from marketleak.domain.observations import PriceObservation as PathPriceObservation
from marketleak.domain.runs import DatasetManifest


NOW = datetime(2026, 7, 12, 20, 0, tzinfo=timezone.utc)


def test_approved_contract_import_paths() -> None:
    assert PathPriceObservation is PriceObservation
    assert DatasetManifest.__name__ == "DatasetManifest"


def lineage(event_time: datetime = NOW) -> dict:
    return {
        "event_time": event_time,
        "ingested_at": NOW + timedelta(seconds=5),
        "source_uid": "polymarket:api-v1",
        "raw_artifact_uid": "raw:sha256-abc",
        "parser_version": "parser-2.0.0",
    }


def activity() -> ActivitySignal:
    return ActivitySignal(
        signal_uid="signal:s-1",
        market_uid="polymarket:m-1",
        outcome_uid="polymarket:o-yes",
        status=ActivityStatus.ABNORMAL,
        detector_name="robust-shock",
        detector_version="2.0.0",
        score=Decimal("3.4"),
        threshold=Decimal("3.0"),
        reason_codes=("robust_z",),
        **lineage(),
    )


def test_platform_qualified_uid_is_stable_and_validated() -> None:
    assert platform_qualified_uid("PolyMarket", "0xabc") == "polymarket:0xabc"
    with pytest.raises(ValueError):
        platform_qualified_uid("bad platform", "m1")


def test_observation_requires_decimal_utc_and_qualified_ids() -> None:
    observation = PriceObservation(
        observation_uid="polymarket:obs-1",
        market_uid="polymarket:m-1",
        outcome_uid="polymarket:o-yes",
        platform="polymarket",
        price=Decimal("0.5210"),
        kind=ObservationKind.PLATFORM_SNAPSHOT,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        **lineage(),
    )
    assert observation.price == Decimal("0.5210")
    assert observation.model_dump(mode="json")["price"] == "0.5210"

    with pytest.raises(ValidationError):
        PriceObservation(
            observation_uid="polymarket:obs-1",
            market_uid="kalshi:m-1",
            outcome_uid="polymarket:o-yes",
            platform="polymarket",
            price=Decimal("0.5"),
            kind=ObservationKind.PLATFORM_SNAPSHOT,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
            **lineage(),
        )
    with pytest.raises(ValidationError):
        PriceObservation(
            observation_uid="polymarket:obs-1",
            market_uid="polymarket:m-1",
            outcome_uid="polymarket:o-yes",
            platform="polymarket",
            price=0.5,
            kind=ObservationKind.PLATFORM_SNAPSHOT,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
            **lineage(),
        )
    with pytest.raises(ValidationError):
        PriceObservation(
            observation_uid="polymarket:obs-1",
            market_uid="polymarket:m-1",
            outcome_uid="polymarket:o-yes",
            platform="polymarket",
            price=Decimal("0.5"),
            kind=ObservationKind.PLATFORM_SNAPSHOT,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
            **lineage(datetime(2026, 7, 12, 20, 0)),
        )


def test_order_book_requires_sorted_non_crossed_levels() -> None:
    snapshot = OrderBookSnapshot(
        snapshot_uid="kalshi:book-1",
        market_uid="kalshi:m-1",
        outcome_uid="kalshi:yes",
        platform="kalshi",
        bids=(
            OrderBookLevel(price=Decimal("0.49"), size=Decimal("10")),
            OrderBookLevel(price=Decimal("0.48"), size=Decimal("8")),
        ),
        asks=(OrderBookLevel(price=Decimal("0.51"), size=Decimal("9")),),
        **lineage(),
    )
    assert snapshot.bids[0].price == Decimal("0.49")

    with pytest.raises(ValidationError, match="bids must be strictly descending"):
        OrderBookSnapshot(
            snapshot_uid="kalshi:book-2",
            market_uid="kalshi:m-1",
            outcome_uid="kalshi:yes",
            platform="kalshi",
            bids=(
                OrderBookLevel(price=Decimal("0.48"), size=Decimal("10")),
                OrderBookLevel(price=Decimal("0.49"), size=Decimal("8")),
            ),
            asks=(),
            **lineage(),
        )


def test_a_b_c_statuses_stay_separate_and_disclaimed() -> None:
    signal = activity()
    public = PublicExplanation(
        explanation_uid="explanation:e-1",
        signal_uid=signal.signal_uid,
        status=PublicExplanationStatus.UNKNOWN_COVERAGE,
        shock_time=NOW,
        summary="Point-in-time public archive coverage is unavailable.",
        **lineage(),
    )
    actor = ActorEvidence(
        actor_evidence_uid="actor-evidence:a-1",
        signal_uid=signal.signal_uid,
        status=ActorEvidenceStatus.NO_ACTOR_DATA,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        summary="The source snapshot does not expose an actor.",
        **lineage(),
    )
    assessment = IntegrityAssessment(
        assessment_uid="assessment:i-1",
        market_uid=signal.market_uid,
        activity=signal,
        public_explanation=public,
        actor_evidence=actor,
        **lineage(),
    )
    dumped = assessment.model_dump(mode="json")
    assert dumped["activity"]["status"] == "abnormal"
    assert dumped["public_explanation"]["status"] == "unknown_coverage"
    assert dumped["actor_evidence"]["status"] == "no_actor_data"
    assert dumped["not_proof_of_fraud"] is True
    assert "fraud_status" not in dumped
    assert "escalation_status" not in dumped


def test_status_claims_require_their_evidence_preconditions() -> None:
    signal = activity()
    with pytest.raises(ValidationError, match="earliest_matching_public_time"):
        PublicExplanation(
            explanation_uid="explanation:e-1",
            signal_uid=signal.signal_uid,
            status=PublicExplanationStatus.OBSERVED_PRE_SHOCK,
            shock_time=NOW,
            summary="Claim without time",
            **lineage(),
        )
    with pytest.raises(ValidationError, match="documented coverage"):
        PublicExplanation(
            explanation_uid="explanation:e-1",
            signal_uid=signal.signal_uid,
            status=PublicExplanationStatus.NO_MATCH_OBSERVED_PRE_SHOCK,
            shock_time=NOW,
            summary="Claim without coverage",
            **lineage(),
        )
    with pytest.raises(ValidationError, match="requires evidence"):
        ActorEvidence(
            actor_evidence_uid="actor-evidence:a-1",
            signal_uid=signal.signal_uid,
            status=ActorEvidenceStatus.CORROBORATED_ACCESS_SIGNAL,
            actor_visibility=ActorVisibility.PUBLIC_WALLET,
            actor_uid="polymarket:wallet-1",
            summary="Claim without linked evidence",
            **lineage(),
        )
