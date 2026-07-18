from datetime import datetime, timedelta, timezone
from decimal import Decimal

from marketleak.detectors import CausalActivityDetector, DetectorConfig
from marketleak.domain import ActorVisibility, ObservationKind, PriceObservation
from marketleak.ingestion.quality import DataQualityGate, DataQualityReport
from marketleak.pipeline_v2 import ValidationFirstPipeline, run_validation_pipeline


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def observations():
    rows = []
    for index in range(122):
        event_time = BASE + timedelta(minutes=5 * index + 1)
        rows.append(
            PriceObservation(
                observation_uid=f"test:obs-{index}",
                market_uid="test:m1",
                outcome_uid="test:yes",
                platform="test",
                price=Decimal("0.50") if index < 120 else Decimal("0.80"),
                kind=ObservationKind.PLATFORM_SNAPSHOT,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
                event_time=event_time,
                ingested_at=event_time,
                source_uid="test:source",
                raw_artifact_uid=f"raw:r-{index}",
                parser_version="2.0.0",
            )
        )
    return rows


def synthetic_pipeline() -> ValidationFirstPipeline:
    config = DetectorConfig(
        min_baseline_observations=30,
        min_baseline_elapsed=timedelta(hours=2),
        empirical_tail_min_observations=30,
    )
    return ValidationFirstPipeline(detector=CausalActivityDetector(config))


def test_explicit_legacy_fixture_is_non_promotable_without_raw_lineage() -> None:
    result = ValidationFirstPipeline().run(ticks_path="demo_data/ticks.parquet")
    assert result.status == "blocked_by_data_quality"
    assert result.assessments == ()
    assert result.detector_batch.incidents == ()
    assert result.data_quality["duplicates"] == 1064
    assert result.data_quality["missing_required"]["raw_artifact_uid"] == 22222
    assert result.data_quality["missing_required"]["source_uid"] == 22222
    assert result.data_quality["gate_passed"] is False
    assert result.actor_availability["derived_from_price_snapshots"] is False


def test_default_validation_path_does_not_silently_use_legacy_fixture(tmp_path) -> None:
    result = run_validation_pipeline(canonical_root=tmp_path)

    assert result.status == "blocked_by_data_quality"
    assert result.pipeline_source == "canonical_normalized_partitions"
    assert result.data_quality["source"].startswith("canonical:")
    assert result.data_quality["received"] == 0
    assert any("legacy fixtures were not used" in note for note in result.data_quality["notes"])


def test_pipeline_keeps_a_b_c_separate_and_cautious() -> None:
    result = synthetic_pipeline().run(
        observations=observations(),
        as_of=BASE + timedelta(hours=12),
    )
    assert result.status == "completed_with_review_candidates"
    assert len(result.assessments) == 1
    payload = result.assessments_payload()[0]
    assert payload["activity"]["status"] == "abnormal"
    assert payload["public_explanation"]["status"] == "unknown_coverage"
    assert payload["actor_evidence"]["status"] == "no_actor_data"
    assert payload["actor_evidence"]["actor_visibility"] == "not_available"
    assert payload["not_proof_of_fraud"] is True
    assert "fraud_probability" not in str(payload)
    assert payload["activity"]["limitations"] == ["The diagnostic score is not a probability of fraud."]


def test_failed_quality_gate_blocks_assessments() -> None:
    pipeline = ValidationFirstPipeline(
        detector=synthetic_pipeline().detector,
        quality_gate=DataQualityGate(min_normalized=10, max_invalid_rate=0),
    )
    quality = DataQualityReport(source="test:bad", received=122, normalized=121, invalid=1)
    result = pipeline.run(
        observations=observations(),
        quality_report=quality,
        as_of=BASE + timedelta(hours=12),
    )
    assert result.status == "blocked_by_data_quality"
    assert result.assessments == ()
    assert result.data_quality["gate_passed"] is False


def test_adequate_legacy_jump_remains_replay_only_without_lineage(tmp_path) -> None:
    import pandas as pd

    rows = []
    for index in range(122):
        rows.append(
            {
                "tick_uid": f"legacy-{index}",
                "market_uid": "test:m1",
                "timestamp": int((BASE + timedelta(minutes=5 * index + 1)).timestamp()),
                "price": 0.5 if index < 120 else 0.8,
            }
        )
    path = tmp_path / "legacy.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    result = synthetic_pipeline().run(
        ticks_path=path,
        as_of=BASE + timedelta(hours=12),
    )

    assert result.status == "blocked_by_data_quality"
    assert result.detector_batch.incidents == ()
    assert result.assessments == ()
    assert result.replay_detector_batch is not None
    assert len(result.replay_detector_batch.incidents) == 1
    assert result.data_quality["missing_required"]["raw_artifact_uid"] == 122
    assert result.data_quality["gate_passed"] is False
