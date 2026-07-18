from datetime import datetime, timezone

from fastapi.testclient import TestClient

import marketleak.api as api
from marketleak.detectors import DetectionBatch
from marketleak.pipeline_v2 import PipelineV2Result


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def fake_result() -> PipelineV2Result:
    return PipelineV2Result(
        run_uid="run:test",
        started_at=NOW,
        completed_at=NOW,
        status="completed_no_actionable_activity",
        data_quality={"received": 10, "normalized": 10, "gate_passed": True},
        detector_batch=DetectionBatch((), (), ()),
        assessments=(),
        actor_availability={"available": False, "visibility": "not_available"},
        source_availability={"point_in_time_public_sources_available": False, "coverage_status": "unknown_coverage"},
        validation_status={
            "validated": False,
            "effectiveness_unknown": True,
            "calibration_available": False,
            "not_proof_of_fraud": True,
        },
    )


def test_v2_golden_contracts_are_non_accusatory() -> None:
    api.app.dependency_overrides[api.get_v2_pipeline_result] = fake_result
    try:
        client = TestClient(api.app)
        capabilities = client.get("/api/v2/capabilities").json()
        assert capabilities["targets_are_independent"] is True
        assert capabilities["scores_are_probabilities"] is False
        assert capabilities["fraud_prediction"] is False

        quality = client.get("/api/v2/data-quality").json()
        assert quality["data_quality"]["gate_passed"] is True
        assert quality["actor_availability"]["available"] is False
        assert quality["source_availability"]["coverage_status"] == "unknown_coverage"

        assessments = client.get("/api/v2/assessments").json()
        assert assessments["count"] == 0
        assert assessments["ppim_score"] is None
        assert assessments["ppim_deprecated"] is True
        assert assessments["leak_risk_forecast"] is None
        assert assessments["not_proof_of_fraud"] is True
        assert assessments["effectiveness_unknown"] is True

        validation = client.get("/api/v2/validation-status").json()
        assert validation["validation"]["validated"] is False
        assert validation["validation"]["effectiveness_unknown"] is True
    finally:
        api.app.dependency_overrides.clear()


def test_api_default_uses_canonical_store_and_never_legacy_fixture(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MARKETLEAK_V2_DATA_ROOT", str(tmp_path))
    api._global_v2_result = None
    try:
        result = api.get_v2_pipeline_result()

        assert result.pipeline_source == "canonical_normalized_partitions"
        assert result.status == "blocked_by_data_quality"
        assert result.data_quality["source"].startswith("canonical:")
        assert result.data_quality["received"] == 0
        assert any("legacy fixtures were not used" in note for note in result.data_quality["notes"])
    finally:
        api._global_v2_result = None
