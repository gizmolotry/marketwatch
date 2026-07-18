from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path

from fastapi.testclient import TestClient

import marketleak.api as api
from marketleak.multimodal.bundles import ARTIFACT_NAMES, FrozenBundleRepository, ServingBundleManifest, sha256_file
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.schemas import (
    MarketStateSlice,
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)
from marketleak.multimodal.serving import (
    FrozenServingAssessment,
    LabelSufficiency,
    ServingBundleRepository,
)


T0 = datetime(2026, 7, 13, 12, tzinfo=UTC)


def _artifacts(root: Path) -> dict[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name in ARTIFACT_NAMES:
        path = root / f"{name}.json"
        path.write_text(json.dumps({"name": name, "version": 1}), encoding="utf-8")
        paths[name] = path
    return paths


def _manifest(paths: dict[str, Path]) -> ServingBundleManifest:
    hashes = {name: sha256_file(path) for name, path in paths.items()}
    return ServingBundleManifest(
        bundle_version="v3-test",
        created_at=T0,
        dataset_hash=hashes["dataset"],
        feature_spec_hash=hashes["feature_spec"],
        model_hash=hashes["model"],
        calibration_hash=hashes["calibration"],
        ood_hash=hashes["ood"],
        conformal_hash=hashes["conformal"],
        retrieval_hash=hashes["retrieval"],
        code_hash=hashes["code"],
        mechanisms=("scheduled_event_response",),
    )


def _verified_repository(tmp_path: Path, *, event_store: EventMemoryStore | None = None) -> ServingBundleRepository:
    paths = _artifacts(tmp_path / "artifacts")
    root = tmp_path / "bundles"
    FrozenBundleRepository(root).publish(_manifest(paths), paths)
    return ServingBundleRepository(
        root,
        event_store=event_store,
        label_sufficiency=LabelSufficiency(status="insufficient", labeled_count=2, reason="small human mechanism registry"),
        clock=lambda: T0,
    )


def _future_market_event() -> MarketStateSlice:
    observed = (ModalityMissingness(modality=Modality.MARKET_STATE, status=MissingnessStatus.OBSERVED),)
    return MarketStateSlice(
        event_uid="event:future-market",
        event_time=T0 + timedelta(hours=1),
        ingested_at=T0 + timedelta(hours=1, minutes=1),
        provenance=Provenance(
            source_uid="source:venue",
            raw_artifact_uid="raw:future",
            parser_version="test",
            content_hash="a" * 64,
            retrieved_at=T0 + timedelta(hours=1),
        ),
        reliability=SourceReliability(
            source_uid="source:venue",
            source_class=SourceClass.OFFICIAL_VENUE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("1"),
            assessed_at=T0,
            rationale="official endpoint",
        ),
        missingness=observed,
        market_uid="market:future",
        window_starts_at=T0 + timedelta(minutes=55),
        window_ends_at=T0 + timedelta(hours=1),
        last_trade_price=Decimal("0.5"),
        fill_count=1,
    )


def _client(repository: ServingBundleRepository) -> TestClient:
    api.app.dependency_overrides[api.get_v3_serving_repository] = lambda: repository
    return TestClient(api.app)


def _clear_overrides() -> None:
    api.app.dependency_overrides.clear()


def test_v3_no_bundle_is_truthfully_unavailable(tmp_path: Path) -> None:
    client = _client(ServingBundleRepository(tmp_path / "empty", clock=lambda: T0))
    try:
        response = client.get("/api/v3/readiness")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "bundle_unavailable"
        assert body["bundle"]["available"] is False
        assert body["not_proof_of_fraud"] is True
        assert body["effectiveness_unknown"] is True
    finally:
        _clear_overrides()


def test_tampered_pointer_and_manifest_are_rejected(tmp_path: Path) -> None:
    repository = _verified_repository(tmp_path)
    pointer_path = tmp_path / "bundles" / "current.json"
    pointer_path.write_text('{"bundle_uid":"not-valid"}', encoding="utf-8")
    assert repository.bundle_state().available is False
    assert repository.bundle_state().reason == "bundle_verification_failed"

    repository = _verified_repository(tmp_path / "manifest")
    manifest_path = next((tmp_path / "manifest" / "bundles" / "bundles").rglob("manifest.json"))
    manifest_path.write_text("{}", encoding="utf-8")
    assert repository.bundle_state().available is False


def test_verified_fake_bundle_is_metadata_only_and_future_events_are_hidden(tmp_path: Path) -> None:
    store = EventMemoryStore([_future_market_event()])
    repository = _verified_repository(tmp_path, event_store=store)
    client = _client(repository)
    try:
        status = client.get("/api/v3/model-status")
        assert status.status_code == 200
        body = status.json()
        assert body["status"] == "verified_metadata_only"
        assert body["model_deserialization_in_request"] is False
        assert "model_path" not in json.dumps(body)

        event = client.get("/api/v3/events/event:future-market").json()
        assert event["status"] == "unavailable"
        assert event["record"] is None
        assert event["reason"] == "event_not_available_at_as_of"
    finally:
        _clear_overrides()


def test_v1_v2_smoke_and_v3_forbidden_terms_are_preserved_or_absent(tmp_path: Path) -> None:
    assessment = FrozenServingAssessment(
        event_uid="event:known",
        as_of=T0 - timedelta(minutes=1),
        decision="abstain_ood",
        coverage_status="partial",
        abstention_reasons=("out_of_distribution",),
        ood_score=0.9,
    )
    repository = _verified_repository(tmp_path)
    repository = ServingBundleRepository(
        tmp_path / "bundles",
        assessments=(assessment,),
        label_sufficiency=LabelSufficiency(status="insufficient", labeled_count=2, reason="small human mechanism registry"),
        clock=lambda: T0,
    )
    client = _client(repository)
    try:
        assert client.get("/api/health").json() == {"status": "ok"}
        assert client.get("/api/v2/capabilities").status_code == 200
        assessments = client.get("/api/v3/assessments").json()
        assert assessments["assessments"][0]["decision"] == "abstain_ood"
        assert assessments["assessments"][0]["coverage_status"] == "partial"
        encoded = json.dumps(assessments).lower()
        assert "fraud_probability" not in encoded
        assert "legal_conclusion" not in encoded
        assert "insider" not in encoded
        assert "misconduct" not in encoded
        assert assessments["not_proof_of_fraud"] is True
    finally:
        _clear_overrides()
