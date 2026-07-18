from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path

import pytest

from marketleak.multimodal.review_case_builder import (
    MISSING_COVERAGE_REASONS,
    build_cocaptured_snapshot,
    build_document,
    build_recorded_snapshot,
    write_document,
)
from marketleak.multimodal.review_cases import (
    FrozenReviewCaseRepository,
    ReviewCaseKind,
    ReviewCheckStatus,
    ReviewCaseStatus,
    ReviewCoverageStatus,
    ReviewDecision,
)


T0 = datetime(2026, 7, 13, 12, tzinfo=UTC)
MARKET = "0xrecorded-market"
ASSET = "100"
OTHER_ASSET = "200"


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_raw(
    capture_root: Path,
    payload: bytes,
    received_at: datetime,
    *,
    platform: str = "polymarket",
    source: str = "ws/market",
) -> str:
    digest = hashlib.sha256(payload).hexdigest()
    object_path = capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
    object_path.parent.mkdir(parents=True, exist_ok=True)
    object_path.write_bytes(payload)
    _write_json(
        capture_root / "raw" / "receipts" / "2026-07-13" / f"{digest}.json",
        {
            "schema_version": 1,
            "sha256": digest,
            "platform": platform,
            "source": source,
            "received_at": received_at.isoformat().replace("+00:00", "Z"),
        },
    )
    return f"{platform}:raw/{digest}"


def _write_observation(
    capture_root: Path,
    *,
    name: str,
    observation_uid: str,
    raw_artifact_uid: str,
    event_time: datetime,
    price: str,
    asset_id: str = ASSET,
) -> None:
    _write_json(
        capture_root / "normalized" / "priceobservation" / "platform=polymarket" / "date=2026-07-13" / f"{name}.json",
        {
            "platform": "polymarket",
            "market_uid": f"polymarket:market/{MARKET}",
            "outcome_uid": f"polymarket:outcome/{asset_id}",
            "observation_uid": observation_uid,
            "raw_artifact_uid": raw_artifact_uid,
            "source_uid": "polymarket:source/ws-market",
            "event_time": event_time.isoformat().replace("+00:00", "Z"),
            "ingested_at": (event_time + timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z"),
            "price": price,
        },
    )


def _registry(path: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "phase15-source-registry-v1",
            "targets": [
                {
                    "target_uid": "market:test-recorded",
                    "venue": "polymarket",
                    "market_uid": f"polymarket:{MARKET}",
                    "polymarket_asset_ids": [ASSET, OTHER_ASSET],
                }
            ],
        },
    )


@pytest.fixture
def recorded_capture(tmp_path: Path) -> tuple[Path, Path]:
    capture_root = tmp_path / "capture"
    registry_path = tmp_path / "registry.json"
    _registry(registry_path)
    first_raw = _write_raw(capture_root, b'{"frame":"first"}', T0 + timedelta(seconds=1))
    middle_raw = _write_raw(capture_root, b'{"frame":"middle"}', T0 + timedelta(minutes=1, seconds=1))
    last_raw = _write_raw(capture_root, b'{"frame":"last"}', T0 + timedelta(minutes=2, seconds=1))
    _write_observation(
        capture_root,
        name="first",
        observation_uid="polymarket:obs/first",
        raw_artifact_uid=first_raw,
        event_time=T0,
        price="0.20",
    )
    _write_observation(
        capture_root,
        name="middle",
        observation_uid="polymarket:obs/middle",
        raw_artifact_uid=middle_raw,
        event_time=T0 + timedelta(minutes=1),
        price="0.40",
    )
    _write_observation(
        capture_root,
        name="last",
        observation_uid="polymarket:obs/last",
        raw_artifact_uid=last_raw,
        event_time=T0 + timedelta(minutes=2),
        price="0.35",
    )
    _write_observation(
        capture_root,
        name="other-outcome",
        observation_uid="polymarket:obs/other",
        raw_artifact_uid=middle_raw,
        event_time=T0 + timedelta(minutes=1),
        price="0.60",
        asset_id=OTHER_ASSET,
    )
    return capture_root, registry_path


def test_builder_is_deterministic_and_writes_repository_valid_document(recorded_capture: tuple[Path, Path], tmp_path: Path) -> None:
    capture_root, registry_path = recorded_capture

    first = build_document(capture_root, registry_path)
    second = build_document(capture_root, registry_path)
    output = write_document(tmp_path / "review-case.json", capture_root, registry_path)

    assert first == second
    case = first["review_cases"][0]
    assert case["case_kind"] == ReviewCaseKind.RECORDED_SNAPSHOT.value
    assert case["trigger"]["observation_count"] == 3
    assert case["trigger"]["price_open"] == "0.20"
    assert case["trigger"]["price_close"] == "0.35"
    assert case["trigger"]["price_change"] == "0.15"
    assert FrozenReviewCaseRepository(output).load().status == ReviewCaseStatus.AVAILABLE_PRECOMPUTED_REVIEW_CASES


def test_builder_keeps_only_hash_verified_raw_lineage(recorded_capture: tuple[Path, Path]) -> None:
    capture_root, registry_path = recorded_capture

    case = build_recorded_snapshot(capture_root, registry_path)

    assert tuple(entry.evidence_uid for entry in case.evidence_ledger) == (
        "polymarket:obs/first",
        "polymarket:obs/last",
    )
    assert tuple(entry.raw_artifact_uid for entry in case.evidence_ledger) == case.trigger.raw_artifact_uids
    for entry in case.evidence_ledger:
        digest = entry.raw_artifact_uid.rsplit("/", 1)[1]
        path = capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry.content_hash

    first_entry = case.evidence_ledger[0]
    digest = first_entry.raw_artifact_uid.rsplit("/", 1)[1]
    corrupt_path = capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
    corrupt_path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="SHA-256"):
        build_recorded_snapshot(capture_root, registry_path)


def test_builder_preserves_missing_coverage_and_abstains(recorded_capture: tuple[Path, Path]) -> None:
    capture_root, registry_path = recorded_capture

    case = build_recorded_snapshot(capture_root, registry_path)

    assert case.coverage.overall_status == ReviewCoverageStatus.PARTIAL
    assert case.coverage.gap_reasons == MISSING_COVERAGE_REASONS
    assert case.routing.decision == ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE
    assert case.routing.abstention_reasons == MISSING_COVERAGE_REASONS
    unavailable = {check.kind for check in case.ordinary_checks if check.status == ReviewCheckStatus.UNAVAILABLE}
    assert {"market_context", "underlying_reference", "public_evidence", "market_mechanics"}.issubset(unavailable)
    assert case.trigger.trade_notional is None
    assert case.trigger.orderbook_snapshot_count == 0


def _raw_entry(capture_root: Path, raw_uid: str, received_at: datetime, *, source: str, platform: str) -> dict[str, object]:
    digest = raw_uid.rsplit("/", 1)[1]
    return {
        "source_uid": source,
        "raw_artifact_uid": raw_uid,
        "sha256": digest,
        "object_path": str(capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"),
        "receipt_path": str(capture_root / "raw" / "receipts" / "2026-07-13" / f"{digest}.json"),
        "receipt_received_at": received_at.isoformat().replace("+00:00", "Z"),
        "received_at": received_at.isoformat().replace("+00:00", "Z"),
        "platform": platform,
    }


def _write_cocapture_context(capture_root: Path) -> None:
    gamma_at = T0 - timedelta(minutes=2)
    clob_at = T0 - timedelta(minutes=1, seconds=59)
    gamma_raw = _write_raw(capture_root, b'{"gamma":true}', gamma_at, source="polymarket:source/gamma-market")
    clob_raw = _write_raw(capture_root, b'{"clob":true}', clob_at, source="polymarket:source/clob-market-info")
    _write_json(
        capture_root / "case-context.json",
        {
            "schema_version": "phase15-polymarket-case-context-v1",
            "status": "collected",
            "availability": {"available": True, "as_of": (T0 - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"), "watermark": clob_at.isoformat().replace("+00:00", "Z")},
            "market": {
                "market_uid": f"polymarket:market/{MARKET}",
                "question": "Will the captured BTC condition occur?",
                "rule": "Captured Binance BTC/USDT final candle High rule.",
                "resolution_source": None,
                "resolution_source_status": "null",
                "outcomes": [{"label": "Yes", "token_id": ASSET}, {"label": "No", "token_id": OTHER_ASSET}],
            },
            "raw_lineage": [
                _raw_entry(capture_root, gamma_raw, gamma_at, source="polymarket:source/gamma-market", platform="polymarket"),
                _raw_entry(capture_root, clob_raw, clob_at, source="polymarket:source/clob-market-info", platform="polymarket"),
            ],
        },
    )


def _write_reference(capture_root: Path, *, candle_end: datetime = T0 + timedelta(minutes=1)) -> str:
    receipt_at = candle_end + timedelta(seconds=10)
    raw_uid = _write_raw(capture_root, b'{"binance":"final-candle"}', receipt_at, platform="binance", source="source:binance-spot-btcusdt-kline")
    raw = _raw_entry(capture_root, raw_uid, receipt_at, source="source:binance-spot-btcusdt-kline", platform="binance")
    mapping = {
        "mapping_uid": "reference:test-binance-candle-high",
        "market_uid": f"polymarket:market/{MARKET}",
        "asset_symbol": "BTC",
        "quote_currency": "USDT",
        "observation_kind": "candle_high",
        "candle_interval": "1m",
        "requires_closed_candle": True,
        "primary_source_uid": "source:binance-spot-btcusdt-kline",
    }
    _write_json(
        capture_root / "binance-btcusdt-reference.json",
        {
            "schema_version": "phase15-binance-btcusdt-reference-v1",
            "status": "collected",
            "market_uid": f"polymarket:market/{MARKET}",
            "as_of": (T0 + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
            "admission": {"status": "admitted", "market_uid": f"polymarket:market/{MARKET}"},
            "mapping": mapping,
            "observation": {
                "reference_price_uid": "binance:reference-price/test-final-candle",
                "market_uid": f"polymarket:market/{MARKET}",
                "observation_kind": "candle_high",
                "source_symbol": "BTCUSDT",
                "candle_interval": "1m",
                "is_final": True,
                "event_time": candle_end.isoformat().replace("+00:00", "Z"),
                "candle_start": (candle_end - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
                "candle_end": candle_end.isoformat().replace("+00:00", "Z"),
                "ingested_at": receipt_at.isoformat().replace("+00:00", "Z"),
                "source_mapping": mapping,
                "provenance": {"source_uid": "source:binance-spot-btcusdt-kline", "raw_artifact_uid": raw_uid},
                "reliability": {"tier": "high"},
            },
            "raw_captures": [raw],
        },
    )
    return raw_uid


def test_cocaptured_builder_uses_context_before_market_capture_and_keeps_abstention(recorded_capture: tuple[Path, Path]) -> None:
    capture_root, registry_path = recorded_capture
    _write_cocapture_context(capture_root)
    _write_reference(capture_root)

    case = build_cocaptured_snapshot(capture_root, registry_path)

    assert case.case_uid == "review:polymarket-btc65k-cocaptured"
    assert case.question == "Will the captured BTC condition occur?"
    assert case.as_of == T0 + timedelta(minutes=2, seconds=1)
    assert case.routing.decision == ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE
    assert {entry.modality for entry in case.evidence_ledger} == {"market_state", "market_context", "reference_price"}
    checks = {check.kind: check for check in case.ordinary_checks}
    assert checks["market_context"].status == ReviewCheckStatus.OBSERVED
    assert "null or absent" in checks["market_context"].summary
    assert checks["underlying_reference"].status == ReviewCheckStatus.OBSERVED
    assert "public_evidence_unavailable" in case.routing.abstention_reasons


def test_tampered_or_late_reference_remains_unavailable_and_abstains(recorded_capture: tuple[Path, Path]) -> None:
    capture_root, registry_path = recorded_capture
    _write_cocapture_context(capture_root)
    raw_uid = _write_reference(capture_root)
    digest = raw_uid.rsplit("/", 1)[1]
    (capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw").write_bytes(b"tampered")

    case = build_cocaptured_snapshot(capture_root, registry_path)

    checks = {check.kind: check for check in case.ordinary_checks}
    assert checks["underlying_reference"].status == ReviewCheckStatus.UNAVAILABLE
    assert "btc_reference_unavailable" in case.routing.abstention_reasons
    assert case.routing.decision == ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE
