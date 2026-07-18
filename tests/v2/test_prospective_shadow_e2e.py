from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from marketleak.cli_v2 import run_prospective_shadow_cycle
from marketleak.domain import ActorVisibility, ObservationKind, PriceObservation
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.ingestion.storage import NormalizedStore
from marketleak.pipeline_v2 import load_canonical_inputs
from marketleak.shadow import ShadowLedger, ShadowRunManifestV2


BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _observation(index: int, *, raw_uid: str, event_time: datetime, price: str) -> PriceObservation:
    return PriceObservation(
        observation_uid=f"test:obs-{index}",
        market_uid="test:market-one",
        outcome_uid="test:outcome-yes",
        platform="test",
        price=Decimal(price),
        kind=ObservationKind.PLATFORM_SNAPSHOT,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        event_time=event_time,
        ingested_at=event_time,
        source_uid="test:source-canonical",
        raw_artifact_uid=raw_uid,
        parser_version="test-v2.0.0",
    )


def _seed_store(
    root: Path,
    *,
    as_of: datetime,
    include_future: bool = True,
    include_malicious: bool = True,
) -> str:
    capture = RawArtifactStore(root / "raw").capture(
        b'{"immutable":"canonical-test-source"}',
        platform="test",
        source="prospective-test",
        request={"method": "GET", "url": "https://example.test/canonical"},
        received_at=BASE,
    )
    raw_uid = f"test:raw/{capture.sha256}"
    observations = []
    for index in range(122):
        event_time = BASE + timedelta(minutes=5 * index + 1)
        observations.append(
            _observation(
                index,
                raw_uid=raw_uid,
                event_time=event_time,
                price="0.50" if index < 120 else "0.80",
            )
        )
    if include_future:
        observations.append(
            _observation(999, raw_uid=raw_uid, event_time=as_of + timedelta(seconds=1), price="0.99")
        )
    result = NormalizedStore(root).write(observations, record_type="price_observation")
    assert result.inserted == 122 + int(include_future)

    if include_malicious:
        malicious = observations[0].model_dump(mode="json")
        malicious.update({"observation_uid": "test:malicious", "is_alert": True, "assessment": {"target": "A"}})
        malicious_path = (
            root
            / "normalized"
            / "price_observation"
            / "platform=test"
            / "date=2026-01-01"
            / "malicious-prepared-outcome.json"
        )
        malicious_path.parent.mkdir(parents=True, exist_ok=True)
        malicious_path.write_text(json.dumps(malicious), encoding="utf-8")
    return raw_uid


def _write_configs(root: Path) -> tuple[Path, Path]:
    shadow_config = root / "shadow.yaml"
    shadow_config.write_text("effectiveness_unknown: true\ntuning_locked: true\n", encoding="utf-8")
    detector_config = root / "detector.yaml"
    detector_config.write_text(
        "\n".join(
            [
                'schema_version: "2.0.0"',
                "bucket_minutes: 5",
                "max_carry_age_minutes: 10",
                "baseline_days: 7",
                "min_baseline_observations: 30",
                "min_baseline_elapsed_hours: 2",
                "empirical_tail_min_observations: 30",
                "fdr_alpha: 0.05",
                "incident_cooldown_minutes: 30",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return shadow_config, detector_config


def test_prospective_cycle_derives_alerts_and_ignores_prepared_outcomes_and_future(tmp_path) -> None:
    as_of = BASE + timedelta(hours=12)
    data_root = tmp_path / "canonical"
    _seed_store(data_root, as_of=as_of)
    shadow_config, detector_config = _write_configs(tmp_path)
    run_dir = tmp_path / "run"

    result = run_prospective_shadow_cycle(
        data_root=data_root,
        run_dir=run_dir,
        config_path=shadow_config,
        detector_config_path=detector_config,
        as_of=as_of,
        coverage_status="partial",
        coverage_limitations=("prospective collection window incomplete",),
        control_sampling_rate=1.0,
        repo_root=Path.cwd(),
    )

    manifest = ShadowRunManifestV2.model_validate_json((run_dir / "manifest.json").read_bytes())
    entries = ShadowLedger(run_dir, manifest).verify()
    assessment_entries = [entry for entry in entries if entry.record_type == "assessment"]

    assert result["pipeline_source"] == "canonical_normalized_partitions"
    assert result["caller_supplied_outcomes_accepted"] is False
    assert result["future_excluded_count"] == 1
    assert result["data_quality"]["invalid"] == 1
    assert result["alert_count"] == 1
    assert assessment_entries
    assert assessment_entries[0].payload["assessment"] is not None
    assert assessment_entries[0].payload["pipeline_source"] == "canonical_normalized_partitions"
    assert "is_alert" not in json.dumps(assessment_entries[0].payload)
    assert all(watermark <= as_of for watermark in manifest.source_high_watermarks.values())
    assert any(name.endswith("malicious-prepared-outcome.json") for name in manifest.input_partition_hashes)

    replay_dir = tmp_path / "run-replay"
    replay = run_prospective_shadow_cycle(
        data_root=data_root,
        run_dir=replay_dir,
        config_path=shadow_config,
        detector_config_path=detector_config,
        as_of=as_of,
        coverage_status="partial",
        coverage_limitations=("prospective collection window incomplete",),
        control_sampling_rate=1.0,
        repo_root=Path.cwd(),
    )
    assert replay["pipeline_run_uid"] == result["pipeline_run_uid"]
    assert (replay_dir / "ledger.jsonl").read_bytes() == (run_dir / "ledger.jsonl").read_bytes()


def test_future_valid_malformed_and_bad_raw_files_do_not_change_frozen_run(tmp_path) -> None:
    as_of = BASE + timedelta(hours=12)
    baseline_root = tmp_path / "baseline-canonical"
    future_root = tmp_path / "future-canonical"
    _seed_store(baseline_root, as_of=as_of, include_future=False, include_malicious=False)
    _seed_store(future_root, as_of=as_of, include_future=True, include_malicious=False)

    future_dir = (
        future_root
        / "normalized"
        / "price_observation"
        / "platform=test"
        / "date=2026-01-02"
    )
    future_dir.mkdir(parents=True, exist_ok=True)
    (future_dir / "malformed-future.json").write_bytes(b"{definitely-not-json")
    bad_raw_future = _observation(
        1001,
        raw_uid=f"test:raw/{'0' * 64}",
        event_time=as_of + timedelta(days=1),
        price="0.99",
    )
    assert NormalizedStore(future_root).write(
        [bad_raw_future], record_type="price_observation"
    ).inserted == 1

    shadow_config, detector_config = _write_configs(tmp_path)
    common = {
        "config_path": shadow_config,
        "detector_config_path": detector_config,
        "as_of": as_of,
        "coverage_status": "partial",
        "coverage_limitations": ("prospective window incomplete",),
        "repo_root": Path.cwd(),
    }
    baseline = run_prospective_shadow_cycle(
        data_root=baseline_root,
        run_dir=tmp_path / "baseline-run",
        **common,
    )
    with_future = run_prospective_shadow_cycle(
        data_root=future_root,
        run_dir=tmp_path / "future-run",
        **common,
    )

    assert with_future["future_excluded_count"] == 3
    assert with_future["data_quality"]["raw_hash_failures"] == 0
    assert with_future["data_quality"]["invalid"] == baseline["data_quality"]["invalid"] == 0
    assert with_future["data_quality"]["received"] == baseline["data_quality"]["received"]
    assert with_future["data_quality"]["normalized"] == baseline["data_quality"]["normalized"]
    assert with_future["partition_count"] == baseline["partition_count"]
    assert with_future["manifest_hash"] == baseline["manifest_hash"]
    assert with_future["pipeline_run_uid"] == baseline["pipeline_run_uid"]
    assert (tmp_path / "future-run" / "ledger.jsonl").read_bytes() == (
        tmp_path / "baseline-run" / "ledger.jsonl"
    ).read_bytes()


def test_loaded_snapshot_is_toctou_safe_when_included_source_file_changes(tmp_path) -> None:
    as_of = BASE + timedelta(hours=12)
    data_root = tmp_path / "canonical"
    _seed_store(data_root, as_of=as_of, include_future=False, include_malicious=False)
    shadow_config, detector_config = _write_configs(tmp_path)
    snapshot = load_canonical_inputs(data_root, as_of=as_of)
    assert snapshot.quality_report.normalized == 122

    common = {
        "data_root": data_root,
        "config_path": shadow_config,
        "detector_config_path": detector_config,
        "as_of": as_of,
        "coverage_status": "complete",
        "repo_root": Path.cwd(),
        "input_snapshot": snapshot,
    }
    before = run_prospective_shadow_cycle(run_dir=tmp_path / "before", **common)
    included_path = snapshot.partitions[0].source_path
    included_path.write_bytes(b'{"mutated":"after-load"}')
    after = run_prospective_shadow_cycle(run_dir=tmp_path / "after", **common)

    assert after["manifest_hash"] == before["manifest_hash"]
    assert after["pipeline_run_uid"] == before["pipeline_run_uid"]
    assert (tmp_path / "after" / "ledger.jsonl").read_bytes() == (
        tmp_path / "before" / "ledger.jsonl"
    ).read_bytes()

    reloaded = load_canonical_inputs(data_root, as_of=as_of)
    assert reloaded.quality_report.invalid == 1
    assert reloaded.partitions[0].payload != snapshot.partitions[0].payload
