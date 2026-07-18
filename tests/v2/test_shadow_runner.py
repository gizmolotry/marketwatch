from datetime import datetime, timedelta, timezone

from marketleak.shadow import ShadowInputRecord, ShadowLedger, ShadowRunner, build_shadow_manifest


UTC = timezone.utc
AS_OF = datetime(2026, 5, 1, 12, tzinfo=UTC)


def _manifest(*, rate=0.25, seed=42, coverage=None):
    return build_shadow_manifest(
        repo_root=".",
        config={"alert_q": "0.01", "control_sampling_rate": rate},
        schema={"assessment": "v2"},
        model={"type": "uncalibrated_rank"},
        detector={"name": "causal-v2"},
        evidence={"name": "pit-v1"},
        graph={"name": "independent-v1"},
        schema_version="2.0.0",
        model_version="rank-v1",
        detector_version="causal-v2",
        evidence_version="pit-v1",
        graph_version="independent-v1",
        random_seed=seed,
        control_sampling_rate=rate,
        input_partitions={"partition": b"same immutable input"},
        source_high_watermarks={"venue": AS_OF},
        source_coverage=coverage or {"venue": {"status": "complete"}},
        as_of=AS_OF,
        started_at=AS_OF - timedelta(minutes=5),
        environment={"mode": "test"},
        git_revision="UNTRACKED",
    )


def test_same_manifest_and_inputs_produce_byte_equivalent_payloads_and_ignore_future(tmp_path):
    manifest = _manifest(rate=1.0)
    called_a = []
    called_b = []

    def evaluator_a(record, _manifest):
        called_a.append(record.record_uid)
        return {"is_alert": bool(record.payload["flag"]), "score": record.payload["score"]}

    def evaluator_b(record, _manifest):
        called_b.append(record.record_uid)
        return {"is_alert": bool(record.payload["flag"]), "score": record.payload["score"]}

    records = [
        ShadowInputRecord(
            record_uid="window:future",
            event_time=AS_OF + timedelta(seconds=1),
            risk_tier="high",
            payload={"flag": True, "score": "99"},
        ),
        ShadowInputRecord(
            record_uid="window:alert",
            event_time=AS_OF - timedelta(minutes=1),
            risk_tier="high",
            payload={"flag": True, "score": "8.2"},
        ),
        ShadowInputRecord(
            record_uid="window:control",
            event_time=AS_OF - timedelta(minutes=2),
            risk_tier="low",
            payload={"flag": False, "score": "0.1"},
        ),
    ]
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = ShadowRunner(
        manifest=manifest,
        ledger=ShadowLedger(first_dir, manifest),
        evaluator=evaluator_a,
    ).run(records)
    second = ShadowRunner(
        manifest=manifest,
        ledger=ShadowLedger(second_dir, manifest),
        evaluator=evaluator_b,
    ).run(reversed(records))

    assert called_a == called_b == ["window:control", "window:alert"]
    assert first.future_excluded_count == second.future_excluded_count == 1
    assert first.alert_count == second.alert_count == 1
    assert first.control_sample_count == second.control_sample_count == 1
    assert first.assessment_payload_bytes == second.assessment_payload_bytes
    assert (first_dir / "ledger.jsonl").read_bytes() == (second_dir / "ledger.jsonl").read_bytes()
    assert b"engineering_validation_only" in first.assessment_payload_bytes[0]
    assert b"effectiveness_unknown" in first.assessment_payload_bytes[0]


def test_low_risk_control_sampling_is_reproducible_with_fixed_seed(tmp_path):
    manifest = _manifest(rate=0.25, seed=12345)
    records = [
        ShadowInputRecord(
            record_uid=f"window:{index:03d}",
            event_time=AS_OF - timedelta(minutes=index),
            risk_tier="low",
            payload={"index": index},
        )
        for index in range(100)
    ]

    def no_alert(_record, _manifest):
        return {"is_alert": False}

    first = ShadowRunner(
        manifest=manifest,
        ledger=ShadowLedger(tmp_path / "a", manifest),
        evaluator=no_alert,
    ).run(records)
    second = ShadowRunner(
        manifest=manifest,
        ledger=ShadowLedger(tmp_path / "b", manifest),
        evaluator=no_alert,
    ).run(reversed(records))

    assert first.control_record_uids == second.control_record_uids
    assert 0 < first.control_sample_count < len(records)
    assert first.assessment_payload_bytes == second.assessment_payload_bytes


def test_runner_surfaces_incomplete_coverage(tmp_path):
    manifest = _manifest(
        coverage={
            "venue": {"status": "complete"},
            "public-wire": {"status": "unavailable", "limitations": ["outage"]},
        }
    )
    result = ShadowRunner(
        manifest=manifest,
        ledger=ShadowLedger(tmp_path, manifest),
        evaluator=lambda _record, _manifest: None,
    ).run([])

    assert result.coverage_complete is False
    assert result.coverage_gaps == ("public-wire",)
    assert result.engineering_validation_only is True
    assert result.effectiveness_unknown is True

