import json
from datetime import datetime, timedelta, timezone

import pytest

from marketleak.shadow import (
    FrozenRunMutationError,
    LedgerTamperError,
    ShadowLedger,
    build_shadow_manifest,
    resolve_git_revision,
)


UTC = timezone.utc
AS_OF = datetime(2026, 4, 1, 12, tzinfo=UTC)


def _manifest(*, config=None, model=None, coverage=None, seed=7):
    return build_shadow_manifest(
        repo_root=".",
        config=config or {"threshold": "0.01"},
        schema={"version": "2"},
        model=model or {"artifact": "model-a"},
        detector={"name": "causal-v2"},
        evidence={"policy": "pit-v1"},
        graph={"policy": "independent-v1"},
        schema_version="2.0.0",
        model_version="untrained-rank-v1",
        detector_version="causal-v2",
        evidence_version="pit-v1",
        graph_version="independent-v1",
        random_seed=seed,
        control_sampling_rate=0.25,
        input_partitions={"ticks": b"fixed input bytes"},
        source_high_watermarks={"venue": AS_OF - timedelta(minutes=1)},
        source_coverage=coverage or {"venue": {"status": "complete"}},
        as_of=AS_OF,
        started_at=AS_OF - timedelta(minutes=5),
        environment={"python": "test", "mode": "shadow"},
        git_revision="UNTRACKED",
    )


def test_manifest_is_deterministic_and_records_unknown_effectiveness():
    first = _manifest()
    second = _manifest()

    assert first == second
    assert first.shadow_run_uid == second.shadow_run_uid
    assert first.git_revision == "UNTRACKED"
    assert first.engineering_validation_only is True
    assert first.effectiveness_unknown is True
    assert first.tuning_locked is True
    assert first.frozen is True
    assert first.coverage_complete is True
    assert first.input_partition_hashes["ticks"]


def test_config_or_model_change_requires_a_new_run_uid():
    original = _manifest()
    changed_config = _manifest(config={"threshold": "0.02"})
    changed_model = _manifest(model={"artifact": "model-b"})

    assert changed_config.config_hash != original.config_hash
    assert changed_model.model_hash != original.model_hash
    assert changed_config.shadow_run_uid != original.shadow_run_uid
    assert changed_model.shadow_run_uid != original.shadow_run_uid


def test_non_repository_has_explicit_untracked_revision(tmp_path):
    assert resolve_git_revision(tmp_path) == "UNTRACKED"


def test_frozen_run_directory_rejects_manifest_mutation(tmp_path):
    ShadowLedger(tmp_path, _manifest())

    with pytest.raises(FrozenRunMutationError, match="frozen run directory"):
        ShadowLedger(tmp_path, _manifest(config={"threshold": "changed"}))


def test_hash_chain_detects_payload_tampering(tmp_path):
    ledger = ShadowLedger(tmp_path, _manifest())
    ledger.append_assessment("record:1", {"is_alert": True, "score": "8.2"})
    row = json.loads(ledger.ledger_path.read_text(encoding="utf-8").splitlines()[0])
    row["payload"]["score"] = "0.0"
    ledger.ledger_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    with pytest.raises(LedgerTamperError, match="canonical|hash mismatch"):
        ledger.verify()


def test_manifest_exposes_incomplete_source_coverage():
    manifest = _manifest(
        coverage={
            "venue": {"status": "complete"},
            "public-wire": {
                "status": "partial",
                "limitations": ["connector outage from 11:40Z to 11:55Z"],
            },
        }
    )

    assert manifest.coverage_complete is False
    assert manifest.source_coverage["public-wire"].status == "partial"
    assert manifest.source_coverage["public-wire"].limitations
