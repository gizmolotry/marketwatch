from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from marketleak.cli_v2 import (
    audit_legacy_fixture,
    collect_once,
    create_shadow_run,
    main,
    run_shadow_cycle,
    verify_shadow_run,
)
from marketleak.ingestion.connectors.http import EvidenceHttpClient, HttpResponse
from marketleak.ingestion.raw_store import RawArtifactStore


class StubTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, params, timeout):
        self.calls.append((method, url, dict(params or {})))
        status, body = self.responses.pop(0)
        return HttpResponse(status, body, {}, url)


def test_capabilities_command_is_honest_and_offline(capsys):
    assert main(["capabilities"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["effectiveness_unknown"] is True
    assert payload["fraud_prediction"] is False
    assert payload["official_public_connectors"]["kalshi_trades"]["actor_visibility"] == "unavailable"
    assert any("historical L2" in item for item in payload["current_limitations"])


def test_legacy_audit_reports_zero_actionable_without_inventing_actor_data(tmp_path):
    fixture = tmp_path / "ticks.parquet"
    frame = pd.DataFrame(
        [
            {"tick_uid": "t1", "market_uid": "legacy:m1", "timestamp": 1767225600, "price": 0.4},
            {"tick_uid": "t2", "market_uid": "legacy:m1", "timestamp": 1767225600, "price": 0.4},
        ]
    )
    frame.to_parquet(fixture, index=False)

    payload = audit_legacy_fixture(fixture)

    assert payload["status"] == "blocked_by_data_quality"
    assert payload["actionable_count"] == 0
    assert payload["data_quality"]["duplicates"] == 1
    assert payload["actor_availability"]["available"] is False
    assert payload["data_quality"]["gate_passed"] is False
    assert payload["effectiveness_unknown"] is True


def test_collect_once_is_bounded_and_writes_raw_before_normalized(tmp_path):
    output = tmp_path / "v2"
    body = b'''[{"proxyWallet":"0x56687bf447db6ffa42ffe2204a05edaa20f55839","side":"BUY","asset":"yes","conditionId":"condition","size":2.0,"price":0.6,"timestamp":1767225600,"outcomeIndex":0,"transactionHash":"0xabc"}]'''
    transport = StubTransport([(200, body)])
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None
    )

    payload = collect_once(
        output_dir=output,
        platform="polymarket",
        page_size=1,
        max_pages=1,
        http_client=http,
    )

    assert len(transport.calls) == 1
    assert payload["bounded"] == {"page_size": 1, "max_pages": 1}
    assert payload["sources"]["polymarket_trades"]["writes"]["fills"]["inserted"] == 1
    assert list((output / "raw" / "objects").rglob("*.raw"))
    assert list((output / "normalized" / "trade_fill").rglob("*.json"))
    assert (output / "coverage" / "ledger.jsonl").is_file()


def test_shadow_cycle_freezes_input_runs_and_verifies_hash_chain(tmp_path):
    records = tmp_path / "records.jsonl"
    records.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "record_uid": "window:alert",
                        "event_time": "2026-01-01T00:00:00Z",
                        "risk_tier": "high",
                        "payload": {"is_alert": True, "assessment": {"target": "A"}},
                    }
                ),
                json.dumps(
                    {
                        "record_uid": "window:control",
                        "event_time": "2026-01-01T00:01:00Z",
                        "risk_tier": "low",
                        "payload": {"is_alert": False},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config = tmp_path / "shadow.yaml"
    config.write_text("effectiveness_unknown: true\n", encoding="utf-8")
    run_dir = tmp_path / "run"
    as_of = datetime(2026, 1, 2, tzinfo=UTC)
    manifest = create_shadow_run(
        input_path=records,
        run_dir=run_dir,
        config_path=config,
        as_of=as_of,
        coverage_status="partial",
        coverage_limitations=("public timeline unavailable",),
        control_sampling_rate=1.0,
    )

    result = run_shadow_cycle(run_dir=run_dir, input_path=records)
    verified = verify_shadow_run(run_dir)

    assert manifest.frozen is True and manifest.effectiveness_unknown is True
    assert result["alert_count"] == 1
    assert result["control_sample_count"] == 1
    assert result["coverage_complete"] is False
    assert result["ledger_verified"] is True
    assert verified["ledger_entries"] == 2

    records.write_text(records.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="hash does not match"):
        run_shadow_cycle(run_dir=run_dir, input_path=records)
