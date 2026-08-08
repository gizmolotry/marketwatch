from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

import marketleak.cli_v2 as cli_v2_module
from marketleak.cli_v2 import (
    audit_legacy_fixture,
    collect_once,
    collect_polymarket_wallet_history,
    create_shadow_run,
    main,
    run_shadow_cycle,
    verify_shadow_run,
)
from marketleak.ingestion.coverage import CoverageLedger
from marketleak.ingestion.connectors.http import EvidenceHttpClient, HttpResponse
from marketleak.ingestion.raw_store import RawArtifactStore


class StubTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, params, timeout, max_response_bytes=None, approved_addresses=None):
        self.calls.append((method, url, dict(params or {})))
        status, body = self.responses.pop(0)
        return HttpResponse(status, body, {}, url, peer_address=approved_addresses[0])


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


def test_wallet_history_cli_serializes_all_explicit_operator_inputs(monkeypatch, capsys):
    captured = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"command": "collect-polymarket-wallet", "complete": False}

    monkeypatch.setattr(cli_v2_module, "collect_polymarket_wallet_history", fake_collect)
    assert main(
        [
            "collect-polymarket-wallet",
            "--output-dir",
            "wallet-data",
            "--user",
            "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-02T00:00:00Z",
            "--continuation",
            "opaque-cursor",
            "--page-size",
            "250",
            "--max-pages",
            "4",
            "--taker-only",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out)["command"] == "collect-polymarket-wallet"
    assert captured == {
        "output_dir": "wallet-data",
        "user": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "end": datetime(2026, 1, 2, tzinfo=UTC),
        "continuation": "opaque-cursor",
        "page_size": 250,
        "max_pages": 4,
        "taker_only": True,
    }

    for unsupported_scope in (("--market", "condition-1"), ("--event-id", "1")):
        with pytest.raises(SystemExit):
            main(
                [
                    "collect-polymarket-wallet",
                    "--user",
                    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
                    *unsupported_scope,
                ]
            )


def test_wallet_history_collection_persists_filter_bound_coverage_and_resumes(tmp_path):
    output = tmp_path / "wallet-history"
    trade = b'''[{"proxyWallet":"0x56687bf447db6ffa42ffe2204a05edaa20f55839","side":"BUY","asset":"yes","conditionId":"condition","size":2.0,"price":0.6,"timestamp":1767225600,"outcomeIndex":0,"transactionHash":"0xabc"}]'''
    transport = StubTransport([(200, trade), (200, b"[]")])
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None
    )

    first = collect_polymarket_wallet_history(
        output_dir=output,
        user="0x56687BF447DB6FFA42FFE2204A05EDAA20F55839",
        page_size=1,
        max_pages=1,
        http_client=http,
        clock=lambda: datetime(2026, 1, 2, 0, 0, 1, tzinfo=UTC),
    )
    resumed = collect_polymarket_wallet_history(
        output_dir=output,
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        continuation=first["continuation"],
        page_size=1,
        max_pages=1,
        http_client=http,
        clock=lambda: (_ for _ in ()).throw(
            AssertionError("resume must inherit end from the continuation")
        ),
    )

    expected_filters = {
        "end": 1767312000,
        "start": 1,
        "takerOnly": False,
        "user": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
    }
    assert first["complete"] is False
    assert first["continuation_state"] == "page"
    assert resumed["complete"] is True
    assert resumed["continuation"] is None
    assert resumed["query_filters"] == expected_filters
    assert resumed["coverage"]["filters"] == expected_filters
    assert resumed["coverage"]["record_count"] == 1
    assert len(resumed["coverage"]["raw_sha256"]) == 2
    assert [call[2]["offset"] for call in transport.calls] == [0, 1]
    assert all(call[2]["takerOnly"] is False for call in transport.calls)
    assert list((output / "raw" / "objects").rglob("*.raw"))
    assert list((output / "normalized" / "trade_fill").rglob("*.json"))

    rows = CoverageLedger(output / "coverage" / "ledger.jsonl").records(
        platform="polymarket", dataset="public_wallet_trades"
    )
    assert len(rows) == 2
    assert rows[0].complete is False
    assert rows[1].complete is True
    assert dict(rows[1].filters) == expected_filters
    assert rows[1].interval_start == datetime.fromtimestamp(1, tz=UTC)
    assert rows[1].interval_end == datetime(2026, 1, 2, tzinfo=UTC)


def test_wallet_history_cli_surfaces_split_required_without_complete_coverage(tmp_path):
    output = tmp_path / "wallet-ceiling"
    trade = b'''[{"proxyWallet":"0x56687bf447db6ffa42ffe2204a05edaa20f55839","side":"BUY","asset":"yes","conditionId":"condition","size":2.0,"price":0.6,"timestamp":1767225600,"outcomeIndex":0,"transactionHash":"0xabc"}]'''
    transport = StubTransport([(200, trade)])
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None
    )

    payload = collect_polymarket_wallet_history(
        output_dir=output,
        user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
        end=datetime(2026, 1, 2, tzinfo=UTC),
        continuation="10000",
        page_size=1,
        max_pages=1,
        http_client=http,
    )

    assert payload["complete"] is False
    assert payload["coverage"]["complete"] is False
    assert payload["continuation_state"] == "split_required"
    assert payload["requires_narrower_time_window"] is True
    assert json.loads(payload["continuation"])["state"] == "split_required"


def test_wallet_history_cli_rejects_subsecond_coverage_bounds(tmp_path):
    output = tmp_path / "wallet-subsecond"
    transport = StubTransport([])
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None
    )

    with pytest.raises(ValueError, match="end must use whole-second"):
        collect_polymarket_wallet_history(
            output_dir=output,
            user="0x56687bf447db6ffa42ffe2204a05edaa20f55839",
            end=datetime(2026, 1, 2, 0, 0, 0, 1, tzinfo=UTC),
            http_client=http,
        )

    assert transport.calls == []
    assert not (output / "coverage" / "ledger.jsonl").exists()


def test_wallet_history_cli_rejects_remote_market_or_event_scope_before_network(tmp_path):
    output = tmp_path / "wallet-remote-scope"
    transport = StubTransport([])
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None
    )
    wallet = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"

    with pytest.raises(ValueError, match="user-only.*local normalized index"):
        collect_polymarket_wallet_history(
            output_dir=output,
            user=wallet,
            market="condition-1",
            http_client=http,
        )
    with pytest.raises(ValueError, match="user-only.*local normalized index"):
        collect_polymarket_wallet_history(
            output_dir=output,
            user=wallet,
            event_id=1,
            http_client=http,
        )

    assert transport.calls == []
    assert not (output / "coverage" / "ledger.jsonl").exists()


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
