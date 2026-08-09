from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import marketleak.cli_v2 as cli_v2_module
from marketleak.cli_v2 import (
    audit_legacy_fixture,
    collect_once,
    collect_polymarket_population,
    collect_polymarket_wallet_history,
    create_shadow_run,
    main,
    run_shadow_cycle,
    verify_shadow_run,
)
from marketleak.ingestion.coverage import CoverageLedger
from marketleak.ingestion.connectors.http import EvidenceHttpClient, HttpResponse
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.ingestion.polymarket_population import PolymarketPopulationError
from marketleak.ingestion.polymarket_population import PolymarketPopulationEvidenceBound
from marketleak.ingestion.raw_store import RawArtifactStore


class StubTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, params, timeout, max_response_bytes=None, approved_addresses=None):
        self.calls.append((method, url, dict(params or {})))
        status, body = self.responses.pop(0)
        return HttpResponse(status, body, {}, url, peer_address=approved_addresses[0])


CONDITION_ID = "0x" + "1a" * 32
OFFICIAL_TRADES_CONTRACT = "https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets"


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _source_bound(
    root: Path,
    *,
    condition_id: str = CONDITION_ID,
    market_activity_lower_bound: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    observed_at: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> tuple[PolymarketPopulationEvidenceBound, Path, frozenset[str]]:
    raw_store = RawArtifactStore(root / "official-contract-raw")
    capture = raw_store.capture(
        b'{"fixture":"official-polymarket-trades-contract"}',
        platform="polymarket",
        source="official-docs/data-api-trades",
        request={
            "method": "GET",
            "url": "https://docs.polymarket.com",
            "parameter_names": [],
            "redacted_parameter_names": [],
            "headers": {},
            "secrets_redacted": False,
            "attempt": 1,
            "public_parameters": {},
        },
        received_at=observed_at,
        response_metadata={
            "status_code": 200,
            "url": "https://docs.polymarket.com",
            "headers": {},
            "secrets_redacted": False,
        },
    )
    metadata_capture = raw_store.capture(
        canonical_json_bytes(
            {
                "id": "123",
                "conditionId": condition_id,
                "acceptingOrdersTimestamp": _timestamp(market_activity_lower_bound),
            }
        ),
        platform="polymarket",
        source="polymarket:source/gamma-market-clock",
        request={"method": "GET", "url": "https://gamma-api.polymarket.com/markets/123", "params": {}},
        received_at=observed_at,
        response_metadata={
            "status_code": 200,
            "url": "https://gamma-api.polymarket.com/markets/123",
            "effective_url_matches_request": True,
            "headers": {},
        },
    )
    bound = PolymarketPopulationEvidenceBound.from_capture(
        condition_id=condition_id,
        gamma_market_id="123",
        official_contract_version="fixture-docs-v1",
        contract_capture=capture,
        market_metadata_capture=metadata_capture,
        official_contract_uri=OFFICIAL_TRADES_CONTRACT,
    )
    path = root / "source-bound.json"
    payload = {
        "schema_version": "polymarket-population-source-bound-input-v1",
        "condition_id": bound.condition_id,
        "gamma_market_id": bound.gamma_market_id,
        "market_activity_lower_bound": _timestamp(bound.market_activity_lower_bound),
        "observed_at": _timestamp(bound.observed_at),
        "official_contract_uri": bound.official_contract_uri,
        "official_contract_version": bound.official_contract_version,
        "official_contract_sha256": bound.official_contract_sha256,
        "contract_capture": {
            "sha256": capture.sha256,
            "byte_length": capture.byte_length,
            "object_path": capture.object_path.relative_to(root).as_posix(),
            "receipt_path": capture.receipt_path.relative_to(root).as_posix(),
            "received_at": _timestamp(capture.received_at),
            "platform": capture.platform,
            "source": capture.source,
        },
        "contract_receipt_id": bound.contract_receipt_id,
        "contract_receipt_sha256": bound.contract_receipt_sha256,
        "market_metadata_sha256": bound.market_metadata_sha256,
        "market_metadata_capture": {
            "sha256": metadata_capture.sha256,
            "byte_length": metadata_capture.byte_length,
            "object_path": metadata_capture.object_path.relative_to(root).as_posix(),
            "receipt_path": metadata_capture.receipt_path.relative_to(root).as_posix(),
            "received_at": _timestamp(metadata_capture.received_at),
            "platform": metadata_capture.platform,
            "source": metadata_capture.source,
        },
        "market_metadata_receipt_id": bound.market_metadata_receipt_id,
        "market_metadata_receipt_sha256": bound.market_metadata_receipt_sha256,
        "market_activity_lower_bound_field": bound.market_activity_lower_bound_field,
    }
    path.write_bytes(canonical_json_bytes(payload))
    approved = frozenset({bound.official_contract_sha256})
    (root / "approved-contracts.json").write_bytes(
        canonical_json_bytes(
            {
                "schema_version": "polymarket-approved-contract-sha256-v1",
                "approved_contract_sha256": sorted(approved),
            }
        )
    )
    return bound, path, approved


class _PopulationManifestFixture:
    def __init__(self, request, *, version: int = 1):
        self.complete = False
        self.coverage_status = "partial"
        self.limitation_reasons = (
            "one_second_side_partition_unavailable",
            "data_api_retention_floor_unknown_or_approximate",
        )
        self.raw_record_count = 2
        self.canonical_record_count = 1
        self.duplicate_record_count = 1
        self.conflict_record_count = 0
        self.raw_sha256 = ("a" * 64,)
        self.leaves = (
            SimpleNamespace(
                status="irreducibly_partial",
                exhausted=False,
                continuation="side-partition-unavailable",
                interval_start=request.interval_start,
                interval_end=request.interval_end,
                retrieved_at=request.interval_end,
                raw_record_count=2,
                raw_sha256=("a" * 64,),
                query_filters=request.query_filters,
            ),
        )
        self.manifest_sha256 = f"fixture-manifest-{version}"
        self._request = request
        self._version = version

    def to_payload(self):
        return {
            "schema_version": "fixture-v1",
            "query_uid": self._request.query_uid,
            "version": self._version,
            "complete": self.complete,
        }


class _PopulationBackfillFixture:
    calls = []
    version = 1

    def __init__(self, connector, normalized_store, *, approved_contract_sha256):
        self.connector = connector
        self.normalized_store = normalized_store
        self.approved_contract_sha256 = approved_contract_sha256

    def collect(self, request):
        type(self).calls.append((self.connector, self.normalized_store, request))
        return SimpleNamespace(
            manifest=_PopulationManifestFixture(request, version=type(self).version),
            storage_write=SimpleNamespace(
                inserted=1,
                duplicates=1,
                conflicts=0,
                rejected=0,
                paths=("normalized/tradefill/fixture.json",),
                quarantine_paths=(),
            ),
        )


class _BudgetExhaustedPopulationBackfillFixture:
    calls = []

    def __init__(self, connector, normalized_store, *, approved_contract_sha256):
        self.connector = connector
        self.normalized_store = normalized_store
        self.approved_contract_sha256 = approved_contract_sha256

    def collect(self, request):
        type(self).calls.append((self.connector, self.normalized_store, request))
        manifest = SimpleNamespace(
            complete=False,
            coverage_status="partial",
            limitation_reasons=(
                "budget_exhausted",
                "data_api_retention_floor_unknown_or_approximate",
            ),
            raw_record_count=0,
            canonical_record_count=0,
            duplicate_record_count=0,
            conflict_record_count=0,
            raw_sha256=(),
            leaves=(
                SimpleNamespace(
                    status="budget_exhausted",
                    exhausted=False,
                    continuation="request-budget-exhausted",
                    interval_start=request.interval_start,
                    interval_end=request.interval_end,
                    retrieved_at=None,
                    raw_record_count=0,
                    raw_sha256=(),
                    query_filters=request.query_filters,
                ),
            ),
            manifest_sha256="budget-exhausted-mechanics-fixture",
            to_payload=lambda: {
                "schema_version": "fixture-v1",
                "query_uid": request.query_uid,
                "budget_exhausted": True,
                "complete": False,
            },
        )
        return SimpleNamespace(
            manifest=manifest,
            storage_write=SimpleNamespace(
                inserted=0,
                duplicates=0,
                conflicts=0,
                rejected=0,
                paths=(),
                quarantine_paths=(),
            ),
        )


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


def test_population_cli_serializes_one_exact_frozen_condition_query(monkeypatch, tmp_path, capsys):
    captured = {}
    _bound, bound_path, approved = _source_bound(
        tmp_path, observed_at=datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    )

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"command": "collect-polymarket-population", "complete": False}

    monkeypatch.setattr(cli_v2_module, "collect_polymarket_population", fake_collect)
    assert main(
        [
            "collect-polymarket-population",
            "--output-dir",
            "population-data",
            "--condition-id",
            CONDITION_ID,
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:05:00Z",
            "--page-size",
            "500",
            "--source-bound",
            str(bound_path),
            "--approved-contract-sha256-file",
            str(tmp_path / "approved-contracts.json"),
            "--max-requests",
            "501",
            "--max-http-attempts",
            "777",
            "--max-leaves",
            "502",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out)["command"] == "collect-polymarket-population"
    assert captured == {
        "output_dir": "population-data",
        "condition_id": CONDITION_ID,
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "end": datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        "source_bound": _bound,
        "approved_contract_sha256": approved,
        "page_size": 500,
        "max_requests": 501,
        "max_http_attempts": 777,
        "max_leaves": 502,
    }


def test_population_collection_persists_canonical_manifest_idempotently(monkeypatch, tmp_path):
    _PopulationBackfillFixture.calls.clear()
    _PopulationBackfillFixture.version = 1
    monkeypatch.setattr(cli_v2_module, "PolymarketPopulationBackfill", _PopulationBackfillFixture)
    output = tmp_path / "population"
    raw_store = RawArtifactStore(output / "raw")
    http = EvidenceHttpClient(raw_store, transport=StubTransport([]), sleep=lambda _: None)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    bound, _bound_path, approved = _source_bound(output, observed_at=end)

    first = collect_polymarket_population(
        output_dir=output,
        condition_id=CONDITION_ID,
        start=start,
        end=end,
        source_bound=bound,
        approved_contract_sha256=approved,
        page_size=1000,
        http_client=http,
    )
    manifest = Path(first["manifest_path"])
    original = manifest.read_bytes()
    replay = collect_polymarket_population(
        output_dir=output,
        condition_id=CONDITION_ID,
        start=start,
        end=end,
        source_bound=bound,
        approved_contract_sha256=approved,
        page_size=1000,
        http_client=http,
    )

    assert first["complete"] is False
    assert first["coverage_status"] == "partial"
    assert first["limitation_reasons"] == [
        "one_second_side_partition_unavailable",
        "data_api_retention_floor_unknown_or_approximate",
    ]
    assert first["coverage"] == {
        "dataset": "public_market_trades",
        "recorded_terminal_leaf_count": 1,
        "recorded_partial_leaf_count": 1,
        "unrecorded_terminal_leaf_count": 0,
        "unrecorded_terminal_leaves": [],
        "complete_leaf_count": 0,
        "data_api_retention_floor": "unknown_or_approximate",
    }
    assert first["query_filters"] == {
        "end": 1767225601,
        "market": CONDITION_ID,
        "start": 1767225600,
        "takerOnly": False,
    }
    assert first["pseudonymous_wallets_only"] is True
    assert first["not_proof_of_fraud"] is True
    assert first["effectiveness_unknown"] is True
    assert first["manifest_file_sha256"] == hashlib.sha256(original).hexdigest()
    assert replay["manifest_preexisting"] is True
    assert manifest.read_bytes() == original
    connector, normalized, request = _PopulationBackfillFixture.calls[0]
    assert isinstance(connector, cli_v2_module.PolymarketConnector)
    assert normalized.root == output
    assert request.condition_id == CONDITION_ID
    assert request.interval_start == start and request.interval_end == end
    assert request.source_bound == bound
    assert request.page_size == 1000
    coverage_rows = CoverageLedger(output / "coverage" / "ledger.jsonl").records(
        platform="polymarket", dataset="public_market_trades"
    )
    assert len(coverage_rows) == 2
    assert all(row.complete is False for row in coverage_rows)
    assert coverage_rows[0].filters == first["query_filters"]
    assert coverage_rows[0].raw_sha256 == ("a" * 64,)
    assert coverage_rows[0].continuation == "side-partition-unavailable"


def test_population_collection_keeps_distinct_capture_manifests_for_same_query(monkeypatch, tmp_path):
    _PopulationBackfillFixture.calls.clear()
    _PopulationBackfillFixture.version = 1
    monkeypatch.setattr(cli_v2_module, "PolymarketPopulationBackfill", _PopulationBackfillFixture)
    output = tmp_path / "population-conflict"
    raw_store = RawArtifactStore(output / "raw")
    http = EvidenceHttpClient(raw_store, transport=StubTransport([]), sleep=lambda _: None)
    bound, _bound_path, approved = _source_bound(
        output, observed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    )
    kwargs = {
        "output_dir": output,
        "condition_id": CONDITION_ID,
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "end": datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        "source_bound": bound,
        "approved_contract_sha256": approved,
        "http_client": http,
    }
    first = collect_polymarket_population(**kwargs)
    original = Path(first["manifest_path"]).read_bytes()
    _PopulationBackfillFixture.version = 2
    replay = collect_polymarket_population(**kwargs)

    assert first["query_uid"] == replay["query_uid"]
    assert first["manifest_sha256"] != replay["manifest_sha256"]
    assert Path(first["manifest_path"]) != Path(replay["manifest_path"])
    assert Path(first["manifest_path"]).read_bytes() == original
    assert Path(replay["manifest_path"]).is_file()


def test_population_collection_refuses_divergent_bytes_at_exact_manifest_path(monkeypatch, tmp_path):
    _PopulationBackfillFixture.calls.clear()
    _PopulationBackfillFixture.version = 1
    monkeypatch.setattr(cli_v2_module, "PolymarketPopulationBackfill", _PopulationBackfillFixture)
    output = tmp_path / "population-manifest-tamper"
    raw_store = RawArtifactStore(output / "raw")
    http = EvidenceHttpClient(raw_store, transport=StubTransport([]), sleep=lambda _: None)
    bound, _bound_path, approved = _source_bound(
        output, observed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    )
    kwargs = {
        "output_dir": output,
        "condition_id": CONDITION_ID,
        "start": datetime(2026, 1, 1, tzinfo=UTC),
        "end": datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        "source_bound": bound,
        "approved_contract_sha256": approved,
        "http_client": http,
    }
    first = collect_polymarket_population(**kwargs)
    manifest_path = Path(first["manifest_path"])
    manifest_path.write_bytes(b"different bytes")

    with pytest.raises(PolymarketPopulationError, match="manifest path already contains different bytes"):
        collect_polymarket_population(**kwargs)

    assert manifest_path.read_bytes() == b"different bytes"


def test_population_manifest_atomic_publication_failure_leaves_no_target_or_pending_file(
    monkeypatch, tmp_path
):
    destination = tmp_path / "population-manifests" / "manifest.json"

    def fail_publication(*_args, **_kwargs):
        raise OSError("simulated atomic publication failure")

    monkeypatch.setattr(cli_v2_module.os, "link", fail_publication)

    with pytest.raises(PolymarketPopulationError, match="could not be atomically published"):
        cli_v2_module._write_immutable_manifest(
            path=destination,
            payload={"schema_version": "fixture-v1", "value": "complete-bytes"},
        )

    assert not destination.exists()
    assert list(destination.parent.glob("*.pending")) == []


def test_population_coverage_uses_terminal_leaf_filters_and_never_completes_an_inconsistent_run(
    tmp_path,
):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    common = {
        "interval_start": start,
        "interval_end": end,
        "retrieved_at": end,
        "raw_record_count": 1,
        "raw_sha256": ("b" * 64,),
    }
    manifest = SimpleNamespace(
        # Deliberately contradictory fixture: a final partial terminal leaf
        # means no row may claim complete coverage despite these root flags.
        complete=True,
        coverage_status="complete",
        limitation_reasons=(),
        leaves=(
            SimpleNamespace(
                **common,
                status="split_required",
                exhausted=False,
                continuation="split",
                query_filters=(("market", CONDITION_ID),),
            ),
            SimpleNamespace(
                **common,
                status="complete",
                exhausted=True,
                continuation=None,
                query_filters=(
                    ("end", int(end.timestamp())),
                    ("market", CONDITION_ID),
                    ("side", "BUY"),
                    ("start", int(start.timestamp())),
                    ("takerOnly", False),
                ),
            ),
            SimpleNamespace(
                **common,
                status="irreducibly_partial",
                exhausted=False,
                continuation="budget-exhausted",
                query_filters=(
                    ("end", int(end.timestamp())),
                    ("market", CONDITION_ID),
                    ("side", "SELL"),
                    ("start", int(start.timestamp())),
                    ("takerOnly", False),
                ),
            ),
        ),
    )

    rows, unrecorded = cli_v2_module._append_population_terminal_coverage(
        CoverageLedger(tmp_path / "coverage" / "ledger.jsonl"), manifest=manifest
    )

    assert len(rows) == 2
    assert unrecorded == ()
    assert all(row.complete is False for row in rows)
    assert rows[0].filters["side"] == "BUY"
    assert rows[1].filters["side"] == "SELL"
    assert rows[1].continuation == "budget-exhausted"
    assert all(row.raw_sha256 == ("b" * 64,) for row in rows)


def test_population_cli_max_request_budget_reports_unrecorded_terminal_interval_without_ledger_row(
    monkeypatch, tmp_path
):
    """Mechanics fixture: an unfetched budget leaf has no invented evidence clock."""

    _BudgetExhaustedPopulationBackfillFixture.calls.clear()
    monkeypatch.setattr(
        cli_v2_module,
        "PolymarketPopulationBackfill",
        _BudgetExhaustedPopulationBackfillFixture,
    )
    output = tmp_path / "population-budget"
    bound, _bound_path, approved = _source_bound(
        output, observed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    )
    http = EvidenceHttpClient(
        RawArtifactStore(output / "raw"), transport=StubTransport([]), sleep=lambda _: None
    )

    payload = collect_polymarket_population(
        output_dir=output,
        condition_id=CONDITION_ID,
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        source_bound=bound,
        approved_contract_sha256=approved,
        max_requests=1,
        http_client=http,
    )

    assert _BudgetExhaustedPopulationBackfillFixture.calls[0][2].max_requests == 1
    assert payload["complete"] is False
    assert payload["coverage_status"] == "partial"
    assert payload["coverage"] == {
        "dataset": "public_market_trades",
        "recorded_terminal_leaf_count": 0,
        "recorded_partial_leaf_count": 0,
        "unrecorded_terminal_leaf_count": 1,
        "unrecorded_terminal_leaves": [
            {
                "interval_start": datetime(2026, 1, 1, tzinfo=UTC),
                "interval_end": datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                "status": "budget_exhausted",
                "continuation": "request-budget-exhausted",
                "query_filters": {
                    "end": 1767225601,
                    "market": CONDITION_ID,
                    "start": 1767225600,
                    "takerOnly": False,
                },
                "reason": "terminal_leaf_has_no_observed_delivery",
            }
        ],
        "complete_leaf_count": 0,
        "data_api_retention_floor": "unknown_or_approximate",
    }
    assert not (output / "coverage" / "ledger.jsonl").exists()


def test_population_collection_rejects_invalid_scope_or_subsecond_time_before_network(tmp_path):
    output = tmp_path / "population-invalid"
    transport = StubTransport([])
    http = EvidenceHttpClient(RawArtifactStore(output / "raw"), transport=transport, sleep=lambda _: None)
    bound, _bound_path, approved = _source_bound(output, observed_at=datetime(2026, 1, 1, tzinfo=UTC))

    with pytest.raises(ValueError, match="0x-prefixed 64-hex"):
        collect_polymarket_population(
            output_dir=output,
            condition_id="condition-1",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 1, tzinfo=UTC),
            source_bound=bound,
            approved_contract_sha256=approved,
            http_client=http,
        )
    with pytest.raises(ValueError, match="whole-second"):
        collect_polymarket_population(
            output_dir=output,
            condition_id=CONDITION_ID,
            start=datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
            end=datetime(2026, 1, 1, tzinfo=UTC),
            source_bound=bound,
            approved_contract_sha256=approved,
            http_client=http,
        )

    assert transport.calls == []
    assert not (output / "population-manifests").exists()


def test_population_cli_returns_nonzero_for_malformed_scope_without_network(tmp_path, capsys):
    _bound, bound_path, _approved = _source_bound(tmp_path, observed_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert main(
        [
            "collect-polymarket-population",
            "--output-dir",
            "unused",
            "--condition-id",
            "not-a-condition-id",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:00:00Z",
            "--source-bound",
            str(bound_path),
            "--approved-contract-sha256-file",
            str(tmp_path / "approved-contracts.json"),
        ]
    ) == 2
    assert "0x-prefixed 64-hex" in capsys.readouterr().err


def test_population_cli_rejects_a_source_bound_that_conflicts_with_its_capture(
    monkeypatch, tmp_path, capsys
):
    _bound, bound_path, _approved = _source_bound(tmp_path, observed_at=datetime(2026, 1, 1, tzinfo=UTC))
    payload = json.loads(bound_path.read_bytes())
    payload["market_activity_lower_bound"] = "2024-01-01T00:00:00.000000Z"
    bound_path.write_bytes(canonical_json_bytes(payload))
    monkeypatch.setattr(
        cli_v2_module,
        "collect_polymarket_population",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("collector must not run")),
    )

    assert main(
        [
            "collect-polymarket-population",
            "--output-dir",
            str(tmp_path / "output"),
            "--condition-id",
            CONDITION_ID,
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:00:00Z",
            "--source-bound",
            str(bound_path),
            "--approved-contract-sha256-file",
            str(tmp_path / "approved-contracts.json"),
        ]
    ) == 2
    assert "conflicts with its raw capture or receipt" in capsys.readouterr().err


def test_population_help_does_not_construct_or_call_network_client(monkeypatch):
    monkeypatch.setattr(
        cli_v2_module,
        "EvidenceHttpClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network client constructed")),
    )
    with pytest.raises(SystemExit, match="0"):
        main(["collect-polymarket-population", "--help"])


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
