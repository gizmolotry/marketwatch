from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

import marketleak.onchain.bitcoin_context as bitcoin_context
from marketleak.cli_v3 import build_parser, run_bitcoin_context_collection_command
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.onchain.bitcoin_context import (
    BitcoinAddressContextCollector,
    BitcoinAddressFormatError,
    BitcoinWatchRegistration,
    MempoolHttpResponse,
    collect_bitcoin_context_once,
    validate_bitcoin_mainnet_address,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


def transaction(*, txid: str, confirmed: bool) -> dict[str, object]:
    status: dict[str, object] = {"confirmed": confirmed}
    if confirmed:
        status.update({"block_height": 800_000, "block_time": int((T0 - timedelta(days=1)).timestamp())})
    return {
        "txid": txid,
        "version": 2,
        "locktime": 0,
        "size": 100,
        "weight": 400,
        "fee": 500,
        "status": status,
    }


def address_summary() -> dict[str, object]:
    stats = {
        "tx_count": 4,
        "funded_txo_count": 5,
        "funded_txo_sum": 100_000,
        "spent_txo_count": 2,
        "spent_txo_sum": 40_000,
    }
    return {"address": ADDRESS, "chain_stats": stats, "mempool_stats": {**stats, "tx_count": 1}}


class FakeTransport:
    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = {key: json.dumps(value).encode("utf-8") for key, value in routes.items()}
        self.requests: list[str] = []

    def get(self, url: str, *, timeout: float, headers):
        self.requests.append(url)
        for suffix, body in self.routes.items():
            if url.endswith(suffix):
                return MempoolHttpResponse(status_code=200, body=body, headers={"content-type": "application/json"}, url=url)
        return MempoolHttpResponse(status_code=404, body=b'{"error":"not found"}', headers={}, url=url)


def registration() -> BitcoinWatchRegistration:
    return BitcoinWatchRegistration(
        registration_uid="bitcoin:watch/demo",
        address=ADDRESS,
        registered_at=T0 - timedelta(days=2),
    )


def routes(*, chain: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        f"/address/{ADDRESS}": address_summary(),
        "/utxo": [{"txid": "c" * 64, "vout": 0, "value": 60_000, "status": {"confirmed": True, "block_height": 800_000, "block_time": int((T0 - timedelta(days=1)).timestamp())}}],
        "/txs/mempool": [transaction(txid="b" * 64, confirmed=False)],
        "/txs/chain": chain if chain is not None else [transaction(txid="a" * 64, confirmed=True)],
    }


def collector(tmp_path, transport: FakeTransport, **kwargs) -> BitcoinAddressContextCollector:
    return BitcoinAddressContextCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        clock=lambda: T0,
        **kwargs,
    )


def test_raw_capture_and_receipt_happen_before_json_parsing(tmp_path, monkeypatch):
    ordering: list[str] = []

    class RecordingStore(RawArtifactStore):
        def capture(self, *args, **kwargs):
            ordering.append("capture")
            return super().capture(*args, **kwargs)

    original_loads = bitcoin_context.json.loads

    def checked_loads(*args, **kwargs):
        assert ordering, "JSON parsing ran before the raw capture"
        ordering.append("parse")
        return original_loads(*args, **kwargs)

    monkeypatch.setattr(bitcoin_context.json, "loads", checked_loads)
    value = BitcoinAddressContextCollector(
        raw_store=RecordingStore(tmp_path / "raw"),
        transport=FakeTransport(routes()),
        clock=lambda: T0,
    )

    snapshot = value.collect([registration()])

    assert ordering[0] == "capture"
    assert ordering.count("capture") == 4
    assert ordering.count("parse") == 4
    assert all(PathLike.exists(item.storage_uri) for item in snapshot.raw_artifacts)


class PathLike:
    @staticmethod
    def exists(uri: str) -> bool:
        from pathlib import Path
        from urllib.parse import urlparse, unquote

        return Path(unquote(urlparse(uri).path.lstrip("/"))).exists() if uri.startswith("file:///") else False


def test_confirmed_transaction_pagination_stops_at_configured_bound(tmp_path):
    first_page = [transaction(txid=f"{index:064x}", confirmed=True) for index in range(25)]
    second_cursor = first_page[-1]["txid"]
    mapping = routes(chain=first_page)
    mapping[f"/txs/chain/{second_cursor}"] = [transaction(txid="d" * 64, confirmed=True)]
    transport = FakeTransport(mapping)

    snapshot = collector(tmp_path, transport, max_confirmed_pages=2).collect([registration()])

    confirmed_urls = [url for url in transport.requests if "/txs/chain" in url]
    assert len(confirmed_urls) == 2
    assert confirmed_urls[-1].endswith(f"/txs/chain/{second_cursor}")
    confirmed_coverage = next(item for item in snapshot.coverage if item.endpoint == "confirmed_txs")
    assert confirmed_coverage.pages_fetched == 2
    assert "confirmed_page_bound_reached" not in confirmed_coverage.reason_codes


def test_invalid_address_is_rejected_locally_without_transport(tmp_path):
    with pytest.raises(BitcoinAddressFormatError):
        validate_bitcoin_mainnet_address("bc1not-a-valid-bitcoin-address")
    with pytest.raises(ValueError):
        BitcoinWatchRegistration(
            registration_uid="bitcoin:watch/invalid",
            address="bc1not-a-valid-bitcoin-address",
            registered_at=T0,
        )

    transport = FakeTransport({})
    with pytest.raises(TypeError):
        collector(tmp_path, transport).collect([{"registration_uid": "bitcoin:watch/nope"}])  # type: ignore[list-item]
    assert transport.requests == []


def test_neutral_facts_keep_confirmed_and_mempool_state_with_full_lineage(tmp_path):
    snapshot = collector(tmp_path, FakeTransport(routes())).collect([registration()])

    assert len(snapshot.summaries) == 1
    assert len(snapshot.utxos) == 1
    assert {item.state for item in snapshot.timeline} == {"confirmed", "mempool"}
    for fact in (*snapshot.summaries, *snapshot.utxos, *snapshot.timeline):
        assert fact.observed_at <= fact.retrieved_at <= fact.as_of
        assert fact.source_uid == "bitcoin:source/mempool-address-rest"
        assert fact.raw_artifact_uid.startswith("bitcoin:raw/")
        assert fact.parser_version.startswith("bitcoin-address-context-")
        assert len(fact.raw_content_sha256) == 64
        assert not hasattr(fact, "actor_uid")


def test_local_watchlist_collection_writes_snapshot_without_a_network_default(tmp_path):
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text(json.dumps({"registrations": [registration().model_dump(mode="json")]}), encoding="utf-8")
    output = tmp_path / "out"

    snapshot = collect_bitcoin_context_once(
        watchlist_path=watchlist,
        output_dir=output,
        transport=FakeTransport(routes()),
        clock=lambda: T0,
    )

    assert snapshot.snapshot_uid.startswith("bitcoin:context-snapshot/")
    assert (output / "bitcoin-context-snapshot.json").is_file()
    assert (output / "raw" / "receipts").is_dir()


def test_cli_contract_uses_watchlist_only_and_stdout_avoids_addresses(tmp_path, monkeypatch):
    watchlist = tmp_path / "watchlist.json"
    watchlist.write_text(json.dumps({"registrations": [registration().model_dump(mode="json")]}), encoding="utf-8")
    output = tmp_path / "out"

    def fake_collect(**kwargs):
        return collect_bitcoin_context_once(
            watchlist_path=kwargs["watchlist_path"],
            output_dir=kwargs["output_dir"],
            transport=FakeTransport(routes()),
            clock=lambda: T0,
            max_confirmed_pages=kwargs["max_confirmed_pages"],
            max_items_per_response=kwargs["max_items_per_response"],
            max_timeline_entries=kwargs["max_timeline_entries"],
            timeout_seconds=kwargs["timeout_seconds"],
        )

    monkeypatch.setattr("marketleak.cli_v3.collect_bitcoin_context_once", fake_collect)
    result = run_bitcoin_context_collection_command(
        watchlist_path=watchlist,
        output_dir=output,
        max_confirmed_pages=1,
        max_items_per_response=100,
        max_timeline_entries=100,
        timeout_seconds=5,
    )

    assert ADDRESS not in json.dumps(result)
    assert result["registrations_count"] == 1
    with pytest.raises(SystemExit):
        build_parser().parse_args([
            "collect-bitcoin-context", "--watchlist", str(watchlist), "--output-dir", str(output), "--address", ADDRESS
        ])
