"""Replay the frozen public case bundle; this is not a model-effectiveness test."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from marketleak.forensics import load_wallet_case_bundle
from marketleak.forensics.wallet_case_bundle import WalletCaseBundleError, _coverage_record


BUNDLE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "datasets"
    / "wallet-cases"
    / "van-dyke-burdensome-mix"
)


def test_frozen_van_dyke_bundle_replays_exact_hindsight_metrics_and_hashes() -> None:
    bundle = load_wallet_case_bundle(BUNDLE_ROOT)
    report = bundle.replay()

    assert bundle.bundle_uid == "wallet-case-bundle:cftc-doj-van-dyke-burdensome-mix-2026:v1"
    assert bundle.captured_record_count == 15
    assert bundle.coverage.record_count == 15
    assert bundle.coverage.query_uid == (
        "polymarket:wallet-replay-query/"
        "5fe7fa2a0956d3d3dfb6b473b1e8e68b6c8a4a311fc46f85ee0c8efda8e9d48e"
    )
    assert bundle.coverage_sha256 == "7deea086972389aad2d8761fc765740aa0d0daca93ed675dda57bff68b8a23f2"
    assert bundle.fills_sha256 == "2249a1b29ee32b776a76e569876184c84a3f7b740d3e84e2d3b45ff841fdb558"
    assert bundle.coverage.raw_sha256 == (
        "9eac0ab947a8591575a96343eb4ee9141ee572986f425e34b98d353381460d70",
    )
    assert bundle.replay_report_file_sha256 == "a00dac14e3accd353e497e4fd16777202cc0beff8db8792c2d02603ff4d7dd95"
    assert report.canonical_bytes() == bundle.frozen_replay_report_bytes
    assert report.canonical_bytes() == (BUNDLE_ROOT / "replay-report.json").read_bytes()

    assert report.replay_mode == "hindsight_reconstructed"
    assert report.reconstruction_status == "descriptive_complete"
    assert report.abstention_reasons == ()
    assert len(report.fill_lineage) == 13
    assert report.excluded_counts.post_cutoff_event == 2
    assert report.prospective.eligible is False
    assert report.prospective.prospectively_available_fill_count == 0
    assert report.prospective.reason_codes == (
        "source_retrieved_after_historical_cutoff",
        "pre_cutoff_events_ingested_after_historical_cutoff",
    )

    assert report.metrics is not None
    assert report.metrics.to_payload() == {
        "observed_fill_count": 13,
        "observed_market_count": 4,
        "observed_outcome_count": 4,
        "observed_buy_fill_count": 13,
        "observed_sell_fill_count": 0,
        "observed_gross_notional": "33934.3399936488214660",
        "directional_concentration": "0.9588617313218291992844203558",
        "dominant_market_notional_share": "0.9588617313218291992844203558",
        "first_observed_event_time": "2025-12-27T05:05:41.000000Z",
        "last_observed_event_time": "2026-01-03T02:58:25.000000Z",
    }
    assert report.analysis_input_sha256 == "a656da1bf7f21d6a95e5f031843275cd1f401e06989f2159ac7de12d4b9619bb"
    assert report.report_sha256 == "861f677d45e7c9bc8ddaa82461eaf14480e3538301907ee968bc339425a7f588"


def test_coverage_loader_rejects_string_boolean_from_malformed_bundle_fixture(tmp_path: Path) -> None:
    # This mechanics fixture is the frozen public-case coverage row with only
    # the JSON type of `complete` malformed; it is not evidence or training data.
    source = BUNDLE_ROOT / "coverage" / "ledger.jsonl"
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["complete"] = "false"
    malformed = tmp_path / "coverage.jsonl"
    malformed.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(WalletCaseBundleError, match="complete must be a JSON boolean"):
        _coverage_record(malformed)
