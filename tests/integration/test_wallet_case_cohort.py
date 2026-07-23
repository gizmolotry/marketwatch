"""Frozen same-market peer replay for the public Van Dyke case bundle.

The temporary raw roots are deliberately not versioned.  When present they let
this integration test verify byte-for-byte regeneration from real captured
deliveries; the checked-in report still has a schema/claim-boundary test when
those local captures have been cleaned up.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from marketleak.forensics.wallet_case_cohort import (
    CANDIDATE_ACTOR_UID,
    EVENT_CUTOFF,
    WalletCaseCohortReplayError,
    build_wallet_case_cohort_replay,
)
from marketleak.ingestion.normalize import canonical_json_bytes
from hashlib import sha256


ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "datasets" / "wallet-cases" / "van-dyke-burdensome-mix"
REPORT = BUNDLE / "cohort-replay-report.json"
HEAD = Path(
    r"C:\Users\Andrew\AppData\Local\Temp\marketleak-van-dyke-market-cohort-probe-7a214b8150ac41c9a2b9bc0b6a4653e0"
)
TAIL = Path(
    r"C:\Users\Andrew\AppData\Local\Temp\marketleak-van-dyke-event-cohort-tail-b1651c0f8dc146d5b6b4dfd7ef7b2aec"
)


def test_checked_in_cohort_report_leads_with_the_historical_signal() -> None:
    payload = json.loads(REPORT.read_text(encoding="utf-8"))

    unsigned = dict(payload)
    report_hash = unsigned.pop("report_sha256")
    assert sha256(canonical_json_bytes(unsigned)).hexdigest() == report_hash

    assert payload["schema_version"] == "wallet-case-cohort-replay-v1"
    assert payload["replay_mode"] == "hindsight_reconstructed"
    assert payload["training_eligible"] is False
    assert "effectiveness_unknown" not in payload
    assert "scores_are_probabilities" not in payload
    assert "limitations" not in payload
    assert "decision_boundary" not in payload
    assert payload["candidate_actor_uid"] == CANDIDATE_ACTOR_UID
    assert payload["event_cutoff"] == "2026-01-03T09:20:59.000000Z"
    assert payload["population"]["raw_input_count"] == 22156
    assert payload["population"]["canonical_fill_count"] == 21785
    assert payload["population"]["exact_duplicate_count"] == 371
    assert payload["population"]["conflict_count"] == 0
    assert payload["population"]["wallet_count"] == 3449
    assert payload["descriptive_ranks"]["focus_outcome_buy_notional"]["rank"] == 7
    assert payload["descriptive_ranks"]["focus_outcome_buy_notional"]["population_size"] == 1339
    assert payload["descriptive_ranks"]["gross_market_notional"]["rank"] == 32
    assert payload["descriptive_ranks"]["gross_market_notional"]["population_size"] == 3449
    assert payload["signal_assessment"] == {
        "classification": "high",
        "review_priority": "high",
        "confidence": "high",
        "coverage": "complete_same_market_population",
        "composite_population_rank": 33,
        "composite_population_size": 3449,
        "focus_yes_buy_notional_rank": payload["descriptive_ranks"]["focus_outcome_buy_notional"],
        "gross_market_notional_rank": payload["descriptive_ranks"]["gross_market_notional"],
        "summary": "High-priority wallet activity signal in the frozen same-market cohort.",
        "decision_owner": "human_reviewer",
    }
    assert payload["cohort_ranking"]["prospective_eligible"] is False
    assert payload["cohort_ranking"]["operational_review_priority"] is None
    assert payload["cohort_ranking"]["status"] == "available"
    assert payload["cohort_ranking"]["abstention_reasons"] == []
    assert payload["cohort_ranking"]["signal_assessment"]["classification"] == "high"
    for noisy_key in ("effectiveness_unknown", "not_training", "human_review_required", "limitations"):
        assert noisy_key not in payload["cohort_ranking"]
    assert "decision_boundary" not in payload["cohort_ranking"]
    assert payload["cohort_ranking"]["policy"]["focus_published_at"] is None
    assert payload["focus_clock_basis"]["publication_status"] == (
        "unmapped_source_deliveries_contain_no_market_publication_timestamp"
    )
    cohort_unsigned = dict(payload["cohort_ranking"])
    cohort_hash = cohort_unsigned.pop("report_sha256")
    assert sha256(canonical_json_bytes(cohort_unsigned)).hexdigest() == cohort_hash

    coverage = payload["population"]["coverage"]
    assert coverage["dataset"] == "public_market_trades"
    assert coverage["raw_sha256"] == [
        "2844c24c01a676e7672b01268477161d8a9bc455fef960135206bb92e7b9f75a",
        "3ecb05d539eb07c903098b2c8d0554710004df62e70dd2b5866e0af47d6ca3ed",
        "d820a38bef0e7c96ee39a3b7de2796dea259d97de134e49c1760eea8bab313c7",
    ]
    assert [
        (
            item["raw_sha256"],
            item["receipt_sha256"],
            item["raw_byte_length"],
            item["raw_record_count"],
            item["request"]["params"]["offset"],
            item["request"]["params"]["start"],
            item["request"]["params"]["end"],
        )
        for item in payload["source_deliveries"]
    ] == [
        (
            "3ecb05d539eb07c903098b2c8d0554710004df62e70dd2b5866e0af47d6ca3ed",
            "f4f7acfc31dfef6176a597b807ea10e6fff69e1697b45f22ee600bd01b686452",
            7754586,
            10000,
            0,
            1,
            1767409105,
        ),
        (
            "d820a38bef0e7c96ee39a3b7de2796dea259d97de134e49c1760eea8bab313c7",
            "c46b85612d891b6f7330bfeb05cde25eead2cf4e9b24d03040e8e92b56ceb323",
            2111088,
            2679,
            10000,
            1,
            1767409105,
        ),
        (
            "2844c24c01a676e7672b01268477161d8a9bc455fef960135206bb92e7b9f75a",
            "fa39cc61b1d710abad9932f91d2f9e4e4b90c494b1771e59fdefe06a0d39ddf6",
            7403691,
            9477,
            0,
            1767409106,
            1767432059,
        ),
    ]
    assert all(item["request"]["method"] == "GET" for item in payload["source_deliveries"])
    assert all(item["request"]["url"] == "https://data-api.polymarket.com/trades" for item in payload["source_deliveries"])
    assert all(item["request"]["params"]["takerOnly"] is False for item in payload["source_deliveries"])
    slices = coverage["slices"]
    assert [(item["raw_record_count"], item["canonical_record_count"], item["duplicate_record_count"], item["conflict_record_count"]) for item in slices] == [
        (12679, 12528, 151, 0),
        (9477, 9257, 220, 0),
    ]
    assert slices[0]["interval_end"] == "2026-01-03T02:58:25.000000Z"
    assert slices[1]["interval_start"] == "2026-01-03T02:58:26.000000Z"
    candidate = next(item for item in payload["cohort_ranking"]["rows"] if item["actor_uid"] == CANDIDATE_ACTOR_UID)
    assert candidate["status"] == "available"
    assert candidate["abstention_reasons"] == []
    assert candidate["signal_classification"] == "high"
    assert candidate["review_priority"] == "high"
    assert "scores_are_probabilities" not in candidate
    assert all("is_misconduct_probability" not in item for item in candidate["feature_surprises"])
    assert candidate["population_rank"] == 33
    assert candidate["population_size"] == 3449
    assert candidate["hindsight_peer_rank"] == 33
    assert candidate["novelty_composite"] is not None
    assert candidate["operational_review_rank"] is None
    assert candidate["statistical_rank"] == 1


@pytest.mark.skipif(not (HEAD.is_dir() and TAIL.is_dir()), reason="frozen local raw deliveries are unavailable")
def test_real_captured_deliveries_regenerate_the_checked_in_cohort_report() -> None:
    replay = build_wallet_case_cohort_replay(HEAD, TAIL)

    assert replay.event_cutoff == EVENT_CUTOFF
    assert replay.raw_input_count == 22156
    assert replay.canonical_fill_count == 21785
    assert replay.exact_duplicate_count == 371
    assert replay.conflict_count == 0
    assert replay.wallet_count == 3449
    assert replay.focus_outcome_buy_notional_rank["rank"] == 7
    assert replay.focus_outcome_buy_notional_rank["population_size"] == 1339
    assert replay.gross_market_notional_rank["rank"] == 32
    assert replay.gross_market_notional_rank["population_size"] == 3449
    assert replay.report.policy.analysis_mode == "hindsight_reconstructed"
    assert replay.report.status == "available"
    assert replay.report.policy.focus_published_at is None
    assert replay.report.abstention_reasons == ()
    candidate = replay.report.rows[0]
    assert candidate.signal_classification == "high"
    assert candidate.population_rank == 33
    assert candidate.population_size == 3449
    assert candidate.hindsight_peer_rank == 33
    assert replay.report.prospective_eligible is False
    assert replay.canonical_bytes() == REPORT.read_bytes()


@pytest.mark.skipif(not HEAD.is_dir(), reason="frozen local raw deliveries are unavailable")
def test_replay_fails_closed_when_a_receipt_contract_is_tampered(tmp_path: Path) -> None:
    # This only copies the receipt, not market facts; it is a mechanics-only
    # fixture for raw/receipt binding rather than synthetic evidence.
    copied = tmp_path / "head"
    (copied / "raw").mkdir(parents=True)
    for source in (HEAD / "raw").iterdir():
        target = copied / "raw" / source.name
        if source.is_dir():
            import shutil

            shutil.copytree(source, target)
    receipt = next((copied / "raw" / "receipts").rglob("*.json"))
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["request"]["params"]["takerOnly"] = True
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(WalletCaseCohortReplayError, match="receipt contract mismatch"):
        build_wallet_case_cohort_replay(copied, TAIL)
