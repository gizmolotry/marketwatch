from __future__ import annotations

import json
from pathlib import Path

from marketleak.cli_v3 import main as cli_main
from marketleak.cli_v3 import run_review_case_command


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REAL_REVIEW_CASE_CONFIG = REPOSITORY_ROOT / "configs" / "phase15" / "polymarket_btc65k_review_case.json"
CASE_UID = "review:polymarket-btc65k-cocaptured"
CUTOFF = "2026-07-14T00:38:53.861000Z"


def test_real_recorded_case_is_available_and_abstains() -> None:
    payload = run_review_case_command(
        config_path=REAL_REVIEW_CASE_CONFIG,
        as_of=CUTOFF,
        case_uid=CASE_UID,
    )

    assert payload["command"] == "review-case"
    assert payload["as_of"] == CUTOFF
    assert payload["status"] == "available_precomputed_review_cases"
    assert payload["read_only"] is True
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False
    assert payload["not_proof_of_fraud"] is True
    assert payload["effectiveness_unknown"] is True

    case = payload["case"]
    assert case["case_uid"] == CASE_UID
    assert case["case_kind"] == "recorded_snapshot"
    assert case["as_of"] == CUTOFF
    assert case["coverage"]["overall_status"] == "partial"
    assert case["routing"] == {
        "decision": "abstain_insufficient_evidence",
        "coverage_status": "partial",
        "abstention_reasons": [
            "public_evidence_unavailable",
            "market_mechanics_partial",
        ],
        "mechanism_hypotheses": [],
    }


def test_review_case_command_lists_admitted_cases_when_uid_is_omitted() -> None:
    payload = run_review_case_command(
        config_path=REAL_REVIEW_CASE_CONFIG,
        as_of=CUTOFF,
    )

    assert payload["status"] == "available_precomputed_review_cases"
    assert [case["case_uid"] for case in payload["review_cases"]] == [CASE_UID]
    assert "case" not in payload


def test_real_recorded_case_is_not_admitted_before_publication_cutoff() -> None:
    payload = run_review_case_command(
        config_path=REAL_REVIEW_CASE_CONFIG,
        as_of="2026-07-14T00:38:53.860999Z",
        case_uid=CASE_UID,
    )

    assert payload["status"] == "unavailable_late_configuration"
    assert payload["case"] is None
    assert payload["review_cases"] == []
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False


def test_mismatched_review_case_content_hash_fails_closed(tmp_path: Path) -> None:
    document = json.loads(REAL_REVIEW_CASE_CONFIG.read_text(encoding="utf-8"))
    document["review_cases"][0]["question"] = "Tampered question"
    tampered = tmp_path / "tampered-review-case.json"
    tampered.write_text(json.dumps(document), encoding="utf-8")

    payload = run_review_case_command(
        config_path=tampered,
        as_of=CUTOFF,
        case_uid=CASE_UID,
    )

    assert payload["status"] == "unavailable"
    assert payload["case"] is None
    assert payload["review_cases"] == []
    assert payload["read_only"] is True
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False


def test_review_case_cli_main_returns_nonzero_before_publication_cutoff(capsys) -> None:
    argv = [
        "review-case",
        "--config",
        str(REAL_REVIEW_CASE_CONFIG),
        "--as-of",
        "2026-07-14T00:38:53.860999Z",
        "--case-uid",
        CASE_UID,
    ]

    assert cli_main(argv) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "unavailable_late_configuration"
    assert payload["case"] is None
    assert payload["not_proof_of_fraud"] is True
    assert payload["effectiveness_unknown"] is True
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False


def test_review_case_cli_main_returns_nonzero_for_mismatched_content_hash(
    tmp_path: Path,
    capsys,
) -> None:
    document = json.loads(REAL_REVIEW_CASE_CONFIG.read_text(encoding="utf-8"))
    document["review_cases"][0]["question"] = "Content changed without refreshing its hash"
    mismatched = tmp_path / "mismatched-review-case.json"
    mismatched.write_text(json.dumps(document), encoding="utf-8")
    argv = [
        "review-case",
        "--config",
        str(mismatched),
        "--as-of",
        CUTOFF,
        "--case-uid",
        CASE_UID,
    ]

    assert cli_main(argv) == 2
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "unavailable"
    assert payload["case"] is None
    assert payload["review_cases"] == []
    assert payload["not_proof_of_fraud"] is True
    assert payload["effectiveness_unknown"] is True
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False


def test_review_case_cli_main_emits_stable_safe_json(capsys) -> None:
    argv = [
        "review-case",
        "--config",
        str(REAL_REVIEW_CASE_CONFIG),
        "--as-of",
        CUTOFF,
        "--case-uid",
        CASE_UID,
    ]

    assert cli_main(argv) == 0
    first = capsys.readouterr().out
    assert cli_main(argv) == 0
    second = capsys.readouterr().out
    payload = json.loads(first)

    assert first == second
    assert payload["status"] == "available_precomputed_review_cases"
    assert payload["case"]["case_uid"] == CASE_UID
    assert payload["not_proof_of_fraud"] is True
    assert payload["effectiveness_unknown"] is True
    assert payload["live_fetch"] is False
    assert payload["live_inference"] is False
    encoded = first.lower()
    for forbidden in ("fraud_probability", "identity", "wallet", "insider"):
        assert forbidden not in encoded
