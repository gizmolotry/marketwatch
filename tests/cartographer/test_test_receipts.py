"""Executed-test receipt mechanics fixtures; they are not effectiveness evidence."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import jsonschema
import pytest

from repo_cartographer.canonical import canonical_json_bytes, canonical_sha256
from repo_cartographer.cli import scan_repository
from repo_cartographer.domain import ScanConfig
from repo_cartographer.render import write_inventory
from repo_cartographer.test_receipts import LEGACY_RECEIPT_FORMAT, ReceiptAttestationSigner, run_pytest_receipt, verify_test_receipt
from repo_cartographer.test_runner_config import PytestRunnerConfig


SIGNER = ReceiptAttestationSigner("fixture-ci-key", b"receipt-attestation-mechanics-key-0001")
TRUSTED_KEYS = {SIGNER.key_id: b"receipt-attestation-mechanics-key-0001"}


def _write_canonical(path, payload):
    path.write_bytes(canonical_json_bytes(payload) + b"\n")


def _legacy_v1(payload):
    legacy = {key: value for key, value in payload.items() if key != "attestation"}
    legacy["format"] = LEGACY_RECEIPT_FORMAT
    config = legacy["runner"]["config"]
    legacy["runner"] = {
        "config_sha256": legacy["runner"]["config_sha256"],
        "allowed_test_paths": config["allowed_test_paths"],
        "timeout_seconds": config["timeout_seconds"],
        "max_output_bytes": config["max_output_bytes"],
    }
    for declaration in legacy["selection"]["declarations"]:
        declaration.pop("line", None)
        declaration.pop("original_name", None)
    for case in legacy["results"]["cases"]:
        case.pop("line", None)
        case.pop("original_name", None)
    legacy["results"].pop("collected_items", None)
    legacy["receipt_sha256"] = canonical_sha256({key: value for key, value in legacy.items() if key != "receipt_sha256"})
    return legacy


def _git(root, *args):
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _fixture(tmp_path, source: str, *, extra: dict[str, str] | None = None):
    root = tmp_path / "repository"; root.mkdir()
    _git(root, "init", "-b", "main"); _git(root, "config", "user.name", "Receipt Fixture"); _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "tests").mkdir(); (root / "tests" / "test_cases.py").write_text(source, encoding="utf-8")
    for path, content in (extra or {}).items():
        target = root / path; target.parent.mkdir(parents=True, exist_ok=True); target.write_text(content, encoding="utf-8")
    _git(root, "add", "."); _git(root, "commit", "-m", "receipt mechanics fixture")
    inventory = tmp_path / "inventory"
    write_inventory(scan_repository(root, config=ScanConfig(include_untracked=True)), inventory)
    tests = [json.loads(line) for line in (inventory / "tests.jsonl").read_text(encoding="utf-8").splitlines()]
    symbols = {row["symbol_uid"]: row for row in (json.loads(line) for line in (inventory / "symbols.jsonl").read_text(encoding="utf-8").splitlines())}
    uids = {symbols[row["symbol_uid"]]["qualified_name"]: row["test_uid"] for row in tests if row.get("symbol_uid")}
    config = PytestRunnerConfig("repo-cartographer-pytest-runner/v2", ("tests",), 30, 4096, 100, 1_000_000)
    return root, inventory, uids, config


def test_plain_and_all_parametrized_cases_pass_and_receipt_has_no_raw_output_or_environment(tmp_path, monkeypatch):
    source = """import pytest
def test_plain():
    print('RECEIPT_OUTPUT_SECRET')
    assert True
@pytest.mark.parametrize('value', [1, 2])
def test_param(value):
    assert value > 0
"""
    root, inventory, uids, config = _fixture(tmp_path, source)
    monkeypatch.setenv("RECEIPT_ENV_SECRET", "must-not-appear")
    receipt_path = tmp_path / "receipt.json"
    receipt = run_pytest_receipt(inventory, config, receipt_path, repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_plain"], uids["test_param"]])
    schema_path = Path(__file__).parents[2] / "configs" / "cartographer" / "schemas" / "test-receipt-v2.schema.json"
    jsonschema.Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(
        json.loads(receipt_path.read_text(encoding="utf-8"))
    )

    assert receipt["results"]["promotable"] is True
    assert all(row["fully_passed"] for row in receipt["results"]["cases"])
    assert len(next(row for row in receipt["results"]["cases"] if row["test_uid"] == uids["test_param"])["cases"]) == 2
    encoded = receipt_path.read_text(encoding="utf-8")
    assert "RECEIPT_OUTPUT_SECRET" not in encoded and "RECEIPT_ENV_SECRET" not in encoded and "must-not-appear" not in encoded
    assert SIGNER._key.decode("utf-8") not in encoded
    assert receipt["attestation"] == {
        "algorithm": "hmac-sha256",
        "key_id": "fixture-ci-key",
        "signature": receipt["attestation"]["signature"],
    }
    assert "fixture-ci-key" not in receipt["execution"]["argv"]
    verified = verify_test_receipt(receipt_path, inventory, config, TRUSTED_KEYS)
    assert verified["receipt_sha256"] == receipt["receipt_sha256"]
    assert verified.attestation_trusted is True and verified.promotion_authorized is True


def test_recomputed_integrity_hash_cannot_forge_external_attestation(tmp_path):
    root, inventory, uids, config = _fixture(tmp_path, "def test_case():\n    assert True\n")
    receipt_path = tmp_path / "receipt.json"
    payload = run_pytest_receipt(
        inventory,
        config,
        receipt_path,
        repository_root=root,
        attestation_signer=SIGNER,
        test_uids=[uids["test_case"]],
    )
    payload["results"]["exit_code"] = 9
    base = {key: value for key, value in payload.items() if key not in {"receipt_sha256", "attestation"}}
    payload["receipt_sha256"] = canonical_sha256(base)
    _write_canonical(receipt_path, payload)
    with pytest.raises(ValueError, match="signature mismatch"):
        verify_test_receipt(receipt_path, inventory, config, TRUSTED_KEYS)


def test_missing_wrong_unknown_and_altered_attestation_keys_fail_closed(tmp_path):
    root, inventory, uids, config = _fixture(tmp_path, "def test_case():\n    assert True\n")
    receipt_path = tmp_path / "receipt.json"
    payload = run_pytest_receipt(
        inventory,
        config,
        receipt_path,
        repository_root=root,
        attestation_signer=SIGNER,
        test_uids=[uids["test_case"]],
    )
    with pytest.raises(ValueError, match="trusted key resolver"):
        verify_test_receipt(receipt_path, inventory, config)
    with pytest.raises(ValueError, match="not trusted"):
        verify_test_receipt(receipt_path, inventory, config, {"different-key": SIGNER._key})
    with pytest.raises(ValueError, match="signature mismatch"):
        verify_test_receipt(receipt_path, inventory, config, {SIGNER.key_id: b"wrong-attestation-key-material-00001"})

    payload["attestation"]["key_id"] = "altered-key-id"
    _write_canonical(receipt_path, payload)
    with pytest.raises(ValueError, match="not trusted"):
        verify_test_receipt(receipt_path, inventory, config, TRUSTED_KEYS)
    payload["attestation"]["key_id"] = SIGNER.key_id
    payload["attestation"]["signature"] = "0" * 64
    _write_canonical(receipt_path, payload)
    with pytest.raises(ValueError, match="signature mismatch"):
        verify_test_receipt(receipt_path, inventory, config, TRUSTED_KEYS)


def test_legacy_v1_is_explicit_integrity_only_and_never_authorizes_promotion(tmp_path):
    root, inventory, uids, config = _fixture(tmp_path, "def test_case():\n    assert True\n")
    signed_path = tmp_path / "signed.json"
    signed = run_pytest_receipt(
        inventory,
        config,
        signed_path,
        repository_root=root,
        attestation_signer=SIGNER,
        test_uids=[uids["test_case"]],
    )
    legacy_path = tmp_path / "legacy-v1.json"
    _write_canonical(legacy_path, _legacy_v1(signed))
    with pytest.raises(ValueError, match="integrity-only"):
        verify_test_receipt(legacy_path, inventory)
    verified = verify_test_receipt(legacy_path, inventory, allow_legacy_v1=True)
    assert verified.integrity_verified is True
    assert verified.legacy_v1 is True
    assert verified.attestation_trusted is False
    assert verified.promotion_authorized is False


@pytest.mark.parametrize("body", [
    "import pytest\ndef test_case():\n    pytest.skip('skip')\n",
    "import pytest\n@pytest.mark.xfail\ndef test_case():\n    assert False\n",
    "import pytest\n@pytest.mark.xfail\ndef test_case():\n    assert True\n",
    "import pytest\n@pytest.fixture\ndef broken():\n    raise RuntimeError('setup')\ndef test_case(broken):\n    pass\n",
    "import pytest\n@pytest.fixture\ndef broken():\n    yield\n    raise RuntimeError('teardown')\ndef test_case(broken):\n    pass\n",
])
def test_skip_xfail_xpass_setup_and_teardown_never_promote(tmp_path, body):
    root, inventory, uids, config = _fixture(tmp_path, body)
    receipt = run_pytest_receipt(inventory, config, tmp_path / "receipt.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    assert receipt["results"]["promotable"] is False
    assert receipt["results"]["cases"][0]["fully_passed"] is False


def test_partial_parametrization_and_nonzero_exit_cancel_all_promotions(tmp_path):
    source = """import pytest
@pytest.mark.parametrize('value', [1, 0])
def test_param(value):
    assert value > 0
def test_plain():
    assert True
"""
    root, inventory, uids, config = _fixture(tmp_path, source)
    receipt = run_pytest_receipt(inventory, config, tmp_path / "receipt.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_param"], uids["test_plain"]])
    assert receipt["results"]["exit_code"] != 0
    assert not any(row["fully_passed"] for row in receipt["results"]["cases"])


def test_collection_error_is_structured_and_not_promotable(tmp_path):
    source = "import module_that_does_not_exist\ndef test_case():\n    assert True\n"
    root, inventory, uids, config = _fixture(tmp_path, source)
    receipt = run_pytest_receipt(inventory, config, tmp_path / "receipt.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    assert receipt["results"]["collection_errors"]
    assert receipt["results"]["promotable"] is False


def test_source_mismatch_and_receipt_tampering_are_rejected(tmp_path):
    root, inventory, uids, config = _fixture(tmp_path, "def test_case():\n    assert True\n")
    receipt_path = tmp_path / "receipt.json"
    run_pytest_receipt(inventory, config, receipt_path, repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    payload = json.loads(receipt_path.read_text(encoding="utf-8")); payload["results"]["exit_code"] = 9
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_test_receipt(receipt_path, inventory, config, TRUSTED_KEYS)
    (root / "tests" / "test_cases.py").write_text("def test_case():\n    assert False\n", encoding="utf-8")
    with pytest.raises(ValueError, match="stale or mismatched"):
        run_pytest_receipt(inventory, config, tmp_path / "stale.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])


def test_frozen_copy_mutation_and_dirty_stable_repository_are_explicit(tmp_path):
    source = "from pathlib import Path\ndef test_case():\n    Path('state.txt').write_text('changed')\n"
    root, inventory, uids, config = _fixture(tmp_path, source, extra={"state.txt":"original"})
    (root / "dirty-note.txt").write_text("stable dirty fixture", encoding="utf-8")
    # The first frozen inventory remains valid because this new untracked path was not part of it.
    receipt = run_pytest_receipt(inventory, config, tmp_path / "receipt.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    assert receipt["mutation"]["frozen_copy_unchanged"] is False
    assert receipt["mutation"]["changed_paths"] == ["state.txt"]
    assert receipt["results"]["promotable"] is False


def test_dirty_but_byte_stable_inventory_can_run(tmp_path):
    root, _, _, config = _fixture(tmp_path, "def test_case():\n    assert True\n")
    (root / "dirty-note.txt").write_text("included stable dirty fixture", encoding="utf-8")
    inventory = tmp_path / "dirty-inventory"
    write_inventory(scan_repository(root, config=ScanConfig(include_untracked=True)), inventory)
    tests = [json.loads(line) for line in (inventory / "tests.jsonl").read_text(encoding="utf-8").splitlines()]
    symbols = {row["symbol_uid"]: row for row in (json.loads(line) for line in (inventory / "symbols.jsonl").read_text(encoding="utf-8").splitlines())}
    test_uid = next(row["test_uid"] for row in tests if symbols.get(row.get("symbol_uid"), {}).get("qualified_name") == "test_case")
    receipt = run_pytest_receipt(inventory, config, tmp_path / "dirty-receipt.json", repository_root=root, attestation_signer=SIGNER, test_uids=[test_uid])
    assert receipt["results"]["promotable"] is True


def test_dynamically_unmapped_nodeid_is_explicit_and_blocks_promotion(tmp_path):
    root, inventory, uids, config = _fixture(
        tmp_path,
        "def test_case():\n    assert True\n",
        extra={"conftest.py": "def pytest_collection_modifyitems(items):\n    for item in items:\n        item._nodeid += '::dynamic'\n"},
    )
    receipt = run_pytest_receipt(inventory, config, tmp_path / "unmapped.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    assert receipt["results"]["unmapped_nodeids"] == ["tests/test_cases.py::test_case::dynamic"]
    assert receipt["results"]["promotable"] is False


def test_oversized_plugin_event_file_is_not_read_and_blocks_promotion(tmp_path):
    source = "import pytest\n@pytest.mark.parametrize('value', range(100))\ndef test_case(value):\n    assert value >= 0\n"
    root, inventory, uids, _ = _fixture(tmp_path, source)
    bounded = PytestRunnerConfig("repo-cartographer-pytest-runner/v2", ("tests",), 30, 1024, 100, 1_000_000)
    receipt = run_pytest_receipt(inventory, bounded, tmp_path / "overflow.json", repository_root=root, attestation_signer=SIGNER, test_uids=[uids["test_case"]])
    assert receipt["results"]["event_file_overflow"] is True
    assert receipt["results"]["event_file_bytes"] <= bounded.max_output_bytes
    assert receipt["results"]["collection_errors"] == [{"nodeid": "plugin_event_file", "outcome": "overflow"}]
    assert receipt["results"]["promotable"] is False
