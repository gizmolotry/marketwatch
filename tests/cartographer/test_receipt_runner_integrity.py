"""Receipt runner-integrity mechanics fixtures only; never effectiveness evidence."""

from __future__ import annotations

import json
import subprocess

import pytest

import repo_cartographer.test_receipts as receipt_module
from repo_cartographer.cli import scan_repository
from repo_cartographer.domain import ScanConfig
from repo_cartographer.render import write_inventory
from repo_cartographer.test_receipts import ReceiptAttestationSigner, run_pytest_receipt
from repo_cartographer.test_runner_config import PytestRunnerConfig


SIGNER = ReceiptAttestationSigner("fixture-runner-integrity", b"runner-integrity-mechanics-key-0001")


def _git(root, *args):
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _fixture(tmp_path, source: str, *, extra: dict[str, str] | None = None):
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Runner Integrity Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    test_path = root / "tests" / "test_cases.py"
    test_path.parent.mkdir()
    test_path.write_text(source, encoding="utf-8")
    (root / "README.md").write_text("Runner integrity mechanics fixture.\n", encoding="utf-8")
    for path, content in (extra or {}).items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "runner integrity mechanics fixture")
    inventory = tmp_path / "inventory"
    write_inventory(scan_repository(root, config=ScanConfig(include_untracked=True)), inventory)
    files = [json.loads(line) for line in (inventory / "files.jsonl").read_text(encoding="utf-8").splitlines()]
    return root, inventory, files


def _config(*, max_source_files: int = 100, max_source_bytes: int = 1_000_000):
    return PytestRunnerConfig(
        "repo-cartographer-pytest-runner/v2",
        ("tests",),
        30,
        4096,
        max_source_files,
        max_source_bytes,
    )


def test_shadowed_duplicate_declaration_never_receives_credit_from_the_live_definition(tmp_path):
    root, inventory, _ = _fixture(
        tmp_path,
        "def test_shadowed():\n    assert False\n\ndef test_shadowed():\n    assert True\n",
    )
    receipt = run_pytest_receipt(
        inventory,
        _config(),
        tmp_path / "receipt.json",
        repository_root=root,
        attestation_signer=SIGNER,
        test_paths=["tests"],
    )

    assert receipt["results"]["promotable"] is False
    cases = sorted(receipt["results"]["cases"], key=lambda row: row["line"])
    assert [row["line"] for row in cases] == [1, 4]
    assert cases[0]["cases"] == []
    assert cases[0]["fully_passed"] is False
    assert len(cases[1]["cases"]) == 1
    assert receipt["results"]["collected_items"][0]["definition_line"] == 4


def test_parametrized_class_method_cases_join_once_to_the_definition_line(tmp_path):
    root, inventory, _ = _fixture(
        tmp_path,
        "import pytest\n\nclass TestCases:\n    @pytest.mark.parametrize('value', [1, 2])\n    def test_method(self, value):\n        assert value > 0\n",
    )
    receipt = run_pytest_receipt(
        inventory,
        _config(),
        tmp_path / "receipt.json",
        repository_root=root,
        attestation_signer=SIGNER,
        test_paths=["tests"],
    )

    assert receipt["results"]["promotable"] is True
    assert len(receipt["results"]["cases"]) == 1
    assert len(receipt["results"]["cases"][0]["cases"]) == 2
    assert {row["line"] for row in receipt["results"]["collected_items"]} == {4}
    assert {row["definition_line"] for row in receipt["results"]["collected_items"]} == {5}
    assert {row["original_name"] for row in receipt["results"]["collected_items"]} == {"test_method"}


def _forbid_execution_or_copy(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("preflight bound must fail before copy or process execution")

    monkeypatch.setattr(receipt_module.shutil, "copyfile", forbidden)
    monkeypatch.setattr(receipt_module, "run_process_tree", forbidden)


def test_source_file_count_bound_fails_before_copy_process_or_receipt(tmp_path, monkeypatch):
    root, inventory, files = _fixture(tmp_path, "def test_case():\n    assert True\n")
    eligible = [row for row in files if not row.get("sensitive") and row.get("tracked_state") != "deleted"]
    _forbid_execution_or_copy(monkeypatch)
    output = tmp_path / "receipt.json"

    with pytest.raises(ValueError, match="file-count bound"):
        run_pytest_receipt(
            inventory,
            _config(max_source_files=len(eligible) - 1),
            output,
            repository_root=root,
            attestation_signer=SIGNER,
            test_paths=["tests"],
        )
    assert not output.exists()


def test_aggregate_source_byte_bound_fails_before_copy_process_or_receipt(tmp_path, monkeypatch):
    root, inventory, files = _fixture(
        tmp_path,
        "def test_case():\n    assert True\n",
        extra={"bounds-fixture.txt": "x" * 2048},
    )
    eligible = [row for row in files if not row.get("sensitive") and row.get("tracked_state") != "deleted"]
    total_bytes = sum(row["byte_size"] for row in eligible)
    assert total_bytes > 1024
    _forbid_execution_or_copy(monkeypatch)
    output = tmp_path / "receipt.json"

    with pytest.raises(ValueError, match="byte bound"):
        run_pytest_receipt(
            inventory,
            _config(max_source_files=len(eligible), max_source_bytes=total_bytes - 1),
            output,
            repository_root=root,
            attestation_signer=SIGNER,
            test_paths=["tests"],
        )
    assert not output.exists()
