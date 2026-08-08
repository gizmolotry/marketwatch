"""Integrity mechanics fixtures; hashes do not establish approval or effectiveness."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import subprocess

import pytest

from repo_cartographer.cli import explain_inventory, scan_repository
from repo_cartographer.domain import ScanConfig
from repo_cartographer.verify import verify_inventory


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _scan(tmp_path):
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "worker.py").write_text("def work():\n    return 'bounded'\n", encoding="utf-8")
    (root / "README.md").write_text("Verifier ordering mechanics fixture.\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture repository")
    output = tmp_path / "inventory"
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False), output_dir=output)
    return root, output, inventory


def test_verify_checks_hashes_counts_roots_and_references(tmp_path):
    root, output, inventory = _scan(tmp_path)
    (root / "second.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "second.py")
    _git(root, "commit", "-m", "second fixture file")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False), output_dir=output)

    result = verify_inventory(output)

    assert result.ok is True
    assert result.errors == ()
    assert result.counts["files"] == len(inventory.files)
    assert result.counts["symbols"] == len(inventory.symbols)


def test_verify_detects_jsonl_tampering(tmp_path):
    _, output, _ = _scan(tmp_path)
    with (output / "files.jsonl").open("ab") as handle:
        handle.write(b"{}\n")

    result = verify_inventory(output)

    assert result.ok is False
    assert any("files.jsonl: hash mismatch" in error for error in result.errors)
    assert any("files.jsonl: count mismatch" in error for error in result.errors)


def test_verify_rejects_noncanonical_jsonl_even_if_manifest_hashes_are_not_consulted(tmp_path):
    _, output, _ = _scan(tmp_path)
    path = output / "files.jsonl"
    records = path.read_text(encoding="utf-8").splitlines()
    assert len(records) >= 2
    path.write_text("\n".join(reversed(records)) + "\n", encoding="utf-8")

    result = verify_inventory(output)

    assert result.ok is False
    assert any("not canonical and UID-sorted" in error for error in result.errors)


def test_explain_uses_only_verified_rendered_output(tmp_path):
    root, output, inventory = _scan(tmp_path)
    capability = inventory.capabilities[0]
    root.rename(tmp_path / "repository-unavailable")

    explanation = explain_inventory(output, capability.capability_uid)

    assert explanation["capability"]["capability_uid"] == capability.capability_uid
    assert explanation["components"]

    with (output / "components.jsonl").open("ab") as handle:
        handle.write(b"{}\n")
    with pytest.raises(ValueError, match="inventory verification failed"):
        explain_inventory(output, capability.capability_uid)


def test_verify_rejects_evidence_whose_source_hash_disagrees_with_file(tmp_path):
    _, output, inventory = _scan(tmp_path)
    evidence = inventory.evidence[0]
    corrupted = replace(evidence, source_sha256="b" * 64)
    from repo_cartographer.render import write_inventory

    write_inventory(replace(inventory, evidence=(corrupted, *inventory.evidence[1:])), output)

    result = verify_inventory(output)
    assert result.ok is False
    assert any("evidence.source_sha256" in error for error in result.errors)


def test_verify_accepts_explicit_good_and_conflicting_artifact_bindings(tmp_path):
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    model_bytes = b"opaque model fixture bytes\n"
    (root / "model.pt").write_bytes(model_bytes)
    digest = hashlib.sha256(model_bytes).hexdigest()
    (root / "manifest-good.json").write_text(
        json.dumps({"files": [{"path": "model.pt", "sha256": digest}]}),
        encoding="utf-8",
    )
    (root / "manifest-bad.json").write_text(
        json.dumps({"files": [{"path": "model.pt", "sha256": "0" * 64}]}),
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "artifact binding fixtures")
    output = tmp_path / "inventory"

    inventory = scan_repository(root, config=ScanConfig(include_untracked=False), output_dir=output)
    target = next(item for item in inventory.artifacts if item.file_uid == next(file.file_uid for file in inventory.files if file.path == "model.pt"))

    assert "artifact_manifest_hash_conflict" in target.reason_codes
    assert verify_inventory(output).ok is True
