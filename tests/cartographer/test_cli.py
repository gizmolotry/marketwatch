"""CLI mechanics fixtures; scans are static and do not run target repository code."""

from __future__ import annotations

import json
import subprocess

import pytest

import repo_cartographer.cli as cartographer_cli
from repo_cartographer.cli import EXIT_FAILED, EXIT_OK, main
from repo_cartographer.domain import ScanConfig
from repo_cartographer.snapshot import SnapshotError


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _repository(root):
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "cli.py").write_text(
        "import argparse\n\ndef main():\n    parser = argparse.ArgumentParser()\n    return parser.parse_args()\n",
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture repository")
    return root


def test_cli_scan_verify_and_explain_exit_codes(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    output = tmp_path / "inventory"

    assert main(["scan", str(root), "--output", str(output)]) == EXIT_OK
    scan_payload = json.loads(capsys.readouterr().out)
    assert len(scan_payload["root_sha256"]) == 64

    assert main(["verify", str(output)]) == EXIT_OK
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["ok"] is True

    capability = json.loads((output / "capabilities.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert main(["explain", str(output), capability["capability_uid"]]) == EXIT_OK
    explanation = json.loads(capsys.readouterr().out)
    assert explanation["capability"]["capability_uid"] == capability["capability_uid"]

    with (output / "symbols.jsonl").open("ab") as handle:
        handle.write(b"{}\n")
    assert main(["verify", str(output)]) == EXIT_FAILED
    assert json.loads(capsys.readouterr().err)["ok"] is False


def test_cli_reports_missing_capability_as_failure(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    output = tmp_path / "inventory"
    assert main(["scan", str(root), "--output", str(output)]) == EXIT_OK
    capsys.readouterr()

    assert main(["explain", str(output), "capability:missing"]) == EXIT_FAILED
    assert "capability not found" in capsys.readouterr().err


def test_cli_accepts_root_option_and_include_untracked_override(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    (root / "untracked.py").write_text("VALUE = 1\n", encoding="utf-8")
    output = tmp_path / "inventory"

    assert main([
        "scan",
        "--root",
        str(root),
        "--output",
        str(output),
        "--include-untracked",
    ]) == EXIT_OK
    capsys.readouterr()

    paths = {
        json.loads(line)["path"]
        for line in (output / "files.jsonl").read_text(encoding="utf-8").splitlines()
    }
    assert "untracked.py" in paths


def test_cli_accepts_inventory_and_capability_options(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    output = tmp_path / "inventory"
    assert main(["scan", "--root", str(root), "--output", str(output)]) == EXIT_OK
    capsys.readouterr()

    assert main(["verify", "--inventory", str(output)]) == EXIT_OK
    capsys.readouterr()
    capability = json.loads((output / "capabilities.jsonl").read_text(encoding="utf-8").splitlines()[0])

    assert main([
        "explain",
        "--inventory",
        str(output),
        "--capability",
        capability["capability_uid"],
    ]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["capability"]["capability_uid"] == capability["capability_uid"]


def test_scan_rejects_repository_changes_during_static_discovery(tmp_path, monkeypatch):
    root = _repository(tmp_path / "repository")
    original = cartographer_cli.discover_repository

    def discover_then_change(*args, **kwargs):
        result = original(*args, **kwargs)
        (root / "cli.py").write_text("def changed_after_snapshot():\n    return True\n", encoding="utf-8")
        return result

    monkeypatch.setattr(cartographer_cli, "discover_repository", discover_then_change)

    with pytest.raises(SnapshotError, match="changed during static discovery"):
        cartographer_cli.scan_repository(root, config=ScanConfig(include_untracked=False))


def test_scan_discovers_frozen_bytes_when_live_file_changes_and_reverts(tmp_path, monkeypatch):
    root = _repository(tmp_path / "repository")
    original = cartographer_cli.discover_repository
    original_bytes = (root / "cli.py").read_bytes()

    def transient_change_before_discovery(*args, **kwargs):
        (root / "cli.py").write_text("def transient_after():\n    return True\n", encoding="utf-8")
        try:
            return original(*args, **kwargs)
        finally:
            (root / "cli.py").write_bytes(original_bytes)

    monkeypatch.setattr(cartographer_cli, "discover_repository", transient_change_before_discovery)
    inventory = cartographer_cli.scan_repository(root, config=ScanConfig(include_untracked=False))

    names = {item.qualified_name for item in inventory.symbols}
    assert "transient_after" not in names
    assert "main" in names


def test_cli_invalid_config_type_returns_operational_failure(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    config = tmp_path / "invalid.json"
    config.write_text('{"include_untracked":"yes"}\n', encoding="utf-8")

    assert main([
        "scan",
        "--root",
        str(root),
        "--config",
        str(config),
        "--output",
        str(tmp_path / "inventory"),
    ]) == EXIT_FAILED
    assert "must be booleans" in capsys.readouterr().err


def test_scan_does_not_parse_source_over_configured_byte_limit(tmp_path):
    root = _repository(tmp_path / "repository")
    (root / "big.py").write_text(
        "def should_not_parse():\n    return '" + ("x" * 200) + "'\n",
        encoding="utf-8",
    )
    _git(root, "add", "big.py")
    _git(root, "commit", "-m", "oversized source fixture")

    inventory = cartographer_cli.scan_repository(
        root,
        config=ScanConfig(include_untracked=False, max_file_bytes=50),
    )

    big_file = next(file for file in inventory.files if file.path == "big.py")
    assert big_file.parse_status == "skipped"
    assert "max_file_bytes_exceeded" in big_file.reason_codes
    assert "should_not_parse" not in {symbol.qualified_name for symbol in inventory.symbols}
