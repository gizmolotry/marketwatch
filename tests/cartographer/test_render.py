"""Rendering mechanics fixtures; these do not evidence MarketLeak effectiveness."""

from __future__ import annotations

from dataclasses import replace
import subprocess

import pytest

from repo_cartographer.cli import scan_repository
from repo_cartographer.domain import CapabilityFact, ScanConfig
from repo_cartographer.render import MANIFEST_NAME, write_inventory


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _repository(root):
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "service.py").write_text(
        "def report(value: int) -> int:\n    return value + 1\n",
        encoding="utf-8",
    )
    (root / "test_service.py").write_text(
        "from service import report\n\ndef test_report():\n    assert report(1) == 2\n",
        encoding="utf-8",
    )
    (root / "service.feature").write_text(
        "@implemented\nFeature: Service report\n  Scenario: Report a value\n    Then a value is reported\n",
        encoding="utf-8",
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture repository")
    return root


def test_render_is_byte_deterministic_for_same_real_git_fixture(tmp_path):
    root = _repository(tmp_path / "repository")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False))
    first = tmp_path / "first"
    second = tmp_path / "second"

    first_result = write_inventory(inventory, first)
    second_result = write_inventory(inventory, second)

    assert first_result.root_sha256 == second_result.root_sha256
    assert {item.name: item.read_bytes() for item in first.iterdir()} == {
        item.name: item.read_bytes() for item in second.iterdir()
    }
    assert (first / MANIFEST_NAME).is_file()
    assert (first / "summary.md").read_text(encoding="utf-8").startswith("# Repository inventory summary")


def test_render_replaces_complete_outputs_and_leaves_no_temporary_files(tmp_path):
    root = _repository(tmp_path / "repository")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False))
    output = tmp_path / "inventory"

    write_inventory(inventory, output)
    write_inventory(inventory, output)

    assert not [item for item in output.iterdir() if item.suffix == ".tmp"]
    assert (output / "files.jsonl").read_bytes().endswith(b"\n")


def test_render_refuses_to_overwrite_an_unmanaged_nonempty_directory(tmp_path):
    root = _repository(tmp_path / "repository")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False))
    output = tmp_path / "inventory"
    output.mkdir()
    (output / "user-notes.txt").write_text("preserve me\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unmanaged non-empty"):
        write_inventory(inventory, output)

    assert (output / "user-notes.txt").read_text(encoding="utf-8") == "preserve me\n"


def test_summary_is_bounded_and_points_to_authoritative_capabilities(tmp_path):
    root = _repository(tmp_path / "repository")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False))
    template = inventory.capabilities[0]
    capabilities = tuple(
        CapabilityFact(
            capability_uid=f"capability:fixture:{index:03d}",
            name=f"fixture capability {index:03d}",
            component_uids=template.component_uids,
            axes=template.axes,
        )
        for index in range(105)
    )
    output = tmp_path / "inventory"

    write_inventory(replace(inventory, capabilities=capabilities), output)
    summary = (output / "summary.md").read_text(encoding="utf-8")

    assert "[capabilities.jsonl](capabilities.jsonl)" in summary
    assert "## Capability axis distributions" in summary
    assert "5 additional capabilities omitted" in summary
    assert "fixture capability 104" not in summary
