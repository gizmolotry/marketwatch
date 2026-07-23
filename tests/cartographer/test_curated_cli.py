"""Curated-map CLI mechanics fixtures; these do not measure MarketLeak."""

from __future__ import annotations

import json
import subprocess

from repo_cartographer.cli import EXIT_FAILED, EXIT_OK, main


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _fixture(tmp_path):
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "service.py").write_text("def report():\n    return 'fixture'\n", encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "CLI mechanics fixture")
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({
        "format": "repo-cartographer-curated-profile/v1",
        "profile_id": "cli-mechanics-fixture-v1",
        "title": "CLI mechanics fixture",
        "capabilities": [{
            "id": "fixture.service",
            "title": "Fixture service",
            "description": "Minimal fixture used only to verify CLI mechanics.",
            "selectors": [{"kind": "capability", "value": "service", "required": True}],
        }],
    }), encoding="utf-8")
    return root, profile


def test_map_explain_and_diff_commands(tmp_path, capsys):
    root, profile = _fixture(tmp_path)
    inventory = tmp_path / "inventory"
    first = tmp_path / "first-map.json"
    second = tmp_path / "second-map.json"
    assert main(["scan", "--root", str(root), "--output", str(inventory)]) == EXIT_OK
    capsys.readouterr()

    assert main(["map", "--inventory", str(inventory), "--profile", str(profile), "--output", str(first)]) == EXIT_OK
    mapped = json.loads(capsys.readouterr().out)
    assert mapped["capability_count"] == 1
    assert len(mapped["profile_sha256"]) == 64

    assert main(["map-explain", "--map", str(first), "--capability", "fixture.service"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["capability"]["capability_id"] == "fixture.service"

    assert main(["map", "--inventory", str(inventory), "--profile", str(profile), "--output", str(second)]) == EXIT_OK
    capsys.readouterr()
    assert main(["map-diff", "--before", str(first), "--after", str(second)]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["changes"] == []


def test_map_requires_verified_inventory_and_exact_capability(tmp_path, capsys):
    root, profile = _fixture(tmp_path)
    inventory = tmp_path / "inventory"
    output = tmp_path / "map.json"
    assert main(["scan", "--root", str(root), "--output", str(inventory)]) == EXIT_OK
    capsys.readouterr()
    with (inventory / "capabilities.jsonl").open("ab") as handle:
        handle.write(b"{}\n")

    assert main(["map", "--inventory", str(inventory), "--profile", str(profile), "--output", str(output)]) == EXIT_FAILED
    assert "inventory verification failed" in capsys.readouterr().err
