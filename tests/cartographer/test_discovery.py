"""Mechanical AST/Gherkin discovery fixtures, not effectiveness evidence."""

from __future__ import annotations

import subprocess

from repo_cartographer.discovery import discover_repository, inventory_tests, parse_gherkin
from repo_cartographer.domain import FileKind, ScanConfig
from repo_cartographer.snapshot import snapshot_repository


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _fixture_repository(tmp_path):
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.name", "Cartographer Fixture")
    _git(tmp_path, "config", "user.email", "fixture@example.invalid")
    tests = tmp_path / "tests"
    specs = tmp_path / "specs"
    tests.mkdir()
    specs.mkdir()
    (tests / "test_mechanics.py").write_text(
        """import pytest

@pytest.fixture
def fixture_value():
    return 1

def test_inventory(fixture_value):
    assert fixture_value == 1
""",
        encoding="utf-8",
    )
    (specs / "inventory.feature").write_text(
        """@implemented
@cartographer
Feature: Repository inventory

  @files
  Rule: Files remain attributable

    @mechanical
    Scenario: Inventory a fixture repository
      Given a repository fixture

    @outline
    Scenario Outline: Inventory each <kind>
      Then the <kind> is present

      Examples:
        | kind |
        | file |
""",
        encoding="utf-8",
    )
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "fixture")
    return tmp_path


def test_parse_gherkin_preserves_feature_rule_scenarios_and_inherited_tags(tmp_path):
    root = _fixture_repository(tmp_path)
    _, files = snapshot_repository(root, ScanConfig())
    fact = next(item for item in files if item.path == "specs/inventory.feature")

    scenarios = parse_gherkin(root / fact.path, fact)

    assert len(scenarios) == 2
    first = scenarios[0]
    assert first.feature == "Repository inventory"
    assert first.rule == "Files remain attributable"
    assert first.scenario == "Inventory a fixture repository"
    assert set(first.tags) == {"implemented", "cartographer", "files", "mechanical"}
    assert scenarios[1].scenario == "Inventory each <kind>"


def test_discover_repository_returns_deterministic_tests_scenarios_and_evidence(tmp_path):
    root = _fixture_repository(tmp_path)
    snapshot, files = snapshot_repository(root, ScanConfig())

    first = discover_repository(root, snapshot, files)
    second = discover_repository(root, snapshot, files)

    assert first == second
    assert {item.qualified_name for item in first.symbols} == {"tests.test_mechanics", "fixture_value", "test_inventory"}
    assert len(first.scenarios) == 2
    assert {item.framework for item in first.tests} == {"pytest", "gherkin"}
    assert any(item.fixture_only for item in first.tests)
    assert any(not item.fixture_only and item.symbol_uid for item in first.tests)
    assert {item.kind.value for item in first.evidence} >= {"ast_symbol"}


def test_inventory_tests_ignores_non_test_symbols_in_test_file(tmp_path):
    root = _fixture_repository(tmp_path)
    snapshot, files = snapshot_repository(root, ScanConfig())
    result = discover_repository(root, snapshot, files)
    python_files = tuple(item for item in files if item.kind is FileKind.TEST)

    tests = inventory_tests(result.symbols, python_files, ())

    assert {item.fixture_only for item in tests} == {False, True}
