"""Curated-map mechanics fixtures; no fixture is evidence of system effectiveness."""

from __future__ import annotations

from dataclasses import replace
import json
import subprocess

import pytest

from repo_cartographer.cli import scan_repository
from repo_cartographer.curated import (
    diff_curated_maps,
    explain_curated_map,
    load_curated_map,
    project_curated_map,
    write_curated_map,
)
from repo_cartographer.curated_config import curated_profile_from_dict
from repo_cartographer.domain import AxisStates, IntegrationState, LearningState, RuntimeState, ScanConfig, VerificationState
from repo_cartographer.render import write_inventory


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _inventory_fixture(tmp_path):
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Cartographer Fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "service.py").write_text("def report(value):\n    return value + 1\n", encoding="utf-8")
    (root / "test_service.py").write_text("from service import report\n\ndef test_report():\n    assert report(1) == 2\n", encoding="utf-8")
    (root / "service.feature").write_text("@implemented\nFeature: Fixture service\n  Scenario: Report a value\n    Then a value is reported\n", encoding="utf-8")
    (root / "fixture.pt").write_bytes(b"opaque-mechanics-fixture")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "curated map mechanics fixture")
    inventory = scan_repository(root, config=ScanConfig(include_untracked=False))
    output = tmp_path / "inventory"
    write_inventory(inventory, output)
    return inventory, output


def _profile(*capabilities, profile_id="mechanics-fixture-v1"):
    return curated_profile_from_dict({
        "format": "repo-cartographer-curated-profile/v1",
        "profile_id": profile_id,
        "title": "Projection mechanics fixture",
        "capabilities": list(capabilities),
    })


def _capability(uid, selectors):
    return {
        "id": uid,
        "title": uid,
        "description": "Minimal fixture used only to verify projection mechanics.",
        "selectors": selectors,
    }


def _selector(kind, value, required=True):
    return {"kind": kind, "value": value, "required": required}


def test_exact_projection_keeps_axes_independent_and_missing_required_explicit(tmp_path):
    _, inventory_dir = _inventory_fixture(tmp_path)
    profile = _profile(
        _capability("fixture.implemented", [_selector("capability", "service")]),
        _capability("fixture.missing", [_selector("capability", "service that does not exist")]),
        _capability("fixture.scenario", [_selector("scenario", "Report a value")]),
        _capability("fixture.tag", [_selector("scenario_tag", "implemented")]),
        _capability("fixture.artifact", [_selector("artifact_path", "fixture.pt")]),
    )

    mapped = project_curated_map(inventory_dir, profile)
    rows = {row.capability_id: row for row in mapped.capabilities}

    assert rows["fixture.implemented"].axes.definition.value == "substantive_implementation"
    assert rows["fixture.missing"].axes.definition.value == "absent"
    assert rows["fixture.missing"].reason_codes == (
        "required_selector_missing:capability:service that does not exist",
    )
    assert rows["fixture.scenario"].axes.definition.value == "docs_only"
    assert rows["fixture.scenario"].axes.verification.value == "test_declared"
    assert rows["fixture.tag"].axes.integration.value == "isolated"
    assert rows["fixture.artifact"].axes.artifact.value == "bytes_present"
    assert rows["fixture.artifact"].axes.learning.value == "not_applicable"


def test_optional_selector_is_traceable_but_never_promotes(tmp_path):
    _, inventory_dir = _inventory_fixture(tmp_path)
    profile = _profile(_capability("fixture.optional", [
        _selector("capability", "service that does not exist"),
        _selector("capability", "service", required=False),
    ]))

    row = project_curated_map(inventory_dir, profile).capabilities[0]

    assert row.axes.definition.value == "absent"
    assert row.selector_results[1].matched is True


def test_test_selector_can_only_contribute_verification_and_evaluation(tmp_path):
    _, inventory_dir = _inventory_fixture(tmp_path)
    profile = _profile(_capability(
        "fixture.test-only",
        [_selector("test_capability", "test report")],
    ))

    row = project_curated_map(inventory_dir, profile).capabilities[0]

    assert row.selector_results[0].matched is True
    assert row.axes.definition.value == "absent"
    assert row.axes.integration.value == "isolated"
    assert row.axes.verification.value == "test_declared"
    assert row.axes.artifact.value == "none"


def test_static_projection_caps_runtime_training_and_test_states(tmp_path):
    inventory, _ = _inventory_fixture(tmp_path)
    service = next(row for row in inventory.capabilities if row.name == "service")
    optimistic = replace(
        service,
        axes=replace(
            AxisStates(),
            integration=IntegrationState.DYNAMICALLY_REACHABLE,
            verification=VerificationState.TEST_PASSED,
            learning=LearningState.TRAINED_VERIFIED,
            runtime=RuntimeState.RUNNING,
        ),
    )
    output = tmp_path / "optimistic-inventory"
    write_inventory(replace(inventory, capabilities=(optimistic,)), output)
    profile = _profile(_capability("fixture.capped", [_selector("capability", "service")]))

    axes = project_curated_map(output, profile).capabilities[0].axes

    assert axes.integration.value == "statically_wired"
    assert axes.verification.value == "test_declared"
    assert axes.learning.value == "trained_unverified"
    assert axes.runtime.value == "not_observed"


def test_canonical_map_is_hash_bound_explainable_and_tamper_evident(tmp_path):
    _, inventory_dir = _inventory_fixture(tmp_path)
    profile = _profile(_capability("fixture.service", [_selector("capability", "service")]))
    mapped = project_curated_map(inventory_dir, profile)
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    first_hash = write_curated_map(mapped, first)
    second_hash = write_curated_map(mapped, second)

    assert first_hash == second_hash
    assert first.read_bytes() == second.read_bytes()
    assert explain_curated_map(first, "fixture.service")["profile_sha256"] == profile.sha256
    payload = json.loads(first.read_text(encoding="utf-8"))
    payload["capabilities"][0]["title"] = "tampered"
    first.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_curated_map(first)


def test_diff_uses_stable_ids_and_rejects_profile_changes(tmp_path):
    _, inventory_dir = _inventory_fixture(tmp_path)
    profile = _profile(_capability("fixture.service", [_selector("capability", "service")]))
    before = tmp_path / "before.json"
    after = tmp_path / "after.json"
    write_curated_map(project_curated_map(inventory_dir, profile), before)
    write_curated_map(project_curated_map(inventory_dir, profile), after)

    assert diff_curated_maps(before, after)["changes"] == []

    changed_profile = _profile(
        _capability("fixture.service", [_selector("capability", "service")]),
        profile_id="different-profile",
    )
    changed = tmp_path / "changed.json"
    write_curated_map(project_curated_map(inventory_dir, changed_profile), changed)
    with pytest.raises(ValueError, match="profile hash mismatch"):
        diff_curated_maps(before, changed)
