"""Curated-profile configuration fixtures; these test mechanics only."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repo_cartographer.curated_config import curated_profile_from_dict, load_curated_profile


def _payload():
    return {
        "format": "repo-cartographer-curated-profile/v1",
        "profile_id": "mechanics-fixture-v1",
        "title": "Mechanics fixture",
        "capabilities": [{
            "id": "fixture.report",
            "title": "Fixture report",
            "description": "Minimal fixture used only to verify projection mechanics.",
            "selectors": [{"kind": "capability", "value": "service", "required": True}],
        }],
    }


def test_loads_strict_versioned_profile(tmp_path):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(_payload()), encoding="utf-8")

    profile = load_curated_profile(path)

    assert profile.profile_id == "mechanics-fixture-v1"
    assert profile.capabilities[0].capability_id == "fixture.report"
    assert len(profile.sha256) == 64


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update({"unexpected": True}), "unknown unexpected"),
        (lambda value: value["capabilities"][0]["selectors"][0].update({"required": "yes"}), "must be a boolean"),
        (lambda value: value.update({"format": "repo-cartographer-curated-profile/v2"}), "unsupported curated profile format"),
        (lambda value: value["capabilities"].append(dict(value["capabilities"][0])), "unique"),
    ],
)
def test_rejects_nonconforming_profiles(mutation, match):
    payload = _payload()
    mutation(payload)

    with pytest.raises(ValueError, match=match):
        curated_profile_from_dict(payload)


def test_requires_at_least_one_required_exact_selector():
    payload = _payload()
    payload["capabilities"][0]["selectors"][0]["required"] = False

    with pytest.raises(ValueError, match="at least one required"):
        curated_profile_from_dict(payload)


def test_published_schema_and_runtime_both_reject_all_optional_capability():
    schema_path = Path(__file__).parents[2] / "configs" / "cartographer" / "schemas" / "curated-capability-map-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    selectors_schema = schema["properties"]["capabilities"]["items"]["properties"]["selectors"]

    assert selectors_schema["minContains"] == 1
    assert selectors_schema["contains"]["properties"]["required"]["const"] is True
    assert "required" in selectors_schema["contains"]["required"]

    payload = _payload()
    payload["capabilities"][0]["selectors"][0]["required"] = False
    with pytest.raises(ValueError, match="at least one required"):
        curated_profile_from_dict(payload)
