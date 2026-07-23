"""Pytest runner policy mechanics fixtures only."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from repo_cartographer.test_runner_config import runner_config_from_dict


def _value():
    return {"format":"repo-cartographer-pytest-runner/v1","allowed_test_paths":["tests"],"timeout_seconds":30,"max_output_bytes":4096}


def test_runner_policy_is_strict_bounded_and_hashable():
    config = runner_config_from_dict(_value())
    assert config.allowed_test_paths == ("tests",)
    assert len(config.sha256) == 64


@pytest.mark.parametrize("field,value", [("timeout_seconds",0),("timeout_seconds",3601),("max_output_bytes",100)])
def test_runner_policy_rejects_unbounded_values(field, value):
    payload = _value(); payload[field] = value
    with pytest.raises(ValueError):
        runner_config_from_dict(payload)


def test_runner_policy_rejects_unknown_keys():
    payload = _value(); payload["pytest_args"] = ["--capture=no"]
    with pytest.raises(ValueError, match="keys"):
        runner_config_from_dict(payload)


def test_published_runner_and_receipt_schemas_bind_the_versioned_contracts():
    schemas = Path(__file__).parents[2] / "configs" / "cartographer" / "schemas"
    runner = json.loads((schemas / "pytest-runner-v1.schema.json").read_text(encoding="utf-8"))
    receipt = json.loads((schemas / "test-receipt-v1.schema.json").read_text(encoding="utf-8"))
    assert runner["properties"]["format"]["const"] == "repo-cartographer-pytest-runner/v1"
    assert receipt["properties"]["format"]["const"] == "repo-cartographer-pytest-receipt/v1"
    assert {"source", "runner", "execution", "selection", "results", "mutation", "receipt_sha256"} <= set(receipt["required"])
