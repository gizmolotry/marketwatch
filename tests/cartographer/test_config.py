from __future__ import annotations

import json

import pytest

from repo_cartographer.config import load_scan_config
from repo_cartographer.domain import ScanConfig


def test_default_config_loads_as_frozen_scan_config() -> None:
    config = load_scan_config("configs/cartographer/default.json")

    assert isinstance(config, ScanConfig)
    assert config.include_untracked is True
    assert config.sha256 == load_scan_config("configs/cartographer/default.json").sha256


def test_config_rejects_unknown_fields(tmp_path) -> None:
    config_path = tmp_path / "fixture-only-config.json"
    config_path.write_text(json.dumps({"unknown_setting": True}), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown fields"):
        load_scan_config(config_path)


def test_config_rejects_non_tuple_constructor_values() -> None:
    with pytest.raises(TypeError, match="include_globs must be a tuple"):
        ScanConfig(include_globs=["**/*.py"])  # type: ignore[arg-type]
