"""Strict configuration for bounded pytest receipt execution."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

from .canonical import canonical_sha256, normalize_repo_path


@dataclass(frozen=True, slots=True)
class PytestRunnerConfig:
    format: str
    allowed_test_paths: tuple[str, ...]
    timeout_seconds: int
    max_output_bytes: int

    def __post_init__(self) -> None:
        if self.format != "repo-cartographer-pytest-runner/v1":
            raise ValueError("unsupported pytest runner config format")
        if not isinstance(self.allowed_test_paths, tuple) or not self.allowed_test_paths:
            raise ValueError("allowed_test_paths must be a non-empty array")
        object.__setattr__(self, "allowed_test_paths", tuple(sorted({normalize_repo_path(path) for path in self.allowed_test_paths})))
        if not isinstance(self.timeout_seconds, int) or isinstance(self.timeout_seconds, bool) or not 1 <= self.timeout_seconds <= 3600:
            raise ValueError("timeout_seconds must be an integer from 1 through 3600")
        if not isinstance(self.max_output_bytes, int) or isinstance(self.max_output_bytes, bool) or not 1024 <= self.max_output_bytes <= 10_000_000:
            raise ValueError("max_output_bytes must be an integer from 1024 through 10000000")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def runner_config_from_dict(value: Mapping[str, Any]) -> PytestRunnerConfig:
    if not isinstance(value, Mapping):
        raise ValueError("pytest runner config must be an object")
    expected = {"format", "allowed_test_paths", "timeout_seconds", "max_output_bytes"}
    if set(value) != expected:
        raise ValueError("pytest runner config keys are invalid")
    paths = value["allowed_test_paths"]
    if not isinstance(paths, list) or any(not isinstance(item, str) for item in paths):
        raise ValueError("allowed_test_paths must be an array of strings")
    return PytestRunnerConfig(value["format"], tuple(paths), value["timeout_seconds"], value["max_output_bytes"])


def load_pytest_runner_config(path: str | Path) -> PytestRunnerConfig:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"pytest runner config is unreadable: {exc}") from exc
    return runner_config_from_dict(value)


__all__ = ["PytestRunnerConfig", "load_pytest_runner_config", "runner_config_from_dict"]
