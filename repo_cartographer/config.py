"""Strict loader for the small JSON scan configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .domain import ScanConfig


_CONFIG_FIELDS = frozenset(ScanConfig.__dataclass_fields__)


def load_scan_config(path: str | Path) -> ScanConfig:
    """Load one JSON configuration and reject misspelled or non-canonical fields."""

    source = Path(path)
    try:
        decoded: Any = json.loads(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read scan config: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"scan config is not valid JSON: {source}") from exc
    if not isinstance(decoded, Mapping):
        raise ValueError("scan config root must be an object")
    unknown = set(decoded) - _CONFIG_FIELDS
    if unknown:
        raise ValueError(f"scan config contains unknown fields: {', '.join(sorted(unknown))}")
    values = dict(decoded)
    for name in ("include_globs", "exclude_globs", "artifact_extensions", "sensitive_globs"):
        if name in values:
            if not isinstance(values[name], list) or any(not isinstance(item, str) for item in values[name]):
                raise ValueError(f"{name} must be an array of strings")
            values[name] = tuple(values[name])
    return ScanConfig(**values)


__all__ = ["ScanConfig", "load_scan_config"]
