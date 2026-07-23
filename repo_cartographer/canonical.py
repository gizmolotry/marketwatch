"""Deterministic serialization and path primitives for repository inventory."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


_VOLATILE_KEYS = frozenset(
    {
        "absolute_root",
        "captured_at",
        "created_at",
        "generated_at",
        "observed_at",
        "repo_root",
        "root_display",
        "scanned_at",
        "timestamp",
        "updated_at",
        "workspace_root",
    }
)
_DRIVE_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")


def normalize_repo_path(path: str | Path, *, repo_root: str | Path | None = None) -> str:
    """Return a normalized, repository-relative POSIX path.

    Absolute paths are accepted only with ``repo_root`` and are never emitted.
    Paths escaping the repository are rejected instead of being silently collapsed.
    """

    raw = str(path).strip()
    if not raw:
        raise ValueError("path must not be empty")
    candidate = Path(raw)
    is_absolute = candidate.is_absolute() or bool(_DRIVE_ABSOLUTE.match(raw))
    if repo_root is not None:
        root = Path(repo_root).resolve()
        resolved = (candidate if is_absolute else root / candidate).resolve()
        try:
            relative = resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError("path must be inside repo_root") from exc
        raw = relative.as_posix()
    elif is_absolute:
        raise ValueError("absolute paths require repo_root")

    normalized = PurePosixPath(raw.replace("\\", "/"))
    if normalized.is_absolute() or any(part == ".." for part in normalized.parts):
        raise ValueError("path must be repository-relative and may not escape it")
    result = normalized.as_posix()
    if result in ("", "."):
        raise ValueError("path must identify a repository file")
    return result


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if dataclasses.is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetimes must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, frozenset, set)):
        values = [_jsonable(item) for item in value]
        return sorted(values, key=lambda item: canonical_json_bytes(item)) if isinstance(value, (frozenset, set)) else values
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats cannot be serialized")
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize supported values in a stable UTF-8 representation."""

    return json.dumps(
        _jsonable(value), ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _stable_value(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {
            field.name: _stable_value(getattr(value, field.name))
            for field in dataclasses.fields(value)
            if field.name not in _VOLATILE_KEYS
        }
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item)
            for key, item in value.items()
            if str(key) not in _VOLATILE_KEYS
        }
    if isinstance(value, (tuple, list)):
        return [_stable_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_stable_value(item) for item in value), key=canonical_json_bytes)
    return value


def stable_sha256(value: Any) -> str:
    """Hash identity-bearing content, omitting display roots and scan timestamps."""

    return canonical_sha256(_stable_value(value))


def sha256_file(path: str | Path) -> str:
    """Return a streamed SHA-256 digest for one regular file."""

    source = Path(path)
    if not source.is_file():
        raise ValueError(f"path is not a regular file: {source}")
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


__all__ = ["canonical_json_bytes", "canonical_sha256", "normalize_repo_path", "sha256_file", "stable_sha256"]
