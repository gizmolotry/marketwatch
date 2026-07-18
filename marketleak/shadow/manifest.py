"""Reproducible, immutable manifests for prospective shadow runs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


UTC = timezone.utc
ZERO_HASH = "0" * 64
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")


def utc_datetime(value: Any, *, field_name: str, allow_none: bool = False) -> datetime | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name} is required")
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 datetime") from exc
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _json_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a value identically across replay runs."""
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def hash_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def resolve_git_revision(repo_root: str | Path) -> str:
    """Return a verified git commit SHA, or the explicit sentinel UNTRACKED."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(repo_root),
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNTRACKED"
    revision = result.stdout.strip().lower()
    return revision if result.returncode == 0 and _GIT_SHA_RE.fullmatch(revision) else "UNTRACKED"


class SourceCoverageState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    status: Literal["complete", "partial", "unavailable", "unknown"]
    limitations: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.status == "complete"


class ShadowRunManifestV2(BaseModel):
    """All inputs capable of changing a shadow result, frozen and hashed."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    shadow_run_uid: str = Field(pattern=r"^shadow:[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    git_revision: str
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    detector_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    graph_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    detector_version: str = Field(min_length=1)
    evidence_version: str = Field(min_length=1)
    graph_version: str = Field(min_length=1)
    random_seed: int
    control_sampling_rate: float = Field(ge=0.0, le=1.0)
    input_partition_hashes: dict[str, str]
    source_high_watermarks: dict[str, datetime]
    source_coverage: dict[str, SourceCoverageState]
    coverage_complete: bool
    as_of: datetime
    started_at: datetime
    ended_at: datetime | None = None
    environment: dict[str, str]
    previous_ledger_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    engineering_validation_only: Literal[True] = True
    effectiveness_unknown: Literal[True] = True
    tuning_locked: Literal[True] = True
    frozen: Literal[True] = True

    @field_validator("git_revision")
    @classmethod
    def _valid_git_revision(cls, value: str) -> str:
        normalized = value.strip()
        if normalized != "UNTRACKED" and not _GIT_SHA_RE.fullmatch(normalized.lower()):
            raise ValueError("git_revision must be a commit SHA or UNTRACKED")
        return normalized

    @field_validator("as_of", "started_at", "ended_at", mode="before")
    @classmethod
    def _utc_times(cls, value, info):
        return utc_datetime(value, field_name=info.field_name, allow_none=info.field_name == "ended_at")

    @field_validator("source_high_watermarks", mode="before")
    @classmethod
    def _utc_watermarks(cls, value):
        return {
            str(source): utc_datetime(timestamp, field_name=f"source_high_watermarks[{source}]")
            for source, timestamp in dict(value).items()
        }

    @field_validator("input_partition_hashes")
    @classmethod
    def _valid_partition_hashes(cls, value: dict[str, str]) -> dict[str, str]:
        for name, digest in value.items():
            if not name or not _HASH_RE.fullmatch(digest):
                raise ValueError("input partition names must be non-empty and hashes must be SHA-256")
        return value

    @model_validator(mode="after")
    def _consistent_manifest(self):
        if self.started_at > self.as_of:
            raise ValueError("started_at cannot be later than as_of")
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot precede started_at")
        calculated_coverage = bool(self.source_coverage) and all(
            state.complete for state in self.source_coverage.values()
        )
        if calculated_coverage != self.coverage_complete:
            raise ValueError("coverage_complete must reflect all declared source coverage")
        manifest_payload = self.model_dump(mode="json")
        supplied_manifest_hash = manifest_payload.pop("manifest_hash")
        if canonical_hash(manifest_payload) != supplied_manifest_hash:
            raise ValueError("manifest_hash does not match the frozen manifest payload")
        supplied_run_uid = manifest_payload.pop("shadow_run_uid")
        if supplied_run_uid != f"shadow:{canonical_hash(manifest_payload)}":
            raise ValueError("shadow_run_uid does not match the frozen run fingerprint")
        return self

    def fingerprint_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload.pop("manifest_hash", None)
        payload.pop("shadow_run_uid", None)
        return payload


def _normalize_hash(value: str | bytes | Mapping[str, Any], *, label: str) -> str:
    if isinstance(value, str) and _HASH_RE.fullmatch(value):
        return value
    if isinstance(value, bytes):
        return hashlib.sha256(value).hexdigest()
    if isinstance(value, str):
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
    try:
        return canonical_hash(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} could not be hashed canonically") from exc


def build_shadow_manifest(
    *,
    repo_root: str | Path,
    config: str | bytes | Mapping[str, Any],
    schema: str | bytes | Mapping[str, Any],
    model: str | bytes | Mapping[str, Any],
    detector: str | bytes | Mapping[str, Any],
    evidence: str | bytes | Mapping[str, Any],
    graph: str | bytes | Mapping[str, Any],
    schema_version: str,
    model_version: str,
    detector_version: str,
    evidence_version: str,
    graph_version: str,
    random_seed: int,
    control_sampling_rate: float,
    input_partitions: Mapping[str, str | Path | bytes],
    source_high_watermarks: Mapping[str, datetime],
    source_coverage: Mapping[str, SourceCoverageState | Mapping[str, Any]],
    as_of: datetime,
    started_at: datetime,
    ended_at: datetime | None = None,
    environment: Mapping[str, str] | None = None,
    previous_ledger_hash: str = ZERO_HASH,
    git_revision: str | None = None,
) -> ShadowRunManifestV2:
    partition_hashes: dict[str, str] = {}
    for name, source in sorted(input_partitions.items()):
        partition_hashes[str(name)] = (
            hashlib.sha256(source).hexdigest()
            if isinstance(source, bytes)
            else hash_file(source)
        )
    coverage = {
        str(source): (
            state if isinstance(state, SourceCoverageState) else SourceCoverageState.model_validate(state)
        )
        for source, state in sorted(source_coverage.items())
    }
    coverage_complete = bool(coverage) and all(state.complete for state in coverage.values())
    revision = git_revision or resolve_git_revision(repo_root)
    fingerprint = {
        "git_revision": revision,
        "config_hash": _normalize_hash(config, label="config"),
        "schema_hash": _normalize_hash(schema, label="schema"),
        "model_hash": _normalize_hash(model, label="model"),
        "detector_hash": _normalize_hash(detector, label="detector"),
        "evidence_hash": _normalize_hash(evidence, label="evidence"),
        "graph_hash": _normalize_hash(graph, label="graph"),
        "schema_version": schema_version,
        "model_version": model_version,
        "detector_version": detector_version,
        "evidence_version": evidence_version,
        "graph_version": graph_version,
        "random_seed": int(random_seed),
        "control_sampling_rate": float(control_sampling_rate),
        "input_partition_hashes": partition_hashes,
        "source_high_watermarks": dict(source_high_watermarks),
        "source_coverage": coverage,
        "coverage_complete": coverage_complete,
        "as_of": as_of,
        "started_at": started_at,
        "ended_at": ended_at,
        "environment": dict(sorted((environment or {"python": os.sys.version.split()[0]}).items())),
        "previous_ledger_hash": previous_ledger_hash,
        "engineering_validation_only": True,
        "effectiveness_unknown": True,
        "tuning_locked": True,
        "frozen": True,
    }
    run_digest = canonical_hash(fingerprint)
    fingerprint["shadow_run_uid"] = f"shadow:{run_digest}"
    manifest_hash = canonical_hash(fingerprint)
    fingerprint["manifest_hash"] = manifest_hash
    return ShadowRunManifestV2.model_validate(fingerprint)


__all__ = [
    "ZERO_HASH",
    "ShadowRunManifestV2",
    "SourceCoverageState",
    "build_shadow_manifest",
    "canonical_hash",
    "canonical_json_bytes",
    "hash_file",
    "resolve_git_revision",
]
