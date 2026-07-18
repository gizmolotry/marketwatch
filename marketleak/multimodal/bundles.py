"""Immutable, hash-verified serving-bundle manifests.

This module publishes JSON manifests and an atomic current-pointer only.  It
never serializes or deserializes model objects, so callers cannot accidentally
load an untrusted pickle as part of bundle selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Mapping

from .fusion import validate_mechanism_name


BUNDLE_SCHEMA_VERSION = "15.0.0"
ARTIFACT_NAMES = (
    "dataset",
    "feature_spec",
    "model",
    "calibration",
    "ood",
    "conformal",
    "retrieval",
    "code",
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_UID_RE = re.compile(r"^bundle-[0-9a-f]{64}$")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class ServingBundleManifest:
    """The complete immutable identity of an approved serving artifact set."""

    bundle_version: str
    created_at: datetime
    dataset_hash: str
    feature_spec_hash: str
    model_hash: str
    calibration_hash: str
    ood_hash: str
    conformal_hash: str
    retrieval_hash: str
    code_hash: str
    mechanisms: tuple[str, ...]
    schema_version: str = BUNDLE_SCHEMA_VERSION
    bundle_uid: str | None = None

    def __post_init__(self) -> None:
        if self.schema_version != BUNDLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported bundle schema version: {self.schema_version}")
        if not _VERSION_RE.fullmatch(str(self.bundle_version)):
            raise ValueError("bundle_version has an invalid format")
        object.__setattr__(self, "created_at", _utc(self.created_at))
        for name, value in self.artifact_hashes.items():
            if not _HASH_RE.fullmatch(value):
                raise ValueError(f"{name}_hash must be a lowercase SHA-256 hex digest")
        mechanisms = tuple(sorted({validate_mechanism_name(name) for name in self.mechanisms}))
        if not mechanisms:
            raise ValueError("at least one operational mechanism is required")
        object.__setattr__(self, "mechanisms", mechanisms)
        expected_uid = f"bundle-{self.manifest_hash}"
        if self.bundle_uid is None:
            object.__setattr__(self, "bundle_uid", expected_uid)
        elif self.bundle_uid != expected_uid or not _UID_RE.fullmatch(self.bundle_uid):
            raise ValueError("bundle_uid does not match immutable manifest content")

    @property
    def artifact_hashes(self) -> Mapping[str, str]:
        return {
            "dataset": self.dataset_hash,
            "feature_spec": self.feature_spec_hash,
            "model": self.model_hash,
            "calibration": self.calibration_hash,
            "ood": self.ood_hash,
            "conformal": self.conformal_hash,
            "retrieval": self.retrieval_hash,
            "code": self.code_hash,
        }

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "bundle_version": self.bundle_version,
            "created_at": self.created_at.isoformat().replace("+00:00", "Z"),
            "artifact_hashes": dict(self.artifact_hashes),
            "mechanisms": list(self.mechanisms),
        }

    @property
    def manifest_hash(self) -> str:
        return hashlib.sha256(_canonical_bytes(self._identity_payload())).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            **self._identity_payload(),
            "bundle_uid": self.bundle_uid,
            "manifest_sha256": self.manifest_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ServingBundleManifest":
        expected = {
            "schema_version",
            "bundle_version",
            "created_at",
            "artifact_hashes",
            "mechanisms",
            "bundle_uid",
            "manifest_sha256",
        }
        if set(payload) != expected:
            raise ValueError("manifest fields do not match the frozen bundle schema")
        hashes = payload["artifact_hashes"]
        if not isinstance(hashes, Mapping) or set(hashes) != set(ARTIFACT_NAMES):
            raise ValueError("manifest must include each required artifact hash exactly once")
        try:
            created = datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("manifest created_at is invalid") from exc
        manifest = cls(
            schema_version=str(payload["schema_version"]),
            bundle_version=str(payload["bundle_version"]),
            created_at=created,
            dataset_hash=str(hashes["dataset"]),
            feature_spec_hash=str(hashes["feature_spec"]),
            model_hash=str(hashes["model"]),
            calibration_hash=str(hashes["calibration"]),
            ood_hash=str(hashes["ood"]),
            conformal_hash=str(hashes["conformal"]),
            retrieval_hash=str(hashes["retrieval"]),
            code_hash=str(hashes["code"]),
            mechanisms=tuple(str(name) for name in payload["mechanisms"]),
            bundle_uid=str(payload["bundle_uid"]),
        )
        if payload["manifest_sha256"] != manifest.manifest_hash:
            raise ValueError("manifest_sha256 does not match immutable manifest content")
        return manifest


@dataclass(frozen=True)
class BundlePointer:
    bundle_uid: str
    bundle_version: str
    manifest_sha256: str
    schema_version: str = BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BUNDLE_SCHEMA_VERSION or not _UID_RE.fullmatch(self.bundle_uid):
            raise ValueError("invalid bundle pointer schema or UID")
        if not _VERSION_RE.fullmatch(self.bundle_version) or not _HASH_RE.fullmatch(self.manifest_sha256):
            raise ValueError("invalid bundle pointer version or hash")

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "bundle_uid": self.bundle_uid,
            "bundle_version": self.bundle_version,
            "manifest_sha256": self.manifest_sha256,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "BundlePointer":
        if set(payload) != {"schema_version", "bundle_uid", "bundle_version", "manifest_sha256"}:
            raise ValueError("pointer fields do not match the frozen pointer schema")
        return cls(
            schema_version=str(payload["schema_version"]),
            bundle_uid=str(payload["bundle_uid"]),
            bundle_version=str(payload["bundle_version"]),
            manifest_sha256=str(payload["manifest_sha256"]),
        )


class FrozenBundleRepository:
    """JSON-only manifest repository with atomic current-pointer publication."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.bundle_root = self.root / "bundles"
        self.pointer_path = self.root / "current.json"

    def verify_artifacts(self, manifest: ServingBundleManifest, artifact_paths: Mapping[str, str | Path]) -> None:
        if set(artifact_paths) != set(ARTIFACT_NAMES):
            raise ValueError("artifact_paths must include each required immutable artifact exactly once")
        for name, expected_hash in manifest.artifact_hashes.items():
            path = Path(artifact_paths[name])
            if not path.is_file():
                raise ValueError(f"artifact path is not a file: {name}")
            if sha256_file(path) != expected_hash:
                raise ValueError(f"artifact hash mismatch: {name}")

    def publish(self, manifest: ServingBundleManifest, artifact_paths: Mapping[str, str | Path]) -> BundlePointer:
        """Verify all artifacts, write an immutable manifest, then switch pointer."""

        self.verify_artifacts(manifest, artifact_paths)
        bundle_dir = self.bundle_root / str(manifest.bundle_uid)
        manifest_path = bundle_dir / "manifest.json"
        payload = _canonical_bytes(manifest.to_dict())
        bundle_dir.mkdir(parents=True, exist_ok=True)
        if manifest_path.exists():
            existing = manifest_path.read_bytes()
            if existing != payload:
                raise ValueError("immutable bundle UID already exists with different manifest bytes")
        else:
            self._atomic_write(manifest_path, payload)
        pointer = BundlePointer(
            bundle_uid=str(manifest.bundle_uid),
            bundle_version=manifest.bundle_version,
            manifest_sha256=manifest.manifest_hash,
        )
        self.root.mkdir(parents=True, exist_ok=True)
        self._atomic_write(self.pointer_path, _canonical_bytes(pointer.to_dict()))
        return pointer

    def read_current(self) -> ServingBundleManifest:
        if not self.pointer_path.is_file():
            raise FileNotFoundError("no current serving bundle pointer is published")
        pointer = BundlePointer.from_dict(self._read_json_object(self.pointer_path))
        manifest_path = self.bundle_root / pointer.bundle_uid / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("current pointer references a missing immutable manifest")
        manifest = ServingBundleManifest.from_dict(self._read_json_object(manifest_path))
        if (
            manifest.bundle_uid != pointer.bundle_uid
            or manifest.bundle_version != pointer.bundle_version
            or manifest.manifest_hash != pointer.manifest_sha256
        ):
            raise ValueError("current pointer does not match referenced manifest")
        return manifest

    @staticmethod
    def _read_json_object(path: Path) -> Mapping[str, object]:
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid JSON artifact: {path.name}") from exc
        if not isinstance(parsed, Mapping):
            raise ValueError("JSON artifact must be an object")
        return parsed

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, path)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)


__all__ = [
    "ARTIFACT_NAMES",
    "BUNDLE_SCHEMA_VERSION",
    "BundlePointer",
    "FrozenBundleRepository",
    "ServingBundleManifest",
    "sha256_file",
]
