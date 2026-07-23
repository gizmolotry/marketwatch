"""Safe, bounded inspection of repository artifacts.

The cartographer treats model files as opaque bytes.  This module never calls
pickle, torch, joblib, ONNX, or any other artifact runtime.  The only structured
format it parses is JSON, and then only within the configured file-size bound.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
from .domain import ArtifactFact, ArtifactState, EvidenceFact, EvidenceKind, FileFact, ScanConfig


_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_MANIFEST_NAMES = ("manifest", "bundle", "checkpoint", "artifact")
_MAGIC_READ_LIMIT = 16


def _uid(prefix: str, *parts: object) -> str:
    payload = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"{prefix}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def _enum_member(enum: type, name: str, value: str):
    """Support both conventional upper-case members and value-based enums."""

    member = getattr(enum, name, None)
    return member if member is not None else enum(value)


def _artifact_state(value: str):
    return _enum_member(ArtifactState, value.upper(), value)


def _metadata(**values: object) -> dict[str, object]:
    return {key: values[key] for key in sorted(values) if values[key] is not None}


def _safe_path(root: Path, relative: str) -> Path | None:
    """Resolve a repository-relative path without permitting root escape."""

    unresolved = root / PurePosixPath(relative)
    if unresolved.is_symlink():
        return None
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    return candidate


def _format_for(path: str, header: bytes) -> str:
    suffix = PurePosixPath(path).suffix.lower().lstrip(".") or "binary"
    if header.startswith(b"PK\x03\x04"):
        return "zip"
    if header.startswith(b"\x93NUMPY"):
        return "npy"
    if header.startswith(b"PAR1"):
        return "parquet"
    if header.startswith(b"SQLite format 3\x00"):
        return "sqlite"
    if header.startswith(b"\x80") and suffix in {"pkl", "pickle", "joblib", "pt", "pth"}:
        return f"{suffix}-pickle-signature"
    return suffix


def _hash_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_manifest(path: str) -> bool:
    name = PurePosixPath(path).name.lower()
    return name.endswith(".json") and any(token in name for token in _MANIFEST_NAMES)


def _hash_reference(value: object) -> str | None:
    if isinstance(value, str) and _SHA256_RE.fullmatch(value):
        return value.lower()
    return None


def _manifest_references(value: object) -> tuple[tuple[str, str], ...]:
    """Extract only explicit path/name-to-SHA-256 bindings from JSON.

    Bare hashes without a subject are intentionally ignored.  Recognized forms
    include ``files: [{path, sha256}]``, ``artifact_hashes: {name: sha}``, and
    mappings whose key ends in ``_sha256`` or ``_hash``.
    """

    references: set[tuple[str, str]] = set()

    def visit(node: object, parent_key: str | None = None) -> None:
        if isinstance(node, Mapping):
            path_value = node.get("path") or node.get("file") or node.get("filename")
            hash_value = node.get("sha256") or node.get("hash")
            explicit_hash = _hash_reference(hash_value)
            if isinstance(path_value, str) and explicit_hash:
                references.add((path_value.replace("\\", "/"), explicit_hash))

            for key, child in node.items():
                key_text = str(key)
                child_hash = _hash_reference(child)
                if child_hash and (
                    parent_key in {"artifact_hashes", "hashes", "artifacts"}
                    or key_text.lower().endswith(("_sha256", "_hash"))
                ):
                    subject = re.sub(r"_(?:sha256|hash)$", "", key_text, flags=re.IGNORECASE)
                    references.add((subject, child_hash))
                visit(child, key_text)
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_key)

    visit(value)
    return tuple(sorted(references))


def _match_reference(
    manifest_path: str,
    reference: str,
    files_by_path: Mapping[str, FileFact],
) -> FileFact | None:
    normalized = reference.replace("\\", "/").lstrip("./")
    base = str(PurePosixPath(manifest_path).parent / normalized)
    candidates = (normalized, base.lstrip("./"))
    for candidate in candidates:
        if candidate in files_by_path:
            return files_by_path[candidate]
    basename_matches = [fact for path, fact in files_by_path.items() if PurePosixPath(path).name == normalized]
    return basename_matches[0] if len(basename_matches) == 1 else None


def _evidence(
    subject_uid: str,
    kind: str,
    file: FileFact,
    observed_value: str,
) -> EvidenceFact:
    return EvidenceFact(
        evidence_uid=_uid("evidence", subject_uid, kind, file.file_uid, observed_value),
        subject_uid=subject_uid,
        kind=EvidenceKind.ARTIFACT_BYTES if kind == "artifact_bytes_present" else EvidenceKind.ARTIFACT_MANIFEST,
        source_file_uid=file.file_uid,
        line_start=None,
        line_end=None,
        observed_value=observed_value,
        source_sha256=file.sha256,
    )


def inspect_artifacts(
    root: str | Path,
    files: Iterable[FileFact],
    config: ScanConfig,
) -> tuple[tuple[ArtifactFact, ...], tuple[EvidenceFact, ...]]:
    """Inspect configured artifact files without loading executable formats.

    A file's bytes may establish presence.  Only an explicit manifest binding
    whose expected hash matches the already-computed file hash establishes
    ``hash_verified``.  Missing or conflicting bindings remain evidence rather
    than being silently discarded.
    """

    repository_root = Path(root).resolve()
    file_tuple = tuple(files)
    files_by_path = {fact.path.replace("\\", "/"): fact for fact in file_tuple}
    allowed_extensions = {str(ext).lower().lstrip(".") for ext in config.artifact_extensions}

    artifacts: dict[str, ArtifactFact] = {}
    evidence: list[EvidenceFact] = []
    manifest_bindings: list[tuple[FileFact, str, str]] = []

    for file in sorted(file_tuple, key=lambda item: item.path):
        suffix = PurePosixPath(file.path).suffix.lower().lstrip(".")
        manifest_candidate = _is_manifest(file.path)
        if suffix not in allowed_extensions and not manifest_candidate:
            continue

        artifact_uid = _uid("artifact", file.file_uid, file.sha256)
        path = _safe_path(repository_root, file.path)
        reasons: list[str] = []
        header = b""
        format_name = suffix or "binary"
        state = _artifact_state("none")
        safe_metadata: dict[str, object] = {"byte_size": file.byte_size}

        if file.sensitive:
            reasons.append("sensitive_artifact_not_inspected")
        elif path is None:
            reasons.append("path_outside_repository")
        elif file.byte_size > config.max_file_bytes:
            reasons.append("artifact_inspection_size_limit")
            # FileFact hashes are inventory evidence, but an uninspected path is
            # not independently promoted by this layer.
            state = _artifact_state("bytes_present") if path.is_file() else state
        elif not path.is_file():
            reasons.append("artifact_bytes_missing")
        else:
            observed_sha256 = _hash_path(path)
            if observed_sha256 != file.sha256:
                reasons.append("artifact_changed_since_snapshot")
            else:
                with path.open("rb") as handle:
                    header = handle.read(_MAGIC_READ_LIMIT)
                format_name = _format_for(file.path, header)
                state = _artifact_state("bytes_present")
                safe_metadata["magic"] = (
                    format_name if format_name != (suffix or "binary") else "unrecognized"
                )
                evidence.append(_evidence(artifact_uid, "artifact_bytes_present", file, file.sha256))

                if manifest_candidate:
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError, OSError):
                        reasons.append("manifest_json_invalid")
                    else:
                        references = _manifest_references(payload)
                        safe_metadata["manifest_reference_count"] = len(references)
                        state = _artifact_state("manifest_only")
                        for reference, expected_hash in references:
                            manifest_bindings.append((file, reference, expected_hash))
                        evidence.append(
                            _evidence(artifact_uid, "artifact_manifest_parsed", file, str(len(references)))
                        )

        artifacts[file.file_uid] = ArtifactFact(
            artifact_uid=artifact_uid,
            file_uid=file.file_uid,
            format=format_name,
            sha256=file.sha256,
            manifest_for=None,
            expected_sha256=None,
            verification=state,
            safe_metadata=_metadata(**safe_metadata),
            reason_codes=tuple(sorted(set(reasons))),
        )

    manifest_targets: dict[str, list[tuple[str, str, str]]] = {}
    binding_artifacts: list[ArtifactFact] = []
    for manifest_file, reference, expected_hash in manifest_bindings:
        target = _match_reference(manifest_file.path, reference, files_by_path)
        manifest_artifact = artifacts.get(manifest_file.file_uid)
        if target is None:
            if manifest_artifact is not None:
                evidence.append(
                    _evidence(
                        manifest_artifact.artifact_uid,
                        "artifact_manifest_target_missing",
                        manifest_file,
                        f"{reference}:{expected_hash}",
                    )
                )
            continue
        manifest_targets.setdefault(target.file_uid, []).append(
            (manifest_file.file_uid, expected_hash, reference)
        )
        manifest_fact = artifacts.get(manifest_file.file_uid)
        if manifest_fact is not None:
            binding_artifacts.append(
                ArtifactFact(
                    artifact_uid=_uid("artifact-binding", manifest_file.file_uid, target.file_uid, expected_hash),
                    file_uid=manifest_file.file_uid,
                    format="json-manifest-binding",
                    sha256=manifest_file.sha256,
                    manifest_for=target.file_uid,
                    expected_sha256=expected_hash,
                    verification=(
                        _artifact_state("hash_verified")
                        if target.sha256 == expected_hash
                        else _artifact_state("manifest_only")
                    ),
                    safe_metadata={"reference": reference},
                    reason_codes=() if target.sha256 == expected_hash else ("artifact_hash_mismatch",),
                )
            )

    for target_uid, bindings in manifest_targets.items():
        current = artifacts.get(target_uid)
        target_file = next((item for item in file_tuple if item.file_uid == target_uid), None)
        if current is None or target_file is None:
            continue
        expected_hashes = {expected for _, expected, _ in bindings}
        matching = current.sha256 in expected_hashes
        reasons = set(current.reason_codes)
        if len(expected_hashes) > 1:
            reasons.add("artifact_manifest_hash_conflict")
            evidence.append(
                _evidence(
                    current.artifact_uid,
                    "artifact_manifest_hash_conflict",
                    target_file,
                    ",".join(sorted(expected_hashes)),
                )
            )
        if matching:
            verification = _artifact_state("hash_verified")
            evidence.append(_evidence(current.artifact_uid, "artifact_hash_verified", target_file, current.sha256))
        else:
            verification = current.verification
            reasons.add("artifact_hash_mismatch")
            evidence.append(
                _evidence(
                    current.artifact_uid,
                    "artifact_hash_mismatch",
                    target_file,
                    ",".join(sorted(expected_hashes)),
                )
            )
        artifacts[target_uid] = ArtifactFact(
            artifact_uid=current.artifact_uid,
            file_uid=current.file_uid,
            format=current.format,
            sha256=current.sha256,
            manifest_for=None,
            expected_sha256=current.sha256 if matching else sorted(expected_hashes)[0],
            verification=verification,
            safe_metadata=current.safe_metadata,
            reason_codes=tuple(sorted(reasons)),
        )

    ordered_artifacts = tuple(sorted((*artifacts.values(), *binding_artifacts), key=lambda item: item.artifact_uid))
    ordered_evidence = tuple(sorted(evidence, key=lambda item: item.evidence_uid))
    return ordered_artifacts, ordered_evidence
