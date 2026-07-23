"""Mechanics-only fixtures for safe artifact inventory behavior."""

from __future__ import annotations

import hashlib
import json

from repo_cartographer.artifacts import inspect_artifacts
from repo_cartographer.domain import ArtifactState, FileFact, FileKind, ScanConfig, TrackedState


def _file_fact(path, root, *, kind=FileKind.ARTIFACT) -> FileFact:
    payload = (root / path).read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    return FileFact(
        file_uid=f"file:{path}",
        path=path,
        kind=kind,
        tracked_state=TrackedState.TRACKED,
        byte_size=len(payload),
        sha256=digest,
        language=None,
        sensitive=False,
        parse_status="complete",
    )


def test_json_manifest_verifies_opaque_checkpoint_bytes(tmp_path):
    """Fixture bytes prove hashing mechanics, not model effectiveness."""

    model = tmp_path / "candidate.pt"
    model.write_bytes(b"opaque-checkpoint-fixture\x00\x01")
    digest = hashlib.sha256(model.read_bytes()).hexdigest()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"files": [{"path": "candidate.pt", "sha256": digest}]}),
        encoding="utf-8",
    )
    files = (_file_fact("candidate.pt", tmp_path), _file_fact("manifest.json", tmp_path, kind=FileKind.CONFIG))

    artifacts, evidence = inspect_artifacts(tmp_path, files, ScanConfig())

    checkpoint = next(item for item in artifacts if item.file_uid == "file:candidate.pt")
    binding = next(item for item in artifacts if item.manifest_for == "file:candidate.pt")
    assert checkpoint.verification is ArtifactState.HASH_VERIFIED
    assert checkpoint.expected_sha256 == digest
    assert binding.verification is ArtifactState.HASH_VERIFIED
    assert any(item.kind.value == "artifact_manifest" for item in evidence)


def test_hash_mismatch_is_retained_as_contradictory_evidence(tmp_path):
    model = tmp_path / "candidate.onnx"
    model.write_bytes(b"opaque-onnx-fixture")
    manifest = tmp_path / "bundle-manifest.json"
    manifest.write_text(
        json.dumps({"artifact_hashes": {"candidate.onnx": "0" * 64}}),
        encoding="utf-8",
    )
    files = (_file_fact("candidate.onnx", tmp_path), _file_fact("bundle-manifest.json", tmp_path))

    artifacts, evidence = inspect_artifacts(tmp_path, files, ScanConfig())

    checkpoint = next(item for item in artifacts if item.file_uid == "file:candidate.onnx")
    assert checkpoint.verification is ArtifactState.BYTES_PRESENT
    assert "artifact_hash_mismatch" in checkpoint.reason_codes
    assert any(
        item.kind.value == "artifact_manifest" and item.observed_value == "0" * 64
        for item in evidence
    )


def test_valid_manifest_binding_wins_expected_hash_without_hiding_conflict(tmp_path):
    model = tmp_path / "candidate.pt"
    model.write_bytes(b"opaque-mixed-manifest-fixture")
    digest = hashlib.sha256(model.read_bytes()).hexdigest()
    valid = tmp_path / "valid-manifest.json"
    valid.write_text(
        json.dumps({"files": [{"path": "candidate.pt", "sha256": digest}]}),
        encoding="utf-8",
    )
    conflicting = tmp_path / "conflicting-manifest.json"
    conflicting.write_text(
        json.dumps({"files": [{"path": "candidate.pt", "sha256": "0" * 64}]}),
        encoding="utf-8",
    )
    files = (
        _file_fact("candidate.pt", tmp_path),
        _file_fact("valid-manifest.json", tmp_path, kind=FileKind.CONFIG),
        _file_fact("conflicting-manifest.json", tmp_path, kind=FileKind.CONFIG),
    )

    artifacts, evidence = inspect_artifacts(tmp_path, files, ScanConfig())

    checkpoint = next(item for item in artifacts if item.file_uid == "file:candidate.pt")
    assert checkpoint.verification is ArtifactState.HASH_VERIFIED
    assert checkpoint.expected_sha256 == digest
    assert "artifact_manifest_hash_conflict" in checkpoint.reason_codes
    assert any(
        item.kind.value == "artifact_manifest" and item.observed_value == digest
        for item in evidence
    )
    assert any(
        item.kind.value == "artifact_manifest"
        and digest in str(item.observed_value)
        and "0" * 64 in str(item.observed_value)
        for item in evidence
    )


def test_pickle_signature_is_never_deserialized(tmp_path, monkeypatch):
    """The fixture resembles pickle but must remain opaque bytes."""

    import pickle

    payload = tmp_path / "unsafe.pkl"
    payload.write_bytes(b"\x80\x04cos\nsystem\nX\x04\x00\x00\x00nope")
    monkeypatch.setattr(pickle, "loads", lambda *_: (_ for _ in ()).throw(AssertionError("must not load")))
    fact = _file_fact("unsafe.pkl", tmp_path)

    artifacts, _ = inspect_artifacts(tmp_path, (fact,), ScanConfig())

    assert len(artifacts) == 1
    assert artifacts[0].format == "pkl-pickle-signature"
    assert artifacts[0].verification is ArtifactState.BYTES_PRESENT
    assert "payload" not in artifacts[0].safe_metadata


def test_manifest_with_missing_target_does_not_invent_artifact(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"files": [{"path": "missing.pt", "sha256": "a" * 64}]}),
        encoding="utf-8",
    )

    artifacts, evidence = inspect_artifacts(
        tmp_path,
        (_file_fact("manifest.json", tmp_path, kind=FileKind.CONFIG),),
        ScanConfig(),
    )

    assert len(artifacts) == 1
    assert artifacts[0].verification is ArtifactState.MANIFEST_ONLY
    assert artifacts[0].manifest_for is None
    assert any("missing.pt" in str(item.observed_value) for item in evidence)


def test_sensitive_artifact_is_not_opened(tmp_path, monkeypatch):
    payload = tmp_path / "secret.pkl"
    payload.write_bytes(b"credential-like-fixture")
    fact = _file_fact("secret.pkl", tmp_path)
    fact = FileFact(
        file_uid=fact.file_uid,
        path=fact.path,
        kind=fact.kind,
        tracked_state=fact.tracked_state,
        byte_size=fact.byte_size,
        sha256=fact.sha256,
        language=fact.language,
        sensitive=True,
        parse_status="skipped",
    )
    monkeypatch.setattr(type(payload), "open", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not open")))

    artifacts, evidence = inspect_artifacts(tmp_path, (fact,), ScanConfig())

    assert artifacts[0].verification is ArtifactState.NONE
    assert artifacts[0].safe_metadata == {"byte_size": len(b"credential-like-fixture")}
    assert artifacts[0].reason_codes == ("sensitive_artifact_not_inspected",)
    assert evidence == ()
