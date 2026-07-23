"""Mechanical repository fixtures; these do not evidence MarketLeak effectiveness."""

from __future__ import annotations

import subprocess

from repo_cartographer.domain import FileKind, ScanConfig, TrackedState
from repo_cartographer.snapshot import classify_file, read_git_state, snapshot_repository


def _git(root, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _repository(tmp_path):
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.name", "Cartographer Fixture")
    _git(tmp_path, "config", "user.email", "fixture@example.invalid")
    (tmp_path / "clean.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "staged.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "deleted.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / ".env").write_text("FIXTURE_SECRET=not-real\n", encoding="utf-8")
    _git(tmp_path, "add", "clean.py", "staged.py", "deleted.py", ".env")
    _git(tmp_path, "commit", "-m", "fixture")
    return tmp_path


def test_read_git_state_records_clean_and_native_dirty_states(tmp_path):
    root = _repository(tmp_path)
    clean = read_git_state(root)
    assert clean.dirty is False
    assert len(clean.head_commit) == 40
    assert len(clean.index_tree) == 40
    assert clean.branch == "main"

    (root / "clean.py").write_text("VALUE = 2\n", encoding="utf-8")
    (root / "staged.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "staged.py")
    (root / "deleted.py").unlink()
    (root / "untracked.py").write_text("VALUE = 3\n", encoding="utf-8")

    dirty = read_git_state(root)
    assert dirty.dirty is True
    assert dirty.staged_paths == ("staged.py",)
    assert dirty.modified_paths == ("clean.py", "staged.py")
    assert dirty.deleted_paths == ("deleted.py",)
    assert dirty.untracked_paths == ("untracked.py",)


def test_snapshot_hashes_files_and_marks_sensitive_without_parsing(tmp_path):
    root = _repository(tmp_path)
    (root / "clean.py").write_text("VALUE = 2\n", encoding="utf-8")
    (root / "staged.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "staged.py")
    (root / "untracked.feature").write_text("Feature: fixture\n", encoding="utf-8")

    first, files = snapshot_repository(root, ScanConfig())
    second, repeated = snapshot_repository(root, ScanConfig())
    by_path = {item.path: item for item in files}

    assert first.snapshot_uid == second.snapshot_uid
    assert files == repeated
    assert by_path["clean.py"].tracked_state is TrackedState.MODIFIED
    assert by_path["staged.py"].tracked_state is TrackedState.STAGED
    assert by_path["untracked.feature"].tracked_state is TrackedState.UNTRACKED
    assert by_path["untracked.feature"].kind is FileKind.SPEC
    assert len(by_path["clean.py"].sha256) == 64
    assert by_path[".env"].sensitive is True
    assert by_path[".env"].parse_status == "skipped"
    assert "sensitive_content_not_parsed" in by_path[".env"].reason_codes


def test_classify_file_uses_frozen_categories():
    config = ScanConfig()
    assert classify_file("marketleak/cli.py", config) is FileKind.SOURCE
    assert classify_file("tests/test_cli.py", config) is FileKind.TEST
    assert classify_file("specs/cartographer.feature", config) is FileKind.SPEC
    assert classify_file("docs/design.md", config) is FileKind.DOC
    assert classify_file("configs/scan.json", config) is FileKind.CONFIG
    assert classify_file("models/candidate.onnx", config) is FileKind.ARTIFACT

