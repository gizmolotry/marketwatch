from __future__ import annotations

import pytest

from repo_cartographer.domain import (
    AxisStates,
    DefinitionState,
    FileFact,
    FileKind,
    GitState,
    Inventory,
    RepositorySnapshot,
    TrackedState,
)


def _snapshot() -> RepositorySnapshot:
    git = GitState(
        head_commit="a" * 40,
        branch="main",
        index_tree="b" * 40,
        dirty=False,
    )
    return RepositorySnapshot(
        snapshot_uid="snapshot:fixture",
        root_display=r"D:\\marketwatch",
        git=git,
        config_sha256="c" * 64,
        scanner_version="0.1",
        files_root_sha256="d" * 64,
    )


def test_file_fact_normalizes_and_freezes_mechanical_fixture() -> None:
    fact = FileFact(
        file_uid="file:fixture",
        path=r"tests\\fixture.py",
        kind=FileKind.TEST,
        tracked_state=TrackedState.UNTRACKED,
        byte_size=1,
        sha256="a" * 64,
        language="python",
        sensitive=False,
        parse_status="parsed",
        reason_codes=("fixture_only", "fixture_only"),
    )

    assert fact.path == "tests/fixture.py"
    assert fact.reason_codes == ("fixture_only",)
    with pytest.raises(AttributeError):
        fact.path = "other.py"  # type: ignore[misc]


def test_inventory_is_stably_ordered_and_snapshot_hash_omits_root_display() -> None:
    first = _snapshot()
    second = RepositorySnapshot(
        snapshot_uid=first.snapshot_uid,
        root_display="/another/checkout",
        git=first.git,
        config_sha256=first.config_sha256,
        scanner_version=first.scanner_version,
        files_root_sha256=first.files_root_sha256,
    )
    assert first.sha256 == second.sha256
    assert Inventory(manifest=first).sha256 == Inventory(manifest=second).sha256
    assert AxisStates().definition is DefinitionState.ABSENT


def test_file_fact_rejects_absolute_or_escaping_paths() -> None:
    fields = dict(
        file_uid="file:fixture", kind=FileKind.SOURCE, tracked_state=TrackedState.TRACKED,
        byte_size=1, sha256="a" * 64, language="python", sensitive=False, parse_status="parsed",
    )
    with pytest.raises(ValueError):
        FileFact(path="../outside.py", **fields)
