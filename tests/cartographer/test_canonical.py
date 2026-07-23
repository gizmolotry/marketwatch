from __future__ import annotations

from datetime import UTC, datetime

import pytest

from repo_cartographer.canonical import (
    canonical_json_bytes,
    canonical_sha256,
    normalize_repo_path,
    sha256_file,
    stable_sha256,
)


def test_canonical_json_is_order_independent() -> None:
    assert canonical_json_bytes({"b": 2, "a": ["x", 1]}) == b'{"a":["x",1],"b":2}'
    assert canonical_sha256({"b": 2, "a": ["x", 1]}) == canonical_sha256({"a": ["x", 1], "b": 2})


def test_stable_hash_omits_root_and_scan_time() -> None:
    first = {
        "root_display": r"D:\\marketwatch",
        "captured_at": datetime(2026, 7, 22, 12, tzinfo=UTC),
        "files": [{"path": "marketleak/app.py", "sha256": "a" * 64}],
    }
    second = {
        "root_display": "/tmp/other-checkout",
        "captured_at": datetime(2026, 7, 23, 12, tzinfo=UTC),
        "files": [{"path": "marketleak/app.py", "sha256": "a" * 64}],
    }
    assert stable_sha256(first) == stable_sha256(second)
    assert canonical_sha256(first) != canonical_sha256(second)


def test_normalize_repo_path_never_emits_absolute_root(tmp_path) -> None:
    nested = tmp_path / "marketleak" / "app.py"
    nested.parent.mkdir()
    nested.write_text("# fixture only\n", encoding="utf-8")

    assert normalize_repo_path(nested, repo_root=tmp_path) == "marketleak/app.py"
    assert normalize_repo_path(r"marketleak\\app.py") == "marketleak/app.py"
    with pytest.raises(ValueError, match="escape"):
        normalize_repo_path("../outside.py")


def test_sha256_file_hashes_fixture_bytes(tmp_path) -> None:
    fixture = tmp_path / "fixture.txt"
    fixture.write_bytes(b"fixture-only mechanical test\n")

    assert sha256_file(fixture) == "f983925f6abf54fd4275f4c6dd150da982edff18e9883fd249e1385ef87f514c"
