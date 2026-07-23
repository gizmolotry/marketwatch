"""Bounded pytest execution and immutable receipt verification overlays."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Iterable, Mapping

import pytest

from .canonical import canonical_json_bytes, canonical_sha256, normalize_repo_path, sha256_file
from .render import MANIFEST_NAME, load_jsonl
from .test_runner_config import PytestRunnerConfig
from .verify import verify_inventory


RECEIPT_FORMAT = "repo-cartographer-pytest-receipt/v1"


def declaration_key(test: Mapping[str, Any], file: Mapping[str, Any], symbol: Mapping[str, Any] | None) -> str:
    return canonical_sha256({
        "test_uid": test["test_uid"],
        "file_path": file["path"],
        "file_sha256": file["sha256"],
        "symbol_uid": test.get("symbol_uid"),
        "qualified_name": symbol.get("qualified_name") if symbol else None,
        "line": test["line"],
    })


def _allowed(path: str, patterns: Iterable[str]) -> bool:
    return any(path == pattern or path.startswith(pattern.rstrip("/") + "/") or fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _nodeid_for(test: Mapping[str, Any], file: Mapping[str, Any], symbol: Mapping[str, Any] | None) -> str:
    name = str(symbol.get("qualified_name")) if symbol else ""
    return file["path"] + ("::" + name.replace(".", "::") if name else "")


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _hash_tree(root: Path, paths: Iterable[str]) -> dict[str, str]:
    result = {}
    for path in sorted(paths):
        target = root / path
        result[path] = sha256_file(target) if target.is_file() else "missing"
    return result


def _map_nodeid(nodeid: str, declarations: list[dict[str, Any]]) -> dict[str, Any] | None:
    base = nodeid.split("[", 1)[0]
    exact = [item for item in declarations if base == item["nodeid"]]
    return exact[0] if len(exact) == 1 else None


def run_pytest_receipt(
    inventory_dir: str | Path,
    config: PytestRunnerConfig,
    output_path: str | Path,
    *,
    repository_root: str | Path,
    test_uids: Iterable[str] = (),
    test_paths: Iterable[str] = (),
) -> dict[str, Any]:
    inventory_root = Path(inventory_dir).resolve()
    verification = verify_inventory(inventory_root)
    if not verification.ok or verification.root_sha256 is None:
        raise ValueError("inventory verification failed: " + "; ".join(verification.errors))
    manifest = json.loads((inventory_root / MANIFEST_NAME).read_text(encoding="utf-8"))
    snapshot = manifest["snapshot"]
    repository_root = Path(repository_root).resolve()
    files = load_jsonl(inventory_root / "files.jsonl")
    files_by_uid = {row["file_uid"]: row for row in files}
    symbols_by_uid = {row["symbol_uid"]: row for row in load_jsonl(inventory_root / "symbols.jsonl")}
    tests = [
        row for row in load_jsonl(inventory_root / "tests.jsonl")
        if row.get("framework") != "gherkin"
        and symbols_by_uid.get(row.get("symbol_uid"), {}).get("kind") in {"function", "async_function", "method", "async_method"}
    ]
    requested_uids = tuple(sorted(set(test_uids)))
    requested_paths = tuple(sorted({normalize_repo_path(path) for path in test_paths}))
    unknown_uids = sorted(set(requested_uids) - {row["test_uid"] for row in tests})
    if unknown_uids:
        raise ValueError("unknown test UID: " + ", ".join(unknown_uids))
    if any(not _allowed(path, config.allowed_test_paths) for path in requested_paths):
        raise ValueError("requested test path is outside the approved runner policy")

    selected = []
    for test in tests:
        file = files_by_uid[test["file_uid"]]
        path = file["path"]
        if not _allowed(path, config.allowed_test_paths):
            continue
        if requested_uids or requested_paths:
            if test["test_uid"] not in requested_uids and not any(path == chosen or path.startswith(chosen.rstrip("/") + "/") for chosen in requested_paths):
                continue
        selected.append(test)
    if not selected:
        raise ValueError("runner selection contains no approved executable test declarations")

    declarations = []
    for test in selected:
        file = files_by_uid[test["file_uid"]]
        symbol = symbols_by_uid.get(test.get("symbol_uid"))
        declarations.append({
            "test_uid": test["test_uid"],
            "declaration_key": declaration_key(test, file, symbol),
            "file_path": file["path"],
            "file_sha256": file["sha256"],
            "fixture_only": bool(test.get("fixture_only")),
            "nodeid": _nodeid_for(test, file, symbol),
        })

    eligible_files = [row for row in files if not row.get("sensitive") and row.get("tracked_state") != "deleted"]
    source_paths = [row["path"] for row in eligible_files]
    for row in eligible_files:
        source = repository_root / row["path"]
        if not source.is_file() or source.is_symlink() or sha256_file(source) != row["sha256"]:
            raise ValueError(f"source inventory is stale or mismatched at {row['path']}")

    with tempfile.TemporaryDirectory(prefix="repo-cartographer-pytest-") as temporary_name:
        frozen = Path(temporary_name)
        for row in eligible_files:
            source = repository_root / row["path"]
            destination = frozen / row["path"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            if sha256_file(destination) != row["sha256"]:
                raise ValueError(f"frozen copy hash mismatch at {row['path']}")
        before = _hash_tree(frozen, source_paths)
        event_path = frozen / ".repo-cartographer-pytest-events.json"
        argv = [sys.executable, "-m", "pytest", "-q", "-p", "repo_cartographer.pytest_receipt_plugin"]
        argv.extend(item["nodeid"] for item in declarations)
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
            "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "REPO_CARTOGRAPHER_PYTEST_EVENTS": str(event_path),
        }
        timed_out = False
        exit_code = 1
        try:
            completed = subprocess.run(
                argv,
                cwd=frozen,
                env=environment,
                shell=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=config.timeout_seconds,
                check=False,
            )
            exit_code = int(completed.returncode)
        except subprocess.TimeoutExpired:
            timed_out = True
            exit_code = 124
        try:
            event_file_bytes = event_path.stat().st_size
        except OSError:
            event_file_bytes = 0
        event_file_overflow = event_file_bytes > config.max_output_bytes
        try:
            if event_file_overflow:
                raise ValueError("bounded plugin event file exceeded")
            events = json.loads(event_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            events = {
                "collected_nodeids": [],
                "phases": [],
                "collection_errors": [{
                    "nodeid": "plugin_event_file",
                    "outcome": "overflow" if event_file_overflow else "unavailable",
                }],
            }
        after = _hash_tree(frozen, source_paths)
        changed_paths = sorted(path for path in source_paths if before.get(path) != after.get(path))

    phases = events.get("phases", []) if isinstance(events.get("phases"), list) else []
    markers = events.get("markers", {}) if isinstance(events.get("markers"), dict) else {}
    collected = sorted(set(events.get("collected_nodeids", []))) if isinstance(events.get("collected_nodeids"), list) else []
    cases = []
    mapped_nodeids = set()
    for declaration in declarations:
        nodeids = sorted(nodeid for nodeid in collected if _map_nodeid(nodeid, [declaration]))
        mapped_nodeids.update(nodeids)
        case_rows = []
        for nodeid in nodeids:
            node_phases = [row for row in phases if row.get("nodeid") == nodeid]
            by_when = {row.get("when"): row for row in node_phases}
            plain_passed = (
                set(by_when) == {"setup", "call", "teardown"}
                and all(row.get("outcome") == "passed" and not row.get("wasxfail") for row in by_when.values())
                and not markers.get(nodeid)
            )
            case_rows.append({"nodeid": nodeid, "markers": markers.get(nodeid, []), "phases": node_phases, "plain_passed": plain_passed})
        cases.append({
            **declaration,
            "cases": case_rows,
            "fully_passed": bool(case_rows) and all(row["plain_passed"] for row in case_rows),
        })
    collection_errors = events.get("collection_errors", []) if isinstance(events.get("collection_errors"), list) else []
    unmapped_nodeids = sorted(set(collected) - mapped_nodeids)
    promotable = (
        exit_code == 0
        and not timed_out
        and not event_file_overflow
        and not collection_errors
        and not unmapped_nodeids
        and not changed_paths
        and all(row["fully_passed"] for row in cases)
    )
    if not promotable:
        for row in cases:
            row["fully_passed"] = False
    base = {
        "format": RECEIPT_FORMAT,
        "source": {
            "inventory_root_sha256": verification.root_sha256,
            "snapshot_uid": snapshot["snapshot_uid"],
            "head_commit": snapshot["git"]["head_commit"],
            "index_tree": snapshot["git"]["index_tree"],
            "files_root_sha256": snapshot["files_root_sha256"],
            "scan_config_sha256": snapshot["config_sha256"],
        },
        "runner": {
            "config_sha256": config.sha256,
            "allowed_test_paths": config.allowed_test_paths,
            "timeout_seconds": config.timeout_seconds,
            "max_output_bytes": config.max_output_bytes,
        },
        "execution": {
            "interpreter": {"implementation": sys.implementation.name, "version": list(sys.version_info[:3])},
            "pytest_version": pytest.__version__,
            "argv": argv,
        },
        "selection": {
            "requested_test_uids": requested_uids,
            "requested_test_paths": requested_paths,
            "declarations": declarations,
            "source_files": [
                {"path": row["path"], "sha256": row["sha256"]}
                for row in sorted(eligible_files, key=lambda item: item["path"])
            ],
        },
        "results": {
            "exit_code": exit_code,
            "timed_out": timed_out,
            "promotable": promotable,
            "event_file_bytes": event_file_bytes,
            "event_file_overflow": event_file_overflow,
            "collected_nodeids": collected,
            "unmapped_nodeids": unmapped_nodeids,
            "collection_errors": collection_errors,
            "cases": cases,
        },
        "mutation": {"frozen_copy_unchanged": not changed_paths, "changed_paths": changed_paths},
    }
    receipt_sha256 = canonical_sha256(base)
    payload = {**base, "receipt_sha256": receipt_sha256}
    _atomic_write(Path(output_path).resolve(), canonical_json_bytes(payload) + b"\n")
    return payload


def verify_test_receipt(
    path: str | Path,
    inventory_dir: str | Path,
    config: PytestRunnerConfig | None = None,
) -> dict[str, Any]:
    receipt_path = Path(path)
    try:
        raw_bytes = receipt_path.read_bytes()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"test receipt is unreadable: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("format") != RECEIPT_FORMAT:
        raise ValueError("test receipt format is unsupported")
    observed = payload.get("receipt_sha256")
    base = {key: value for key, value in payload.items() if key != "receipt_sha256"}
    if not isinstance(observed, str) or observed != canonical_sha256(base):
        raise ValueError("test receipt hash mismatch")
    if raw_bytes != canonical_json_bytes(payload) + b"\n":
        raise ValueError("test receipt is not canonical")
    verification = verify_inventory(inventory_dir)
    if not verification.ok or verification.root_sha256 != payload.get("source", {}).get("inventory_root_sha256"):
        raise ValueError("test receipt source inventory mismatch")
    manifest = json.loads((Path(inventory_dir) / MANIFEST_NAME).read_text(encoding="utf-8"))
    source = payload.get("source", {})
    snapshot = manifest["snapshot"]
    expected = {
        "snapshot_uid": snapshot["snapshot_uid"],
        "head_commit": snapshot["git"]["head_commit"],
        "index_tree": snapshot["git"]["index_tree"],
        "files_root_sha256": snapshot["files_root_sha256"],
        "scan_config_sha256": snapshot["config_sha256"],
    }
    if any(source.get(key) != value for key, value in expected.items()):
        raise ValueError("test receipt snapshot binding mismatch")
    if config is not None and payload.get("runner", {}).get("config_sha256") != config.sha256:
        raise ValueError("test receipt runner config mismatch")
    file_rows = load_jsonl(Path(inventory_dir) / "files.jsonl")
    files = {row["path"]: row for row in file_rows}
    expected_source_files = sorted(
        ({"path": row["path"], "sha256": row["sha256"]} for row in file_rows if not row.get("sensitive") and row.get("tracked_state") != "deleted"),
        key=lambda row: row["path"],
    )
    if payload.get("selection", {}).get("source_files") != expected_source_files:
        raise ValueError("test receipt source file binding mismatch")
    tests = {row["test_uid"]: row for row in load_jsonl(Path(inventory_dir) / "tests.jsonl")}
    symbols = {row["symbol_uid"]: row for row in load_jsonl(Path(inventory_dir) / "symbols.jsonl")}
    declarations = payload.get("selection", {}).get("declarations", [])
    for row in declarations:
        test = tests.get(row.get("test_uid"))
        file = files.get(row.get("file_path"))
        symbol = symbols.get(test.get("symbol_uid")) if test else None
        if test is None or file is None or row.get("declaration_key") != declaration_key(test, file, symbol) or row.get("file_sha256") != file.get("sha256"):
            raise ValueError("test receipt declaration binding mismatch")
    declaration_keys = {row.get("declaration_key") for row in declarations}
    cases = payload.get("results", {}).get("cases", [])
    if {row.get("declaration_key") for row in cases} != declaration_keys:
        raise ValueError("test receipt result mapping mismatch")
    results = payload.get("results", {})
    mutation = payload.get("mutation", {})
    def plain_case(case: Mapping[str, Any]) -> bool:
        phases = case.get("phases", [])
        by_when = {row.get("when"): row for row in phases if isinstance(row, Mapping)}
        return (
            set(by_when) == {"setup", "call", "teardown"}
            and all(row.get("outcome") == "passed" and not row.get("wasxfail") for row in by_when.values())
            and not case.get("markers")
            and case.get("plain_passed") is True
        )

    logically_promotable = (
        results.get("exit_code") == 0
        and results.get("timed_out") is False
        and results.get("event_file_overflow") is False
        and not results.get("collection_errors")
        and not results.get("unmapped_nodeids")
        and mutation.get("frozen_copy_unchanged") is True
        and not mutation.get("changed_paths")
        and bool(cases)
        and all(
            row.get("fully_passed") is True
            and bool(row.get("cases"))
            and all(plain_case(case) for case in row.get("cases", []))
            for row in cases
        )
    )
    if bool(results.get("promotable")) != logically_promotable:
        raise ValueError("test receipt promotion state is inconsistent")
    return payload


__all__ = ["RECEIPT_FORMAT", "declaration_key", "run_pytest_receipt", "verify_test_receipt"]
