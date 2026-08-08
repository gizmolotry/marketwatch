"""Bounded pytest execution and immutable receipt verification overlays."""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import tempfile
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping

import pytest

from .canonical import canonical_json_bytes, canonical_sha256, normalize_repo_path, sha256_file
from .process_tree import run_process_tree
from .render import MANIFEST_NAME, load_jsonl
from .test_runner_config import PytestRunnerConfig
from .verify import verify_inventory


LEGACY_RECEIPT_FORMAT = "repo-cartographer-pytest-receipt/v1"
RECEIPT_FORMAT = "repo-cartographer-pytest-receipt/v2"
ATTESTATION_ALGORITHM = "hmac-sha256"


def _key_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for character in value)
    ):
        raise ValueError("attestation key_id must use 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def _secret_bytes(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) < 32:
        raise ValueError("attestation key material must contain at least 32 bytes")
    return bytes(value)


@dataclass(frozen=True, slots=True, repr=False)
class ReceiptAttestationSigner:
    """In-memory signing authority; key material is never serialized or represented."""

    key_id: str
    _key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "key_id", _key_id(self.key_id))
        object.__setattr__(self, "_key", _secret_bytes(self._key))

    def sign(self, payload: Mapping[str, Any]) -> str:
        return _receipt_signature(payload, self._key, self.key_id)


@dataclass(frozen=True, slots=True)
class VerifiedTestReceipt(Mapping[str, Any]):
    """A parsed receipt plus verifier-owned trust and promotion decisions."""

    payload: Mapping[str, Any] = field(repr=False)
    integrity_verified: bool
    attestation_trusted: bool
    promotion_authorized: bool
    legacy_v1: bool

    def __getitem__(self, key: str) -> Any:
        return self.payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)


def _receipt_signature(payload: Mapping[str, Any], key: bytes, key_id: str) -> str:
    signature_input = {
        "attestation": {"algorithm": ATTESTATION_ALGORITHM, "key_id": _key_id(key_id)},
        "receipt": payload,
    }
    return hmac.new(_secret_bytes(key), canonical_json_bytes(signature_input), hashlib.sha256).hexdigest()


def _runner_config_payload(config: PytestRunnerConfig) -> dict[str, Any]:
    return json.loads(canonical_json_bytes(asdict(config)).decode("utf-8"))


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


def _declaration_identity(row: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return row.get("file_path"), row.get("line"), row.get("original_name")


def _collected_identity(row: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return row.get("file_path"), row.get("definition_line"), row.get("original_name")


def _nodeid_matches_declaration(nodeid: str, declaration: Mapping[str, Any]) -> bool:
    declared = declaration.get("nodeid")
    return isinstance(declared, str) and (
        nodeid == declared
        or (nodeid.startswith(declared + "[") and nodeid.endswith("]"))
    )


def run_pytest_receipt(
    inventory_dir: str | Path,
    config: PytestRunnerConfig,
    output_path: str | Path,
    *,
    repository_root: str | Path,
    attestation_signer: ReceiptAttestationSigner,
    test_uids: Iterable[str] = (),
    test_paths: Iterable[str] = (),
) -> dict[str, Any]:
    if not isinstance(attestation_signer, ReceiptAttestationSigner):
        raise ValueError("an explicit receipt attestation signer is required")
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
            "line": test["line"],
            "original_name": str(symbol.get("qualified_name", "")).rsplit(".", 1)[-1] if symbol else "",
        })

    eligible_files = [row for row in files if not row.get("sensitive") and row.get("tracked_state") != "deleted"]
    if len(eligible_files) > config.max_source_files:
        raise ValueError("source inventory exceeds the approved runner file-count bound")
    total_source_bytes = sum(int(row.get("byte_size", 0)) for row in eligible_files)
    if total_source_bytes > config.max_source_bytes:
        raise ValueError("source inventory exceeds the approved runner byte bound")
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
        argv = [sys.executable, "-m", "pytest", "-q", "--rootdir=.", "-p", "repo_cartographer.pytest_receipt_plugin"]
        argv.extend(sorted({item["nodeid"] for item in declarations}))
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
            "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "REPO_CARTOGRAPHER_PYTEST_EVENTS": str(event_path),
            "REPO_CARTOGRAPHER_PYTEST_EVENTS_MAX_BYTES": str(config.max_output_bytes),
        }
        completed = run_process_tree(
            argv,
            cwd=frozen,
            env=environment,
            timeout_seconds=config.timeout_seconds,
        )
        timed_out = completed.timed_out
        exit_code = completed.returncode
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
                "collected_items": [],
                "phases": [],
                "collection_errors": [{
                    "nodeid": "plugin_event_file",
                    "outcome": "overflow" if event_file_overflow else "unavailable",
                }],
                "event_overflow": event_file_overflow,
            }
        after = _hash_tree(frozen, source_paths)
        changed_paths = sorted(path for path in source_paths if before.get(path) != after.get(path))

    plugin_event_overflow = events.get("event_overflow") is True
    event_file_overflow = event_file_overflow or plugin_event_overflow
    phases = events.get("phases", []) if isinstance(events.get("phases"), list) else []
    markers = events.get("markers", {}) if isinstance(events.get("markers"), dict) else {}
    collected = sorted(set(events.get("collected_nodeids", []))) if isinstance(events.get("collected_nodeids"), list) else []
    raw_collected_items = events.get("collected_items", []) if isinstance(events.get("collected_items"), list) else []
    collected_items = [row for row in raw_collected_items if isinstance(row, Mapping)]
    declaration_groups: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for declaration in declarations:
        declaration_groups.setdefault(_declaration_identity(declaration), []).append(declaration)
    collected_row_keys = [
        (row.get("nodeid"), *_collected_identity(row))
        for row in collected_items
    ]
    collection_errors = events.get("collection_errors", []) if isinstance(events.get("collection_errors"), list) else []
    collection_errors = list(collection_errors)
    if len(collected_items) != len(raw_collected_items):
        collection_errors.append({"nodeid": "plugin_event_file", "outcome": "invalid_collected_item"})
    if len(collected_row_keys) != len(set(collected_row_keys)):
        collection_errors.append({"nodeid": "plugin_event_file", "outcome": "duplicate_collected_identity"})
    if sorted(row.get("nodeid") for row in collected_items if isinstance(row.get("nodeid"), str)) != collected:
        collection_errors.append({"nodeid": "plugin_event_file", "outcome": "collected_nodeid_mismatch"})
    collected_by_declaration: dict[str, list[Mapping[str, Any]]] = {}
    unmapped_nodeids: list[str] = []
    for item in collected_items:
        matches = declaration_groups.get(_collected_identity(item), [])
        nodeid = item.get("nodeid")
        if len(matches) != 1 or not isinstance(nodeid, str) or not _nodeid_matches_declaration(nodeid, matches[0]):
            if isinstance(nodeid, str):
                unmapped_nodeids.append(nodeid)
            continue
        collected_by_declaration.setdefault(matches[0]["declaration_key"], []).append(item)
    cases = []
    for declaration in declarations:
        nodeids = sorted({str(row["nodeid"]) for row in collected_by_declaration.get(declaration["declaration_key"], [])})
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
    unmapped_nodeids = sorted(set(unmapped_nodeids))
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
            "config": _runner_config_payload(config),
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
            "collected_items": collected_items,
            "unmapped_nodeids": unmapped_nodeids,
            "collection_errors": collection_errors,
            "cases": cases,
        },
        "mutation": {"frozen_copy_unchanged": not changed_paths, "changed_paths": changed_paths},
    }
    receipt_sha256 = canonical_sha256(base)
    signed_payload = {**base, "receipt_sha256": receipt_sha256}
    payload = {
        **signed_payload,
        "attestation": {
            "algorithm": ATTESTATION_ALGORITHM,
            "key_id": attestation_signer.key_id,
            "signature": attestation_signer.sign(signed_payload),
        },
    }
    _atomic_write(Path(output_path).resolve(), canonical_json_bytes(payload) + b"\n")
    return payload


def verify_test_receipt(
    path: str | Path,
    inventory_dir: str | Path,
    config: PytestRunnerConfig | None = None,
    trusted_keys: Mapping[str, bytes] | None = None,
    *,
    allow_legacy_v1: bool = False,
) -> VerifiedTestReceipt:
    receipt_path = Path(path)
    try:
        raw_bytes = receipt_path.read_bytes()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"test receipt is unreadable: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("format") not in {RECEIPT_FORMAT, LEGACY_RECEIPT_FORMAT}:
        raise ValueError("test receipt format is unsupported")
    legacy_v1 = payload["format"] == LEGACY_RECEIPT_FORMAT
    if legacy_v1 and not allow_legacy_v1:
        raise ValueError("legacy v1 receipt is integrity-only and requires explicit legacy verification")
    signed_payload = (
        payload
        if legacy_v1
        else {key: value for key, value in payload.items() if key != "attestation"}
    )
    observed = signed_payload.get("receipt_sha256")
    base = {key: value for key, value in signed_payload.items() if key != "receipt_sha256"}
    if not isinstance(observed, str) or observed != canonical_sha256(base):
        raise ValueError("test receipt hash mismatch")
    if raw_bytes != canonical_json_bytes(payload) + b"\n":
        raise ValueError("test receipt is not canonical")
    attestation_trusted = False
    if not legacy_v1:
        if config is None:
            raise ValueError("trusted v2 receipt verification requires an approved runner config")
        if not trusted_keys:
            raise ValueError("trusted v2 receipt verification requires a trusted key resolver")
        attestation = payload.get("attestation")
        if not isinstance(attestation, Mapping) or set(attestation) != {"algorithm", "key_id", "signature"}:
            raise ValueError("test receipt attestation is invalid")
        if attestation.get("algorithm") != ATTESTATION_ALGORITHM:
            raise ValueError("test receipt attestation algorithm is unsupported")
        key_id = attestation.get("key_id")
        try:
            trusted_key = trusted_keys.get(_key_id(key_id)) if isinstance(key_id, str) else None
        except ValueError as exc:
            raise ValueError("test receipt attestation key_id is invalid") from exc
        if trusted_key is None:
            raise ValueError("test receipt attestation key_id is not trusted")
        signature = attestation.get("signature")
        if (
            not isinstance(signature, str)
            or len(signature) != 64
            or any(character not in "0123456789abcdef" for character in signature)
        ):
            raise ValueError("test receipt attestation signature is invalid")
        expected_signature = _receipt_signature(signed_payload, trusted_key, key_id)
        if not hmac.compare_digest(signature, expected_signature):
            raise ValueError("test receipt attestation signature mismatch")
        attestation_trusted = True
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
    runner = payload.get("runner", {})
    if config is not None:
        if runner.get("config_sha256") != config.sha256:
            raise ValueError("test receipt runner config mismatch")
        if not legacy_v1 and runner.get("config") != _runner_config_payload(config):
            raise ValueError("test receipt embedded runner policy mismatch")
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
    if not isinstance(declarations, list):
        raise ValueError("test receipt declarations are invalid")
    for row in declarations:
        if not isinstance(row, Mapping):
            raise ValueError("test receipt declaration binding mismatch")
        test = tests.get(row.get("test_uid"))
        file = files.get(row.get("file_path"))
        symbol = symbols.get(test.get("symbol_uid")) if test else None
        if test is None or file is None or row.get("declaration_key") != declaration_key(test, file, symbol) or row.get("file_sha256") != file.get("sha256"):
            raise ValueError("test receipt declaration binding mismatch")
        if not legacy_v1:
            expected_name = str(symbol.get("qualified_name", "")).rsplit(".", 1)[-1] if symbol else ""
            if row.get("line") != test.get("line") or row.get("original_name") != expected_name or row.get("nodeid") != _nodeid_for(test, file, symbol):
                raise ValueError("test receipt declaration location binding mismatch")
    declaration_keys = {row.get("declaration_key") for row in declarations}
    cases = payload.get("results", {}).get("cases", [])
    if not isinstance(cases, list):
        raise ValueError("test receipt cases are invalid")
    if {row.get("declaration_key") for row in cases} != declaration_keys:
        raise ValueError("test receipt result mapping mismatch")
    results = payload.get("results", {})
    mutation = payload.get("mutation", {})
    if not legacy_v1:
        collected_items = results.get("collected_items")
        if not isinstance(collected_items, list) or any(not isinstance(row, Mapping) for row in collected_items):
            raise ValueError("test receipt collected item bindings are invalid")
        declaration_groups: dict[tuple[Any, Any, Any], list[Mapping[str, Any]]] = {}
        for declaration in declarations:
            declaration_groups.setdefault(_declaration_identity(declaration), []).append(declaration)
        row_keys = [(row.get("nodeid"), *_collected_identity(row)) for row in collected_items]
        if len(row_keys) != len(set(row_keys)):
            raise ValueError("test receipt collected item bindings are duplicated")
        collected_nodeids = sorted(row.get("nodeid") for row in collected_items if isinstance(row.get("nodeid"), str))
        if collected_nodeids != results.get("collected_nodeids"):
            raise ValueError("test receipt collected node bindings mismatch")
        expected_case_nodeids: dict[str, set[str]] = {str(key): set() for key in declaration_keys}
        for item in collected_items:
            matches = declaration_groups.get(_collected_identity(item), [])
            if (
                len(matches) != 1
                or not isinstance(item.get("nodeid"), str)
                or not _nodeid_matches_declaration(item["nodeid"], matches[0])
            ):
                raise ValueError("test receipt collected item is unmapped or ambiguous")
            expected_case_nodeids[matches[0]["declaration_key"]].add(item["nodeid"])
        for row in cases:
            if not isinstance(row, Mapping):
                raise ValueError("test receipt case binding is invalid")
            actual = {case.get("nodeid") for case in row.get("cases", []) if isinstance(case, Mapping)}
            if actual != expected_case_nodeids.get(str(row.get("declaration_key")), set()):
                raise ValueError("test receipt case location binding mismatch")
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
    return VerifiedTestReceipt(
        payload=payload,
        integrity_verified=True,
        attestation_trusted=attestation_trusted,
        promotion_authorized=attestation_trusted and logically_promotable,
        legacy_v1=legacy_v1,
    )


__all__ = [
    "ATTESTATION_ALGORITHM",
    "LEGACY_RECEIPT_FORMAT",
    "RECEIPT_FORMAT",
    "ReceiptAttestationSigner",
    "VerifiedTestReceipt",
    "declaration_key",
    "run_pytest_receipt",
    "verify_test_receipt",
]
