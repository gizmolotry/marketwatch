"""Offline integrity and reference verification for rendered inventories."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from .canonical import canonical_json_bytes, canonical_sha256, stable_sha256
from .render import INVENTORY_FORMAT, MANIFEST_NAME, SUMMARY_NAME, load_jsonl


_GROUPS: tuple[tuple[str, str], ...] = (
    ("files", "file_uid"),
    ("symbols", "symbol_uid"),
    ("imports", "import_uid"),
    ("entrypoints", "entrypoint_uid"),
    ("tests", "test_uid"),
    ("scenarios", "scenario_uid"),
    ("artifacts", "artifact_uid"),
    ("evidence", "evidence_uid"),
    ("components", "component_uid"),
    ("capabilities", "capability_uid"),
)


@dataclass(frozen=True, slots=True)
class VerificationResult:
    ok: bool
    root_sha256: str | None
    errors: tuple[str, ...]
    counts: dict[str, int]


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ids(records: Iterable[dict[str, Any]], field: str, label: str, errors: list[str]) -> set[str]:
    result: set[str] = set()
    for record in records:
        value = record.get(field)
        if not isinstance(value, str) or not value:
            errors.append(f"{label}: missing {field}")
        elif value in result:
            errors.append(f"{label}: duplicate {field} {value}")
        else:
            result.add(value)
    return result


def _references(records: Iterable[dict[str, Any]], field: str) -> Iterable[str]:
    for record in records:
        value = record.get(field)
        if isinstance(value, str) and value:
            yield value


def _check_subset(values: Iterable[str], valid: set[str], label: str, errors: list[str]) -> None:
    for value in values:
        if value not in valid:
            errors.append(f"{label}: missing reference {value}")


def verify_inventory(output_dir: str | Path) -> VerificationResult:
    """Verify rendered bytes, counts, root identity, and basic references."""

    root = Path(output_dir).resolve()
    errors: list[str] = []
    counts: dict[str, int] = {}
    manifest_path = root / MANIFEST_NAME
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return VerificationResult(False, None, (f"manifest unreadable: {exc}",), counts)
    if not isinstance(manifest, dict):
        return VerificationResult(False, None, ("manifest root is not an object",), counts)

    root_sha256 = manifest.get("root_sha256") if isinstance(manifest.get("root_sha256"), str) else None
    if manifest.get("format") != INVENTORY_FORMAT:
        errors.append("manifest format is unsupported")
    base = {key: value for key, value in manifest.items() if key != "root_sha256"}
    if root_sha256 != canonical_sha256(base):
        errors.append("manifest root hash mismatch")

    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        return VerificationResult(False, root_sha256, tuple((*errors, "manifest outputs is not an object")), counts)
    expected_outputs = {f"{group}.jsonl" for group, _ in _GROUPS} | {SUMMARY_NAME}
    unexpected_outputs = set(outputs) - expected_outputs
    missing_outputs = expected_outputs - set(outputs)
    if unexpected_outputs:
        errors.append("manifest contains unexpected outputs: " + ", ".join(sorted(unexpected_outputs)))
    if missing_outputs:
        errors.append("manifest omits outputs: " + ", ".join(sorted(missing_outputs)))

    loaded: dict[str, tuple[dict[str, Any], ...]] = {}
    for group, _ in _GROUPS:
        filename = f"{group}.jsonl"
        metadata = outputs.get(filename)
        target = root / filename
        if not isinstance(metadata, dict):
            errors.append(f"manifest missing output metadata for {filename}")
            continue
        try:
            observed_hash = _digest(target)
            records = load_jsonl(target)
        except (OSError, UnicodeError, ValueError) as exc:
            errors.append(f"{filename}: unreadable: {exc}")
            continue
        loaded[group] = records
        counts[group] = len(records)
        if metadata.get("sha256") != observed_hash:
            errors.append(f"{filename}: hash mismatch")
        if metadata.get("count") != len(records):
            errors.append(f"{filename}: count mismatch")
        uid_field = dict(_GROUPS)[group]
        canonical_payload = b"".join(
            canonical_json_bytes(record) + b"\n"
            for record in sorted(records, key=lambda item: str(item.get(uid_field, "")))
        )
        try:
            if target.read_bytes() != canonical_payload:
                errors.append(f"{filename}: records are not canonical and UID-sorted")
        except OSError as exc:
            errors.append(f"{filename}: unreadable: {exc}")

    for filename in (SUMMARY_NAME,):
        metadata = outputs.get(filename)
        target = root / filename
        if not isinstance(metadata, dict):
            errors.append(f"{filename}: invalid output metadata")
            continue
        try:
            observed_hash = _digest(target)
            line_count = len(target.read_text(encoding="utf-8").splitlines())
        except (OSError, UnicodeError) as exc:
            errors.append(f"{filename}: unreadable: {exc}")
            continue
        if metadata.get("sha256") != observed_hash:
            errors.append(f"{filename}: hash mismatch")
        if metadata.get("count") != line_count:
            errors.append(f"{filename}: count mismatch")

    if len(loaded) == len(_GROUPS):
        ids = {group: _ids(loaded[group], field, group, errors) for group, field in _GROUPS}
        file_ids = ids["files"]
        symbol_ids = ids["symbols"]
        evidence_ids = ids["evidence"]
        component_ids = ids["components"]

        for group in ("symbols", "imports", "entrypoints", "tests", "scenarios", "artifacts"):
            _check_subset(_references(loaded[group], "file_uid"), file_ids, f"{group}.file_uid", errors)
        _check_subset(_references(loaded["evidence"], "source_file_uid"), file_ids, "evidence.source_file_uid", errors)
        _check_subset(
            (record["symbol_uid"] for group in ("entrypoints", "tests") for record in loaded[group] if record.get("symbol_uid") is not None),
            symbol_ids,
            "symbol_uid",
            errors,
        )

        subject_ids = set().union(*(ids[group] for group in ("files", "symbols", "imports", "entrypoints", "tests", "scenarios", "artifacts")))
        _check_subset(_references(loaded["evidence"], "subject_uid"), subject_ids, "evidence.subject_uid", errors)
        files_by_uid = {record["file_uid"]: record for record in loaded["files"] if isinstance(record.get("file_uid"), str)}
        for record in loaded["evidence"]:
            source = files_by_uid.get(record.get("source_file_uid"))
            if source is not None and record.get("source_sha256") != source.get("sha256"):
                errors.append(f"evidence.source_sha256: does not match source file {record.get('source_file_uid')}")
        for record in loaded["artifacts"]:
            artifact_file = files_by_uid.get(record.get("file_uid"))
            if artifact_file is not None and record.get("sha256") != artifact_file.get("sha256"):
                errors.append(f"artifacts.sha256: does not match artifact file {record.get('file_uid')}")
            manifest_for = record.get("manifest_for")
            if manifest_for is not None:
                if manifest_for not in file_ids:
                    errors.append(f"artifacts.manifest_for: missing reference {manifest_for}")
                if record.get("expected_sha256") is None:
                    errors.append(f"artifacts.expected_sha256: missing for manifest binding {record.get('artifact_uid')}")
            expected_target = files_by_uid.get(manifest_for or record.get("file_uid"))
            if (
                record.get("verification") == "hash_verified"
                and record.get("expected_sha256") is not None
                and expected_target is not None
                and record.get("expected_sha256") != expected_target.get("sha256")
            ):
                errors.append(f"artifacts.expected_sha256: verified binding does not match {manifest_for or record.get('file_uid')}")
        for record in loaded["components"]:
            _check_subset(record.get("root_subject_uids", ()), subject_ids, "components.root_subject_uids", errors)
            _check_subset(record.get("supporting_evidence_uids", ()), evidence_ids, "components.supporting_evidence_uids", errors)
            _check_subset(record.get("refuting_evidence_uids", ()), evidence_ids, "components.refuting_evidence_uids", errors)
        for record in loaded["capabilities"]:
            _check_subset(record.get("component_uids", ()), component_ids, "capabilities.component_uids", errors)
            _check_subset(record.get("supporting_evidence_uids", ()), evidence_ids, "capabilities.supporting_evidence_uids", errors)
            _check_subset(record.get("refuting_evidence_uids", ()), evidence_ids, "capabilities.refuting_evidence_uids", errors)

        snapshot = manifest.get("snapshot")
        if isinstance(snapshot, dict):
            file_rows = [
                {
                    "file_uid": item.get("file_uid"),
                    "path": item.get("path"),
                    "sha256": item.get("sha256"),
                    "byte_size": item.get("byte_size"),
                    "tracked_state": item.get("tracked_state"),
                }
                for item in sorted(loaded["files"], key=lambda value: str(value.get("path", "")))
            ]
            if snapshot.get("files_root_sha256") != canonical_sha256(file_rows):
                errors.append("snapshot files root hash mismatch")
            inventory_value = {"manifest": snapshot, **{group: list(loaded[group]) for group, _ in _GROUPS}}
            if manifest.get("inventory_sha256") != stable_sha256(inventory_value):
                errors.append("inventory hash mismatch")
        else:
            errors.append("manifest snapshot is not an object")

    return VerificationResult(not errors, root_sha256, tuple(sorted(set(errors))), counts)


__all__ = ["VerificationResult", "verify_inventory"]
