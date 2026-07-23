"""Deterministic on-disk rendering for repository inventories."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable

from .canonical import canonical_json_bytes, canonical_sha256
from .domain import Inventory


INVENTORY_FORMAT = "repo-cartographer-inventory/v1"
MANIFEST_NAME = "inventory-manifest.json"
SUMMARY_NAME = "summary.md"

_GROUPS: tuple[tuple[str, str, str], ...] = (
    ("files", "files.jsonl", "file_uid"),
    ("symbols", "symbols.jsonl", "symbol_uid"),
    ("imports", "imports.jsonl", "import_uid"),
    ("entrypoints", "entrypoints.jsonl", "entrypoint_uid"),
    ("tests", "tests.jsonl", "test_uid"),
    ("scenarios", "scenarios.jsonl", "scenario_uid"),
    ("artifacts", "artifacts.jsonl", "artifact_uid"),
    ("evidence", "evidence.jsonl", "evidence_uid"),
    ("components", "components.jsonl", "component_uid"),
    ("capabilities", "capabilities.jsonl", "capability_uid"),
)


@dataclass(frozen=True, slots=True)
class RenderResult:
    output_dir: Path
    manifest_path: Path
    root_sha256: str


def _jsonl(records: Iterable[Any], uid_field: str) -> bytes:
    ordered = sorted(records, key=lambda record: str(getattr(record, uid_field)))
    return b"".join(canonical_json_bytes(record) + b"\n" for record in ordered)


def _atomic_write(path: Path, payload: bytes) -> None:
    """Replace one output only after its complete contents reach a sibling file."""

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


def _summary(inventory: Inventory) -> bytes:
    lines = [
        "# Repository inventory summary",
        "",
        f"- Snapshot: `{inventory.manifest.snapshot_uid}`",
        f"- HEAD: `{inventory.manifest.git.head_commit}`",
        f"- Branch: `{inventory.manifest.git.branch}`",
        f"- Dirty worktree: `{str(inventory.manifest.git.dirty).lower()}`",
        "- Scope: static repository evidence only; no tests, services, or model artifacts were executed.",
        "",
        "## Record counts",
        "",
        "| Record | Count |",
        "|---|---:|",
    ]
    for attribute, _, _ in _GROUPS:
        lines.append(f"| {attribute} | {len(getattr(inventory, attribute))} |")

    lines.extend(("", "## Capability axis distributions", "", "| Axis | State | Count |", "|---|---|---:|"))
    axis_names = ("definition", "integration", "verification", "artifact", "learning", "evaluation", "runtime")
    for axis_name in axis_names:
        distribution = Counter(getattr(item.axes, axis_name).value for item in inventory.capabilities)
        for state, count in sorted(distribution.items()):
            lines.append(f"| {axis_name} | {state} | {count} |")

    lines.extend(("", "## Capability review sample", "", "The complete authoritative inventory is [capabilities.jsonl](capabilities.jsonl).", "", "| Capability | Definition | Integration | Verification | Artifact | Learning | Evaluation | Runtime | Contradictions |", "|---|---|---|---|---|---|---|---|---|"))

    def review_priority(item: Any) -> tuple[Any, ...]:
        return (
            0 if item.contradiction_codes else 1,
            0 if item.axes.learning.value != "not_applicable" else 1,
            0 if item.axes.artifact.value != "none" else 1,
            0 if item.axes.integration.value in {"statically_wired", "dynamically_reachable"} else 1,
            item.name,
            item.capability_uid,
        )

    ordered_capabilities = sorted(inventory.capabilities, key=review_priority)
    displayed_capabilities = ordered_capabilities[:100]
    for capability in displayed_capabilities:
        name = capability.name.replace("|", "\\|")
        contradictions = ", ".join(capability.contradiction_codes).replace("|", "\\|") or "none"
        axes = capability.axes
        lines.append(
            f"| {name} | {axes.definition.value} | {axes.integration.value} | "
            f"{axes.verification.value} | {axes.artifact.value} | {axes.learning.value} | "
            f"{axes.evaluation.value} | {axes.runtime.value} | {contradictions} |"
        )
    omitted = len(ordered_capabilities) - len(displayed_capabilities)
    if omitted:
        lines.extend(("", f"{omitted} additional capabilities omitted from this summary; inspect `capabilities.jsonl` for all records."))
    lines.extend(("", "Hash verification establishes byte identity only; it does not establish approval, training provenance, runtime reachability, or effectiveness.", ""))
    return "\n".join(lines).encode("utf-8")


def write_inventory(inventory: Inventory, output_dir: str | Path) -> RenderResult:
    """Write canonical inventory records and a root-hash manifest.

    The manifest is written last, so readers never observe a new manifest that
    refers to output files which have not yet been replaced.
    """

    destination = Path(output_dir).resolve()
    if destination.exists() and not destination.is_dir():
        raise ValueError(f"inventory output is not a directory: {destination}")
    if destination.is_dir() and any(destination.iterdir()) and not (destination / MANIFEST_NAME).is_file():
        raise ValueError(f"refusing to replace unmanaged non-empty output directory: {destination}")
    destination.mkdir(parents=True, exist_ok=True)

    payloads: dict[str, bytes] = {}
    output_metadata: dict[str, dict[str, Any]] = {}
    for attribute, filename, uid_field in _GROUPS:
        records = getattr(inventory, attribute)
        payload = _jsonl(records, uid_field)
        payloads[filename] = payload
        output_metadata[filename] = {
            "count": len(records),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    summary = _summary(inventory)
    payloads[SUMMARY_NAME] = summary
    output_metadata[SUMMARY_NAME] = {
        "count": len(summary.decode("utf-8").splitlines()),
        "sha256": hashlib.sha256(summary).hexdigest(),
    }

    manifest_base = {
        "format": INVENTORY_FORMAT,
        "inventory_sha256": inventory.sha256,
        "outputs": output_metadata,
        "snapshot": inventory.manifest,
    }
    root_sha256 = canonical_sha256(manifest_base)
    manifest_payload = canonical_json_bytes({**manifest_base, "root_sha256": root_sha256}) + b"\n"

    for filename in sorted(payloads):
        _atomic_write(destination / filename, payloads[filename])
    _atomic_write(destination / MANIFEST_NAME, manifest_payload)
    return RenderResult(destination, destination / MANIFEST_NAME, root_sha256)


def load_jsonl(path: str | Path) -> tuple[dict[str, Any], ...]:
    """Read a rendered JSONL file without importing or executing repository code."""

    source = Path(path)
    records: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL in {source.name} at line {line_number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"JSONL record in {source.name} at line {line_number} is not an object")
            records.append(value)
    return tuple(records)


__all__ = [
    "INVENTORY_FORMAT",
    "MANIFEST_NAME",
    "RenderResult",
    "SUMMARY_NAME",
    "load_jsonl",
    "write_inventory",
]
