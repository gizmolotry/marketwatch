"""Deterministic projection of verified inventories into a curated capability map."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable, Mapping

from .canonical import canonical_json_bytes, canonical_sha256
from .domain import (
    ArtifactState,
    AxisStates,
    CuratedCapabilityFact,
    CuratedCapabilityMap,
    CuratedProfile,
    CuratedSelector,
    CuratedSelectorKind,
    CuratedSelectorResult,
    DefinitionState,
    EvaluationState,
    IntegrationState,
    LearningState,
    RuntimeState,
    VerificationState,
)
from .render import MANIFEST_NAME, load_jsonl
from .verify import verify_inventory
from .test_receipts import declaration_key, verify_test_receipt


CURATED_MAP_FORMAT = "repo-cartographer-curated-map/v1"

_ORDERS: dict[str, tuple[str, ...]] = {
    "definition": tuple(item.value for item in DefinitionState),
    "integration": tuple(item.value for item in IntegrationState),
    "verification": tuple(item.value for item in VerificationState),
    "artifact": tuple(item.value for item in ArtifactState),
    "learning": tuple(item.value for item in LearningState),
    "evaluation": tuple(item.value for item in EvaluationState),
    "runtime": tuple(item.value for item in RuntimeState),
}
_ENUMS = {
    "definition": DefinitionState,
    "integration": IntegrationState,
    "verification": VerificationState,
    "artifact": ArtifactState,
    "learning": LearningState,
    "evaluation": EvaluationState,
    "runtime": RuntimeState,
}
_SELECTOR_AXES = {
    CuratedSelectorKind.CAPABILITY: frozenset(_ORDERS),
    CuratedSelectorKind.TEST_CAPABILITY: frozenset(("verification", "evaluation")),
    CuratedSelectorKind.SCENARIO: frozenset(("definition", "verification")),
    CuratedSelectorKind.SCENARIO_TAG: frozenset(("definition", "verification")),
    CuratedSelectorKind.ARTIFACT_PATH: frozenset(("artifact",)),
}
_DEFAULTS = {
    "definition": DefinitionState.ABSENT.value,
    "integration": IntegrationState.ISOLATED.value,
    "verification": VerificationState.NONE.value,
    "artifact": ArtifactState.NONE.value,
    "learning": LearningState.NOT_APPLICABLE.value,
    "evaluation": EvaluationState.NONE.value,
    "runtime": RuntimeState.NOT_OBSERVED.value,
}


def _static_cap(axis: str, value: str) -> str:
    caps = {
        ("integration", IntegrationState.DYNAMICALLY_REACHABLE.value): IntegrationState.STATICALLY_WIRED.value,
        ("verification", VerificationState.TEST_PASSED.value): VerificationState.TEST_DECLARED.value,
        ("learning", LearningState.TRAINED_VERIFIED.value): LearningState.TRAINED_UNVERIFIED.value,
        ("runtime", RuntimeState.RUNNING.value): RuntimeState.NOT_OBSERVED.value,
    }
    return caps.get((axis, value), value)


def _weakest(axis: str, values: Iterable[str]) -> str:
    order = _ORDERS[axis]
    items = tuple(values)
    if not items:
        return _DEFAULTS[axis]
    try:
        return min(items, key=order.index)
    except ValueError as exc:
        raise ValueError(f"unsupported {axis} state in rendered inventory") from exc


def _selector_matches(
    selector: CuratedSelector,
    *,
    capabilities: tuple[dict[str, Any], ...],
    scenarios: tuple[dict[str, Any], ...],
    artifacts: tuple[dict[str, Any], ...],
    files_by_uid: Mapping[str, dict[str, Any]],
) -> tuple[tuple[str, ...], dict[str, list[str]]]:
    states: dict[str, list[str]] = {axis: [] for axis in _ORDERS}
    matched_uids: list[str] = []
    if selector.kind in {CuratedSelectorKind.CAPABILITY, CuratedSelectorKind.TEST_CAPABILITY}:
        matches = [row for row in capabilities if row.get("name") == selector.value]
        for row in matches:
            uid = row.get("capability_uid")
            if isinstance(uid, str):
                matched_uids.append(uid)
            axes = row.get("axes")
            if not isinstance(axes, Mapping):
                continue
            for axis in _SELECTOR_AXES[selector.kind]:
                value = axes.get(axis)
                if isinstance(value, str):
                    states[axis].append(_static_cap(axis, value))
    elif selector.kind in {CuratedSelectorKind.SCENARIO, CuratedSelectorKind.SCENARIO_TAG}:
        if selector.kind is CuratedSelectorKind.SCENARIO:
            matches = [row for row in scenarios if row.get("scenario") == selector.value]
        else:
            matches = [row for row in scenarios if selector.value in row.get("tags", ())]
        for row in matches:
            uid = row.get("scenario_uid")
            if isinstance(uid, str):
                matched_uids.append(uid)
        if matches:
            states["definition"].append(DefinitionState.DOCS_ONLY.value)
            states["verification"].append(VerificationState.TEST_DECLARED.value)
    elif selector.kind is CuratedSelectorKind.ARTIFACT_PATH:
        matches = [
            row for row in artifacts
            if files_by_uid.get(str(row.get("file_uid")), {}).get("path") == selector.value
        ]
        for row in matches:
            uid = row.get("artifact_uid")
            if isinstance(uid, str):
                matched_uids.append(uid)
            value = row.get("verification")
            if isinstance(value, str):
                states["artifact"].append(_static_cap("artifact", value))
    return tuple(sorted(set(matched_uids))), states


def project_curated_map(
    inventory_dir: str | Path,
    profile: CuratedProfile,
    *,
    test_receipts: Iterable[str | Path] = (),
) -> CuratedCapabilityMap:
    """Project exact selectors over a verified rendered inventory."""

    root = Path(inventory_dir).resolve()
    verification = verify_inventory(root)
    if not verification.ok or verification.root_sha256 is None:
        raise ValueError("inventory verification failed: " + "; ".join(verification.errors))
    manifest = json.loads((root / MANIFEST_NAME).read_text(encoding="utf-8"))
    snapshot = manifest.get("snapshot")
    if not isinstance(snapshot, Mapping) or not isinstance(snapshot.get("snapshot_uid"), str):
        raise ValueError("verified inventory manifest lacks snapshot identity")
    capabilities = load_jsonl(root / "capabilities.jsonl")
    capabilities_by_uid = {row["capability_uid"]: row for row in capabilities}
    components_by_uid = {row["component_uid"]: row for row in load_jsonl(root / "components.jsonl")}
    tests_by_uid = {row["test_uid"]: row for row in load_jsonl(root / "tests.jsonl")}
    symbols_by_uid = {row["symbol_uid"]: row for row in load_jsonl(root / "symbols.jsonl")}
    scenarios = load_jsonl(root / "scenarios.jsonl")
    artifacts = load_jsonl(root / "artifacts.jsonl")
    files_by_uid = {str(row["file_uid"]): row for row in load_jsonl(root / "files.jsonl")}
    receipt_hashes: list[str] = []
    passed_declaration_keys: set[str] = set()
    for receipt_path in test_receipts:
        receipt = verify_test_receipt(receipt_path, root)
        receipt_hashes.append(receipt["receipt_sha256"])
        if receipt.get("results", {}).get("promotable"):
            passed_declaration_keys.update(
                row["declaration_key"]
                for row in receipt.get("results", {}).get("cases", [])
                if row.get("fully_passed") and isinstance(row.get("declaration_key"), str)
            )

    projected: list[CuratedCapabilityFact] = []
    for definition in profile.capabilities:
        has_required_test_selector = any(
            selector.required and selector.kind is CuratedSelectorKind.TEST_CAPABILITY
            for selector in definition.selectors
        )
        axis_values: dict[str, list[str]] = {axis: [] for axis in _ORDERS}
        results: list[CuratedSelectorResult] = []
        reasons: list[str] = []
        for selector in definition.selectors:
            matched_uids, states = _selector_matches(
                selector,
                capabilities=capabilities,
                scenarios=scenarios,
                artifacts=artifacts,
                files_by_uid=files_by_uid,
            )
            if selector.required and selector.kind is CuratedSelectorKind.TEST_CAPABILITY and matched_uids:
                required_keys: set[str] = set()

                def add_eligible(test: Mapping[str, Any]) -> None:
                    symbol = symbols_by_uid.get(test.get("symbol_uid"))
                    if test.get("fixture_only") or not symbol or symbol.get("kind") not in {"function", "async_function", "method", "async_method"}:
                        return
                    file = files_by_uid.get(str(test.get("file_uid")))
                    if file is not None:
                        required_keys.add(declaration_key(test, file, symbol))

                for capability_uid in matched_uids:
                    capability_row = capabilities_by_uid.get(capability_uid, {})
                    for component_uid in capability_row.get("component_uids", ()):
                        component = components_by_uid.get(component_uid, {})
                        if component.get("component_kind") != "test":
                            continue
                        for test_uid in component.get("root_subject_uids", ()):
                            test = tests_by_uid.get(test_uid)
                            symbol = symbols_by_uid.get(test.get("symbol_uid")) if test is not None else None
                            if test is None or symbol is None:
                                continue
                            if symbol.get("kind") == "module":
                                for candidate in tests_by_uid.values():
                                    if candidate.get("file_uid") == test.get("file_uid"):
                                        add_eligible(candidate)
                            else:
                                add_eligible(test)
                if required_keys and required_keys <= passed_declaration_keys:
                    states["verification"] = [VerificationState.TEST_PASSED.value]
                elif not required_keys:
                    reasons.append(f"test_selector_has_no_eligible_declarations:{selector.value}")
                elif receipt_hashes:
                    reasons.append(f"test_receipt_incomplete:{selector.value}")
            results.append(CuratedSelectorResult(selector, matched_uids, bool(matched_uids)))
            if selector.required:
                applicable_axes = _SELECTOR_AXES[selector.kind]
                if has_required_test_selector and selector.kind is CuratedSelectorKind.CAPABILITY:
                    applicable_axes = applicable_axes - {"verification", "evaluation"}
                for axis in applicable_axes:
                    axis_values[axis].append(_weakest(axis, states[axis]))
                if not matched_uids:
                    reasons.append(f"required_selector_missing:{selector.kind.value}:{selector.value}")
            elif not matched_uids:
                reasons.append(f"optional_selector_missing:{selector.kind.value}:{selector.value}")
        values = {axis: _weakest(axis, axis_values[axis]) for axis in _ORDERS}
        axes = AxisStates(**{axis: _ENUMS[axis](value) for axis, value in values.items()})
        projected.append(
            CuratedCapabilityFact(
                capability_id=definition.capability_id,
                title=definition.title,
                description=definition.description,
                axes=axes,
                selector_results=tuple(results),
                reason_codes=tuple(reasons),
            )
        )
    return CuratedCapabilityMap(
        format=CURATED_MAP_FORMAT,
        profile_id=profile.profile_id,
        profile_sha256=profile.sha256,
        source_inventory_root_sha256=verification.root_sha256,
        source_snapshot_uid=snapshot["snapshot_uid"],
        capabilities=tuple(sorted(projected, key=lambda item: item.capability_id)),
        test_receipt_sha256s=tuple(receipt_hashes),
    )


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


def write_curated_map(value: CuratedCapabilityMap, path: str | Path) -> str:
    target = Path(path).resolve()
    base = asdict(value)
    if not base.get("test_receipt_sha256s"):
        base.pop("test_receipt_sha256s", None)
    digest = canonical_sha256(base)
    _atomic_write(target, canonical_json_bytes({**base, "map_sha256": digest}) + b"\n")
    return digest


def load_curated_map(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"curated map is unreadable: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("format") != CURATED_MAP_FORMAT:
        raise ValueError("curated map format is unsupported")
    observed = payload.get("map_sha256")
    base = {key: value for key, value in payload.items() if key != "map_sha256"}
    if not isinstance(observed, str) or observed != canonical_sha256(base):
        raise ValueError("curated map hash mismatch")
    if not isinstance(payload.get("capabilities"), list):
        raise ValueError("curated map capabilities are invalid")
    ids = [row.get("capability_id") for row in payload["capabilities"] if isinstance(row, dict)]
    if len(ids) != len(payload["capabilities"]) or any(not isinstance(uid, str) or not uid for uid in ids) or len(ids) != len(set(ids)):
        raise ValueError("curated map capability identifiers are invalid")
    if ids != sorted(ids):
        raise ValueError("curated map capabilities are not identifier-sorted")
    return payload


def explain_curated_map(path: str | Path, capability_id: str) -> dict[str, Any]:
    payload = load_curated_map(path)
    matches = [row for row in payload["capabilities"] if row["capability_id"] == capability_id]
    if not matches:
        raise LookupError(f"curated capability not found: {capability_id}")
    return {
        "capability": matches[0],
        "profile_id": payload["profile_id"],
        "profile_sha256": payload["profile_sha256"],
        "source_inventory_root_sha256": payload["source_inventory_root_sha256"],
        "source_snapshot_uid": payload["source_snapshot_uid"],
        "test_receipt_sha256s": payload.get("test_receipt_sha256s", []),
    }


def diff_curated_maps(before_path: str | Path, after_path: str | Path) -> dict[str, Any]:
    before = load_curated_map(before_path)
    after = load_curated_map(after_path)
    if before.get("profile_sha256") != after.get("profile_sha256"):
        raise ValueError("curated map profile hash mismatch")
    before_rows = {row["capability_id"]: row for row in before["capabilities"]}
    after_rows = {row["capability_id"]: row for row in after["capabilities"]}
    ids = sorted(set(before_rows) | set(after_rows))
    changes = []
    for capability_id in ids:
        old = before_rows.get(capability_id)
        new = after_rows.get(capability_id)
        if old != new:
            changes.append({"capability_id": capability_id, "before": old, "after": new})
    return {
        "format": "repo-cartographer-curated-diff/v1",
        "profile_id": before.get("profile_id"),
        "profile_sha256": before.get("profile_sha256"),
        "before_source_inventory_root_sha256": before.get("source_inventory_root_sha256"),
        "after_source_inventory_root_sha256": after.get("source_inventory_root_sha256"),
        "changes": changes,
    }


__all__ = [
    "CURATED_MAP_FORMAT",
    "diff_curated_maps",
    "explain_curated_map",
    "load_curated_map",
    "project_curated_map",
    "write_curated_map",
]
