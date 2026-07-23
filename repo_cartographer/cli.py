"""Command-line and library orchestration for the Repo Cartographer MVP."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Sequence

from .artifacts import inspect_artifacts
from .canonical import canonical_json_bytes, sha256_file
from .config import load_scan_config
from .curated import diff_curated_maps, explain_curated_map, project_curated_map, write_curated_map
from .curated_config import load_curated_profile
from .discovery import discover_repository
from .domain import Inventory, ScanConfig
from .render import MANIFEST_NAME, load_jsonl, write_inventory
from .snapshot import SnapshotError, snapshot_repository
from .status import derive_capabilities, derive_components
from .verify import verify_inventory
from .test_receipts import run_pytest_receipt, verify_test_receipt
from .test_runner_config import load_pytest_runner_config


EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2


def scan_repository(
    root: str | Path,
    *,
    config: ScanConfig | None = None,
    output_dir: str | Path | None = None,
) -> Inventory:
    """Run the bounded static scan and optionally render its canonical output."""

    effective_config = config or ScanConfig()
    repository_root = Path(root).resolve()
    if output_dir is not None:
        output_root = Path(output_dir).resolve()
        git_root = repository_root / ".git"
        if output_root == repository_root or output_root == git_root or git_root in output_root.parents:
            raise ValueError("inventory output must not be the repository root or reside inside .git")
    snapshot, files = snapshot_repository(repository_root, effective_config)
    with tempfile.TemporaryDirectory(prefix="repo-cartographer-frozen-") as frozen_name:
        frozen_root = Path(frozen_name)
        for file in files:
            if file.sensitive:
                continue
            source = (repository_root / file.path).resolve()
            try:
                source.relative_to(repository_root)
            except ValueError as exc:
                raise SnapshotError(f"repository file escapes root during capture: {file.path}") from exc
            if source.is_symlink() or not source.is_file():
                raise SnapshotError(f"repository file unavailable during capture: {file.path}")
            destination = frozen_root / file.path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            if destination.stat().st_size != file.byte_size or sha256_file(destination) != file.sha256:
                raise SnapshotError(f"repository file changed during frozen capture: {file.path}")
        discovery_files = tuple(file for file in files if file.parse_status == "pending")
        discovery = discover_repository(frozen_root, snapshot, discovery_files)
        artifacts, artifact_evidence = inspect_artifacts(frozen_root, files, effective_config)
    final_snapshot, final_files = snapshot_repository(repository_root, effective_config)
    if final_snapshot != snapshot or final_files != files:
        raise SnapshotError("repository changed during static discovery; no mixed-state inventory was emitted")
    components, status_evidence = derive_components(snapshot, files, discovery, artifacts)
    evidence_by_uid = {}
    for item in (*discovery.evidence, *artifact_evidence, *status_evidence):
        existing = evidence_by_uid.get(item.evidence_uid)
        if existing is not None and existing != item:
            raise ValueError(f"conflicting evidence records share identifier {item.evidence_uid}")
        evidence_by_uid[item.evidence_uid] = item
    evidence = tuple(evidence_by_uid[uid] for uid in sorted(evidence_by_uid))
    capabilities = derive_capabilities(components, evidence)
    inventory = Inventory(
        manifest=snapshot,
        files=files,
        symbols=discovery.symbols,
        imports=discovery.imports,
        entrypoints=discovery.entrypoints,
        tests=discovery.tests,
        scenarios=discovery.scenarios,
        artifacts=artifacts,
        evidence=evidence,
        components=components,
        capabilities=capabilities,
    )
    if output_dir is not None:
        write_inventory(inventory, output_dir)
    return inventory


def explain_inventory(output_dir: str | Path, capability_query: str) -> dict[str, Any]:
    """Resolve one capability and its evidence using rendered output only."""

    root = Path(output_dir).resolve()
    verification = verify_inventory(root)
    if not verification.ok:
        raise ValueError("inventory verification failed: " + "; ".join(verification.errors))

    capabilities = load_jsonl(root / "capabilities.jsonl")
    query = capability_query.strip()
    if not query:
        raise ValueError("capability query must not be empty")
    matches = [item for item in capabilities if item.get("capability_uid") == query]
    if not matches:
        normalized = query.casefold().replace("_", " ").replace("-", " ")
        matches = [item for item in capabilities if str(item.get("name", "")).casefold() == normalized]
    if not matches:
        raise LookupError(f"capability not found: {query}")
    if len(matches) != 1:
        raise LookupError(f"capability query is ambiguous: {query}")

    capability = matches[0]
    components_by_uid = {item["component_uid"]: item for item in load_jsonl(root / "components.jsonl")}
    evidence_by_uid = {item["evidence_uid"]: item for item in load_jsonl(root / "evidence.jsonl")}
    files_by_uid = {item["file_uid"]: item for item in load_jsonl(root / "files.jsonl")}

    def expand_evidence(uid: str) -> dict[str, Any]:
        fact = dict(evidence_by_uid[uid])
        source = files_by_uid.get(str(fact.get("source_file_uid")))
        if source is not None:
            fact["source_path"] = source.get("path")
        return fact

    return {
        "capability": capability,
        "components": [components_by_uid[uid] for uid in capability.get("component_uids", ())],
        "refuting_evidence": [expand_evidence(uid) for uid in capability.get("refuting_evidence_uids", ())],
        "supporting_evidence": [expand_evidence(uid) for uid in capability.get("supporting_evidence_uids", ())],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m repo_cartographer")
    commands = parser.add_subparsers(dest="command", required=True)

    scan = commands.add_parser("scan", help="scan a Git working tree without executing its code")
    scan.add_argument("root_positional", nargs="?")
    scan.add_argument("--root", dest="root_option")
    scan.add_argument("--config", type=Path)
    scan.add_argument("--output", "-o", type=Path, required=True)
    scan.add_argument("--include-untracked", action=argparse.BooleanOptionalAction, default=None)

    verify = commands.add_parser("verify", help="verify a rendered inventory")
    verify.add_argument("inventory_positional", nargs="?", type=Path)
    verify.add_argument("--inventory", dest="inventory_option", type=Path)

    explain = commands.add_parser("explain", help="explain one rendered capability")
    explain.add_argument("inventory_positional", nargs="?", type=Path)
    explain.add_argument("capability_positional", nargs="?")
    explain.add_argument("--inventory", dest="inventory_option", type=Path)
    explain.add_argument("--capability", dest="capability_option")

    map_command = commands.add_parser("map", help="project a verified inventory through a strict curated profile")
    map_command.add_argument("--inventory", type=Path, required=True)
    map_command.add_argument("--profile", type=Path, required=True)
    map_command.add_argument("--output", type=Path, required=True)
    map_command.add_argument("--test-receipt", type=Path, action="append", default=[])

    map_explain = commands.add_parser("map-explain", help="explain one stable capability in a curated map")
    map_explain.add_argument("--map", dest="map_path", type=Path, required=True)
    map_explain.add_argument("--capability", required=True)

    map_diff = commands.add_parser("map-diff", help="diff curated maps built from the identical profile hash")
    map_diff.add_argument("--before", type=Path, required=True)
    map_diff.add_argument("--after", type=Path, required=True)

    run_tests = commands.add_parser("run-tests", help="execute approved tests from a verified frozen inventory copy")
    run_tests.add_argument("--inventory", type=Path, required=True)
    run_tests.add_argument("--root", type=Path, required=True)
    run_tests.add_argument("--config", type=Path, required=True)
    run_tests.add_argument("--output", type=Path, required=True)
    run_tests.add_argument("--test-uid", action="append", default=[])
    run_tests.add_argument("--test-path", action="append", default=[])

    verify_receipt = commands.add_parser("verify-receipt", help="verify an immutable pytest receipt against its inventory")
    verify_receipt.add_argument("--receipt", type=Path, required=True)
    verify_receipt.add_argument("--inventory", type=Path, required=True)
    verify_receipt.add_argument("--config", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "scan":
            config = load_scan_config(args.config) if args.config else ScanConfig()
            if args.include_untracked is not None:
                config = replace(config, include_untracked=args.include_untracked)
            root = args.root_option or args.root_positional or "."
            inventory = scan_repository(root, config=config, output_dir=args.output)
            manifest = json.loads((args.output.resolve() / MANIFEST_NAME).read_text(encoding="utf-8"))
            print(json.dumps({"snapshot_uid": inventory.manifest.snapshot_uid, "root_sha256": manifest["root_sha256"], "output": str(args.output.resolve())}, sort_keys=True))
            return EXIT_OK
        if args.command == "verify":
            inventory_path = args.inventory_option or args.inventory_positional
            if inventory_path is None:
                raise ValueError("verify requires --inventory PATH or a positional inventory path")
            result = verify_inventory(inventory_path)
            stream = sys.stdout if result.ok else sys.stderr
            print(json.dumps({"ok": result.ok, "root_sha256": result.root_sha256, "errors": result.errors, "counts": result.counts}, sort_keys=True), file=stream)
            return EXIT_OK if result.ok else EXIT_FAILED
        if args.command == "explain":
            inventory_path = args.inventory_option or args.inventory_positional
            capability = args.capability_option or args.capability_positional
            if inventory_path is None or capability is None:
                raise ValueError("explain requires inventory and capability arguments")
            explanation = explain_inventory(inventory_path, capability)
            sys.stdout.buffer.write(canonical_json_bytes(explanation) + b"\n")
            return EXIT_OK
        if args.command == "map":
            profile = load_curated_profile(args.profile)
            mapped = project_curated_map(args.inventory, profile, test_receipts=args.test_receipt)
            map_sha256 = write_curated_map(mapped, args.output)
            print(json.dumps({
                "capability_count": len(mapped.capabilities),
                "map_sha256": map_sha256,
                "output": str(args.output.resolve()),
                "profile_sha256": mapped.profile_sha256,
                "source_inventory_root_sha256": mapped.source_inventory_root_sha256,
                "test_receipt_sha256s": mapped.test_receipt_sha256s,
            }, sort_keys=True))
            return EXIT_OK
        if args.command == "map-explain":
            sys.stdout.buffer.write(canonical_json_bytes(explain_curated_map(args.map_path, args.capability)) + b"\n")
            return EXIT_OK
        if args.command == "map-diff":
            sys.stdout.buffer.write(canonical_json_bytes(diff_curated_maps(args.before, args.after)) + b"\n")
            return EXIT_OK
        if args.command == "run-tests":
            receipt = run_pytest_receipt(
                args.inventory,
                load_pytest_runner_config(args.config),
                args.output,
                repository_root=args.root,
                test_uids=args.test_uid,
                test_paths=args.test_path,
            )
            print(json.dumps({
                "exit_code": receipt["results"]["exit_code"],
                "output": str(args.output.resolve()),
                "promotable": receipt["results"]["promotable"],
                "receipt_sha256": receipt["receipt_sha256"],
            }, sort_keys=True))
            return EXIT_OK
        if args.command == "verify-receipt":
            config = load_pytest_runner_config(args.config) if args.config else None
            receipt = verify_test_receipt(args.receipt, args.inventory, config)
            print(json.dumps({"ok": True, "promotable": receipt["results"]["promotable"], "receipt_sha256": receipt["receipt_sha256"]}, sort_keys=True))
            return EXIT_OK
    except (OSError, RuntimeError, TypeError, ValueError, LookupError) as exc:
        print(f"repo-cartographer: {exc}", file=sys.stderr)
        return EXIT_FAILED
    return EXIT_USAGE


__all__ = [
    "EXIT_FAILED",
    "EXIT_OK",
    "EXIT_USAGE",
    "explain_inventory",
    "diff_curated_maps",
    "explain_curated_map",
    "main",
    "project_curated_map",
    "scan_repository",
    "write_curated_map",
]
