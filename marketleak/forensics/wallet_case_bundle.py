"""Verified loader for immutable public-wallet historical case bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from marketleak.domain import TradeFill
from marketleak.ingestion.coverage import CoverageRecord
from marketleak.ingestion.normalize import utc_datetime

from .polymarket_wallet_replay import (
    PolymarketWalletCase,
    WalletForensicReplayReport,
    WalletReplayCoverage,
    build_polymarket_wallet_forensic_replay,
)


class WalletCaseBundleError(ValueError):
    """A frozen bundle failed structural, checksum, or policy verification."""


def _required_text(value: Any, *, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise WalletCaseBundleError(f"{field_name} must be non-empty")
    return text


def _mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WalletCaseBundleError(f"{field_name} must be an object")
    return value


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(path.read_text(encoding="utf-8")), field_name=str(path))
    except (OSError, json.JSONDecodeError) as exc:
        raise WalletCaseBundleError(f"cannot read valid JSON from {path}") from exc


def _safe_file(root: Path, relative: str) -> Path:
    relative_path = Path(_required_text(relative, field_name="bundle file path"))
    if relative_path.is_absolute():
        raise WalletCaseBundleError("bundle file path must be relative")
    resolved = (root / relative_path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise WalletCaseBundleError("bundle file path escapes the bundle root") from exc
    if not resolved.is_file():
        raise WalletCaseBundleError(f"bundle file is missing: {relative}")
    return resolved


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verified_checksums(root: Path) -> dict[str, str]:
    checksum_path = _safe_file(root, "checksums.sha256")
    result: dict[str, str] = {}
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            digest, relative = line.split("  ", 1)
        except ValueError as exc:
            raise WalletCaseBundleError("checksums.sha256 contains a malformed row") from exc
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise WalletCaseBundleError("checksums.sha256 contains an invalid digest")
        if relative in result:
            raise WalletCaseBundleError("checksums.sha256 contains a duplicate path")
        actual = _file_hash(_safe_file(root, relative))
        if actual != digest:
            raise WalletCaseBundleError(f"bundle checksum mismatch: {relative}")
        result[relative] = digest
    for required in ("manifest.json", "case.json", "coverage/ledger.jsonl", "fills.jsonl"):
        if required not in result:
            raise WalletCaseBundleError(f"checksums.sha256 does not bind {required}")
    return result


def _coverage_record(path: Path) -> CoverageRecord:
    rows = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(rows) != 1:
        raise WalletCaseBundleError("wallet case bundle must contain exactly one coverage row")
    try:
        payload = json.loads(rows[0])
        complete = payload["complete"]
        if not isinstance(complete, bool):
            raise WalletCaseBundleError("coverage complete must be a JSON boolean")
        return CoverageRecord(
            platform=payload["platform"],
            dataset=payload["dataset"],
            interval_start=utc_datetime(payload["interval_start"]),
            interval_end=utc_datetime(payload["interval_end"]),
            fetched_at=utc_datetime(payload["fetched_at"]),
            record_count=int(payload["record_count"]),
            complete=complete,
            raw_sha256=tuple(payload.get("raw_sha256", ())),
            continuation=payload.get("continuation"),
            filters=payload.get("filters", {}),
        )
    except WalletCaseBundleError:
        raise
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise WalletCaseBundleError("coverage ledger row is malformed") from exc


def _fills(path: Path) -> tuple[TradeFill, ...]:
    result: list[TradeFill] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            result.append(TradeFill.model_validate_json(line))
        except ValueError as exc:
            raise WalletCaseBundleError(f"fills.jsonl row {index} is invalid") from exc
    return tuple(result)


@dataclass(frozen=True, slots=True)
class LoadedWalletCaseBundle:
    root: Path
    bundle_uid: str
    manifest_sha256: str
    case_sha256: str
    coverage_sha256: str
    fills_sha256: str
    replay_report_file_sha256: str
    frozen_replay_report_bytes: bytes
    reconstructed_at: datetime
    case: PolymarketWalletCase
    coverage: WalletReplayCoverage
    fills: tuple[TradeFill, ...]

    @property
    def captured_record_count(self) -> int:
        return len(self.fills)

    def replay(self) -> WalletForensicReplayReport:
        return build_polymarket_wallet_forensic_replay(
            self.case,
            reconstructed_at=self.reconstructed_at,
            coverage=self.coverage,
            fills=self.fills,
        )


def load_wallet_case_bundle(root: str | Path) -> LoadedWalletCaseBundle:
    """Verify and load one self-contained, non-training historical bundle."""

    bundle_root = Path(root).resolve()
    if not bundle_root.is_dir():
        raise WalletCaseBundleError("wallet case bundle root must be a directory")
    checksums = _verified_checksums(bundle_root)
    manifest = _read_json(_safe_file(bundle_root, "manifest.json"))
    case_payload = _read_json(_safe_file(bundle_root, _required_text(manifest.get("case_file"), field_name="case_file")))

    files = manifest.get("files")
    if not isinstance(files, list):
        raise WalletCaseBundleError("manifest files must be a list")
    manifest_file_hashes: dict[str, str] = {}
    for entry_value in files:
        entry = _mapping(entry_value, field_name="manifest file entry")
        relative = _required_text(entry.get("path"), field_name="manifest file path")
        path = _safe_file(bundle_root, relative)
        expected_hash = _required_text(entry.get("sha256"), field_name="manifest file sha256")
        if relative in manifest_file_hashes:
            raise WalletCaseBundleError("manifest files contains a duplicate path")
        manifest_file_hashes[relative] = expected_hash
        if _file_hash(path) != expected_hash or checksums.get(relative) != expected_hash:
            raise WalletCaseBundleError(f"manifest does not bind exact bytes for {relative}")
        if path.stat().st_size != entry.get("byte_length"):
            raise WalletCaseBundleError(f"manifest byte length mismatch for {relative}")

    policy = _mapping(manifest.get("policy"), field_name="manifest policy")
    replay_policy = _mapping(case_payload.get("replay_policy"), field_name="case replay_policy")
    if policy.get("training_eligible") is not False or replay_policy.get("training_eligible") is not False:
        raise WalletCaseBundleError("historical replay bundle must be non-training")
    if replay_policy.get("retrospective_forensic") != "hindsight_reconstructed":
        raise WalletCaseBundleError("case replay mode must be hindsight_reconstructed")

    case = PolymarketWalletCase(
        case_uid=_required_text(case_payload.get("case_uid"), field_name="case_uid"),
        actor_uid=_required_text(case_payload.get("actor_uid"), field_name="actor_uid"),
        historical_cutoff=utc_datetime(case_payload.get("decision_cutoff"), "decision_cutoff"),
    )
    coverage_path = _safe_file(
        bundle_root,
        _required_text(manifest.get("coverage_file"), field_name="coverage_file"),
    )
    fills_path = _safe_file(
        bundle_root,
        _required_text(manifest.get("canonical_fills_file"), field_name="canonical_fills_file"),
    )
    replay_report_relative = _required_text(
        manifest.get("replay_report_file"),
        field_name="replay_report_file",
    )
    replay_report_path = _safe_file(bundle_root, replay_report_relative)
    if replay_report_relative not in checksums:
        raise WalletCaseBundleError("checksums.sha256 does not bind replay_report_file")
    if manifest_file_hashes.get(replay_report_relative) != checksums[replay_report_relative]:
        raise WalletCaseBundleError("manifest files does not bind replay_report_file")
    frozen_report_bytes = replay_report_path.read_bytes()
    try:
        frozen_report_payload = _mapping(
            json.loads(frozen_report_bytes),
            field_name="replay_report_file",
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WalletCaseBundleError("replay_report_file is not valid JSON") from exc
    coverage_record = _coverage_record(coverage_path)
    fills = _fills(fills_path)
    sources = {item.source_uid for item in fills}
    if len(sources) != 1:
        raise WalletCaseBundleError("bundle fills must have exactly one source_uid")
    coverage = WalletReplayCoverage.from_coverage_record(
        coverage_record,
        source_uid=next(iter(sources)),
        queried_actor_uid=case.actor_uid,
    )
    declared_wallet = _required_text(case_payload.get("wallet"), field_name="case wallet").lower()
    if case.actor_uid != f"polymarket:wallet/{declared_wallet}":
        raise WalletCaseBundleError("case wallet and actor_uid do not agree")
    query_wallet = coverage.filter_value("user")
    if not isinstance(query_wallet, str) or query_wallet.lower() != declared_wallet:
        raise WalletCaseBundleError("coverage query is not bound to the declared case wallet")

    collection = _mapping(case_payload.get("collection"), field_name="case collection")
    collection_query_uid = _required_text(collection.get("query_uid"), field_name="collection query_uid")
    manifest_query_uid = _required_text(manifest.get("query_uid"), field_name="manifest query_uid")
    replay_output = _mapping(case_payload.get("replay_output"), field_name="case replay_output")
    replay_query_uid = _required_text(replay_output.get("query_uid"), field_name="replay_output query_uid")
    if {coverage.query_uid, collection_query_uid, manifest_query_uid, replay_query_uid} != {coverage.query_uid}:
        raise WalletCaseBundleError("derived and declared wallet query_uid values do not agree")
    if _required_text(replay_output.get("path"), field_name="replay_output path") != replay_report_relative:
        raise WalletCaseBundleError("case replay_output path does not match manifest replay_report_file")

    declared_count = manifest.get("record_count")
    if declared_count != len(fills) or coverage.record_count != len(fills) or collection.get("record_count") != len(fills):
        raise WalletCaseBundleError("bundle record counts do not agree")
    if {item.actor_uid for item in fills} != {case.actor_uid}:
        raise WalletCaseBundleError("bundle contains a fill for a different actor")
    raw_digest = _required_text(manifest.get("raw_object_sha256"), field_name="raw_object_sha256")
    if coverage.raw_sha256 != (raw_digest,):
        raise WalletCaseBundleError("coverage raw lineage does not match manifest raw object")

    loaded = LoadedWalletCaseBundle(
        root=bundle_root,
        bundle_uid=_required_text(manifest.get("bundle_uid"), field_name="bundle_uid"),
        manifest_sha256=checksums["manifest.json"],
        case_sha256=checksums["case.json"],
        coverage_sha256=checksums["coverage/ledger.jsonl"],
        fills_sha256=checksums["fills.jsonl"],
        replay_report_file_sha256=checksums[replay_report_relative],
        frozen_replay_report_bytes=frozen_report_bytes,
        reconstructed_at=utc_datetime(manifest.get("created_at"), "created_at"),
        case=case,
        coverage=coverage,
        fills=fills,
    )
    derived_report = loaded.replay()
    derived_bytes = derived_report.canonical_bytes()
    if derived_bytes != frozen_report_bytes:
        raise WalletCaseBundleError("derived replay does not reproduce replay_report_file bytes")

    derived_analysis_hash = derived_report.analysis_input_sha256
    derived_report_hash = derived_report.report_sha256
    declared_analysis_hashes = {
        _required_text(manifest.get("analysis_input_sha256"), field_name="manifest analysis_input_sha256"),
        _required_text(replay_output.get("analysis_input_sha256"), field_name="replay_output analysis_input_sha256"),
        _required_text(frozen_report_payload.get("analysis_input_sha256"), field_name="frozen report analysis_input_sha256"),
    }
    if declared_analysis_hashes != {derived_analysis_hash}:
        raise WalletCaseBundleError("derived and declared analysis_input_sha256 values do not agree")
    declared_report_hashes = {
        _required_text(manifest.get("report_sha256"), field_name="manifest report_sha256"),
        _required_text(replay_output.get("report_sha256"), field_name="replay_output report_sha256"),
        _required_text(frozen_report_payload.get("report_sha256"), field_name="frozen report report_sha256"),
    }
    if declared_report_hashes != {derived_report_hash}:
        raise WalletCaseBundleError("derived and declared report_sha256 values do not agree")

    derived_file_hash = sha256(derived_bytes).hexdigest()
    declared_file_hashes = {
        checksums[replay_report_relative],
        _required_text(
            manifest.get("replay_report_canonical_file_sha256"),
            field_name="manifest replay_report_canonical_file_sha256",
        ),
        _required_text(replay_output.get("canonical_file_sha256"), field_name="replay_output canonical_file_sha256"),
    }
    if declared_file_hashes != {derived_file_hash}:
        raise WalletCaseBundleError("derived and declared replay report file hashes do not agree")
    if replay_output.get("byte_length") != len(derived_bytes):
        raise WalletCaseBundleError("case replay_output byte_length does not match derived report")
    return loaded
