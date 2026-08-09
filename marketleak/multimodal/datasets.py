"""Deterministic, offline freezing of canonical prediction-market fill corpora.

The freezer in this module is deliberately observational.  It authenticates
what is present under an explicitly selected local root, but it never converts
a clean hash tree into a claim of historical source completeness and it never
creates labels or effectiveness evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Self

from pydantic import ConfigDict, Field, model_validator

from marketleak.domain import TradeFill
from marketleak.domain.common import NonEmptyStr, StableUID, StrictDomainModel
from marketleak.ingestion.coverage import CoverageRecord
from marketleak.ingestion.normalize import canonical_json_bytes


FREEZER_VERSION = "trade-fill-observational-freezer-v1"
SELECTION_POLICY_VERSION = "explicit-platform-source-v1"
_READ_WORKERS = 8
_READ_BATCH_SIZE = 256
_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "receipt_id",
        "sha256",
        "byte_length",
        "platform",
        "source",
        "received_at",
        "request",
        "response_metadata",
    }
)
_RECEIPT_ID_RE = re.compile(r"^(?P<clock>\d{8}T\d{6}\.\d{6}Z)-[0-9a-f]{32}$")
_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class CorpusFreezeError(RuntimeError):
    """Raised when a source cannot be frozen without guessing or repairing it."""


class CanonicalCorpusFileError(CorpusFreezeError):
    """A purported canonical TradeFill file is malformed or non-canonical."""


class ManifestVerificationError(CorpusFreezeError):
    """A manifest is non-canonical, self-inconsistent, or no longer reproducible."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _utc(value, "timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _hash_rows(rows: Iterable[object]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_json_bytes(row))
        digest.update(b"\n")
    return digest.hexdigest()


def _canonical_filter_json(filters: Mapping[str, Any]) -> str:
    try:
        payload = canonical_json_bytes(dict(filters))
        decoded = json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("coverage_filters must be canonical-JSON compatible") from exc
    if not isinstance(decoded, dict):
        raise ValueError("coverage_filters must encode a JSON object")
    return payload.decode("utf-8")


def _iter_json_files(root: Path) -> Iterable[Path]:
    """Yield local JSON files in stable path order without materializing the tree."""

    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_names.sort()
        for directory_name in directory_names:
            linked_directory = Path(directory) / directory_name
            if linked_directory.is_symlink():
                raise CanonicalCorpusFileError(
                    f"canonical source directories cannot be symlinks: {linked_directory}"
                )
        for file_name in sorted(file_names):
            if file_name.endswith(".json"):
                path = Path(directory) / file_name
                if path.is_symlink():
                    raise CanonicalCorpusFileError(f"canonical source files cannot be symlinks: {path}")
                yield path


class ReceiptSourceContract(StrictDomainModel):
    """Exact mapping from a canonical source UID to raw receipt source names."""

    source_uid: StableUID
    receipt_sources: tuple[NonEmptyStr, ...]

    @model_validator(mode="after")
    def validate_sources(self) -> Self:
        if not self.receipt_sources:
            raise ValueError("receipt_sources cannot be empty")
        if len(set(self.receipt_sources)) != len(self.receipt_sources):
            raise ValueError("receipt_sources cannot contain duplicates")
        if tuple(sorted(self.receipt_sources)) != self.receipt_sources:
            raise ValueError("receipt_sources must be sorted")
        return self


class CorpusSelectionPolicy(StrictDomainModel):
    """The explicit, immutable boundary of an observational corpus freeze."""

    policy_version: Literal["explicit-platform-source-v1"] = SELECTION_POLICY_VERSION
    source_root_label: NonEmptyStr
    platform: NonEmptyStr
    source_uids: tuple[StableUID, ...]
    record_type: Literal["TradeFill"] = "TradeFill"
    file_pattern: Literal["**/*.json"] = "**/*.json"
    coverage_dataset: NonEmptyStr
    coverage_filters_json: NonEmptyStr
    receipt_source_contracts: tuple[ReceiptSourceContract, ...]

    @model_validator(mode="after")
    def validate_selection(self) -> Self:
        if self.platform != self.platform.lower():
            raise ValueError("platform must be lowercase")
        if not self.source_uids:
            raise ValueError("source_uids cannot be empty")
        if tuple(sorted(self.source_uids)) != self.source_uids or len(set(self.source_uids)) != len(self.source_uids):
            raise ValueError("source_uids must be sorted and unique")
        contracts = tuple(item.source_uid for item in self.receipt_source_contracts)
        if contracts != tuple(sorted(contracts)) or len(set(contracts)) != len(contracts):
            raise ValueError("receipt_source_contracts must be sorted and unique by source_uid")
        if set(contracts) != set(self.source_uids):
            raise ValueError("every selected source_uid requires exactly one receipt source contract")
        try:
            filters = json.loads(self.coverage_filters_json)
        except json.JSONDecodeError as exc:
            raise ValueError("coverage_filters_json is invalid") from exc
        if not isinstance(filters, dict) or canonical_json_bytes(filters).decode("utf-8") != self.coverage_filters_json:
            raise ValueError("coverage_filters_json must be a canonical JSON object")
        return self

    @property
    def coverage_filters(self) -> dict[str, Any]:
        return dict(json.loads(self.coverage_filters_json))

    @property
    def receipt_sources_by_source_uid(self) -> dict[str, frozenset[str]]:
        return {
            contract.source_uid: frozenset(contract.receipt_sources)
            for contract in self.receipt_source_contracts
        }


class RevisionBinding(StrictDomainModel):
    code_revision: NonEmptyStr
    code_revision_binding: Literal["caller_asserted"] = "caller_asserted"
    freezer_version: Literal["trade-fill-observational-freezer-v1"] = FREEZER_VERSION
    freezer_code_sha256: str = Field(pattern=_SHA256_PATTERN)
    source_contract_version: NonEmptyStr
    trade_fill_schema_versions: tuple[NonEmptyStr, ...]
    parser_versions: tuple[NonEmptyStr, ...]


class CorpusFileSummary(StrictDomainModel):
    canonical_file_count: int = Field(ge=0)
    selected_file_count: int = Field(ge=0)
    excluded_file_count: int = Field(ge=0)
    unique_record_count: int = Field(ge=0)
    duplicate_file_count: int = Field(ge=0)
    conflict_uid_count: int = Field(ge=0)
    unique_record_version_count: int = Field(ge=0)
    source_file_set_sha256: str = Field(pattern=_SHA256_PATTERN)
    selected_record_set_sha256: str = Field(pattern=_SHA256_PATTERN)
    excluded_platform_counts: tuple[tuple[str, int], ...]
    excluded_source_counts: tuple[tuple[str, int], ...]

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.selected_file_count + self.excluded_file_count != self.canonical_file_count:
            raise ValueError("selected and excluded file counts must equal canonical_file_count")
        if self.unique_record_count > self.selected_file_count:
            raise ValueError("unique_record_count cannot exceed selected_file_count")
        if self.unique_record_version_count < self.unique_record_count:
            raise ValueError("unique_record_version_count cannot be below unique_record_count")
        if self.unique_record_version_count + self.duplicate_file_count != self.selected_file_count:
            raise ValueError("record versions and duplicate files must equal selected_file_count")
        return self


class CorpusPopulationSummary(StrictDomainModel):
    min_event_time: datetime | None
    max_event_time: datetime | None
    min_ingested_at: datetime | None
    max_ingested_at: datetime | None
    unique_actor_count: int = Field(ge=0)
    unique_market_count: int = Field(ge=0)
    unique_outcome_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_clocks(self) -> Self:
        for field_name in ("min_event_time", "max_event_time", "min_ingested_at", "max_ingested_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _utc(value, field_name))
        if self.min_event_time is not None and self.max_event_time is not None and self.min_event_time > self.max_event_time:
            raise ValueError("event clock bounds are reversed")
        if self.min_ingested_at is not None and self.max_ingested_at is not None and self.min_ingested_at > self.max_ingested_at:
            raise ValueError("ingestion clock bounds are reversed")
        return self


class RawLineageAudit(StrictDomainModel):
    unique_raw_artifact_count: int = Field(ge=0)
    verified_raw_object_count: int = Field(ge=0)
    missing_raw_object_count: int = Field(ge=0)
    corrupt_raw_object_count: int = Field(ge=0)
    raw_uid_format_error_count: int = Field(ge=0)
    matching_receipt_count: int = Field(ge=0)
    raw_objects_with_receipt_count: int = Field(ge=0)
    missing_receipt_count: int = Field(ge=0)
    corrupt_relevant_receipt_count: int = Field(ge=0)
    nonmatching_replay_receipt_count: int = Field(ge=0)
    raw_object_set_sha256: str = Field(pattern=_SHA256_PATTERN)
    matching_receipt_set_sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_totals(self) -> Self:
        if (
            self.verified_raw_object_count
            + self.missing_raw_object_count
            + self.corrupt_raw_object_count
            + self.raw_uid_format_error_count
            != self.unique_raw_artifact_count
        ):
            raise ValueError("raw lineage outcome counts must cover every unique raw artifact")
        if self.raw_objects_with_receipt_count + self.missing_receipt_count != self.unique_raw_artifact_count:
            raise ValueError("receipt outcome counts must cover every unique raw artifact")
        return self


class CoverageLedgerSummary(StrictDomainModel):
    ledger_present: bool
    coverage_ledger_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    relevant_row_count: int = Field(ge=0)
    exact_selection_row_count: int = Field(ge=0)
    complete_row_count: int = Field(ge=0)
    incomplete_row_count: int = Field(ge=0)
    continuation_row_count: int = Field(ge=0)
    declared_record_count: int = Field(ge=0)
    min_interval_start: datetime | None
    max_interval_end: datetime | None
    selected_raw_artifacts_declared_count: int = Field(ge=0)
    selected_raw_artifacts_undeclared_count: int = Field(ge=0)
    status: Literal["complete", "partial", "unknown"]
    absence_claims_permitted: bool
    reason_codes: tuple[NonEmptyStr, ...]

    @model_validator(mode="after")
    def validate_coverage(self) -> Self:
        if self.ledger_present != (self.coverage_ledger_sha256 is not None):
            raise ValueError("coverage ledger presence and hash disagree")
        if self.complete_row_count + self.incomplete_row_count != self.exact_selection_row_count:
            raise ValueError("complete and incomplete rows must cover the exact selection rows")
        if self.status != "complete" and self.absence_claims_permitted:
            raise ValueError("absence claims require mechanically complete coverage")
        for field_name in ("min_interval_start", "max_interval_end"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _utc(value, field_name))
        return self


class ObservationalCorpusManifest(StrictDomainModel):
    """Immutable manifest for an observational fill corpus, not a label set."""

    manifest_schema_version: Literal[1] = 1
    artifact_type: Literal["observational_trade_fill_corpus"] = "observational_trade_fill_corpus"
    status: Literal["ready", "ready_with_limitations", "failed_closed"]
    integrity_usable: bool
    observational_only: Literal[True] = True
    labels_included: Literal[False] = False
    effectiveness_status: Literal["effectiveness_unknown"] = "effectiveness_unknown"
    selection: CorpusSelectionPolicy
    revision: RevisionBinding
    files: CorpusFileSummary
    population: CorpusPopulationSummary
    lineage: RawLineageAudit
    coverage: CoverageLedgerSummary
    corpus_sha256: str = Field(pattern=_SHA256_PATTERN)
    failure_reasons: tuple[NonEmptyStr, ...]
    limitation_reasons: tuple[NonEmptyStr, ...]
    manifest_sha256: str = Field(pattern=_SHA256_PATTERN)

    def unsigned_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude={"manifest_sha256"})

    def expected_manifest_sha256(self) -> str:
        return _sha256(canonical_json_bytes(self.unsigned_payload()))

    def verify(self) -> None:
        if self.manifest_sha256 != self.expected_manifest_sha256():
            raise ManifestVerificationError("manifest_sha256 does not authenticate the manifest payload")
        expected_corpus = _sha256(
            canonical_json_bytes(
                {
                    "coverage_ledger_sha256": self.coverage.coverage_ledger_sha256,
                    "matching_receipt_set_sha256": self.lineage.matching_receipt_set_sha256,
                    "platform": self.selection.platform,
                    "raw_object_set_sha256": self.lineage.raw_object_set_sha256,
                    "selected_record_set_sha256": self.files.selected_record_set_sha256,
                    "selection_policy_sha256": _sha256(canonical_json_bytes(self.selection)),
                    "source_file_set_sha256": self.files.source_file_set_sha256,
                }
            )
        )
        if self.corpus_sha256 != expected_corpus:
            raise ManifestVerificationError("corpus_sha256 does not bind the manifest evidence hashes")
        if self.status == "failed_closed" and self.integrity_usable:
            raise ManifestVerificationError("a failed-closed corpus cannot be integrity-usable")
        if self.status != "failed_closed" and not self.integrity_usable:
            raise ManifestVerificationError("a non-failed corpus must be integrity-usable")

    def to_bytes(self) -> bytes:
        self.verify()
        return canonical_json_bytes(self)

    def write_new(self, path: str | Path) -> Path:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_bytes()
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.pending-",
            suffix=".tmp",
            dir=destination.parent,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                # A same-filesystem hard link publishes the fully fsynced inode
                # atomically and fails rather than replacing an existing winner.
                os.link(temporary, destination)
            except FileExistsError:
                try:
                    winner = destination.read_bytes()
                except OSError as exc:
                    raise ManifestVerificationError(
                        f"cannot verify the existing manifest publication winner: {destination}"
                    ) from exc
                if winner != payload:
                    raise ManifestVerificationError(
                        f"a different immutable manifest already exists: {destination}"
                    )
                ObservationalCorpusManifest.from_bytes(winner)
                return destination
            except OSError as exc:
                raise ManifestVerificationError(
                    "atomic no-overwrite manifest publication is unavailable"
                ) from exc
            try:
                published = destination.read_bytes()
            except OSError as exc:
                raise ManifestVerificationError(
                    f"cannot verify newly published manifest: {destination}"
                ) from exc
            if published != payload:
                raise ManifestVerificationError("published manifest bytes differ from the fsynced candidate")
            ObservationalCorpusManifest.from_bytes(published)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return destination

    @classmethod
    def from_bytes(cls, payload: bytes) -> Self:
        try:
            manifest = cls.model_validate_json(payload)
        except Exception as exc:
            raise ManifestVerificationError("manifest JSON does not match the frozen schema") from exc
        if payload != canonical_json_bytes(manifest):
            raise ManifestVerificationError("manifest bytes are not exact canonical JSON")
        manifest.verify()
        return manifest

    @classmethod
    def read(cls, path: str | Path) -> Self:
        try:
            payload = Path(path).read_bytes()
        except OSError as exc:
            raise ManifestVerificationError(f"cannot read manifest: {path}") from exc
        return cls.from_bytes(payload)


def make_selection_policy(
    *,
    source_root_label: str,
    platform: str,
    source_uids: Iterable[str],
    coverage_dataset: str,
    coverage_filters: Mapping[str, Any],
    receipt_sources_by_source_uid: Mapping[str, Iterable[str]],
) -> CorpusSelectionPolicy:
    """Build a normalized explicit policy without inferring source mappings."""

    normalized_sources = tuple(sorted(set(source_uids)))
    contracts = tuple(
        ReceiptSourceContract(
            source_uid=source_uid,
            receipt_sources=tuple(sorted(set(receipt_sources_by_source_uid.get(source_uid, ())))),
        )
        for source_uid in normalized_sources
    )
    return CorpusSelectionPolicy(
        source_root_label=source_root_label,
        platform=platform.strip().lower(),
        source_uids=normalized_sources,
        coverage_dataset=coverage_dataset,
        coverage_filters_json=_canonical_filter_json(coverage_filters),
        receipt_source_contracts=contracts,
    )


def _validate_fill_file(path: Path) -> tuple[TradeFill, bytes, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise CanonicalCorpusFileError(f"cannot read canonical TradeFill file: {path}") from exc
    try:
        fill = TradeFill.model_validate_json(raw)
    except Exception as exc:
        raise CanonicalCorpusFileError(f"invalid canonical TradeFill file: {path}") from exc
    if raw != canonical_json_bytes(fill):
        raise CanonicalCorpusFileError(f"TradeFill file is not exact canonical JSON: {path}")
    return fill, raw, _sha256(raw)


def _iter_validated_fill_files(root: Path) -> Iterable[tuple[Path, TradeFill, bytes, str]]:
    """Validate a stable file stream with bounded parallel small-file reads."""

    paths = iter(_iter_json_files(root))
    with ThreadPoolExecutor(max_workers=_READ_WORKERS, thread_name_prefix="corpus-freeze-read") as executor:
        while batch := tuple(islice(paths, _READ_BATCH_SIZE)):
            # executor.map preserves input order.  The bounded batch prevents
            # one Future per corpus file while hiding NTFS small-file latency.
            for path, (fill, payload, digest) in zip(batch, executor.map(_validate_fill_file, batch)):
                yield path, fill, payload, digest


def _parse_raw_uid(raw_uid: str, *, platform: str) -> str | None:
    prefix = f"{platform}:raw/"
    if not raw_uid.startswith(prefix):
        return None
    digest = raw_uid.removeprefix(prefix)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        return None
    return digest


def _audit_lineage(
    *,
    raw_root: Path,
    platform: str,
    raw_lineage_pairs: Mapping[str, set[tuple[str, str]]],
    receipt_sources_by_source_uid: Mapping[str, frozenset[str]],
) -> RawLineageAudit:
    raw_rows: list[object] = []
    expected_by_digest: dict[str, str] = {}
    verified = missing = corrupt = malformed = 0
    object_lengths: dict[str, int] = {}

    for raw_uid in sorted(raw_lineage_pairs):
        digest = _parse_raw_uid(raw_uid, platform=platform)
        if digest is None:
            malformed += 1
            raw_rows.append([raw_uid, "invalid_raw_artifact_uid"])
            continue
        expected_by_digest[digest] = raw_uid
        object_path = raw_root / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
        try:
            payload = object_path.read_bytes()
        except FileNotFoundError:
            missing += 1
            raw_rows.append([raw_uid, "missing", None, None])
            continue
        except OSError:
            corrupt += 1
            raw_rows.append([raw_uid, "unreadable", None, None])
            continue
        observed = _sha256(payload)
        object_lengths[digest] = len(payload)
        if observed != digest:
            corrupt += 1
            raw_rows.append([raw_uid, "corrupt", observed, len(payload)])
        else:
            verified += 1
            raw_rows.append([raw_uid, "verified", observed, len(payload)])

    valid_receipt_rows: list[tuple[str, str, str]] = []
    matching_digests: set[str] = set()
    corrupt_relevant_receipts = 0
    nonmatching_replay_receipts = 0
    receipts_root = raw_root / "receipts"
    if receipts_root.exists():
        for path in _iter_json_files(receipts_root):
            try:
                raw = path.read_bytes()
                item = json.loads(raw)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                # A receipt that cannot expose a digest cannot be deterministically
                # linked to this selected corpus, so it is outside this audit.
                continue
            digest_value = item.get("sha256") if isinstance(item, dict) else None
            if not isinstance(digest_value, str) or digest_value not in expected_by_digest:
                continue
            relative = path.relative_to(receipts_root).as_posix()
            try:
                if raw != canonical_json_bytes(item):
                    raise ValueError("non-canonical receipt")
                if set(item) != _RECEIPT_KEYS:
                    raise ValueError("receipt fields do not match RawArtifactStore schema")
                if type(item["schema_version"]) is not int or item["schema_version"] != 1:
                    raise ValueError("unsupported receipt schema")
                if type(item["byte_length"]) is not int or item["byte_length"] < 0:
                    raise ValueError("receipt byte_length must be a non-negative integer")
                if item["byte_length"] != object_lengths.get(digest_value):
                    raise ValueError("receipt byte length mismatch")
                receipt_id = item["receipt_id"]
                source = item["source"]
                received_at = item["received_at"]
                if not isinstance(receipt_id, str) or _RECEIPT_ID_RE.fullmatch(receipt_id) is None:
                    raise ValueError("receipt_id is invalid")
                if not isinstance(item["sha256"], str) or item["sha256"] != digest_value:
                    raise ValueError("receipt sha256 is invalid")
                if not isinstance(item["platform"], str) or not item["platform"]:
                    raise ValueError("receipt platform is invalid")
                if not isinstance(source, str) or not source or not isinstance(received_at, str):
                    raise ValueError("receipt source/time missing")
                if not isinstance(item["request"], dict) or not isinstance(item["response_metadata"], dict):
                    raise ValueError("receipt request/response provenance must be objects")
                raw_uid = expected_by_digest[digest_value]
                normalized_received = _time(datetime.fromisoformat(received_at.replace("Z", "+00:00")))
                if received_at != normalized_received:
                    raise ValueError("receipt received_at is not canonical UTC")
                receipt_match = _RECEIPT_ID_RE.fullmatch(receipt_id)
                assert receipt_match is not None
                if receipt_match.group("clock") != _utc(
                    datetime.fromisoformat(received_at.replace("Z", "+00:00")), "receipt received_at"
                ).strftime("%Y%m%dT%H%M%S.%fZ"):
                    raise ValueError("receipt_id clock does not match received_at")
                expected_relative = f"{received_at[:10]}/{receipt_id}.json"
                if relative != expected_relative:
                    raise ValueError("receipt ID/date does not match its storage path")
            except (TypeError, ValueError):
                corrupt_relevant_receipts += 1
                continue
            allowed_pairs = {
                (receipt_source, ingested_at)
                for source_uid, ingested_at in raw_lineage_pairs[raw_uid]
                for receipt_source in receipt_sources_by_source_uid.get(source_uid, frozenset())
            }
            if (
                item.get("platform") != platform
                or (source, normalized_received) not in allowed_pairs
            ):
                # Content-addressed raw bytes may have valid receipts from later
                # replays (or another source that returned identical bytes).
                # They are not the receipt retained by these canonical records,
                # but their mere existence is not corruption.
                nonmatching_replay_receipts += 1
                continue
            matching_digests.add(digest_value)
            valid_receipt_rows.append((relative, _sha256(raw), digest_value))

    raw_objects_with_receipts = 0
    missing_receipts = 0
    for raw_uid in raw_lineage_pairs:
        digest = _parse_raw_uid(raw_uid, platform=platform)
        if digest is not None and digest in matching_digests:
            raw_objects_with_receipts += 1
        else:
            missing_receipts += 1

    return RawLineageAudit(
        unique_raw_artifact_count=len(raw_lineage_pairs),
        verified_raw_object_count=verified,
        missing_raw_object_count=missing,
        corrupt_raw_object_count=corrupt,
        raw_uid_format_error_count=malformed,
        matching_receipt_count=len(valid_receipt_rows),
        raw_objects_with_receipt_count=raw_objects_with_receipts,
        missing_receipt_count=missing_receipts,
        corrupt_relevant_receipt_count=corrupt_relevant_receipts,
        nonmatching_replay_receipt_count=nonmatching_replay_receipts,
        raw_object_set_sha256=_hash_rows(raw_rows),
        matching_receipt_set_sha256=_hash_rows(sorted(valid_receipt_rows)),
    )


def _coverage_summary(
    *,
    ledger_path: Path,
    selection: CorpusSelectionPolicy,
    min_event_time: datetime | None,
    max_event_time: datetime | None,
    raw_artifact_uids: Iterable[str],
) -> CoverageLedgerSummary:
    if not ledger_path.exists():
        return CoverageLedgerSummary(
            ledger_present=False,
            coverage_ledger_sha256=None,
            relevant_row_count=0,
            exact_selection_row_count=0,
            complete_row_count=0,
            incomplete_row_count=0,
            continuation_row_count=0,
            declared_record_count=0,
            min_interval_start=None,
            max_interval_end=None,
            selected_raw_artifacts_declared_count=0,
            selected_raw_artifacts_undeclared_count=len(set(raw_artifact_uids)),
            status="unknown",
            absence_claims_permitted=False,
            reason_codes=("coverage_ledger_missing",),
        )
    try:
        ledger_bytes = ledger_path.read_bytes()
    except OSError as exc:
        raise CorpusFreezeError(f"cannot read coverage ledger: {ledger_path}") from exc

    relevant_rows = 0
    selected_rows: list[CoverageRecord] = []
    for line_number, raw_line in enumerate(ledger_bytes.splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            item = json.loads(raw_line)
            if not isinstance(item, dict) or canonical_json_bytes(item) != raw_line:
                raise ValueError("coverage row is not canonical JSON")
            row = CoverageRecord(
                platform=item["platform"],
                dataset=item["dataset"],
                interval_start=item["interval_start"],
                interval_end=item["interval_end"],
                fetched_at=item["fetched_at"],
                record_count=item["record_count"],
                complete=item["complete"],
                raw_sha256=tuple(item.get("raw_sha256", ())),
                continuation=item.get("continuation"),
                filters=item.get("filters", {}),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CorpusFreezeError(f"invalid coverage ledger row {line_number}: {ledger_path}") from exc
        if row.platform == selection.platform and row.dataset == selection.coverage_dataset:
            relevant_rows += 1
            if canonical_json_bytes(dict(row.filters)) == canonical_json_bytes(selection.coverage_filters):
                selected_rows.append(row)

    complete_rows = sum(1 for row in selected_rows if row.complete)
    incomplete_rows = len(selected_rows) - complete_rows
    continuation_rows = sum(1 for row in selected_rows if row.continuation is not None)
    declared_record_count = sum(row.record_count for row in selected_rows)
    interval_start = min((row.interval_start for row in selected_rows), default=None)
    interval_end = max((row.interval_end for row in selected_rows), default=None)
    declared_digests = {digest for row in selected_rows for digest in row.raw_sha256}
    selected_digests = {
        digest
        for raw_uid in raw_artifact_uids
        if (digest := _parse_raw_uid(raw_uid, platform=selection.platform)) is not None
    }
    declared = len(selected_digests & declared_digests)
    undeclared = len(selected_digests - declared_digests)

    reasons: list[str] = []
    complete = bool(selected_rows)
    if not selected_rows:
        reasons.append("no_exact_selection_coverage_rows")
        complete = False
    if incomplete_rows:
        reasons.append("incomplete_coverage_rows_present")
        complete = False
    if continuation_rows:
        reasons.append("continuation_tokens_present")
        complete = False
    if any(not row.raw_sha256 for row in selected_rows):
        reasons.append("coverage_rows_missing_raw_hashes")
        complete = False
    if undeclared:
        reasons.append("selected_raw_artifacts_absent_from_coverage_ledger")
        complete = False
    if min_event_time is None or max_event_time is None:
        reasons.append("selected_corpus_is_empty")
        complete = False
    elif selected_rows:
        cursor = min_event_time
        intervals = sorted(
            ((row.interval_start, row.interval_end) for row in selected_rows if row.complete),
            key=lambda interval: interval[0],
        )
        for start, end in intervals:
            if end < cursor:
                continue
            if start > cursor:
                break
            if end > cursor:
                cursor = end
        if cursor < max_event_time:
            reasons.append("complete_rows_do_not_contiguously_cover_selected_event_clocks")
            complete = False

    if complete:
        status: Literal["complete", "partial", "unknown"] = "complete"
        reasons.append("mechanically_complete_for_exact_declared_selection_interval")
    elif selected_rows:
        status = "partial"
    else:
        status = "unknown"
    return CoverageLedgerSummary(
        ledger_present=True,
        coverage_ledger_sha256=_sha256(ledger_bytes),
        relevant_row_count=relevant_rows,
        exact_selection_row_count=len(selected_rows),
        complete_row_count=complete_rows,
        incomplete_row_count=incomplete_rows,
        continuation_row_count=continuation_rows,
        declared_record_count=declared_record_count,
        min_interval_start=interval_start,
        max_interval_end=interval_end,
        selected_raw_artifacts_declared_count=declared,
        selected_raw_artifacts_undeclared_count=undeclared,
        status=status,
        absence_claims_permitted=complete,
        reason_codes=tuple(sorted(set(reasons))),
    )


def freeze_observational_trade_fills(
    *,
    source_root: str | Path,
    raw_root: str | Path,
    coverage_ledger_path: str | Path,
    selection: CorpusSelectionPolicy,
    code_revision: str,
    source_contract_version: str,
) -> ObservationalCorpusManifest:
    """Freeze canonical TradeFill files already present on disk.

    This function performs no network I/O and writes no corpus data.  It reads
    every JSON file under ``source_root`` as a canonical TradeFill, selects by
    validated record fields, and audits selected raw lineage and coverage.
    """

    try:
        root = Path(source_root).resolve(strict=True)
        raw = Path(raw_root).resolve(strict=True)
    except OSError as exc:
        raise CorpusFreezeError("source_root and raw_root must already exist") from exc
    if not root.is_dir() or not raw.is_dir():
        raise CorpusFreezeError("source_root and raw_root must be directories")
    revision_text = code_revision.strip()
    source_version_text = source_contract_version.strip()
    if not revision_text or not source_version_text:
        raise ValueError("code_revision and source_contract_version are required")

    source_file_digest = hashlib.sha256()
    uid_digests: dict[str, str] = {}
    conflict_versions: dict[str, set[str]] = {}
    selected_files = duplicate_files = 0
    excluded_platform: dict[str, int] = {}
    excluded_source: dict[str, int] = {}
    canonical_files = 0
    actor_uids: set[str] = set()
    market_uids: set[str] = set()
    outcome_uids: set[str] = set()
    parser_versions: set[str] = set()
    schema_versions: set[str] = set()
    raw_lineage_pairs: dict[str, set[tuple[str, str]]] = {}
    min_event: datetime | None = None
    max_event: datetime | None = None
    min_ingested: datetime | None = None
    max_ingested: datetime | None = None

    for path, fill, payload, content_digest in _iter_validated_fill_files(root):
        canonical_files += 1
        relative = path.relative_to(root).as_posix()
        source_file_digest.update(canonical_json_bytes([relative, content_digest]))
        source_file_digest.update(b"\n")
        if fill.platform != selection.platform:
            excluded_platform[fill.platform] = excluded_platform.get(fill.platform, 0) + 1
            continue
        if fill.source_uid not in selection.source_uids:
            excluded_source[fill.source_uid] = excluded_source.get(fill.source_uid, 0) + 1
            continue

        selected_files += 1
        parser_versions.add(fill.parser_version)
        schema_versions.add(fill.schema_version)
        raw_lineage_pairs.setdefault(fill.raw_artifact_uid, set()).add(
            (fill.source_uid, _time(fill.ingested_at) or "")
        )
        existing = uid_digests.get(fill.fill_uid)
        if existing is not None:
            if existing == content_digest:
                duplicate_files += 1
            else:
                versions = conflict_versions.setdefault(fill.fill_uid, {existing})
                if content_digest in versions:
                    duplicate_files += 1
                else:
                    versions.add(content_digest)
            continue
        uid_digests[fill.fill_uid] = content_digest
        market_uids.add(fill.market_uid)
        outcome_uids.add(fill.outcome_uid)
        for actor in (fill.actor_uid, fill.maker.actor_uid if fill.maker else None, fill.taker.actor_uid if fill.taker else None):
            if actor is not None:
                actor_uids.add(actor)
        min_event = fill.event_time if min_event is None else min(min_event, fill.event_time)
        max_event = fill.event_time if max_event is None else max(max_event, fill.event_time)
        min_ingested = fill.ingested_at if min_ingested is None else min(min_ingested, fill.ingested_at)
        max_ingested = fill.ingested_at if max_ingested is None else max(max_ingested, fill.ingested_at)

    unique_record_versions = len(uid_digests) + sum(
        len(versions) - 1 for versions in conflict_versions.values()
    )

    def record_rows() -> Iterable[object]:
        for fill_uid in sorted(uid_digests):
            versions = tuple(sorted(conflict_versions.get(fill_uid, {uid_digests[fill_uid]})))
            yield [fill_uid, versions]

    selected_record_set_sha256 = _hash_rows(record_rows())
    files = CorpusFileSummary(
        canonical_file_count=canonical_files,
        selected_file_count=selected_files,
        excluded_file_count=canonical_files - selected_files,
        unique_record_count=len(uid_digests),
        duplicate_file_count=duplicate_files,
        conflict_uid_count=len(conflict_versions),
        unique_record_version_count=unique_record_versions,
        source_file_set_sha256=source_file_digest.hexdigest(),
        selected_record_set_sha256=selected_record_set_sha256,
        excluded_platform_counts=tuple(sorted(excluded_platform.items())),
        excluded_source_counts=tuple(sorted(excluded_source.items())),
    )
    population = CorpusPopulationSummary(
        min_event_time=min_event,
        max_event_time=max_event,
        min_ingested_at=min_ingested,
        max_ingested_at=max_ingested,
        unique_actor_count=len(actor_uids),
        unique_market_count=len(market_uids),
        unique_outcome_count=len(outcome_uids),
    )
    lineage = _audit_lineage(
        raw_root=raw,
        platform=selection.platform,
        raw_lineage_pairs=raw_lineage_pairs,
        receipt_sources_by_source_uid=selection.receipt_sources_by_source_uid,
    )
    coverage = _coverage_summary(
        ledger_path=Path(coverage_ledger_path),
        selection=selection,
        min_event_time=min_event,
        max_event_time=max_event,
        raw_artifact_uids=raw_lineage_pairs,
    )

    failure_reasons: list[str] = []
    if not selected_files:
        failure_reasons.append("no_selected_trade_fills")
    if files.conflict_uid_count:
        failure_reasons.append("conflicting_fill_uids")
    if lineage.missing_raw_object_count:
        failure_reasons.append("missing_raw_objects")
    if lineage.corrupt_raw_object_count:
        failure_reasons.append("corrupt_raw_objects")
    if lineage.raw_uid_format_error_count:
        failure_reasons.append("invalid_raw_artifact_uids")
    if lineage.missing_receipt_count:
        failure_reasons.append("missing_matching_raw_receipts")
    if lineage.corrupt_relevant_receipt_count:
        failure_reasons.append("corrupt_or_mismatched_raw_receipts")

    limitations: list[str] = []
    if coverage.status != "complete":
        limitations.append(f"coverage_{coverage.status}")
    limitations.extend(coverage.reason_codes)
    integrity_usable = not failure_reasons
    if not integrity_usable:
        status: Literal["ready", "ready_with_limitations", "failed_closed"] = "failed_closed"
    elif limitations:
        status = "ready_with_limitations"
    else:
        status = "ready"

    revision = RevisionBinding(
        code_revision=revision_text,
        freezer_code_sha256=_sha256(Path(__file__).read_bytes()),
        source_contract_version=source_version_text,
        trade_fill_schema_versions=tuple(sorted(schema_versions)),
        parser_versions=tuple(sorted(parser_versions)),
    )
    selection_hash = _sha256(canonical_json_bytes(selection))
    corpus_sha256 = _sha256(
        canonical_json_bytes(
            {
                "coverage_ledger_sha256": coverage.coverage_ledger_sha256,
                "matching_receipt_set_sha256": lineage.matching_receipt_set_sha256,
                "platform": selection.platform,
                "raw_object_set_sha256": lineage.raw_object_set_sha256,
                "selected_record_set_sha256": files.selected_record_set_sha256,
                "selection_policy_sha256": selection_hash,
                "source_file_set_sha256": files.source_file_set_sha256,
            }
        )
    )
    unsigned = {
        "manifest_schema_version": 1,
        "artifact_type": "observational_trade_fill_corpus",
        "status": status,
        "integrity_usable": integrity_usable,
        "observational_only": True,
        "labels_included": False,
        "effectiveness_status": "effectiveness_unknown",
        "selection": selection,
        "revision": revision,
        "files": files,
        "population": population,
        "lineage": lineage,
        "coverage": coverage,
        "corpus_sha256": corpus_sha256,
        "failure_reasons": tuple(sorted(set(failure_reasons))),
        "limitation_reasons": tuple(sorted(set(limitations))),
    }
    manifest = ObservationalCorpusManifest(
        **unsigned,
        manifest_sha256=_sha256(canonical_json_bytes(unsigned)),
    )
    manifest.verify()
    return manifest


def verify_observational_trade_fill_freeze(
    manifest: ObservationalCorpusManifest,
    *,
    source_root: str | Path,
    raw_root: str | Path,
    coverage_ledger_path: str | Path,
) -> None:
    """Re-freeze the same explicit roots and require byte-identical evidence."""

    manifest.verify()
    reproduced = freeze_observational_trade_fills(
        source_root=source_root,
        raw_root=raw_root,
        coverage_ledger_path=coverage_ledger_path,
        selection=manifest.selection,
        code_revision=manifest.revision.code_revision,
        source_contract_version=manifest.revision.source_contract_version,
    )
    if reproduced.to_bytes() != manifest.to_bytes():
        raise ManifestVerificationError("current corpus, lineage, coverage, or freezer code differs from the manifest")


__all__ = [
    "CanonicalCorpusFileError",
    "CorpusFreezeError",
    "CorpusSelectionPolicy",
    "ManifestVerificationError",
    "ObservationalCorpusManifest",
    "ReceiptSourceContract",
    "freeze_observational_trade_fills",
    "make_selection_policy",
    "verify_observational_trade_fill_freeze",
]
