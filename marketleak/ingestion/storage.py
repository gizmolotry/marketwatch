"""Atomic normalized-record storage with replay and conflict semantics."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .normalize import canonical_json_bytes, require_text, utc_datetime


_RECEIPT_LINEAGE_FIELDS = frozenset({"ingested_at", "raw_artifact_uid"})


def _without_receipt_lineage(value: Any) -> Any:
    """Remove retrieval-envelope fields while retaining semantic lineage.

    A normalized event can legitimately be observed in multiple API pages or
    collection runs. ``ingested_at`` and ``raw_artifact_uid`` identify those
    retrievals, not a different fill/observation/book. Raw receipts remain
    append-only in RawArtifactStore; the normalized store retains the first
    complete lineage envelope.
    """

    if isinstance(value, Mapping):
        return {
            key: _without_receipt_lineage(item)
            for key, item in value.items()
            if key not in _RECEIPT_LINEAGE_FIELDS
        }
    if isinstance(value, list):
        return [_without_receipt_lineage(item) for item in value]
    return value


def _semantic_payload(payload: bytes) -> bytes:
    return canonical_json_bytes(_without_receipt_lineage(json.loads(payload)))


def _same_semantic_content(existing: bytes, incoming: bytes) -> bool:
    try:
        return _semantic_payload(existing) == _semantic_payload(incoming)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return False


@dataclass(frozen=True, slots=True)
class ConflictRecord:
    uid: str
    existing_path: Path
    quarantine_path: Path


@dataclass(slots=True)
class WriteResult:
    inserted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    rejected: int = 0
    paths: list[Path] = field(default_factory=list)
    quarantined: list[ConflictRecord] = field(default_factory=list)


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _field(record: Any, names: tuple[str, ...]) -> Any:
    for name in names:
        if isinstance(record, Mapping) and name in record:
            return record[name]
        if hasattr(record, name):
            return getattr(record, name)
    return None


class NormalizedStore:
    """Write one immutable canonical record per UID into date partitions.

    Exact replay is a no-op. Reuse of a UID with different normalized content
    never overwrites trusted data; the incoming record is written to quarantine.
    """

    UID_FIELDS = (
        "observation_uid",
        "fill_uid",
        "trade_uid",
        "snapshot_uid",
        "change_uid",
        "update_uid",
        "lifecycle_uid",
        "unknown_uid",
        "market_uid",
        "outcome_uid",
        "uid",
        "record_uid",
    )
    TIME_FIELDS = (
        "event_time",
        "observed_at",
        "executed_at",
        "captured_at",
        "timestamp",
        "as_of",
        "occurred_at",
        "received_at",
    )

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.records_root = self.root / "normalized"
        self.quarantine_root = self.root / "quarantine" / "uid-conflicts"

    def write(self, records: Iterable[Any], *, record_type: str | None = None) -> WriteResult:
        result = WriteResult()
        for record in records:
            try:
                self._write_one(record, result, record_type=record_type)
            except (TypeError, ValueError):
                result.rejected += 1
        return result

    def _write_one(self, record: Any, result: WriteResult, *, record_type: str | None) -> None:
        uid = require_text(_field(record, self.UID_FIELDS), "record uid")
        platform = require_text(_field(record, ("platform", "venue")), "platform").lower()
        timestamp_value = _field(record, self.TIME_FIELDS)
        timestamp = utc_datetime(timestamp_value or datetime.now(UTC), "record timestamp")
        kind = (record_type or type(record).__name__).strip().lower()
        if not kind:
            raise ValueError("record_type is required")
        payload = canonical_json_bytes(record)
        uid_digest = hashlib.sha256(uid.encode("utf-8")).hexdigest()
        platform_root = self.records_root / kind / f"platform={platform}"
        path = (
            platform_root
            / f"date={timestamp.date().isoformat()}"
            / f"{uid_digest}.json"
        )
        # UID identity is global within record type/platform, not scoped to a
        # date partition. This also catches an illegal event_time mutation that
        # would otherwise land the same UID under a second date.
        existing_paths = sorted(platform_root.glob(f"date=*/{uid_digest}.json"))
        existing_path = existing_paths[0] if existing_paths else None
        if existing_path is not None:
            existing = existing_path.read_bytes()
            if existing == payload or _same_semantic_content(existing, payload):
                result.duplicates += 1
                return
            self._quarantine(uid, existing_path, existing, payload, result)
            return
        _atomic_replace(path, payload)
        result.inserted += 1
        result.paths.append(path)

    def _quarantine(
        self,
        uid: str,
        existing_path: Path,
        existing: bytes,
        incoming: bytes,
        result: WriteResult,
    ) -> None:
        uid_digest = hashlib.sha256(uid.encode("utf-8")).hexdigest()
        incoming_digest = hashlib.sha256(incoming).hexdigest()
        path = self.quarantine_root / uid_digest / f"{incoming_digest}.json"
        envelope = canonical_json_bytes(
            {
                "schema_version": 1,
                "reason": "same UID has different normalized content",
                "uid": uid,
                "existing_path": str(existing_path),
                "existing_sha256": hashlib.sha256(existing).hexdigest(),
                "incoming_sha256": incoming_digest,
                "existing_record": existing.decode("utf-8"),
                "incoming_record": incoming.decode("utf-8"),
            }
        )
        if not path.exists():
            _atomic_replace(path, envelope)
        result.conflicts += 1
        result.quarantined.append(ConflictRecord(uid, existing_path, path))
