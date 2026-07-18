"""Canonical-JSON, hash-chained, append-only shadow ledger."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from threading import RLock
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from marketleak.shadow.manifest import (
    ShadowRunManifestV2,
    canonical_hash,
    canonical_json_bytes,
)


class LedgerTamperError(RuntimeError):
    pass


class FrozenRunMutationError(RuntimeError):
    pass


class LedgerEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(ge=0)
    entry_uid: str = Field(pattern=r"^ledger:[0-9a-f]{64}$")
    record_type: Literal["assessment", "control_sample"]
    record_uid: str = Field(min_length=1)
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: dict[str, Any]
    previous_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    def hash_payload(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        value.pop("entry_hash", None)
        return value


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class ShadowLedger:
    """Append assessments without permitting run configuration mutation."""

    def __init__(self, run_dir: str | Path, manifest: ShadowRunManifestV2):
        self.run_dir = Path(run_dir)
        self.manifest = manifest
        self.manifest_path = self.run_dir / "manifest.json"
        self.ledger_path = self.run_dir / "ledger.jsonl"
        self._lock = RLock()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        manifest_bytes = canonical_json_bytes(manifest)
        if self.manifest_path.exists():
            existing = self.manifest_path.read_bytes()
            if existing != manifest_bytes:
                raise FrozenRunMutationError(
                    "A frozen run directory cannot be reopened with a changed config, model, or manifest."
                )
        else:
            _atomic_write(self.manifest_path, manifest_bytes)
        if self.ledger_path.exists():
            self.verify()

    def assert_manifest(self, manifest: ShadowRunManifestV2) -> None:
        if canonical_json_bytes(manifest) != canonical_json_bytes(self.manifest):
            raise FrozenRunMutationError("Frozen run parameters cannot be tuned or replaced in place.")

    def _read_without_verification(self) -> tuple[LedgerEntry, ...]:
        if not self.ledger_path.exists():
            return ()
        entries: list[LedgerEntry] = []
        for line_number, raw_line in enumerate(self.ledger_path.read_bytes().splitlines(), start=1):
            if not raw_line.strip():
                raise LedgerTamperError(f"Blank ledger line at {line_number}")
            try:
                entry = LedgerEntry.model_validate(json.loads(raw_line))
            except Exception as exc:
                raise LedgerTamperError(f"Invalid ledger entry at line {line_number}") from exc
            if raw_line != canonical_json_bytes(entry):
                raise LedgerTamperError(f"Non-canonical ledger encoding at line {line_number}")
            entries.append(entry)
        return tuple(entries)

    def verify(self) -> tuple[LedgerEntry, ...]:
        entries = self._read_without_verification()
        previous_hash = self.manifest.previous_ledger_hash
        for sequence, entry in enumerate(entries):
            if entry.sequence != sequence:
                raise LedgerTamperError(f"Ledger sequence mismatch at entry {sequence}")
            if entry.manifest_hash != self.manifest.manifest_hash:
                raise LedgerTamperError(f"Manifest hash mismatch at entry {sequence}")
            if entry.previous_hash != previous_hash:
                raise LedgerTamperError(f"Hash-chain predecessor mismatch at entry {sequence}")
            expected_hash = canonical_hash(entry.hash_payload())
            if entry.entry_hash != expected_hash:
                raise LedgerTamperError(f"Entry hash mismatch at entry {sequence}")
            payload_hash = canonical_hash(entry.payload)
            expected_uid = "ledger:" + hashlib.sha256(
                f"{self.manifest.manifest_hash}|{sequence}|{entry.record_type}|{entry.record_uid}|{payload_hash}".encode("utf-8")
            ).hexdigest()
            if entry.entry_uid != expected_uid:
                raise LedgerTamperError(f"Entry UID mismatch at entry {sequence}")
            previous_hash = entry.entry_hash
        return entries

    def entries(self) -> tuple[LedgerEntry, ...]:
        with self._lock:
            return self.verify()

    @property
    def tail_hash(self) -> str:
        entries = self.entries()
        return entries[-1].entry_hash if entries else self.manifest.previous_ledger_hash

    def append_assessment(self, record_uid: str, payload: Mapping[str, Any]) -> LedgerEntry:
        return self._append("assessment", record_uid, payload)

    def append_control_sample(self, record_uid: str, payload: Mapping[str, Any]) -> LedgerEntry:
        return self._append("control_sample", record_uid, payload)

    def _append(
        self,
        record_type: Literal["assessment", "control_sample"],
        record_uid: str,
        payload: Mapping[str, Any],
    ) -> LedgerEntry:
        with self._lock:
            entries = list(self.verify())
            if any(entry.record_uid == record_uid and entry.record_type == record_type for entry in entries):
                raise FrozenRunMutationError(
                    f"Append-only ledger already contains {record_type} record {record_uid}."
                )
            sequence = len(entries)
            previous_hash = entries[-1].entry_hash if entries else self.manifest.previous_ledger_hash
            base = {
                "sequence": sequence,
                "record_type": record_type,
                "record_uid": str(record_uid),
                "manifest_hash": self.manifest.manifest_hash,
                "payload": dict(payload),
                "previous_hash": previous_hash,
            }
            payload_hash = canonical_hash(base["payload"])
            entry_uid = "ledger:" + hashlib.sha256(
                f"{self.manifest.manifest_hash}|{sequence}|{record_type}|{record_uid}|{payload_hash}".encode("utf-8")
            ).hexdigest()
            hash_payload = {**base, "entry_uid": entry_uid}
            entry_hash = canonical_hash(hash_payload)
            entry = LedgerEntry.model_validate({**hash_payload, "entry_hash": entry_hash})
            serialized = b"\n".join(canonical_json_bytes(item) for item in [*entries, entry]) + b"\n"
            _atomic_write(self.ledger_path, serialized)
            self.verify()
            return entry


__all__ = [
    "FrozenRunMutationError",
    "LedgerEntry",
    "LedgerTamperError",
    "ShadowLedger",
]
