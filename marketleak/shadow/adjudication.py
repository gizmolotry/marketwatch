"""Append-only analyst adjudications with explicit supersession."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from marketleak.shadow.ledger import FrozenRunMutationError, LedgerTamperError, _atomic_write
from marketleak.shadow.manifest import ZERO_HASH, canonical_hash, canonical_json_bytes, utc_datetime


class AdjudicationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(ge=0)
    adjudication_uid: str = Field(pattern=r"^adjudication:[0-9a-f]{64}$")
    record_uid: str = Field(min_length=1)
    label: Literal["positive", "negative", "unknown", "insufficient"]
    rationale: str = Field(min_length=1)
    evidence_uids: tuple[str, ...] = ()
    adjudicator_uid: str = Field(min_length=1)
    adjudicated_at: datetime
    supersedes_uid: str | None = None
    previous_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("adjudicated_at", mode="before")
    @classmethod
    def _time_is_utc(cls, value):
        return utc_datetime(value, field_name="adjudicated_at")

    def hash_payload(self):
        payload = self.model_dump(mode="json")
        payload.pop("entry_hash", None)
        return payload


class AdjudicationStore:
    def __init__(self, run_dir: str | Path):
        self.path = Path(run_dir) / "adjudications.jsonl"
        self._lock = RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            self.verify()

    def _read(self) -> tuple[AdjudicationRecord, ...]:
        if not self.path.exists():
            return ()
        result = []
        for line_number, raw in enumerate(self.path.read_bytes().splitlines(), start=1):
            try:
                record = AdjudicationRecord.model_validate(json.loads(raw))
            except Exception as exc:
                raise LedgerTamperError(f"Invalid adjudication at line {line_number}") from exc
            if raw != canonical_json_bytes(record):
                raise LedgerTamperError(f"Non-canonical adjudication encoding at line {line_number}")
            result.append(record)
        return tuple(result)

    def verify(self) -> tuple[AdjudicationRecord, ...]:
        records = self._read()
        previous = ZERO_HASH
        for sequence, record in enumerate(records):
            if record.sequence != sequence or record.previous_hash != previous:
                raise LedgerTamperError(f"Adjudication chain mismatch at entry {sequence}")
            if record.entry_hash != canonical_hash(record.hash_payload()):
                raise LedgerTamperError(f"Adjudication hash mismatch at entry {sequence}")
            payload_hash = canonical_hash(
                {
                    "record_uid": record.record_uid,
                    "label": record.label,
                    "rationale": record.rationale,
                    "evidence_uids": record.evidence_uids,
                    "adjudicator_uid": record.adjudicator_uid,
                    "adjudicated_at": record.adjudicated_at,
                    "supersedes_uid": record.supersedes_uid,
                }
            )
            expected_uid = "adjudication:" + hashlib.sha256(
                f"{sequence}|{payload_hash}".encode("utf-8")
            ).hexdigest()
            if record.adjudication_uid != expected_uid:
                raise LedgerTamperError(f"Adjudication UID mismatch at entry {sequence}")
            previous = record.entry_hash
        return records

    def append(
        self,
        *,
        record_uid: str,
        label: Literal["positive", "negative", "unknown", "insufficient"],
        rationale: str,
        adjudicator_uid: str,
        adjudicated_at: datetime,
        evidence_uids: tuple[str, ...] = (),
        supersedes_uid: str | None = None,
    ) -> AdjudicationRecord:
        with self._lock:
            records = list(self.verify())
            if supersedes_uid is not None:
                superseded = next(
                    (item for item in records if item.adjudication_uid == supersedes_uid),
                    None,
                )
                if superseded is None:
                    raise FrozenRunMutationError("supersedes_uid does not reference an existing adjudication")
                if superseded.record_uid != record_uid:
                    raise FrozenRunMutationError("an adjudication can supersede only the same shadow record")
                if any(item.supersedes_uid == supersedes_uid for item in records):
                    raise FrozenRunMutationError("an adjudication has already been superseded")

            sequence = len(records)
            normalized_time = utc_datetime(adjudicated_at, field_name="adjudicated_at")
            identity_payload = {
                "record_uid": record_uid,
                "label": label,
                "rationale": rationale,
                "evidence_uids": evidence_uids,
                "adjudicator_uid": adjudicator_uid,
                "adjudicated_at": normalized_time,
                "supersedes_uid": supersedes_uid,
            }
            payload_hash = canonical_hash(identity_payload)
            adjudication_uid = "adjudication:" + hashlib.sha256(
                f"{sequence}|{payload_hash}".encode("utf-8")
            ).hexdigest()
            base = {
                "sequence": sequence,
                "adjudication_uid": adjudication_uid,
                **identity_payload,
                "previous_hash": records[-1].entry_hash if records else ZERO_HASH,
            }
            entry_hash = canonical_hash(base)
            record = AdjudicationRecord.model_validate({**base, "entry_hash": entry_hash})
            content = b"\n".join(canonical_json_bytes(item) for item in [*records, record]) + b"\n"
            _atomic_write(self.path, content)
            self.verify()
            return record


__all__ = ["AdjudicationRecord", "AdjudicationStore"]
