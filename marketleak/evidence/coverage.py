"""Coverage assessment and append-only hash-verifiable collection ledger."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from marketleak.domain.enums import CoverageStatus
from marketleak.evidence.normalize import utc_datetime


ZERO_HASH = "0" * 64


class CoverageLedgerIntegrityError(RuntimeError):
    pass


class SourceCoverageInterval(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    source: str = Field(min_length=1)
    started_at: datetime
    ended_at: datetime
    status: CoverageStatus
    connector_version: str = Field(min_length=1)
    scope: str = "public"
    details: str = ""

    @field_validator("started_at", "ended_at", mode="before")
    @classmethod
    def _timestamps_are_utc(cls, value, info):
        return utc_datetime(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _valid_interval(self):
        if self.ended_at <= self.started_at:
            raise ValueError("coverage interval must have positive duration")
        return self


class CoverageAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    adequate: bool
    window_start: datetime
    window_end: datetime
    required_sources: tuple[str, ...]
    complete_ratio_by_source: dict[str, float]
    missing_sources: tuple[str, ...] = ()
    outage_sources: tuple[str, ...] = ()
    rationale: str


class CoverageLedgerEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sequence: int = Field(ge=0)
    interval: SourceCoverageInterval
    previous_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    def hash_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload.pop("entry_hash", None)
        return payload


def _canonical_bytes(value: Any) -> bytes:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _union_duration(intervals: list[tuple[datetime, datetime]]) -> timedelta:
    if not intervals:
        return timedelta(0)
    ordered = sorted(intervals)
    current_start, current_end = ordered[0]
    duration = timedelta(0)
    for start, end in ordered[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            duration += current_end - current_start
            current_start, current_end = start, end
    return duration + (current_end - current_start)


class CoverageLedger:
    def __init__(
        self,
        intervals: Iterable[SourceCoverageInterval] | str | Path = (),
        *,
        path: str | Path | None = None,
    ):
        if isinstance(intervals, (str, Path)):
            if path is not None:
                raise ValueError("coverage ledger path was supplied twice")
            path = intervals
            intervals = ()
        self.path = Path(path) if path is not None else None
        self._lock = RLock()
        self._intervals: list[SourceCoverageInterval] = []
        self._entries: list[CoverageLedgerEntry] = []
        if self.path is not None and self.path.exists():
            self._load()
        for item in intervals:
            self.add(item)

    @property
    def intervals(self) -> tuple[SourceCoverageInterval, ...]:
        return tuple(self._intervals)

    @property
    def tail_hash(self) -> str:
        return self._entries[-1].entry_hash if self._entries else ZERO_HASH

    def _load(self) -> None:
        assert self.path is not None
        entries: list[CoverageLedgerEntry] = []
        for line_number, raw in enumerate(self.path.read_bytes().splitlines(), start=1):
            if not raw:
                raise CoverageLedgerIntegrityError(f"blank coverage ledger line {line_number}")
            try:
                entry = CoverageLedgerEntry.model_validate_json(raw)
            except Exception as exc:
                raise CoverageLedgerIntegrityError(
                    f"invalid coverage ledger line {line_number}"
                ) from exc
            if raw != _canonical_bytes(entry):
                raise CoverageLedgerIntegrityError(
                    f"non-canonical coverage ledger line {line_number}"
                )
            entries.append(entry)
        previous = ZERO_HASH
        for sequence, entry in enumerate(entries):
            if entry.sequence != sequence or entry.previous_hash != previous:
                raise CoverageLedgerIntegrityError(
                    f"coverage hash-chain mismatch at entry {sequence}"
                )
            if entry.entry_hash != _hash(entry.hash_payload()):
                raise CoverageLedgerIntegrityError(
                    f"coverage entry hash mismatch at entry {sequence}"
                )
            previous = entry.entry_hash
        self._entries = entries
        self._intervals = [entry.interval for entry in entries]

    def verify(self) -> tuple[SourceCoverageInterval, ...]:
        if self.path is None:
            return self.intervals
        check = CoverageLedger(path=self.path)
        if check._entries != self._entries:
            raise CoverageLedgerIntegrityError(
                "in-memory and persisted coverage ledgers differ"
            )
        return check.intervals

    def add(self, interval: SourceCoverageInterval) -> SourceCoverageInterval:
        normalized = (
            interval
            if isinstance(interval, SourceCoverageInterval)
            else SourceCoverageInterval.model_validate(interval)
        )
        with self._lock:
            sequence = len(self._entries)
            base = {
                "sequence": sequence,
                "interval": normalized.model_dump(mode="json"),
                "previous_hash": self.tail_hash,
            }
            entry = CoverageLedgerEntry.model_validate(
                {**base, "entry_hash": _hash(base)}
            )
            if self.path is not None:
                content = b"\n".join(
                    _canonical_bytes(item) for item in [*self._entries, entry]
                ) + b"\n"
                _atomic_write(self.path, content)
            self._entries.append(entry)
            self._intervals.append(normalized)
            return normalized

    append = add

    def assess(
        self,
        *,
        required_sources: Iterable[str],
        window_start: datetime,
        window_end: datetime,
        minimum_complete_ratio: float = 0.95,
    ) -> CoverageAssessment:
        start = utc_datetime(window_start, field_name="window_start")
        end = utc_datetime(window_end, field_name="window_end")
        if end <= start:
            raise ValueError("coverage window must have positive duration")
        if not 0.0 < minimum_complete_ratio <= 1.0:
            raise ValueError("minimum_complete_ratio must be in (0, 1]")

        sources = tuple(sorted({str(source).strip() for source in required_sources if str(source).strip()}))
        total_seconds = (end - start).total_seconds()
        ratios: dict[str, float] = {}
        missing: list[str] = []
        outages: list[str] = []

        for source in sources:
            relevant = [
                interval
                for interval in self._intervals
                if interval.source == source and interval.started_at < end and interval.ended_at > start
            ]
            if not relevant:
                ratios[source] = 0.0
                missing.append(source)
                continue

            completed = [
                (max(start, item.started_at), min(end, item.ended_at))
                for item in relevant
                if item.status == CoverageStatus.COMPLETE
            ]
            ratios[source] = min(1.0, _union_duration(completed).total_seconds() / total_seconds)
            if any(item.status == CoverageStatus.UNAVAILABLE for item in relevant):
                outages.append(source)

        adequate = bool(sources) and not missing and not outages and all(
            ratios.get(source, 0.0) >= minimum_complete_ratio for source in sources
        )
        if not sources:
            rationale = "No monitored public sources were declared."
        elif adequate:
            rationale = "All required monitored sources met the coverage threshold."
        else:
            rationale = "Monitored-source coverage was incomplete or included an outage."

        return CoverageAssessment(
            adequate=adequate,
            window_start=start,
            window_end=end,
            required_sources=sources,
            complete_ratio_by_source=ratios,
            missing_sources=tuple(missing),
            outage_sources=tuple(sorted(set(outages))),
            rationale=rationale,
        )


class PersistentCoverageLedger(CoverageLedger):
    def __init__(self, path: str | Path):
        super().__init__(path=path)


__all__ = [
    "CoverageAssessment",
    "CoverageLedger",
    "CoverageLedgerEntry",
    "CoverageLedgerIntegrityError",
    "CoverageStatus",
    "PersistentCoverageLedger",
    "SourceCoverageInterval",
]
