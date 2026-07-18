"""Source capability declarations and immutable ingestion-coverage ledger."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

from .normalize import canonical_json_bytes, require_text, utc_datetime


@dataclass(frozen=True, slots=True)
class CapabilityMetadata:
    platform: str
    dataset: str
    actor_visibility: str
    direction_visibility: str
    depth_visibility: str
    pagination: str
    official_documentation: str
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        choices = {"available", "unavailable", "partial"}
        for name in ("actor_visibility", "direction_visibility", "depth_visibility"):
            if getattr(self, name) not in choices:
                raise ValueError(f"{name} must be available, unavailable, or partial")


@dataclass(frozen=True, slots=True)
class CoverageRecord:
    platform: str
    dataset: str
    interval_start: datetime
    interval_end: datetime
    fetched_at: datetime
    record_count: int
    complete: bool
    raw_sha256: tuple[str, ...] = ()
    continuation: str | None = None
    filters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        start = utc_datetime(self.interval_start, "interval_start")
        end = utc_datetime(self.interval_end, "interval_end")
        fetched = utc_datetime(self.fetched_at, "fetched_at")
        if end < start:
            raise ValueError("interval_end must be >= interval_start")
        if self.record_count < 0:
            raise ValueError("record_count must be non-negative")
        object.__setattr__(self, "platform", require_text(self.platform, "platform").lower())
        object.__setattr__(self, "dataset", require_text(self.dataset, "dataset"))
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        object.__setattr__(self, "fetched_at", fetched)


class CoverageLedger:
    """Append coverage claims; derive gaps without silently claiming completeness."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def append(self, record: CoverageRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = canonical_json_bytes(record) + b"\n"
        # One O_APPEND write prevents interleaving for these small ledger rows.
        descriptor = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            os.write(descriptor, line)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def records(self, *, platform: str | None = None, dataset: str | None = None) -> list[CoverageRecord]:
        if not self.path.exists():
            return []
        rows: list[CoverageRecord] = []
        for raw_line in self.path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            item = json.loads(raw_line)
            row = CoverageRecord(
                platform=item["platform"],
                dataset=item["dataset"],
                interval_start=utc_datetime(item["interval_start"]),
                interval_end=utc_datetime(item["interval_end"]),
                fetched_at=utc_datetime(item["fetched_at"]),
                record_count=int(item["record_count"]),
                complete=bool(item["complete"]),
                raw_sha256=tuple(item.get("raw_sha256", ())),
                continuation=item.get("continuation"),
                filters=item.get("filters", {}),
            )
            if platform is not None and row.platform != platform.lower():
                continue
            if dataset is not None and row.dataset != dataset:
                continue
            rows.append(row)
        return rows

    def gaps(
        self,
        *,
        platform: str,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        desired_start = utc_datetime(start, "start")
        desired_end = utc_datetime(end, "end")
        intervals = sorted(
            (
                (max(row.interval_start, desired_start), min(row.interval_end, desired_end))
                for row in self.records(platform=platform, dataset=dataset)
                if row.complete and row.interval_end >= desired_start and row.interval_start <= desired_end
            ),
            key=lambda value: value[0],
        )
        gaps: list[tuple[datetime, datetime]] = []
        cursor = desired_start
        for interval_start, interval_end in intervals:
            if interval_start > cursor:
                gaps.append((cursor, interval_start))
            if interval_end > cursor:
                cursor = interval_end
        if cursor < desired_end:
            gaps.append((cursor, desired_end))
        return gaps

