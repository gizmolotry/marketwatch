"""Append-only, retry-safe in-memory event memory with causal as-of snapshots."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from threading import RLock
from typing import Iterable

from pydantic import Field

from marketleak.domain.common import StableUID
from marketleak.multimodal.schemas import (
    EventFact,
    EventMemorySnapshot,
    Phase15Model,
    canonical_hash,
    snapshot_manifest_for,
    _utc,
)


class AppendStatus(str, Enum):
    INSERTED = "inserted"
    IDEMPOTENT = "idempotent"
    QUARANTINED = "quarantined"


class EventCollision(Phase15Model):
    """An immutable record of a same-UID, different-content write attempt."""

    collision_uid: StableUID
    event_uid: StableUID
    existing_semantic_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_semantic_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str = "event_uid already names different immutable content"


class AppendResult(Phase15Model):
    status: AppendStatus
    event_uid: StableUID
    semantic_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    collision: EventCollision | None = None


def _record_order_key(record: EventFact) -> tuple[datetime, datetime, str, str]:
    return (record.available_at, record.event_time, record.modality.value, record.event_uid)


class EventMemoryStore:
    """A small deterministic core intended to sit below durable storage.

    This object permits only three outcomes: insert a new immutable fact,
    recognize an exact semantic retry, or quarantine a UID collision.  It has
    intentionally no update/delete API.
    """

    def __init__(self, initial_records: Iterable[EventFact] = ()) -> None:
        self._lock = RLock()
        self._events: dict[str, EventFact] = {}
        self._collisions: dict[str, EventCollision] = {}
        for record in initial_records:
            result = self.append(record)
            if result.status == AppendStatus.QUARANTINED:
                raise ValueError(f"initial records contain collision for {record.event_uid}")

    def append(self, record: EventFact) -> AppendResult:
        """Append ``record`` or preserve conflict evidence without overwriting."""

        with self._lock:
            semantic_hash = record.semantic_hash
            existing = self._events.get(record.event_uid)
            if existing is None:
                self._events[record.event_uid] = record
                return AppendResult(
                    status=AppendStatus.INSERTED,
                    event_uid=record.event_uid,
                    semantic_hash=semantic_hash,
                )
            if existing.semantic_hash == semantic_hash:
                return AppendResult(
                    status=AppendStatus.IDEMPOTENT,
                    event_uid=record.event_uid,
                    semantic_hash=semantic_hash,
                )
            collision_payload = {
                "event_uid": record.event_uid,
                "existing_semantic_hash": existing.semantic_hash,
                "candidate_semantic_hash": semantic_hash,
            }
            collision_uid = f"collision:{canonical_hash(collision_payload)}"
            collision = self._collisions.setdefault(
                collision_uid,
                EventCollision(collision_uid=collision_uid, **collision_payload),
            )
            return AppendResult(
                status=AppendStatus.QUARANTINED,
                event_uid=record.event_uid,
                semantic_hash=semantic_hash,
                collision=collision,
            )

    @property
    def event_count(self) -> int:
        with self._lock:
            return len(self._events)

    def events(self) -> tuple[EventFact, ...]:
        with self._lock:
            return tuple(sorted(self._events.values(), key=_record_order_key))

    def quarantined_collisions(self) -> tuple[EventCollision, ...]:
        with self._lock:
            return tuple(sorted(self._collisions.values(), key=lambda item: item.collision_uid))

    def records_as_of(self, as_of: datetime) -> tuple[EventFact, ...]:
        """Return only facts both occurring and becoming observable by ``as_of``."""

        cutoff = _utc(as_of, field_name="as_of")
        with self._lock:
            admissible = (
                record
                for record in self._events.values()
                if record.event_time <= cutoff and record.available_at <= cutoff
            )
            return tuple(sorted(admissible, key=_record_order_key))

    def snapshot_as_of(self, as_of: datetime) -> EventMemorySnapshot:
        """Freeze a causally admissible, deterministic record selection."""

        cutoff = _utc(as_of, field_name="as_of")
        records = self.records_as_of(cutoff)
        manifest = snapshot_manifest_for(as_of=cutoff, records=records)
        return EventMemorySnapshot(manifest=manifest, records=records)


__all__ = [
    "AppendResult",
    "AppendStatus",
    "EventCollision",
    "EventMemoryStore",
]
