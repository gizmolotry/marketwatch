"""Durable, fail-closed persistence for public-wallet watch mechanics.

The JSONL ledger stores immutable wallet-watch records in a verified hash chain.
The SQLite index stores only causal lookup metadata and a pointer to the
canonical normalized ``TradeFill`` file; it intentionally does not duplicate
the evidence payload or infer identity, control, intent, or misconduct.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
from threading import RLock
import time
from typing import Any, Iterable, Iterator, Mapping

from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill
from marketleak.ingestion.normalize import canonical_json_bytes, utc_datetime
from marketleak.wallet_tracking import (
    BackgroundCohortMarker,
    FillLineage,
    FollowUpStatus,
    WalletHistorySnapshot,
    WalletMarketScope,
    WalletSourceScope,
    WatchFollowUpState,
    WatchRegistration,
)


ZERO_HASH = "0" * 64
LEDGER_NAME = "wallet_tracking.jsonl"
SCHEMA_VERSION = 1


@contextmanager
def _exclusive_path_lock(path: Path, *, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Hold a cross-process one-byte lock for a ledger path.

    Windows uses ``msvcrt.locking`` and POSIX uses ``flock``.  The persistent
    sidecar contains no evidence; it only provides a stable inode/file handle
    shared by independent ``WalletTrackingStore`` instances and processes.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        deadline = time.monotonic() + timeout_seconds
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise WalletStoreError("timed out acquiring wallet ledger writer lock") from exc
                    time.sleep(0.01)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise WalletStoreError("timed out acquiring wallet ledger writer lock") from exc
                    time.sleep(0.01)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


class WalletStoreError(RuntimeError):
    """Base class for durable wallet-store failures."""


class WalletLedgerTamperError(WalletStoreError):
    """The on-disk ledger is malformed, non-canonical, or hash-invalid."""


class WalletRecordConflictError(WalletStoreError):
    """An immutable UID was replayed with different content."""

    def __init__(self, record_uid: str, quarantine_path: Path) -> None:
        self.record_uid = record_uid
        self.quarantine_path = quarantine_path
        super().__init__(
            f"conflicting immutable wallet record {record_uid!r}; "
            f"incoming record quarantined at {quarantine_path}"
        )


class WalletFillConflictError(WalletStoreError):
    """A fill UID was indexed with conflicting pointer or lineage metadata."""

    def __init__(self, fill_uid: str) -> None:
        self.fill_uid = fill_uid
        super().__init__(f"conflicting immutable fill index row for {fill_uid!r}")


class WalletRecordType(str, Enum):
    REGISTRATION = "registration"
    HISTORY_SNAPSHOT = "history_snapshot"
    FOLLOW_UP_STATE = "follow_up_state"


class StoreWriteStatus(str, Enum):
    INSERTED = "inserted"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class WalletLedgerEntry:
    sequence: int
    schema_version: int
    record_type: WalletRecordType
    record_uid: str
    payload_hash: str
    payload: Mapping[str, Any]
    previous_hash: str
    entry_hash: str


@dataclass(frozen=True, slots=True)
class WalletStoreWriteResult:
    status: StoreWriteStatus
    entry: WalletLedgerEntry


@dataclass(frozen=True, slots=True)
class DueWalletWatch:
    registration: WatchRegistration
    state: WatchFollowUpState | None
    due_at: datetime


@dataclass(frozen=True, slots=True)
class IndexedFillRef:
    actor_uid: str
    event_time: datetime
    available_at: datetime
    fill_uid: str
    market_uid: str
    outcome_uid: str
    source_uid: str
    raw_artifact_uid: str
    transaction_uid: str | None
    normalized_path: Path
    normalized_sha256: str


@dataclass(frozen=True, slots=True)
class FillIndexWriteResult:
    status: StoreWriteStatus
    reference: IndexedFillRef


def _time(value: datetime) -> str:
    parsed = utc_datetime(value)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_time(value: Any, field: str) -> datetime:
    return utc_datetime(value, field)


def _duration_microseconds(value: timedelta) -> int:
    return ((value.days * 86_400 + value.seconds) * 1_000_000) + value.microseconds


def _scope_payload(scope: WalletSourceScope) -> dict[str, Any]:
    return {
        "scope_uid": scope.scope_uid,
        "platform": scope.platform,
        "source_uids": list(scope.source_uids),
        "market_scope": scope.market_scope.value,
        "market_uids": list(scope.market_uids),
        "record_types": list(scope.record_types),
    }


def _scope_from_payload(payload: Mapping[str, Any]) -> WalletSourceScope:
    return WalletSourceScope(
        scope_uid=str(payload["scope_uid"]),
        platform=str(payload["platform"]),
        source_uids=tuple(str(item) for item in payload["source_uids"]),
        market_scope=WalletMarketScope(str(payload["market_scope"])),
        market_uids=tuple(str(item) for item in payload["market_uids"]),
        record_types=tuple(str(item) for item in payload["record_types"]),  # type: ignore[arg-type]
    )


def _cohort_payload(cohort: BackgroundCohortMarker) -> dict[str, Any]:
    return {
        "cohort_uid": cohort.cohort_uid,
        "selection_policy_uid": cohort.selection_policy_uid,
        "scope_uid": cohort.scope_uid,
        "ascertained_at": _time(cohort.ascertained_at),
        "frozen_at": _time(cohort.frozen_at),
    }


def _cohort_from_payload(payload: Mapping[str, Any]) -> BackgroundCohortMarker:
    return BackgroundCohortMarker(
        cohort_uid=str(payload["cohort_uid"]),
        selection_policy_uid=str(payload["selection_policy_uid"]),
        scope_uid=str(payload["scope_uid"]),
        ascertained_at=_parse_time(payload["ascertained_at"], "ascertained_at"),
        frozen_at=_parse_time(payload["frozen_at"], "frozen_at"),
    )


def _lineage_payload(lineage: FillLineage) -> dict[str, Any]:
    return {
        "fill_uid": lineage.fill_uid,
        "canonical_sha256": lineage.canonical_sha256,
        "event_time": _time(lineage.event_time),
        "available_at": _time(lineage.available_at),
        "source_uid": lineage.source_uid,
        "raw_artifact_uid": lineage.raw_artifact_uid,
        "transaction_uid": lineage.transaction_uid,
    }


def _lineage_from_payload(payload: Mapping[str, Any]) -> FillLineage:
    transaction_uid = payload.get("transaction_uid")
    return FillLineage(
        fill_uid=str(payload["fill_uid"]),
        canonical_sha256=str(payload["canonical_sha256"]),
        event_time=_parse_time(payload["event_time"], "event_time"),
        available_at=_parse_time(payload["available_at"], "available_at"),
        source_uid=str(payload["source_uid"]),
        raw_artifact_uid=str(payload["raw_artifact_uid"]),
        transaction_uid=None if transaction_uid is None else str(transaction_uid),
    )


def _registration_payload(record: WatchRegistration) -> dict[str, Any]:
    return {
        "registration_uid": record.registration_uid,
        "request_uid": record.request_uid,
        "incident_uid": record.incident_uid,
        "actor_uid": record.actor_uid,
        "actor_visibility": record.actor_visibility.value,
        "enrolled_at": _time(record.enrolled_at),
        "trigger_as_of": _time(record.trigger_as_of),
        "source_scope": _scope_payload(record.source_scope),
        "trigger_source_uid": record.trigger_source_uid,
        "trigger_raw_artifact_uids": list(record.trigger_raw_artifact_uids),
        "fill_lineage": [_lineage_payload(item) for item in record.fill_lineage],
        "attributable_notional": str(record.attributable_notional),
        "history_window_starts_at": _time(record.history_window_starts_at),
        "history_window_ends_at": _time(record.history_window_ends_at),
        "next_follow_up_at": _time(record.next_follow_up_at),
        "expires_at": _time(record.expires_at),
        "cooldown_until": _time(record.cooldown_until),
        "follow_up_interval_microseconds": _duration_microseconds(record.follow_up_interval),
        "retry_delay_microseconds": _duration_microseconds(record.retry_delay),
        "max_retries": record.max_retries,
        "background_cohort": _cohort_payload(record.background_cohort),
        "limitations": list(record.limitations),
    }


def _registration_from_payload(payload: Mapping[str, Any]) -> WatchRegistration:
    return WatchRegistration(
        registration_uid=str(payload["registration_uid"]),
        request_uid=str(payload["request_uid"]),
        incident_uid=str(payload["incident_uid"]),
        actor_uid=str(payload["actor_uid"]),
        actor_visibility=ActorVisibility(str(payload["actor_visibility"])),
        enrolled_at=_parse_time(payload["enrolled_at"], "enrolled_at"),
        trigger_as_of=_parse_time(payload["trigger_as_of"], "trigger_as_of"),
        source_scope=_scope_from_payload(payload["source_scope"]),
        trigger_source_uid=str(payload["trigger_source_uid"]),
        trigger_raw_artifact_uids=tuple(str(item) for item in payload["trigger_raw_artifact_uids"]),
        fill_lineage=tuple(_lineage_from_payload(item) for item in payload["fill_lineage"]),
        attributable_notional=Decimal(str(payload["attributable_notional"])),
        history_window_starts_at=_parse_time(payload["history_window_starts_at"], "history_window_starts_at"),
        history_window_ends_at=_parse_time(payload["history_window_ends_at"], "history_window_ends_at"),
        next_follow_up_at=_parse_time(payload["next_follow_up_at"], "next_follow_up_at"),
        expires_at=_parse_time(payload["expires_at"], "expires_at"),
        cooldown_until=_parse_time(payload["cooldown_until"], "cooldown_until"),
        follow_up_interval=timedelta(microseconds=int(payload["follow_up_interval_microseconds"])),
        retry_delay=timedelta(microseconds=int(payload["retry_delay_microseconds"])),
        max_retries=int(payload["max_retries"]),
        background_cohort=_cohort_from_payload(payload["background_cohort"]),
        limitations=tuple(str(item) for item in payload["limitations"]),
    )


def _snapshot_payload(record: WalletHistorySnapshot) -> dict[str, Any]:
    return {
        "snapshot_uid": record.snapshot_uid,
        "registration_uid": record.registration_uid,
        "actor_uid": record.actor_uid,
        "as_of": _time(record.as_of),
        "available_at": _time(record.available_at),
        "source_scope": _scope_payload(record.source_scope),
        "window_starts_at": _time(record.window_starts_at),
        "window_ends_at": _time(record.window_ends_at),
        "fill_lineage": [_lineage_payload(item) for item in record.fill_lineage],
        "trigger_eligible_fill_uids": list(record.trigger_eligible_fill_uids),
        "follow_up_only_fill_uids": list(record.follow_up_only_fill_uids),
        "coverage_status": record.coverage_status.value,
        "complete_through": None if record.complete_through is None else _time(record.complete_through),
        "continuation_token": record.continuation_token,
        "missing_reasons": list(record.missing_reasons),
        "late_excluded_count": record.late_excluded_count,
        "supports_original_trigger": record.supports_original_trigger,
        "background_cohort": _cohort_payload(record.background_cohort),
    }


def _snapshot_from_payload(payload: Mapping[str, Any]) -> WalletHistorySnapshot:
    complete_through = payload.get("complete_through")
    continuation_token = payload.get("continuation_token")
    return WalletHistorySnapshot(
        snapshot_uid=str(payload["snapshot_uid"]),
        registration_uid=str(payload["registration_uid"]),
        actor_uid=str(payload["actor_uid"]),
        as_of=_parse_time(payload["as_of"], "as_of"),
        available_at=_parse_time(payload["available_at"], "available_at"),
        source_scope=_scope_from_payload(payload["source_scope"]),
        window_starts_at=_parse_time(payload["window_starts_at"], "window_starts_at"),
        window_ends_at=_parse_time(payload["window_ends_at"], "window_ends_at"),
        fill_lineage=tuple(_lineage_from_payload(item) for item in payload["fill_lineage"]),
        trigger_eligible_fill_uids=tuple(str(item) for item in payload["trigger_eligible_fill_uids"]),
        follow_up_only_fill_uids=tuple(str(item) for item in payload["follow_up_only_fill_uids"]),
        coverage_status=CoverageStatus(str(payload["coverage_status"])),
        complete_through=None if complete_through is None else _parse_time(complete_through, "complete_through"),
        continuation_token=None if continuation_token is None else str(continuation_token),
        missing_reasons=tuple(str(item) for item in payload["missing_reasons"]),
        late_excluded_count=int(payload["late_excluded_count"]),
        supports_original_trigger=bool(payload["supports_original_trigger"]),
        background_cohort=_cohort_from_payload(payload["background_cohort"]),
    )


def _state_payload(record: WatchFollowUpState) -> dict[str, Any]:
    return {
        "state_uid": record.state_uid,
        "registration_uid": record.registration_uid,
        "status": record.status.value,
        "as_of": _time(record.as_of),
        "available_at": _time(record.available_at),
        "next_due_at": None if record.next_due_at is None else _time(record.next_due_at),
        "expires_at": _time(record.expires_at),
        "retry_count": record.retry_count,
        "max_retries": record.max_retries,
        "continuation_token": record.continuation_token,
        "coverage_status": record.coverage_status.value,
        "missing_reasons": list(record.missing_reasons),
        "last_snapshot_uid": record.last_snapshot_uid,
    }


def _state_from_payload(payload: Mapping[str, Any]) -> WatchFollowUpState:
    next_due_at = payload.get("next_due_at")
    continuation_token = payload.get("continuation_token")
    last_snapshot_uid = payload.get("last_snapshot_uid")
    return WatchFollowUpState(
        state_uid=str(payload["state_uid"]),
        registration_uid=str(payload["registration_uid"]),
        status=FollowUpStatus(str(payload["status"])),
        as_of=_parse_time(payload["as_of"], "as_of"),
        available_at=_parse_time(payload["available_at"], "available_at"),
        next_due_at=None if next_due_at is None else _parse_time(next_due_at, "next_due_at"),
        expires_at=_parse_time(payload["expires_at"], "expires_at"),
        retry_count=int(payload["retry_count"]),
        max_retries=int(payload["max_retries"]),
        continuation_token=None if continuation_token is None else str(continuation_token),
        coverage_status=CoverageStatus(str(payload["coverage_status"])),
        missing_reasons=tuple(str(item) for item in payload["missing_reasons"]),
        last_snapshot_uid=None if last_snapshot_uid is None else str(last_snapshot_uid),
    )


def _unsigned_entry(
    *,
    sequence: int,
    record_type: WalletRecordType,
    record_uid: str,
    payload_hash: str,
    payload: Mapping[str, Any],
    previous_hash: str,
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "schema_version": SCHEMA_VERSION,
        "record_type": record_type.value,
        "record_uid": record_uid,
        "payload_hash": payload_hash,
        "payload": dict(payload),
        "previous_hash": previous_hash,
    }


def _entry_from_mapping(item: Mapping[str, Any]) -> WalletLedgerEntry:
    return WalletLedgerEntry(
        sequence=int(item["sequence"]),
        schema_version=int(item["schema_version"]),
        record_type=WalletRecordType(str(item["record_type"])),
        record_uid=str(item["record_uid"]),
        payload_hash=str(item["payload_hash"]),
        payload=dict(item["payload"]),
        previous_hash=str(item["previous_hash"]),
        entry_hash=str(item["entry_hash"]),
    )


class WalletTrackingStore:
    """Hash-chained append-only wallet-watch ledger with restart replay."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.ledger_path = self.root / LEDGER_NAME
        self.lock_path = self.root / ".wallet_tracking.lock"
        self.quarantine_root = self.root / "quarantine" / "uid-conflicts"
        self.quarantine_root.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._entries: tuple[WalletLedgerEntry, ...] = ()
        self._by_uid: dict[str, WalletLedgerEntry] = {}
        self._refresh()

    @contextmanager
    def _coordinated(self) -> Iterator[None]:
        """Refresh and operate while holding instance and same-path locks."""

        with self._lock:
            with _exclusive_path_lock(self.lock_path):
                self._refresh_unlocked()
                yield

    def _refresh(self) -> None:
        with self._coordinated():
            pass

    def _refresh_unlocked(self) -> None:
        entries: list[WalletLedgerEntry] = []
        by_uid: dict[str, WalletLedgerEntry] = {}
        previous_hash = ZERO_HASH
        if not self.ledger_path.exists():
            self._entries = ()
            self._by_uid = {}
            return
        try:
            lines = self.ledger_path.read_bytes().splitlines(keepends=True)
            for expected_sequence, raw_line in enumerate(lines, start=1):
                if not raw_line.endswith(b"\n"):
                    raise WalletLedgerTamperError("wallet ledger ends with a partial line")
                body = raw_line[:-1]
                item = json.loads(body.decode("utf-8"))
                if not isinstance(item, dict) or canonical_json_bytes(item) != body:
                    raise WalletLedgerTamperError("wallet ledger line is not canonical JSON")
                entry = _entry_from_mapping(item)
                if entry.schema_version != SCHEMA_VERSION:
                    raise WalletLedgerTamperError("unsupported wallet ledger schema version")
                if entry.sequence != expected_sequence or entry.previous_hash != previous_hash:
                    raise WalletLedgerTamperError("wallet ledger sequence or hash-chain link is invalid")
                unsigned = _unsigned_entry(
                    sequence=entry.sequence,
                    record_type=entry.record_type,
                    record_uid=entry.record_uid,
                    payload_hash=entry.payload_hash,
                    payload=entry.payload,
                    previous_hash=entry.previous_hash,
                )
                expected_entry_hash = sha256(canonical_json_bytes(unsigned)).hexdigest()
                expected_payload_hash = sha256(canonical_json_bytes(entry.payload)).hexdigest()
                if entry.entry_hash != expected_entry_hash or entry.payload_hash != expected_payload_hash:
                    raise WalletLedgerTamperError("wallet ledger payload or entry hash is invalid")
                if entry.record_uid in by_uid:
                    raise WalletLedgerTamperError("wallet ledger repeats an immutable record UID")
                # Reconstructing here also validates the persisted domain contract.
                self._decode(entry)
                entries.append(entry)
                by_uid[entry.record_uid] = entry
                previous_hash = entry.entry_hash
        except WalletLedgerTamperError:
            raise
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WalletLedgerTamperError("wallet ledger cannot be safely replayed") from exc
        self._entries = tuple(entries)
        self._by_uid = by_uid

    @staticmethod
    def _decode(entry: WalletLedgerEntry) -> WatchRegistration | WalletHistorySnapshot | WatchFollowUpState:
        if entry.record_type == WalletRecordType.REGISTRATION:
            return _registration_from_payload(entry.payload)
        if entry.record_type == WalletRecordType.HISTORY_SNAPSHOT:
            return _snapshot_from_payload(entry.payload)
        return _state_from_payload(entry.payload)

    def entries(self) -> tuple[WalletLedgerEntry, ...]:
        with self._coordinated():
            return self._entries

    def _registrations_current(self) -> tuple[WatchRegistration, ...]:
        return tuple(
            record
            for entry in self._entries
            if entry.record_type == WalletRecordType.REGISTRATION
            for record in (self._decode(entry),)
            if isinstance(record, WatchRegistration)
        )

    def _snapshots_current(self, registration_uid: str | None = None) -> tuple[WalletHistorySnapshot, ...]:
        return tuple(
            record
            for entry in self._entries
            if entry.record_type == WalletRecordType.HISTORY_SNAPSHOT
            for record in (self._decode(entry),)
            if isinstance(record, WalletHistorySnapshot)
            and (registration_uid is None or record.registration_uid == registration_uid)
        )

    def _states_current(self, registration_uid: str | None = None) -> tuple[WatchFollowUpState, ...]:
        return tuple(
            record
            for entry in self._entries
            if entry.record_type == WalletRecordType.FOLLOW_UP_STATE
            for record in (self._decode(entry),)
            if isinstance(record, WatchFollowUpState)
            and (registration_uid is None or record.registration_uid == registration_uid)
        )

    def registrations(self) -> tuple[WatchRegistration, ...]:
        with self._coordinated():
            return self._registrations_current()

    def history_snapshots(self, registration_uid: str | None = None) -> tuple[WalletHistorySnapshot, ...]:
        with self._coordinated():
            return self._snapshots_current(registration_uid)

    def follow_up_states(self, registration_uid: str | None = None) -> tuple[WatchFollowUpState, ...]:
        with self._coordinated():
            return self._states_current(registration_uid)

    def latest_follow_up_state(self, registration_uid: str) -> WatchFollowUpState | None:
        with self._coordinated():
            states = self._states_current(registration_uid)
            return states[-1] if states else None

    def _quarantine_conflict(
        self,
        existing: WalletLedgerEntry,
        record_type: WalletRecordType,
        payload: Mapping[str, Any],
        incoming_hash: str,
    ) -> Path:
        safe_uid_hash = sha256(existing.record_uid.encode("utf-8")).hexdigest()
        path = self.quarantine_root / f"{safe_uid_hash}-{incoming_hash}.json"
        envelope = {
            "reason": "immutable_uid_conflict",
            "record_uid": existing.record_uid,
            "existing_record_type": existing.record_type.value,
            "incoming_record_type": record_type.value,
            "existing_payload_hash": existing.payload_hash,
            "incoming_payload_hash": incoming_hash,
            "existing_entry_hash": existing.entry_hash,
            "incoming_payload": dict(payload),
        }
        encoded = canonical_json_bytes(envelope)
        if path.exists():
            if path.read_bytes() != encoded:
                raise WalletLedgerTamperError("wallet conflict quarantine path contains different bytes")
            return path
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        return path

    def _append_current(
        self,
        record_type: WalletRecordType,
        record_uid: str,
        payload: Mapping[str, Any],
    ) -> WalletStoreWriteResult:
        payload_hash = sha256(canonical_json_bytes(payload)).hexdigest()
        existing = self._by_uid.get(record_uid)
        if existing is not None:
            if (
                existing.record_type == record_type
                and existing.payload_hash == payload_hash
                and canonical_json_bytes(existing.payload) == canonical_json_bytes(payload)
            ):
                return WalletStoreWriteResult(StoreWriteStatus.DUPLICATE, existing)
            path = self._quarantine_conflict(existing, record_type, payload, payload_hash)
            raise WalletRecordConflictError(record_uid, path)
        previous_hash = self._entries[-1].entry_hash if self._entries else ZERO_HASH
        unsigned = _unsigned_entry(
            sequence=len(self._entries) + 1,
            record_type=record_type,
            record_uid=record_uid,
            payload_hash=payload_hash,
            payload=payload,
            previous_hash=previous_hash,
        )
        item = {**unsigned, "entry_hash": sha256(canonical_json_bytes(unsigned)).hexdigest()}
        encoded = canonical_json_bytes(item) + b"\n"
        with self.ledger_path.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        self._refresh_unlocked()
        entry = self._by_uid[record_uid]
        return WalletStoreWriteResult(StoreWriteStatus.INSERTED, entry)

    def _replay_if_present_current(
        self,
        record_type: WalletRecordType,
        record_uid: str,
        payload: Mapping[str, Any],
    ) -> WalletStoreWriteResult | None:
        """Resolve duplicate/conflict semantics before transition validation.

        This keeps an exact replay idempotent even when later immutable records
        have already advanced the watch, while still quarantining a reused UID
        before any incoming fields can influence relationship checks.
        """

        if record_uid not in self._by_uid:
            return None
        return self._append_current(record_type, record_uid, payload)

    def append_registration(self, registration: WatchRegistration) -> WalletStoreWriteResult:
        if not isinstance(registration, WatchRegistration):
            raise TypeError("registration must be a WatchRegistration")
        with self._coordinated():
            return self._append_current(
                WalletRecordType.REGISTRATION,
                registration.registration_uid,
                _registration_payload(registration),
            )

    def append_history_snapshot(self, snapshot: WalletHistorySnapshot) -> WalletStoreWriteResult:
        if not isinstance(snapshot, WalletHistorySnapshot):
            raise TypeError("snapshot must be a WalletHistorySnapshot")
        payload = _snapshot_payload(snapshot)
        with self._coordinated():
            replay = self._replay_if_present_current(
                WalletRecordType.HISTORY_SNAPSHOT,
                snapshot.snapshot_uid,
                payload,
            )
            if replay is not None:
                return replay
            registration = next(
                (
                    item
                    for item in self._registrations_current()
                    if item.registration_uid == snapshot.registration_uid
                ),
                None,
            )
            if registration is None:
                raise WalletStoreError("history snapshot references an unknown registration")
            if (
                snapshot.actor_uid != registration.actor_uid
                or snapshot.source_scope != registration.source_scope
                or snapshot.background_cohort != registration.background_cohort
            ):
                raise WalletStoreError("history snapshot conflicts with its watch registration")
            if (
                snapshot.window_starts_at != registration.history_window_starts_at
                or snapshot.window_ends_at != registration.history_window_ends_at
            ):
                raise WalletStoreError("history snapshot must preserve the registration's bounded history window")
            return self._append_current(
                WalletRecordType.HISTORY_SNAPSHOT,
                snapshot.snapshot_uid,
                payload,
            )

    def append_follow_up_state(self, state: WatchFollowUpState) -> WalletStoreWriteResult:
        if not isinstance(state, WatchFollowUpState):
            raise TypeError("state must be a WatchFollowUpState")
        payload = _state_payload(state)
        with self._coordinated():
            return self._append_follow_up_state_current(state, payload)

    def _append_follow_up_state_current(
        self,
        state: WatchFollowUpState,
        payload: Mapping[str, Any],
    ) -> WalletStoreWriteResult:
        replay = self._replay_if_present_current(
            WalletRecordType.FOLLOW_UP_STATE,
            state.state_uid,
            payload,
        )
        if replay is not None:
            return replay
        registration = next(
            (
                item
                for item in self._registrations_current()
                if item.registration_uid == state.registration_uid
            ),
            None,
        )
        if registration is None:
            raise WalletStoreError("follow-up state references an unknown registration")
        if state.expires_at != registration.expires_at or state.max_retries != registration.max_retries:
            raise WalletStoreError("follow-up state conflicts with its watch registration")
        if state.as_of < registration.enrolled_at or state.available_at < registration.enrolled_at:
            raise WalletStoreError("follow-up state cannot precede watch enrollment")
        states = self._states_current(state.registration_uid)
        previous = states[-1] if states else None
        if previous is not None:
            terminal = {
                FollowUpStatus.COMPLETE,
                FollowUpStatus.EXPIRED,
                FollowUpStatus.RETRIES_EXHAUSTED,
            }
            if previous.status in terminal:
                raise WalletStoreError("terminal follow-up states are irreversible")
            if state.as_of < previous.as_of or state.available_at < previous.available_at:
                raise WalletStoreError("follow-up state attempts to move a watch clock backward")
            if state.retry_count < previous.retry_count:
                raise WalletStoreError("follow-up state attempts to reduce the retry count")
        referenced_snapshot: WalletHistorySnapshot | None = None
        if state.last_snapshot_uid is not None:
            referenced_snapshot = next(
                (
                    item
                    for item in self._snapshots_current(state.registration_uid)
                    if item.snapshot_uid == state.last_snapshot_uid
                ),
                None,
            )
            if referenced_snapshot is None:
                raise WalletStoreError("follow-up state references an unknown history snapshot")
            if (
                referenced_snapshot.as_of > state.as_of
                or referenced_snapshot.available_at > state.available_at
            ):
                raise WalletStoreError("follow-up state cannot reference a later history snapshot")
        if state.status == FollowUpStatus.COMPLETE:
            if (
                referenced_snapshot is None
                or not referenced_snapshot.complete
                or referenced_snapshot.complete_through is None
                or referenced_snapshot.complete_through < registration.expires_at
            ):
                raise WalletStoreError(
                    "complete follow-up state requires a persisted snapshot complete through watch expiry"
                )
            if (
                state.coverage_status != referenced_snapshot.coverage_status
                or state.continuation_token != referenced_snapshot.continuation_token
                or state.missing_reasons != referenced_snapshot.missing_reasons
            ):
                raise WalletStoreError("complete follow-up state must agree with its persisted snapshot metadata")
        if state.status == FollowUpStatus.EXPIRED and state.as_of < registration.expires_at:
            raise WalletStoreError("expired follow-up state requires the watch expiry clock to be reached")
        if (
            state.status == FollowUpStatus.RETRIES_EXHAUSTED
            and state.retry_count <= registration.max_retries
        ):
            raise WalletStoreError("retries-exhausted state requires the declared retry budget to be exceeded")
        return self._append_current(
            WalletRecordType.FOLLOW_UP_STATE,
            state.state_uid,
            payload,
        )

    def due_watches(self, as_of: datetime) -> tuple[DueWalletWatch, ...]:
        cutoff = utc_datetime(as_of, "as_of")
        with self._coordinated():
            registrations = self._registrations_current()
            latest: dict[str, WatchFollowUpState] = {}
            for state in self._states_current():
                latest[state.registration_uid] = state
            terminal = {
                FollowUpStatus.COMPLETE,
                FollowUpStatus.EXPIRED,
                FollowUpStatus.RETRIES_EXHAUSTED,
            }
            due: list[DueWalletWatch] = []
            for registration in registrations:
                state = latest.get(registration.registration_uid)
                if state is not None and state.status in terminal:
                    continue
                due_at = registration.next_follow_up_at if state is None else state.next_due_at
                if due_at is not None and due_at <= cutoff:
                    due.append(DueWalletWatch(registration, state, due_at))
            return tuple(sorted(due, key=lambda item: (item.due_at, item.registration.registration_uid)))


class WalletFillIndex:
    """SQLite pointer index for canonical public-wallet fill files.

    Rows contain only identity-free actor pseudonyms, causal clocks, raw lineage,
    and a verified path/hash pointer.  Price, size, side, maker/taker, and all
    other evidence fields remain solely in the canonical normalized file.
    """

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database_path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS wallet_fill_index (
                    fill_uid TEXT PRIMARY KEY,
                    actor_uid TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    market_uid TEXT NOT NULL,
                    outcome_uid TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    raw_artifact_uid TEXT NOT NULL,
                    transaction_uid TEXT,
                    normalized_path TEXT NOT NULL,
                    normalized_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS wallet_fill_actor_event
                    ON wallet_fill_index(actor_uid, event_time, fill_uid);
                CREATE INDEX IF NOT EXISTS wallet_fill_available
                    ON wallet_fill_index(available_at);
                CREATE TABLE IF NOT EXISTS wallet_fill_conflicts (
                    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fill_uid TEXT NOT NULL,
                    existing_fingerprint TEXT NOT NULL,
                    incoming_fingerprint TEXT NOT NULL,
                    incoming_path TEXT NOT NULL,
                    incoming_sha256 TEXT NOT NULL,
                    UNIQUE(fill_uid, incoming_fingerprint)
                );
                """
            )

    @staticmethod
    def _row_payload(fill: TradeFill, normalized_path: Path, content_hash: str) -> dict[str, Any]:
        if fill.actor_visibility != ActorVisibility.PUBLIC_WALLET or fill.actor_uid is None:
            raise ValueError("wallet fill index accepts only attributable PUBLIC_WALLET fills")
        if fill.event_time > fill.ingested_at:
            raise ValueError("fill event_time cannot be later than its availability clock")
        return {
            "fill_uid": fill.fill_uid,
            "actor_uid": fill.actor_uid,
            "event_time": _time(fill.event_time),
            "available_at": _time(fill.ingested_at),
            "market_uid": fill.market_uid,
            "outcome_uid": fill.outcome_uid,
            "source_uid": fill.source_uid,
            "raw_artifact_uid": fill.raw_artifact_uid,
            "transaction_uid": fill.transaction_uid,
            "normalized_path": str(normalized_path),
            "normalized_sha256": content_hash,
        }

    @staticmethod
    def _reference(payload: Mapping[str, Any]) -> IndexedFillRef:
        transaction_uid = payload.get("transaction_uid")
        return IndexedFillRef(
            actor_uid=str(payload["actor_uid"]),
            event_time=_parse_time(payload["event_time"], "event_time"),
            available_at=_parse_time(payload["available_at"], "available_at"),
            fill_uid=str(payload["fill_uid"]),
            market_uid=str(payload["market_uid"]),
            outcome_uid=str(payload["outcome_uid"]),
            source_uid=str(payload["source_uid"]),
            raw_artifact_uid=str(payload["raw_artifact_uid"]),
            transaction_uid=None if transaction_uid is None else str(transaction_uid),
            normalized_path=Path(str(payload["normalized_path"])),
            normalized_sha256=str(payload["normalized_sha256"]),
        )

    def index_fill(self, fill: TradeFill, normalized_path: str | Path) -> FillIndexWriteResult:
        if not isinstance(fill, TradeFill):
            raise TypeError("fill must be a canonical TradeFill")
        path = Path(normalized_path).resolve(strict=True)
        raw = path.read_bytes()
        expected = canonical_json_bytes(fill)
        if raw != expected:
            raise ValueError("normalized_path bytes do not exactly match the canonical TradeFill")
        content_hash = sha256(raw).hexdigest()
        payload = self._row_payload(fill, path, content_hash)
        columns = tuple(payload)
        values = tuple(payload[column] for column in columns)
        incoming_fingerprint = sha256(canonical_json_bytes(payload)).hexdigest()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing_row = connection.execute(
                "SELECT * FROM wallet_fill_index WHERE fill_uid = ?",
                (fill.fill_uid,),
            ).fetchone()
            if existing_row is not None:
                existing = {column: existing_row[column] for column in columns}
                if existing == payload:
                    connection.commit()
                    return FillIndexWriteResult(StoreWriteStatus.DUPLICATE, self._reference(existing))
                existing_fingerprint = sha256(canonical_json_bytes(existing)).hexdigest()
                connection.execute(
                    """
                    INSERT OR IGNORE INTO wallet_fill_conflicts (
                        fill_uid, existing_fingerprint, incoming_fingerprint,
                        incoming_path, incoming_sha256
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        fill.fill_uid,
                        existing_fingerprint,
                        incoming_fingerprint,
                        str(path),
                        content_hash,
                    ),
                )
                connection.commit()
                raise WalletFillConflictError(fill.fill_uid)
            placeholders = ",".join("?" for _ in columns)
            connection.execute(
                f"INSERT INTO wallet_fill_index ({','.join(columns)}) VALUES ({placeholders})",
                values,
            )
            connection.commit()
        return FillIndexWriteResult(StoreWriteStatus.INSERTED, self._reference(payload))

    def query_actor(
        self,
        actor_uid: str,
        *,
        start: datetime,
        end: datetime,
        as_of: datetime,
        limit: int = 1_000,
    ) -> tuple[IndexedFillRef, ...]:
        normalized_actor = str(actor_uid).strip()
        if not normalized_actor:
            raise ValueError("actor_uid must be non-empty")
        start_at = utc_datetime(start, "start")
        end_at = utc_datetime(end, "end")
        cutoff = utc_datetime(as_of, "as_of")
        if end_at < start_at:
            raise ValueError("end cannot precede start")
        if not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        causal_end = min(end_at, cutoff)
        if causal_end < start_at:
            return ()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM wallet_fill_index
                WHERE actor_uid = ?
                  AND event_time >= ?
                  AND event_time <= ?
                  AND available_at <= ?
                ORDER BY event_time ASC, fill_uid ASC
                LIMIT ?
                """,
                (
                    normalized_actor,
                    _time(start_at),
                    _time(causal_end),
                    _time(cutoff),
                    limit,
                ),
            ).fetchall()
        return tuple(self._reference(dict(row)) for row in rows)

    def conflict_count(self, fill_uid: str | None = None) -> int:
        with self._connect() as connection:
            if fill_uid is None:
                row = connection.execute("SELECT COUNT(*) AS count FROM wallet_fill_conflicts").fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM wallet_fill_conflicts WHERE fill_uid = ?",
                    (fill_uid,),
                ).fetchone()
        return int(row["count"])
