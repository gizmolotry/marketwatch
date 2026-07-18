"""Bounded, raw-first durable runner for approved venue stream targets.

This module owns transport lifecycle, raw receipts, checkpoint recovery, and
book freshness gates.  Parsers remain external callbacks: the runner neither
decodes venue payloads itself nor derives a market state from them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import hashlib
import inspect
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Literal, Protocol, runtime_checkable

from marketleak.ingestion.config.phase15_registry import ApprovedMarketTarget
from marketleak.ingestion.normalize import canonical_json_bytes, utc_datetime
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture


RUNNER_SCHEMA_VERSION = "venue-stream-runner-v1"
ItemKind = Literal["raw", "control", "error", "reconnect"]
BookSnapshotState = Literal["requires_full_snapshot", "fresh"]
ConnectionState = Literal["idle", "connected", "reconnecting", "complete", "partial"]


class VenueStreamRuntimeError(ValueError):
    """Raised when an approved bounded stream target cannot be run safely."""


@dataclass(frozen=True, slots=True)
class VenueStreamItem:
    """Opaque inbound material, preserved before a decoder sees it."""

    kind: ItemKind
    payload: bytes | str | Mapping[str, Any]
    venue_sequence: int | None = None
    received_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DecodedVenueRecord:
    """Decoder-supplied delivery metadata; no market semantics are inferred."""

    record: Any
    venue_event_time: datetime | None = None
    is_book_derived: bool = False
    is_full_book_snapshot: bool = False


@dataclass(frozen=True, slots=True)
class StreamGapInterval:
    """An explicit discontinuity that keeps coverage partial until remediated."""

    started_at: datetime
    ended_at: datetime | None
    reason_code: str
    expected_sequence: int | None = None
    observed_sequence: int | None = None

    def __post_init__(self) -> None:
        start = utc_datetime(self.started_at, "started_at")
        end = None if self.ended_at is None else utc_datetime(self.ended_at, "ended_at")
        if end is not None and end < start:
            raise ValueError("gap ended_at cannot precede started_at")
        if not self.reason_code.strip():
            raise ValueError("gap reason_code is required")
        if self.expected_sequence is not None and self.expected_sequence < 0:
            raise ValueError("expected_sequence must be non-negative")
        if self.observed_sequence is not None and self.observed_sequence < 0:
            raise ValueError("observed_sequence must be non-negative")
        object.__setattr__(self, "started_at", start)
        object.__setattr__(self, "ended_at", end)


@dataclass(frozen=True, slots=True)
class VenueStreamCheckpoint:
    """Restartable state persisted atomically after each captured delivery."""

    schema_version: str
    target_uid: str
    venue: Literal["polymarket", "kalshi"]
    connection_state: ConnectionState
    local_receipt_high_watermark: datetime | None
    venue_event_high_watermark: datetime | None
    latest_raw_uid: str | None
    last_venue_sequence: int | None
    processed_item_count: int
    admitted_record_count: int
    withheld_book_record_count: int
    book_snapshot_state: BookSnapshotState
    snapshot_sequence: int | None
    gap_intervals: tuple[StreamGapInterval, ...] = ()

    def __post_init__(self) -> None:
        if self.schema_version != RUNNER_SCHEMA_VERSION:
            raise ValueError("unsupported checkpoint schema_version")
        if not self.target_uid or ":" not in self.target_uid:
            raise ValueError("target_uid must be a stable target identifier")
        if self.venue not in {"polymarket", "kalshi"}:
            raise ValueError("venue must be polymarket or kalshi")
        if self.processed_item_count < 0 or self.admitted_record_count < 0 or self.withheld_book_record_count < 0:
            raise ValueError("checkpoint counts must be non-negative")
        if self.last_venue_sequence is not None and self.last_venue_sequence < 0:
            raise ValueError("last_venue_sequence must be non-negative")
        if self.snapshot_sequence is not None and self.snapshot_sequence < 0:
            raise ValueError("snapshot_sequence must be non-negative")
        for field_name in ("local_receipt_high_watermark", "venue_event_high_watermark"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, utc_datetime(value, field_name))

    @property
    def coverage_status(self) -> Literal["complete", "partial"]:
        return "partial" if self.gap_intervals or self.book_snapshot_state != "fresh" else "complete"

    def to_json_dict(self) -> dict[str, Any]:
        def timestamp(value: datetime | None) -> str | None:
            return None if value is None else value.isoformat().replace("+00:00", "Z")

        return {
            "schema_version": self.schema_version,
            "target_uid": self.target_uid,
            "venue": self.venue,
            "connection_state": self.connection_state,
            "local_receipt_high_watermark": timestamp(self.local_receipt_high_watermark),
            "venue_event_high_watermark": timestamp(self.venue_event_high_watermark),
            "latest_raw_uid": self.latest_raw_uid,
            "last_venue_sequence": self.last_venue_sequence,
            "processed_item_count": self.processed_item_count,
            "admitted_record_count": self.admitted_record_count,
            "withheld_book_record_count": self.withheld_book_record_count,
            "book_snapshot_state": self.book_snapshot_state,
            "snapshot_sequence": self.snapshot_sequence,
            "gap_intervals": [
                {
                    "started_at": timestamp(item.started_at),
                    "ended_at": timestamp(item.ended_at),
                    "reason_code": item.reason_code,
                    "expected_sequence": item.expected_sequence,
                    "observed_sequence": item.observed_sequence,
                }
                for item in self.gap_intervals
            ],
        }

    @classmethod
    def from_json_dict(cls, value: Mapping[str, Any]) -> "VenueStreamCheckpoint":
        gaps = tuple(
            StreamGapInterval(
                started_at=utc_datetime(item["started_at"], "gap.started_at"),
                ended_at=None if item.get("ended_at") is None else utc_datetime(item["ended_at"], "gap.ended_at"),
                reason_code=str(item["reason_code"]),
                expected_sequence=item.get("expected_sequence"),
                observed_sequence=item.get("observed_sequence"),
            )
            for item in value.get("gap_intervals", ())
        )
        return cls(
            schema_version=str(value["schema_version"]),
            target_uid=str(value["target_uid"]),
            venue=str(value["venue"]),  # type: ignore[arg-type]
            connection_state=str(value["connection_state"]),  # type: ignore[arg-type]
            local_receipt_high_watermark=(
                None
                if value.get("local_receipt_high_watermark") is None
                else utc_datetime(value["local_receipt_high_watermark"], "local_receipt_high_watermark")
            ),
            venue_event_high_watermark=(
                None
                if value.get("venue_event_high_watermark") is None
                else utc_datetime(value["venue_event_high_watermark"], "venue_event_high_watermark")
            ),
            latest_raw_uid=None if value.get("latest_raw_uid") is None else str(value["latest_raw_uid"]),
            last_venue_sequence=value.get("last_venue_sequence"),
            processed_item_count=value["processed_item_count"],
            admitted_record_count=value["admitted_record_count"],
            withheld_book_record_count=value["withheld_book_record_count"],
            book_snapshot_state=str(value["book_snapshot_state"]),  # type: ignore[arg-type]
            snapshot_sequence=value.get("snapshot_sequence"),
            gap_intervals=gaps,
        )


@dataclass(frozen=True, slots=True)
class VenueStreamRunResult:
    """Bounded run result containing only decoder-approved opaque records."""

    checkpoint: VenueStreamCheckpoint
    checkpoint_path: Path
    admitted_records: tuple[Any, ...]
    raw_captures: tuple[RawCapture, ...]
    terminated_by: Literal["message_bound", "duration_bound", "input_exhausted", "reconnect_bound"]


@runtime_checkable
class VenueStreamSession(Protocol):
    async def recv(self) -> VenueStreamItem: ...

    async def close(self) -> None: ...


Decoder = Callable[[VenueStreamItem, RawCapture], DecodedVenueRecord | None | Awaitable[DecodedVenueRecord | None]]
SessionFactory = Callable[[ApprovedMarketTarget, int], VenueStreamSession | Awaitable[VenueStreamSession]]
Clock = Callable[[], datetime]
MonotonicClock = Callable[[], float]


def _target_bounds(target: ApprovedMarketTarget) -> tuple[str, Literal["polymarket", "kalshi"], int, float, int]:
    """Accept only registry-validated targets, never target-shaped user input."""

    if not isinstance(target, ApprovedMarketTarget):
        raise VenueStreamRuntimeError("runner requires an ApprovedMarketTarget from the source registry")
    target_uid = target.target_uid
    venue = target.venue
    identifiers = target.polymarket_asset_ids if venue == "polymarket" else target.kalshi_tickers
    if not identifiers:
        raise VenueStreamRuntimeError("approved target requires venue-specific identifiers")
    max_messages = target.stream_settings.max_messages
    duration_seconds = target.stream_settings.duration_seconds
    max_reconnects = target.stream_settings.max_reconnects
    if isinstance(max_messages, bool) or not isinstance(max_messages, int) or not 1 <= max_messages <= 10_000:
        raise VenueStreamRuntimeError("target stream_settings.max_messages must be in [1, 10000]")
    if isinstance(duration_seconds, bool) or not isinstance(duration_seconds, (int, float)) or not 0 < float(duration_seconds) <= 3_600:
        raise VenueStreamRuntimeError("target stream_settings.duration_seconds must be in (0, 3600]")
    if isinstance(max_reconnects, bool) or not isinstance(max_reconnects, int) or not 0 <= max_reconnects <= 10:
        raise VenueStreamRuntimeError("target stream_settings.max_reconnects must be in [0, 10]")
    return target_uid, venue, max_messages, float(duration_seconds), max_reconnects


def _payload_bytes(payload: bytes | str | Mapping[str, Any]) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, Mapping):
        return canonical_json_bytes(dict(payload))
    raise TypeError("stream item payload must be bytes, str, or a mapping")


def _atomic_json_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class VenueStreamRunner:
    """Run one approved bounded stream with durable raw-first checkpoints."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        checkpoint_root: str | Path,
        session_factory: SessionFactory,
        decoder: Decoder,
        clock: Clock | None = None,
        monotonic_clock: MonotonicClock = time.monotonic,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be a RawArtifactStore")
        self.raw_store = raw_store
        self.checkpoint_root = Path(checkpoint_root)
        self.session_factory = session_factory
        self.decoder = decoder
        self.clock = clock or (lambda: datetime.now(UTC))
        self.monotonic_clock = monotonic_clock

    def checkpoint_path_for(self, target: ApprovedMarketTarget) -> Path:
        target_uid, _venue, _max_messages, _duration_seconds, _max_reconnects = _target_bounds(target)
        digest = hashlib.sha256(target_uid.encode("utf-8")).hexdigest()
        return self.checkpoint_root / f"{digest}.json"

    def load_checkpoint(self, target: ApprovedMarketTarget) -> VenueStreamCheckpoint | None:
        target_uid, venue, _max_messages, _duration_seconds, _max_reconnects = _target_bounds(target)
        path = self.checkpoint_path_for(target)
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, Mapping):
                raise ValueError("checkpoint must be a JSON object")
            checkpoint = VenueStreamCheckpoint.from_json_dict(value)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise VenueStreamRuntimeError("checkpoint is unavailable or invalid") from exc
        if checkpoint.target_uid != target_uid or checkpoint.venue != venue:
            raise VenueStreamRuntimeError("checkpoint does not match approved target")
        return checkpoint

    def _initial_checkpoint(self, *, target_uid: str, venue: Literal["polymarket", "kalshi"]) -> VenueStreamCheckpoint:
        return VenueStreamCheckpoint(
            schema_version=RUNNER_SCHEMA_VERSION,
            target_uid=target_uid,
            venue=venue,
            connection_state="idle",
            local_receipt_high_watermark=None,
            venue_event_high_watermark=None,
            latest_raw_uid=None,
            last_venue_sequence=None,
            processed_item_count=0,
            admitted_record_count=0,
            withheld_book_record_count=0,
            book_snapshot_state="requires_full_snapshot",
            snapshot_sequence=None,
            gap_intervals=(),
        )

    def _save_checkpoint(self, checkpoint: VenueStreamCheckpoint, path: Path) -> None:
        _atomic_json_write(path, checkpoint.to_json_dict())

    def _capture_item(
        self,
        *,
        target_uid: str,
        venue: Literal["polymarket", "kalshi"],
        item: VenueStreamItem,
        received_at: datetime,
    ) -> RawCapture:
        return self.raw_store.capture(
            _payload_bytes(item.payload),
            platform=venue,
            source=f"runtime/{item.kind}",
            request={
                "target_uid": target_uid,
                "venue": venue,
                "item_kind": item.kind,
                "venue_sequence": item.venue_sequence,
            },
            received_at=received_at,
            response_metadata={"transport": "injected_stream_session"},
        )

    @staticmethod
    def _raw_uid(capture: RawCapture) -> str:
        return f"{capture.platform}:raw/{capture.sha256}"

    @staticmethod
    def _close_open_gaps(gaps: tuple[StreamGapInterval, ...], *, ended_at: datetime) -> tuple[StreamGapInterval, ...]:
        return tuple(
            item
            if item.ended_at is not None
            else StreamGapInterval(
                started_at=item.started_at,
                ended_at=ended_at,
                reason_code=item.reason_code,
                expected_sequence=item.expected_sequence,
                observed_sequence=item.observed_sequence,
            )
            for item in gaps
        )

    @staticmethod
    def _append_gap(
        checkpoint: VenueStreamCheckpoint,
        *,
        at: datetime,
        reason_code: str,
        expected_sequence: int | None = None,
        observed_sequence: int | None = None,
    ) -> VenueStreamCheckpoint:
        return replace(
            checkpoint,
            connection_state="partial",
            book_snapshot_state="requires_full_snapshot",
            gap_intervals=(
                *checkpoint.gap_intervals,
                StreamGapInterval(
                    started_at=at,
                    ended_at=None,
                    reason_code=reason_code,
                    expected_sequence=expected_sequence,
                    observed_sequence=observed_sequence,
                ),
            ),
        )

    @staticmethod
    def _replace_checkpoint(checkpoint: VenueStreamCheckpoint, **updates: Any) -> VenueStreamCheckpoint:
        return replace(checkpoint, **updates)

    @staticmethod
    def _record_receipt(
        checkpoint: VenueStreamCheckpoint,
        *,
        received_at: datetime,
        capture: RawCapture,
        venue_sequence: int | None,
        advance_sequence: bool,
    ) -> VenueStreamCheckpoint:
        """Advance only monotonic receipt and sequence watermarks.

        Raw receipts are durable even when their associated delivery is
        rejected.  The watermarks deliberately describe the greatest trusted
        values observed, never merely the last item received by a reconnecting
        transport.
        """

        local_high_watermark = checkpoint.local_receipt_high_watermark
        if local_high_watermark is None or received_at > local_high_watermark:
            local_high_watermark = received_at

        last_venue_sequence = checkpoint.last_venue_sequence
        if advance_sequence and venue_sequence is not None and (
            last_venue_sequence is None or venue_sequence > last_venue_sequence
        ):
            last_venue_sequence = venue_sequence

        return replace(
            checkpoint,
            local_receipt_high_watermark=local_high_watermark,
            latest_raw_uid=VenueStreamRunner._raw_uid(capture),
            last_venue_sequence=last_venue_sequence,
            processed_item_count=checkpoint.processed_item_count + 1,
        )

    async def _decode(self, item: VenueStreamItem, capture: RawCapture) -> DecodedVenueRecord | None:
        decoded = self.decoder(item, capture)
        if inspect.isawaitable(decoded):
            decoded = await decoded
        if decoded is not None and not isinstance(decoded, DecodedVenueRecord):
            raise TypeError("decoder must return DecodedVenueRecord or None")
        return decoded

    async def _make_session(self, target: ApprovedMarketTarget, attempt: int) -> VenueStreamSession:
        session = self.session_factory(target, attempt)
        if inspect.isawaitable(session):
            session = await session
        if not isinstance(session, VenueStreamSession):
            raise TypeError("session_factory must return a VenueStreamSession-compatible object")
        return session

    @staticmethod
    async def _close_quietly(session: VenueStreamSession) -> None:
        try:
            await session.close()
        except Exception:
            return

    async def run(self, target: ApprovedMarketTarget) -> VenueStreamRunResult:
        """Run a finite target sample and atomically checkpoint every receipt."""

        target_uid, venue, max_messages, duration_seconds, max_reconnects = _target_bounds(target)
        checkpoint_path = self.checkpoint_path_for(target)
        restored = self.load_checkpoint(target)
        checkpoint = restored or self._initial_checkpoint(target_uid=target_uid, venue=venue)
        # A resumed connection cannot assume the prior book remains valid.
        if restored is not None:
            checkpoint = self._append_gap(checkpoint, at=utc_datetime(self.clock(), "reconnect_at"), reason_code="resumed_connection")
        checkpoint = self._replace_checkpoint(checkpoint, connection_state="connected")
        self._save_checkpoint(checkpoint, checkpoint_path)

        started = self.monotonic_clock()
        decoded_records: list[Any] = []
        captures: list[RawCapture] = []
        processed_this_run = 0
        reconnects = 0
        attempt = 0
        terminated_by: Literal["message_bound", "duration_bound", "input_exhausted", "reconnect_bound"] = "input_exhausted"

        while processed_this_run < max_messages:
            remaining_seconds = duration_seconds - (self.monotonic_clock() - started)
            if remaining_seconds <= 0:
                terminated_by = "duration_bound"
                break
            session: VenueStreamSession | None = None
            try:
                session = await self._make_session(target, attempt)
                checkpoint = self._replace_checkpoint(checkpoint, connection_state="connected")
                self._save_checkpoint(checkpoint, checkpoint_path)
                while processed_this_run < max_messages:
                    remaining_seconds = duration_seconds - (self.monotonic_clock() - started)
                    if remaining_seconds <= 0:
                        terminated_by = "duration_bound"
                        break
                    try:
                        item = await asyncio.wait_for(session.recv(), timeout=remaining_seconds)
                    except StopAsyncIteration:
                        terminated_by = "input_exhausted"
                        break
                    except asyncio.TimeoutError:
                        terminated_by = "duration_bound"
                        break
                    received_at = utc_datetime(item.received_at or self.clock(), "received_at")
                    capture = self._capture_item(target_uid=target_uid, venue=venue, item=item, received_at=received_at)
                    captures.append(capture)
                    processed_this_run += 1
                    prior_sequence = checkpoint.last_venue_sequence
                    expected_sequence = None if prior_sequence is None else prior_sequence + 1
                    if item.venue_sequence is not None and (
                        isinstance(item.venue_sequence, bool) or not isinstance(item.venue_sequence, int) or item.venue_sequence < 0
                    ):
                        checkpoint = self._append_gap(checkpoint, at=received_at, reason_code="invalid_venue_sequence")
                        checkpoint = self._record_receipt(
                            checkpoint,
                            received_at=received_at,
                            capture=capture,
                            venue_sequence=None,
                            advance_sequence=False,
                        )
                        self._save_checkpoint(checkpoint, checkpoint_path)
                        if processed_this_run >= max_messages:
                            terminated_by = "message_bound"
                            break
                        continue

                    if item.venue_sequence is not None and prior_sequence is not None and item.venue_sequence <= prior_sequence:
                        checkpoint = self._append_gap(
                            checkpoint,
                            at=received_at,
                            reason_code="replayed_or_out_of_order_sequence",
                            expected_sequence=expected_sequence,
                            observed_sequence=item.venue_sequence,
                        )
                        checkpoint = self._record_receipt(
                            checkpoint,
                            received_at=received_at,
                            capture=capture,
                            venue_sequence=item.venue_sequence,
                            advance_sequence=False,
                        )
                        self._save_checkpoint(checkpoint, checkpoint_path)
                        if processed_this_run >= max_messages:
                            terminated_by = "message_bound"
                            break
                        continue

                    sequence_gap = item.venue_sequence is not None and expected_sequence is not None and item.venue_sequence != expected_sequence
                    if sequence_gap:
                        checkpoint = self._append_gap(
                            checkpoint,
                            at=received_at,
                            reason_code="venue_sequence_gap",
                            expected_sequence=expected_sequence,
                            observed_sequence=item.venue_sequence,
                        )
                    if item.kind in {"error", "reconnect"}:
                        checkpoint = self._append_gap(checkpoint, at=received_at, reason_code=f"stream_{item.kind}")

                    try:
                        decoded = await self._decode(item, capture)
                    except Exception:
                        checkpoint = self._append_gap(checkpoint, at=received_at, reason_code="decoder_unavailable")
                        decoded = None
                    if decoded is not None:
                        event_time = None if decoded.venue_event_time is None else utc_datetime(decoded.venue_event_time, "venue_event_time")
                        if event_time is not None and (checkpoint.venue_event_high_watermark is None or event_time > checkpoint.venue_event_high_watermark):
                            checkpoint = self._replace_checkpoint(checkpoint, venue_event_high_watermark=event_time)
                        deliver_record = True
                        if sequence_gap and not decoded.is_full_book_snapshot:
                            checkpoint = self._append_gap(
                                checkpoint,
                                at=received_at,
                                reason_code="noncontiguous_non_snapshot",
                                expected_sequence=expected_sequence,
                                observed_sequence=item.venue_sequence,
                            )
                            if decoded.is_book_derived:
                                checkpoint = self._replace_checkpoint(
                                    checkpoint,
                                    withheld_book_record_count=checkpoint.withheld_book_record_count + 1,
                                )
                            deliver_record = False
                        elif decoded.is_full_book_snapshot:
                            checkpoint = self._replace_checkpoint(
                                checkpoint,
                                book_snapshot_state="fresh",
                                snapshot_sequence=item.venue_sequence,
                                gap_intervals=self._close_open_gaps(checkpoint.gap_intervals, ended_at=received_at),
                            )
                        if deliver_record and decoded.is_book_derived and checkpoint.book_snapshot_state != "fresh":
                            checkpoint = self._replace_checkpoint(
                                checkpoint,
                                withheld_book_record_count=checkpoint.withheld_book_record_count + 1,
                            )
                        elif deliver_record:
                            decoded_records.append(decoded.record)
                            checkpoint = self._replace_checkpoint(
                                checkpoint,
                                admitted_record_count=checkpoint.admitted_record_count + 1,
                            )

                    checkpoint = self._record_receipt(
                        checkpoint,
                        received_at=received_at,
                        capture=capture,
                        venue_sequence=item.venue_sequence,
                        advance_sequence=True,
                    )
                    self._save_checkpoint(checkpoint, checkpoint_path)
                    if processed_this_run >= max_messages:
                        terminated_by = "message_bound"
                        break
                if terminated_by in {"message_bound", "duration_bound", "input_exhausted"}:
                    break
            except (ConnectionError, OSError):
                reconnect_at = utc_datetime(self.clock(), "reconnect_at")
                internal = VenueStreamItem(
                    kind="reconnect",
                    payload={"event": "connection_interrupted", "attempt": attempt},
                    received_at=reconnect_at,
                )
                capture = self._capture_item(target_uid=target_uid, venue=venue, item=internal, received_at=reconnect_at)
                captures.append(capture)
                processed_this_run += 1
                try:
                    await self._decode(internal, capture)
                except Exception:
                    # The reconnect receipt remains durable even when an
                    # external decoder cannot consume a transport marker.
                    pass
                checkpoint = self._append_gap(checkpoint, at=reconnect_at, reason_code="connection_interrupted")
                checkpoint = self._record_receipt(
                    checkpoint,
                    received_at=reconnect_at,
                    capture=capture,
                    venue_sequence=None,
                    advance_sequence=False,
                )
                checkpoint = self._replace_checkpoint(
                    checkpoint,
                    connection_state="reconnecting",
                )
                self._save_checkpoint(checkpoint, checkpoint_path)
                if reconnects >= max_reconnects or processed_this_run >= max_messages:
                    terminated_by = "reconnect_bound"
                    break
                reconnects += 1
                attempt += 1
                continue
            finally:
                if session is not None:
                    await self._close_quietly(session)

        state: ConnectionState = "complete" if checkpoint.coverage_status == "complete" else "partial"
        checkpoint = self._replace_checkpoint(checkpoint, connection_state=state)
        self._save_checkpoint(checkpoint, checkpoint_path)
        return VenueStreamRunResult(
            checkpoint=checkpoint,
            checkpoint_path=checkpoint_path,
            admitted_records=tuple(decoded_records),
            raw_captures=tuple(captures),
            terminated_by=terminated_by,
        )


__all__ = [
    "BookSnapshotState",
    "ConnectionState",
    "DecodedVenueRecord",
    "RUNNER_SCHEMA_VERSION",
    "StreamGapInterval",
    "VenueStreamCheckpoint",
    "VenueStreamItem",
    "VenueStreamRunResult",
    "VenueStreamRunner",
    "VenueStreamRuntimeError",
    "VenueStreamSession",
]
