from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

from marketleak.ingestion.config.phase15_registry import ApprovedMarketTarget, BoundedStreamSettings
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.ingestion.runtime.venue_stream_runner import (
    DecodedVenueRecord,
    VenueStreamItem,
    VenueStreamRunner,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def target(*, max_messages: int = 10, max_reconnects: int = 1) -> ApprovedMarketTarget:
    return ApprovedMarketTarget(
        target_uid="target:polymarket-bounded-001",
        venue="polymarket",
        market_uid="polymarket:market/condition-001",
        polymarket_asset_ids=("12345678", "87654321"),
        metadata_ref="metadata:market-001",
        stream_config_ref="stream:market-001",
        stream_settings=BoundedStreamSettings(
            max_messages=max_messages,
            duration_seconds=30,
            max_reconnects=max_reconnects,
        ),
    )


class FakeSession:
    def __init__(self, deliveries) -> None:
        self.deliveries = list(deliveries)
        self.closed = False
        self.recv_count = 0

    async def recv(self) -> VenueStreamItem:
        self.recv_count += 1
        if not self.deliveries:
            raise StopAsyncIteration
        next_item = self.deliveries.pop(0)
        if isinstance(next_item, BaseException):
            raise next_item
        return next_item

    async def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self, sessions: list[FakeSession]) -> None:
        self.sessions = list(sessions)
        self.calls: list[int] = []

    async def __call__(self, _target, attempt: int) -> FakeSession:
        self.calls.append(attempt)
        if not self.sessions:
            raise AssertionError("runner attempted an unconfigured session")
        return self.sessions.pop(0)


def item(kind: str, sequence: int, type_name: str, *, at: datetime | None = None) -> VenueStreamItem:
    return VenueStreamItem(
        kind=kind,  # type: ignore[arg-type]
        payload={"type": type_name, "sequence": sequence},
        venue_sequence=sequence,
        received_at=at or T0 + timedelta(seconds=sequence),
    )


def decoding(item: VenueStreamItem, _capture) -> DecodedVenueRecord:
    payload = item.payload
    assert isinstance(payload, dict)
    type_name = payload["type"]
    return DecodedVenueRecord(
        record={"type": type_name, "sequence": item.venue_sequence},
        venue_event_time=T0 + timedelta(seconds=int(item.venue_sequence or 0)),
        is_book_derived=type_name in {"book", "delta"},
        is_full_book_snapshot=type_name == "book",
    )


def test_persists_raw_receipt_before_decoder_callback(tmp_path):
    order: list[str] = []

    class RecordingStore(RawArtifactStore):
        def capture(self, *args, **kwargs):
            order.append("raw")
            return super().capture(*args, **kwargs)

    session = FakeSession([item("raw", 1, "trade")])

    def decoder(value, capture):
        assert order == ["raw"]
        assert capture.object_path.exists() and capture.receipt_path.exists()
        order.append("decode")
        return decoding(value, capture)

    runner = VenueStreamRunner(
        raw_store=RecordingStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([session]),
        decoder=decoder,
        clock=lambda: T0,
    )

    result = asyncio.run(runner.run(target(max_messages=1)))

    assert order == ["raw", "decode"]
    assert len(result.raw_captures) == 1
    assert result.checkpoint.latest_raw_uid == f"polymarket:raw/{result.raw_captures[0].sha256}"
    assert result.checkpoint_path.is_file()


def test_replays_checkpoint_and_advances_receipt_and_venue_watermarks(tmp_path):
    first = FakeSession([item("raw", 1, "book"), item("raw", 2, "trade")])
    runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([first]),
        decoder=decoding,
        clock=lambda: T0,
    )
    first_result = asyncio.run(runner.run(target(max_messages=2)))
    stored = json.loads(first_result.checkpoint_path.read_text(encoding="utf-8"))

    assert stored["connection_state"] == "complete"
    assert stored["last_venue_sequence"] == 2
    assert stored["snapshot_sequence"] == 1
    assert stored["local_receipt_high_watermark"].endswith("Z")
    assert stored["venue_event_high_watermark"].endswith("Z")

    second = FakeSession([item("raw", 3, "trade")])
    resumed = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([second]),
        decoder=decoding,
        clock=lambda: T0 + timedelta(minutes=1),
    )
    second_result = asyncio.run(resumed.run(target(max_messages=2)))

    assert second_result.checkpoint.processed_item_count == 3
    assert second_result.checkpoint.last_venue_sequence == 3
    assert second_result.checkpoint.venue_event_high_watermark == T0 + timedelta(seconds=3)
    assert second_result.checkpoint.latest_raw_uid == f"polymarket:raw/{second_result.raw_captures[-1].sha256}"
    assert any(gap.reason_code == "resumed_connection" for gap in second_result.checkpoint.gap_intervals)


def test_runner_never_reads_beyond_approved_message_bound(tmp_path):
    session = FakeSession([item("raw", index, "trade") for index in range(1, 6)])
    runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([session]),
        decoder=decoding,
        clock=lambda: T0,
    )

    result = asyncio.run(runner.run(target(max_messages=2)))

    assert result.terminated_by == "message_bound"
    assert session.recv_count == 2
    assert len(result.raw_captures) == 2
    assert result.checkpoint.processed_item_count == 2


def test_gap_and_reconnect_withhold_book_state_until_fresh_snapshot(tmp_path):
    initial = FakeSession([item("raw", 1, "book"), ConnectionError("test transport interruption")])
    resumed = FakeSession([item("raw", 3, "delta"), item("raw", 4, "book"), item("raw", 5, "delta")])
    factory = FakeFactory([initial, resumed])
    runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=factory,
        decoder=decoding,
        clock=lambda: T0,
    )

    result = asyncio.run(runner.run(target(max_messages=5, max_reconnects=1)))

    assert factory.calls == [0, 1]
    assert [record["sequence"] for record in result.admitted_records] == [1, 4, 5]
    assert result.checkpoint.withheld_book_record_count == 1
    assert result.checkpoint.book_snapshot_state == "fresh"
    assert result.checkpoint.coverage_status == "partial"
    assert {gap.reason_code for gap in result.checkpoint.gap_intervals} >= {"connection_interrupted", "venue_sequence_gap"}
    assert all(capture.receipt_path.exists() for capture in result.raw_captures)


def test_replayed_and_out_of_order_deliveries_are_raw_captured_but_never_reemitted(tmp_path):
    first = FakeSession([item("raw", 1, "book"), item("raw", 2, "trade")])
    initial_runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([first]),
        decoder=decoding,
        clock=lambda: T0,
    )
    initial = asyncio.run(initial_runner.run(target(max_messages=2)))

    replayed = FakeSession(
        [
            item("raw", 2, "trade", at=T0 + timedelta(seconds=1)),
            item("raw", 1, "trade", at=T0),
        ]
    )
    resumed_runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([replayed]),
        decoder=decoding,
        clock=lambda: T0,
    )

    result = asyncio.run(resumed_runner.run(target(max_messages=2)))

    assert result.admitted_records == ()
    assert len(result.raw_captures) == 2
    assert result.checkpoint.last_venue_sequence == initial.checkpoint.last_venue_sequence == 2
    assert result.checkpoint.local_receipt_high_watermark == initial.checkpoint.local_receipt_high_watermark == T0 + timedelta(seconds=2)
    assert result.checkpoint.venue_event_high_watermark == initial.checkpoint.venue_event_high_watermark == T0 + timedelta(seconds=2)
    assert {gap.reason_code for gap in result.checkpoint.gap_intervals} >= {"replayed_or_out_of_order_sequence"}


def test_invalid_sequence_is_checkpointed_as_a_raw_captured_rejection(tmp_path):
    decoded_items: list[VenueStreamItem] = []
    invalid = VenueStreamItem(
        kind="raw",
        payload={"type": "trade", "sequence": -1},
        venue_sequence=-1,
        received_at=T0,
    )
    runner = VenueStreamRunner(
        raw_store=RawArtifactStore(tmp_path / "raw"),
        checkpoint_root=tmp_path / "checkpoints",
        session_factory=FakeFactory([FakeSession([invalid])]),
        decoder=lambda value, capture: decoded_items.append(value) or decoding(value, capture),
        clock=lambda: T0,
    )

    result = asyncio.run(runner.run(target(max_messages=1)))
    stored = json.loads(result.checkpoint_path.read_text(encoding="utf-8"))

    assert decoded_items == []
    assert result.terminated_by == "message_bound"
    assert len(result.raw_captures) == 1
    assert result.checkpoint.processed_item_count == 1
    assert result.checkpoint.last_venue_sequence is None
    assert result.checkpoint.latest_raw_uid == f"polymarket:raw/{result.raw_captures[0].sha256}"
    assert stored["processed_item_count"] == 1
    assert {gap.reason_code for gap in result.checkpoint.gap_intervals} >= {"invalid_venue_sequence"}
