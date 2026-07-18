from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

from marketleak.cli_v3 import main as cli_main
from marketleak.ingestion.connectors.polymarket_ws import (
    CollectionResult,
    MarketChannelSubscription,
    PolymarketMarketWsCollector,
)
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.streaming import StreamEventCoordinator


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _collector(tmp_path, *, now: datetime = T0) -> PolymarketMarketWsCollector:
    async def unused_socket_factory():
        raise AssertionError("streaming coordinator tests must not open a socket")

    return PolymarketMarketWsCollector(
        socket_factory=unused_socket_factory,
        raw_store=RawArtifactStore(tmp_path / "raw"),
        subscription=MarketChannelSubscription(asset_ids=("100",)),
        clock=lambda: now,
    )


def _trade_message(tmp_path, *, timestamp: datetime = T0, received_at: datetime = T0):
    payload = {
        "event_type": "last_trade_price",
        "asset_id": "100",
        "market": "0xmarket",
        "price": "0.456",
        "side": "BUY",
        "size": "2",
        "timestamp": str(int(timestamp.timestamp() * 1000)),
    }
    return _collector(tmp_path, now=received_at).process_frame(json.dumps(payload))


def test_admits_only_canonical_records_with_verified_raw_lineage(tmp_path):
    message = _trade_message(tmp_path)
    fill = message.fills[0]
    unlineaged_fill = fill.model_copy(update={"raw_artifact_uid": "polymarket:raw/not-in-receipt"})
    unlineaged_message = message.__class__(
        raw_artifact=message.raw_artifact,
        event_type=message.event_type,
        fills=(unlineaged_fill,),
    )
    coordinator = StreamEventCoordinator(EventMemoryStore())

    admitted = coordinator.admit_polymarket(
        [message],
        as_of=T0 + timedelta(seconds=1),
        requested_max_messages=3,
        requested_duration_seconds=5,
    )
    rejected = coordinator.admit_polymarket(
        [unlineaged_message],
        as_of=T0 + timedelta(seconds=1),
        requested_max_messages=3,
        requested_duration_seconds=5,
    )

    assert admitted.inserted_count >= 1
    assert admitted.event_snapshot.records[0].provenance.raw_artifact_uid == message.raw_artifact.raw_artifact_uid
    assert len(admitted.event_snapshot.records[0].provenance.content_hash) == 64
    assert rejected.inserted_count == 0
    assert {item.reason_code for item in rejected.unavailable_frames} == {"missing_verified_raw_lineage"}


def test_excludes_future_and_unknown_frames_without_inventing_market_state(tmp_path):
    future = _trade_message(
        tmp_path / "future",
        timestamp=T0 + timedelta(minutes=10),
        received_at=T0 + timedelta(minutes=10),
    )
    unknown = _collector(tmp_path / "unknown").process_frame(
        json.dumps({"event_type": "unrecognized_event", "timestamp": str(int(T0.timestamp() * 1000))})
    )
    coordinator = StreamEventCoordinator(EventMemoryStore())

    snapshot = coordinator.admit_polymarket(
        CollectionResult(messages=(future, unknown), connection_attempts=1, reconnect_delays=()),
        as_of=T0 + timedelta(minutes=1),
        requested_max_messages=3,
        requested_duration_seconds=5,
    )

    assert snapshot.event_snapshot.records == ()
    reasons = {item.reason_code for item in snapshot.unavailable_frames}
    assert "future_or_late_available_event" in reasons
    assert "unknown_or_malformed_market_frame" in reasons


def test_repeated_raw_lineaged_messages_are_idempotent(tmp_path):
    message = _trade_message(tmp_path)
    store = EventMemoryStore()
    coordinator = StreamEventCoordinator(store)

    first = coordinator.admit_polymarket(
        [message],
        as_of=T0 + timedelta(seconds=1),
        requested_max_messages=1,
        requested_duration_seconds=5,
    )
    second = coordinator.admit_polymarket(
        [message],
        as_of=T0 + timedelta(seconds=1),
        requested_max_messages=1,
        requested_duration_seconds=5,
    )

    assert first.inserted_count >= 1
    assert second.inserted_count == 0
    assert second.idempotent_count == first.inserted_count
    assert store.event_count == first.inserted_count


def test_bounded_collector_stops_at_duration_without_network(tmp_path):
    class SlowCollector:
        def __init__(self) -> None:
            self.requested_max_messages: int | None = None

        async def collect(self, *, max_messages: int):
            self.requested_max_messages = max_messages
            await asyncio.sleep(60)
            raise AssertionError("wait_for should cancel the fake before it returns")

    collector = SlowCollector()
    snapshot = asyncio.run(
        StreamEventCoordinator(EventMemoryStore()).collect_polymarket_bounded(
            collector,
            as_of=T0,
            max_messages=2,
            duration_seconds=0.001,
        )
    )

    assert collector.requested_max_messages == 2
    assert snapshot.event_snapshot.records == ()
    assert "duration_bound_elapsed" in snapshot.coverage.reason_codes


def test_stream_cli_fails_closed_for_kalshi_without_server_auth(capsys):
    assert cli_main(
        [
            "stream-market",
            "--platform",
            "kalshi",
            "--kalshi-ticker",
            "KXHIGHNY-26JUL13-T90",
            "--max-messages",
            "2",
            "--duration-seconds",
            "5",
        ]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "unavailable_authentication_required"
    assert payload["transport"] == "not_started_without_injected_server_transport"


def test_stream_cli_redacts_server_configuration_values(capsys):
    sensitive_value = "server-only-value-must-not-appear"
    assert cli_main(
        [
            "stream-market",
            "--platform",
            "kalshi",
            "--kalshi-ticker",
            "KXHIGHNY-26JUL13-T90",
            "--kalshi-server-auth-config",
            sensitive_value,
            "--max-messages",
            "2",
            "--duration-seconds",
            "5",
        ]
    ) == 0

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["status"] == "unavailable_transport_not_configured"
    assert sensitive_value not in output
    assert "kalshi_server_auth_config" not in output
