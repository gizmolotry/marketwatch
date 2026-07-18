from __future__ import annotations

import json
from pathlib import Path

import pytest

from marketleak.cli_v3 import build_parser
from marketleak.ingestion.config.phase15_registry import load_phase15_source_registry
from marketleak.ingestion.connectors.polymarket_ws import MARKET_CHANNEL_ENDPOINT
from marketleak.ingestion.connectors.polymarket_ws_live import (
    LIVE_ASSET_IDS,
    LIVE_DURATION_SECONDS,
    LIVE_MARKET_UID,
    LIVE_MAX_MESSAGES,
    LIVE_MAX_RECONNECTS,
    LIVE_REFERENCE_MAPPING_UID,
    LIVE_TARGET_UID,
    open_public_market_socket,
    run_approved_polymarket_market_channel,
    validate_live_target,
)


REGISTRY_PATH = Path(__file__).resolve().parents[2] / "configs" / "phase15" / "polymarket_btc65k_live.json"

BOOK_FRAME = json.dumps(
    {
        "event_type": "book",
        "asset_id": LIVE_ASSET_IDS[0],
        "market": LIVE_MARKET_UID.removeprefix("polymarket:"),
        "bids": [{"price": "0.48", "size": "30"}],
        "asks": [{"price": "0.52", "size": "25"}],
        "timestamp": "1783964782000",
        "hash": "0xbounded-book",
    }
)


class RepeatingConnection:
    def __init__(self, frame: str, *, remaining: int = LIVE_MAX_MESSAGES) -> None:
        self.frame = frame
        self.remaining = remaining
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str:
        if self.remaining <= 0:
            raise AssertionError("collector read beyond its fixed message bound")
        self.remaining -= 1
        return self.frame

    async def close(self) -> None:
        self.closed = True


class DisconnectingConnection(RepeatingConnection):
    def __init__(self) -> None:
        super().__init__(BOOK_FRAME, remaining=0)

    async def recv(self) -> str:
        raise ConnectionError("injected disconnect")


def test_static_registry_is_only_the_approved_bounded_target():
    registry = load_phase15_source_registry(REGISTRY_PATH)
    target = registry.target(LIVE_TARGET_UID)

    validate_live_target(target)
    assert len(registry.documented_btc_reference_mappings) == 1
    mapping = registry.reference_mapping(LIVE_REFERENCE_MAPPING_UID)
    assert mapping.mapping_uid == LIVE_REFERENCE_MAPPING_UID
    assert mapping.instrument == "BTC-USDT"
    assert mapping.observation_kind == "candle_high"
    assert mapping.candle_interval == "1m"
    assert mapping.requires_closed_candle is True
    assert target.target_uid == LIVE_TARGET_UID
    assert target.market_uid == LIVE_MARKET_UID
    assert target.polymarket_asset_ids == LIVE_ASSET_IDS
    assert target.reference_mapping_uid == LIVE_REFERENCE_MAPPING_UID
    assert target.stream_settings.max_messages == LIVE_MAX_MESSAGES
    assert target.stream_settings.duration_seconds == LIVE_DURATION_SECONDS
    assert target.stream_settings.max_reconnects == LIVE_MAX_RECONNECTS


def test_transport_adapter_uses_only_documented_public_endpoint_and_bounded_options():
    captured: dict[str, object] = {}
    connection = RepeatingConnection(BOOK_FRAME, remaining=1)

    async def injected_connect(uri: str, **kwargs):
        captured["uri"] = uri
        captured["kwargs"] = kwargs
        return connection

    async def scenario() -> None:
        socket = await open_public_market_socket(connect=injected_connect)
        await socket.send("test")
        await socket.close()

    import asyncio

    asyncio.run(scenario())

    assert captured == {
        "uri": MARKET_CHANNEL_ENDPOINT,
        "kwargs": {
            "open_timeout": 10,
            "close_timeout": 10,
            "ping_interval": 20,
            "ping_timeout": 20,
            "max_size": 2 * 1024 * 1024,
            "max_queue": (64, 16),
        },
    }
    assert connection.sent == ["test"]
    assert connection.closed is True


def test_collection_subscribes_persists_raw_first_and_returns_only_redacted_summary(tmp_path):
    connections: list[RepeatingConnection] = []
    connect_calls: list[tuple[str, dict[str, object]]] = []

    async def injected_connect(uri: str, **kwargs):
        connect_calls.append((uri, kwargs))
        connection = RepeatingConnection(BOOK_FRAME)
        connections.append(connection)
        return connection

    result = run_approved_polymarket_market_channel(
        registry_path=REGISTRY_PATH,
        target_uid=LIVE_TARGET_UID,
        output_dir=tmp_path,
        connect=injected_connect,
    )

    assert len(connect_calls) == 1
    assert connect_calls[0][0] == MARKET_CHANNEL_ENDPOINT
    assert len(connections) == 1
    assert connections[0].remaining == 0
    assert connections[0].closed is True
    assert json.loads(connections[0].sent[0]) == {
        "assets_ids": list(LIVE_ASSET_IDS),
        "custom_feature_enabled": True,
        "type": "market",
    }
    assert result == {
        "command": "collect-polymarket-ws",
        "status": "collected",
        "counts": {
            "messages": LIVE_MAX_MESSAGES,
            "records": LIVE_MAX_MESSAGES,
            "connection_attempts": 1,
            "reconnects": 0,
            "normalization": {
                "inserted": 1,
                "duplicates": LIVE_MAX_MESSAGES - 1,
                "conflicts": 0,
                "rejected": 0,
            },
        },
        "paths": {
            "raw": str(tmp_path / "raw"),
            "normalized": str(tmp_path),
        },
    }
    assert len(list((tmp_path / "raw" / "receipts").rglob("*.json"))) == LIVE_MAX_MESSAGES
    summary = json.dumps(result, sort_keys=True)
    assert LIVE_TARGET_UID not in summary
    assert LIVE_MARKET_UID not in summary
    assert all(asset_id not in summary for asset_id in LIVE_ASSET_IDS)
    assert MARKET_CHANNEL_ENDPOINT not in summary
    assert "event_type" not in summary
    assert "0.48" not in summary


def test_reconnects_are_limited_to_two_after_the_initial_attempt(tmp_path):
    attempts = 0

    async def injected_connect(uri: str, **kwargs):
        nonlocal attempts
        assert uri == MARKET_CHANNEL_ENDPOINT
        attempts += 1
        return DisconnectingConnection()

    result = run_approved_polymarket_market_channel(
        registry_path=REGISTRY_PATH,
        target_uid=LIVE_TARGET_UID,
        output_dir=tmp_path,
        connect=injected_connect,
    )

    assert attempts == LIVE_MAX_RECONNECTS + 1
    assert result["counts"]["connection_attempts"] == LIVE_MAX_RECONNECTS + 1
    assert result["counts"]["reconnects"] == LIVE_MAX_RECONNECTS
    assert result["counts"]["messages"] == 0


def test_cli_new_command_accepts_only_registry_target_and_output_paths():
    parser = build_parser()
    args = parser.parse_args(
        [
            "collect-polymarket-ws",
            "--registry",
            str(REGISTRY_PATH),
            "--target-uid",
            LIVE_TARGET_UID,
            "--output-dir",
            "local-output",
        ]
    )

    assert vars(args) == {
        "command": "collect-polymarket-ws",
        "registry": str(REGISTRY_PATH),
        "target_uid": LIVE_TARGET_UID,
        "output_dir": "local-output",
    }
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "collect-polymarket-ws",
                "--registry",
                str(REGISTRY_PATH),
                "--target-uid",
                LIVE_TARGET_UID,
                "--output-dir",
                "local-output",
                "--polymarket-token",
                LIVE_ASSET_IDS[0],
            ]
        )
