from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from marketleak.ingestion.config.phase15_registry import load_phase15_source_registry
from marketleak.ingestion.connectors.binance_btcusdt_kline import (
    BINANCE_MAPPING_UID,
    BINANCE_SOURCE_UID,
    BINANCE_STREAM_URL,
    MAX_CLOCK_SKEW,
    MANIFEST_FILENAME,
    collect_approved_binance_btcusdt_reference,
)
from marketleak.ingestion.raw_store import RawArtifactStore


T0 = datetime(2026, 7, 13, 12, 2, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "configs" / "phase15" / "polymarket_btc65k_live.json"
MARKET = "polymarket:market/0xc9c9790c8f26dd9c8cabae9dd76be37aa86a6ded7de660e1da9d19324cf618d4"


class FakeSocket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)
        self.closed = False

    async def recv(self) -> str:
        if not self.frames:
            raise asyncio.TimeoutError()
        return self.frames.pop(0)

    async def close(self) -> None:
        self.closed = True


def _context(root: Path) -> None:
    store = RawArtifactStore(root / "raw")
    gamma = store.capture(
        b'{"gamma":"captured"}',
        platform="polymarket",
        source="polymarket:source/gamma-market",
        request={"method": "GET"},
        received_at=T0 - timedelta(minutes=2),
    )
    (root / "case-context.json").write_text(
        json.dumps(
            {
                "status": "collected",
                "market": {"market_uid": MARKET},
                "raw_lineage": [
                    {
                        "source_uid": "polymarket:source/gamma-market",
                        "sha256": gamma.sha256,
                        "raw_artifact_uid": f"polymarket:raw/{gamma.sha256}",
                        "receipt_path": str(gamma.receipt_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _final_frame(*, candle_end_offset_ms: int = -1000, event_after_end_ms: int = 1) -> str:
    end = int(T0.timestamp() * 1000) + candle_end_offset_ms
    start = end - 60_000
    return json.dumps({"e": "kline", "s": "BTCUSDT", "E": end + event_after_end_ms, "k": {"s": "BTCUSDT", "i": "1m", "x": True, "h": "65000.25", "t": start, "T": end}})


def _collect(tmp_path: Path, frames: list[str], *, context: bool = True):
    if context:
        _context(tmp_path)
    socket = FakeSocket(frames)
    connected = False

    async def connect(url: str, **kwargs):
        nonlocal connected
        connected = True
        assert url == BINANCE_STREAM_URL
        return socket

    result = asyncio.run(
        collect_approved_binance_btcusdt_reference(
            registry=load_phase15_source_registry(REGISTRY),
            target_uid="market:polymarket-btc65k-july",
            output_dir=tmp_path,
            connect=connect,
            now=lambda: T0,
        )
    )
    return result, socket, connected, json.loads((tmp_path / MANIFEST_FILENAME).read_text(encoding="utf-8"))


def test_registry_mapping_and_stream_are_exact_fixed_contracts() -> None:
    registry = load_phase15_source_registry(REGISTRY)
    target = registry.target("market:polymarket-btc65k-july")
    mapping = registry.reference_mapping(BINANCE_MAPPING_UID)

    assert target.reference_mapping_uid == BINANCE_MAPPING_UID
    assert mapping.primary_source_uid == BINANCE_SOURCE_UID
    assert mapping.instrument == "BTC-USDT"
    assert mapping.observation_kind == "candle_high"
    assert mapping.candle_interval == "1m"
    assert mapping.requires_closed_candle is True
    assert BINANCE_STREAM_URL == "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"


def test_final_btcusdt_candle_is_raw_first_admitted_and_manifested(tmp_path: Path) -> None:
    result, socket, _connected, manifest = _collect(tmp_path, [_final_frame()])

    assert result["status"] == "collected"
    assert manifest["status"] == "collected"
    assert manifest["admission"]["status"] == "admitted"
    assert manifest["observation"]["source_symbol"] == "BTCUSDT"
    assert manifest["observation"]["candle_interval"] == "1m"
    assert manifest["observation"]["is_final"] is True
    raw = manifest["raw_captures"][0]
    assert raw["source"] == BINANCE_SOURCE_UID
    assert Path(raw["object_path"]).is_file()
    assert manifest["clock_adjustment"]["policy"] == "bounded_exchange_clock_skew_v1"
    assert socket.closed is True


def test_final_candle_just_within_fixed_clock_skew_is_admitted_at_conservative_time(tmp_path: Path) -> None:
    result, _socket, _connected, manifest = _collect(tmp_path, [_final_frame(candle_end_offset_ms=700, event_after_end_ms=28)])

    assert result["status"] == "collected"
    adjustment = manifest["clock_adjustment"]
    assert adjustment["max_clock_skew_seconds"] == str(MAX_CLOCK_SKEW.total_seconds())
    assert adjustment["candle_end_ahead_seconds"] == "0.7"
    assert adjustment["exchange_event_ahead_seconds"] == "0.728"
    assert manifest["observation"]["ingested_at"] == adjustment["admissible_at"]
    assert manifest["raw_captures"][0]["received_at"] != adjustment["admissible_at"]


def test_final_candle_beyond_fixed_clock_skew_is_receipted_but_not_admitted(tmp_path: Path) -> None:
    result, _socket, _connected, manifest = _collect(
        tmp_path,
        [_final_frame(candle_end_offset_ms=int((MAX_CLOCK_SKEW + timedelta(milliseconds=1)).total_seconds() * 1000), event_after_end_ms=1)],
    )

    assert result["status"] == "unavailable_reference"
    assert manifest["observation"] is None
    assert manifest["clock_adjustment"] is None
    assert "clock-skew allowance" in manifest["reason"]
    assert len(manifest["raw_captures"]) == 1


def test_malformed_frame_is_receipted_before_parse_then_not_admitted(tmp_path: Path) -> None:
    result, socket, connected, manifest = _collect(tmp_path, ["{not-json"])

    assert connected is True
    assert result["status"] == "unavailable_reference"
    assert manifest["observation"] is None
    assert len(manifest["raw_captures"]) == 1
    assert all(Path(item["object_path"]).is_file() for item in manifest["raw_captures"])
    assert manifest["coverage"][0]["complete"] is False
    assert socket.closed is True


@pytest.mark.parametrize(
    "frame",
    [
        json.dumps({"e": "kline", "s": "BTCUSDT", "k": {"s": "BTCUSDT", "i": "1m", "x": False, "h": "1", "t": 1, "T": 2}}),
        json.dumps({"e": "kline", "s": "ETHUSDT", "k": {"s": "ETHUSDT", "i": "1m", "x": True, "h": "1", "t": 1, "T": 2}}),
        json.dumps({"e": "kline", "s": "BTCUSDT", "k": {"s": "BTCUSDT", "i": "5m", "x": True, "h": "1", "t": 1, "T": 2}}),
    ],
)
def test_partial_wrong_symbol_or_wrong_interval_is_preserved_without_admission(tmp_path: Path, frame: str) -> None:
    result, _socket, _connected, manifest = _collect(tmp_path, [frame])

    assert result["status"] == "unavailable_reference"
    assert manifest["admission"]["status"] != "admitted"
    assert manifest["observation"] is None
    assert len(manifest["raw_captures"]) == 1


def test_context_raw_mismatch_refuses_connection_and_never_creates_reference_raw(tmp_path: Path) -> None:
    _context(tmp_path)
    context = json.loads((tmp_path / "case-context.json").read_text(encoding="utf-8"))
    digest = context["raw_lineage"][0]["sha256"]
    object_path = tmp_path / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
    object_path.write_bytes(b"tampered")

    result, _socket, connected, manifest = _collect(tmp_path, [], context=False)

    assert connected is False
    assert result["status"] == "unavailable_reference"
    assert manifest["mapping"] is None
    assert manifest["observation"] is None
    assert manifest["raw_captures"] == []
