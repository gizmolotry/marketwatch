"""Bounded live adapter for one approved public Polymarket market channel.

The module owns only transport adaptation and command-level collection wiring.
The existing collector remains responsible for raw-first frame capture and
documented market-channel parsing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from marketleak.ingestion.config.phase15_registry import (
    ApprovedMarketTarget,
    Phase15SourceRegistry,
    load_phase15_source_registry,
)
from marketleak.ingestion.connectors.polymarket_ws import (
    MARKET_CHANNEL_ENDPOINT,
    MarketChannelParser,
    MarketChannelSubscription,
    MarketSocket,
    PolymarketMarketWsCollector,
    ReconnectPolicy,
)
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.ingestion.storage import NormalizedStore, WriteResult


LIVE_TARGET_UID = "market:polymarket-btc65k-july"
LIVE_MARKET_UID = "polymarket:0xc9c9790c8f26dd9c8cabae9dd76be37aa86a6ded7de660e1da9d19324cf618d4"
LIVE_ASSET_IDS = (
    "37863990088639017224129863896084706036599112986230542056774425461928248691792",
    "10222646434785930729270432914646282814690567121763865370487081241993787794954",
)
LIVE_MAX_MESSAGES = 500
LIVE_DURATION_SECONDS = 120.0
LIVE_MAX_RECONNECTS = 2
LIVE_REFERENCE_MAPPING_UID = "reference:polymarket-btc65k-binance-btcusdt-high"


class WebSocketClientConnection(Protocol):
    """The small subset of a websockets client used by ``MarketSocket``."""

    async def send(self, payload: str) -> Any: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> Any: ...


WebSocketConnect = Callable[..., Awaitable[WebSocketClientConnection]]


class LiveMarketSocket:
    """Adapt a concrete ``websockets`` connection to the existing protocol."""

    def __init__(self, connection: WebSocketClientConnection) -> None:
        self._connection = connection

    async def send(self, payload: str) -> Any:
        return await self._connection.send(payload)

    async def recv(self) -> str | bytes:
        frame = await self._connection.recv()
        if not isinstance(frame, (str, bytes)):
            raise TypeError("public market channel frame must be text or bytes")
        return frame

    async def close(self) -> Any:
        return await self._connection.close()


async def open_public_market_socket(*, connect: WebSocketConnect | None = None) -> MarketSocket:
    """Open only the documented public market endpoint with bounded buffers.

    Tests inject ``connect`` and never construct a network transport.  The
    production default is imported lazily so merely importing this module has
    no network effect.
    """

    if connect is None:
        from websockets.asyncio.client import connect as websocket_connect

        connect = websocket_connect
    connection = await connect(
        MARKET_CHANNEL_ENDPOINT,
        open_timeout=10,
        close_timeout=10,
        ping_interval=20,
        ping_timeout=20,
        max_size=2 * 1024 * 1024,
        max_queue=(64, 16),
    )
    return LiveMarketSocket(connection)


def validate_live_target(target: ApprovedMarketTarget) -> None:
    """Fail closed unless this command receives the one approved static target."""

    if target.target_uid != LIVE_TARGET_UID:
        raise ValueError("target_uid is not approved for this live collector")
    if target.venue != "polymarket" or target.market_uid != LIVE_MARKET_UID:
        raise ValueError("target does not match the approved public market")
    if target.polymarket_asset_ids != LIVE_ASSET_IDS or target.kalshi_tickers is not None:
        raise ValueError("target asset mapping does not match the approved public market")
    if target.reference_mapping_uid != LIVE_REFERENCE_MAPPING_UID:
        raise ValueError("live market target must name the one approved BTCUSDT candle mapping")
    settings = target.stream_settings
    if (
        settings.max_messages != LIVE_MAX_MESSAGES
        or settings.duration_seconds != LIVE_DURATION_SECONDS
        or settings.max_reconnects != LIVE_MAX_RECONNECTS
    ):
        raise ValueError("target bounded stream settings do not match the approved limits")


@dataclass(slots=True)
class _PersistingParser:
    """Persist parsed records after the underlying collector stores raw bytes."""

    parser: MarketChannelParser
    normalized: NormalizedStore
    message_count: int = 0
    record_count: int = 0
    writes: WriteResult | None = None

    def parse_raw(self, raw: bytes | str, capture):
        message = self.parser.parse_raw(raw, capture)
        result = self.normalized.write(message.records)
        self.message_count += 1
        self.record_count += len(message.records)
        if self.writes is None:
            self.writes = result
        else:
            self.writes.inserted += result.inserted
            self.writes.duplicates += result.duplicates
            self.writes.conflicts += result.conflicts
            self.writes.rejected += result.rejected
        return message

    def write_counts(self) -> dict[str, int]:
        result = self.writes or WriteResult()
        return {
            "inserted": result.inserted,
            "duplicates": result.duplicates,
            "conflicts": result.conflicts,
            "rejected": result.rejected,
        }


async def collect_approved_polymarket_market_channel(
    *,
    registry: Phase15SourceRegistry,
    target_uid: str,
    output_dir: str | Path,
    connect: WebSocketConnect | None = None,
) -> dict[str, Any]:
    """Collect one fixed bounded public market-channel sample.

    The returned payload deliberately omits target identifiers, asset IDs,
    endpoints, raw contents, and parsed values.  Each frame passes through the
    existing collector, which captures it before this parser persists records.
    """

    target = registry.target(target_uid)
    validate_live_target(target)
    if len(registry.documented_btc_reference_mappings) != 1:
        raise ValueError("live market registry must contain exactly the one approved reference mapping")
    mapping = registry.reference_mapping(LIVE_REFERENCE_MAPPING_UID)
    if mapping.target_uid != target.target_uid or mapping.instrument != "BTC-USDT" or mapping.observation_kind != "candle_high":
        raise ValueError("live market registry reference mapping does not match the approved BTCUSDT candle contract")

    root = Path(output_dir)
    raw_store = RawArtifactStore(root / "raw")
    normalized = NormalizedStore(root)
    persisting_parser = _PersistingParser(parser=MarketChannelParser(), normalized=normalized)

    async def socket_factory() -> MarketSocket:
        return await open_public_market_socket(connect=connect)

    collector = PolymarketMarketWsCollector(
        socket_factory=socket_factory,
        raw_store=raw_store,
        subscription=MarketChannelSubscription(asset_ids=target.polymarket_asset_ids),
        parser=persisting_parser,  # type: ignore[arg-type]
        reconnect_policy=ReconnectPolicy(max_attempts=LIVE_MAX_RECONNECTS + 1),
    )
    timed_out = False
    attempts = 0
    reconnects = 0
    try:
        collection = await asyncio.wait_for(
            collector.collect(max_messages=LIVE_MAX_MESSAGES),
            timeout=LIVE_DURATION_SECONDS,
        )
        attempts = collection.connection_attempts
        reconnects = len(collection.reconnect_delays)
    except asyncio.TimeoutError:
        timed_out = True

    return {
        "command": "collect-polymarket-ws",
        "status": "duration_bound" if timed_out else "collected",
        "counts": {
            "messages": persisting_parser.message_count,
            "records": persisting_parser.record_count,
            "connection_attempts": attempts,
            "reconnects": reconnects,
            "normalization": persisting_parser.write_counts(),
        },
        "paths": {
            "raw": str(raw_store.root),
            "normalized": str(normalized.root),
        },
    }


def run_approved_polymarket_market_channel(
    *,
    registry_path: str | Path,
    target_uid: str,
    output_dir: str | Path,
    connect: WebSocketConnect | None = None,
) -> dict[str, Any]:
    """Load one static registry and execute the approved bounded collection."""

    registry = load_phase15_source_registry(registry_path)
    return asyncio.run(
        collect_approved_polymarket_market_channel(
            registry=registry,
            target_uid=target_uid,
            output_dir=output_dir,
            connect=connect,
        )
    )


__all__ = [
    "LIVE_ASSET_IDS",
    "LIVE_DURATION_SECONDS",
    "LIVE_MARKET_UID",
    "LIVE_MAX_MESSAGES",
    "LIVE_MAX_RECONNECTS",
    "LIVE_REFERENCE_MAPPING_UID",
    "LIVE_TARGET_UID",
    "LiveMarketSocket",
    "WebSocketClientConnection",
    "WebSocketConnect",
    "collect_approved_polymarket_market_channel",
    "open_public_market_socket",
    "run_approved_polymarket_market_channel",
    "validate_live_target",
]
