"""Fixed, bounded Binance Spot BTCUSDT final-candle reference collector.

There is deliberately no symbol, interval, price-field, or venue parameter.
The only admissible observation is the documented BTCUSDT 1-minute *final*
candle High for the one approved Polymarket target.  Every WebSocket frame is
persisted before it is decoded; malformed, partial, wrong-symbol, or absent
final candles remain explicit non-admissions rather than a fallback price.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Protocol

from marketleak.ingestion.config.phase15_registry import (
    ApprovedMarketTarget,
    DocumentedBtcReferenceMapping,
    Phase15SourceRegistry,
    load_phase15_source_registry,
)
from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture
from marketleak.multimodal.reference_price import (
    DocumentedReferenceSource,
    PrimarySourceAvailability,
    PrimarySourceStatus,
    ReferenceAdmissionStatus,
    ReferencePriceAdmission,
    ReferencePriceObservation,
    admit_reference_price,
)
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability


BINANCE_STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
BINANCE_PLATFORM = "binance"
BINANCE_SOURCE_UID = "source:binance-spot-btcusdt-kline"
BINANCE_MAPPING_UID = "reference:polymarket-btc65k-binance-btcusdt-high"
BINANCE_ENDPOINT_REF = "config:binance-spot-btcusdt-kline-1m-v1"
LIVE_TARGET_UID = "market:polymarket-btc65k-july"
LIVE_MARKET_UID = "polymarket:market/0xc9c9790c8f26dd9c8cabae9dd76be37aa86a6ded7de660e1da9d19324cf618d4"
MAX_FRAMES = 64
MAX_DURATION_SECONDS = 75.0
MAX_CLOCK_SKEW = timedelta(seconds=2)
MANIFEST_FILENAME = "binance-btcusdt-reference.json"
MANIFEST_SCHEMA_VERSION = "phase15-binance-btcusdt-reference-v1"
PARSER_VERSION = "phase15-binance-btcusdt-kline-v1.0.0"


class BinanceConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def close(self) -> Any: ...


WebSocketConnect = Callable[..., Awaitable[BinanceConnection]]


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp_millis(value: object, *, field: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer epoch in milliseconds")
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _utc_iso(value: datetime) -> str:
    return _utc(value, field="timestamp").isoformat().replace("+00:00", "Z")


def _raw_bytes(frame: object) -> bytes:
    if isinstance(frame, bytes):
        return frame
    if isinstance(frame, str):
        return frame.encode("utf-8")
    return repr(frame).encode("utf-8", errors="backslashreplace")


async def open_binance_btcusdt_kline_socket(*, connect: WebSocketConnect | None = None) -> BinanceConnection:
    if connect is None:
        from websockets.asyncio.client import connect as websocket_connect

        connect = websocket_connect
    return await connect(
        BINANCE_STREAM_URL,
        open_timeout=10,
        close_timeout=10,
        ping_interval=20,
        ping_timeout=20,
        max_size=2 * 1024 * 1024,
        max_queue=(64, 16),
    )


def validate_binance_target(target: ApprovedMarketTarget, mapping: DocumentedBtcReferenceMapping) -> None:
    """Require the one explicit market-to-candle mapping; no generic BTC feed."""

    if (
        target.target_uid != LIVE_TARGET_UID
        or target.venue != "polymarket"
        or target.canonical_market_uid != LIVE_MARKET_UID
        or target.reference_mapping_uid != BINANCE_MAPPING_UID
    ):
        raise ValueError("target is not approved for the fixed Binance BTCUSDT candle collector")
    expected = {
        "mapping_uid": BINANCE_MAPPING_UID,
        "target_uid": LIVE_TARGET_UID,
        "market_rule_source_uid": "polymarket:source/gamma-market",
        "market_rule_source_url": "https://gamma-api.polymarket.com/markets/2758340",
        "settlement_source_uid": "binance:source/btcusdt-1m-high",
        "settlement_source_url": "https://www.binance.com/en/trade/BTC_USDT",
        "primary_source_uid": BINANCE_SOURCE_UID,
        "primary_source_url": "https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams",
        "instrument": "BTC-USDT",
        "observation_kind": "candle_high",
        "candle_interval": "1m",
        "requires_closed_candle": True,
        "configured_endpoint_ref": BINANCE_ENDPOINT_REF,
    }
    for field, expected_value in expected.items():
        if getattr(mapping, field) != expected_value:
            raise ValueError(f"documented mapping {field} does not match the fixed Binance candle contract")


def _context_mapping(
    *,
    root: Path,
    target: ApprovedMarketTarget,
    registry_mapping: DocumentedBtcReferenceMapping,
) -> DocumentedReferenceSource:
    """Use the previously raw-captured Gamma rule; never manufacture a rule source."""

    context_path = root / "case-context.json"
    try:
        context = json.loads(context_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("a collected case-context manifest is required before reference collection") from exc
    if not isinstance(context, Mapping) or context.get("status") != "collected":
        raise ValueError("case-context manifest is not collected")
    market = context.get("market")
    if not isinstance(market, Mapping) or market.get("market_uid") != target.canonical_market_uid:
        raise ValueError("case-context market does not match the approved target")
    lineage = context.get("raw_lineage")
    if not isinstance(lineage, list):
        raise ValueError("case-context raw lineage is missing")
    gamma = next((item for item in lineage if isinstance(item, Mapping) and item.get("source_uid") == registry_mapping.market_rule_source_uid), None)
    if gamma is None:
        raise ValueError("case-context lacks the raw Gamma market-rule artifact")
    digest = str(gamma.get("sha256", ""))
    raw_uid = str(gamma.get("raw_artifact_uid", ""))
    if raw_uid != f"polymarket:raw/{digest}" or len(digest) != 64:
        raise ValueError("case-context Gamma raw lineage is malformed")
    store = RawArtifactStore(root / "raw")
    if not store.verify(digest):
        raise ValueError("case-context Gamma raw artifact fails hash verification")
    receipt_path = Path(str(gamma.get("receipt_path", "")))
    try:
        receipt = store.read_receipt(receipt_path)
        retrieved_at = _utc(datetime.fromisoformat(str(receipt["received_at"]).replace("Z", "+00:00")), field="Gamma receipt time")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("case-context Gamma receipt is unreadable") from exc
    if receipt.get("sha256") != digest or receipt.get("platform") != "polymarket" or receipt.get("source") != registry_mapping.market_rule_source_uid:
        raise ValueError("case-context Gamma receipt conflicts with its manifest")
    reliability = SourceReliability(
        source_uid=registry_mapping.market_rule_source_uid,
        source_class=SourceClass.OFFICIAL_VENUE,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.95"),
        assessed_at=retrieved_at,
        rationale="raw-captured Polymarket Gamma market description and rule",
    )
    return DocumentedReferenceSource(
        mapping_uid=registry_mapping.mapping_uid,
        market_uid=target.canonical_market_uid,
        market_rule_source_uid=registry_mapping.market_rule_source_uid,
        settlement_source_uid=registry_mapping.settlement_source_uid,
        primary_source_uid=registry_mapping.primary_source_uid,
        asset_symbol="BTC",
        quote_currency="USDT",
        observation_kind="candle_high",
        candle_interval="1m",
        requires_closed_candle=True,
        settlement_rule_url=registry_mapping.settlement_source_url,
        primary_source_url=registry_mapping.primary_source_url,
        event_time=retrieved_at,
        first_seen_at=retrieved_at,
        retrieved_at=retrieved_at,
        ingested_at=retrieved_at,
        provenance=Provenance(
            source_uid=registry_mapping.market_rule_source_uid,
            raw_artifact_uid=raw_uid,
            parser_version="phase15-polymarket-case-context-v1.0.0",
            content_hash=digest,
            retrieved_at=retrieved_at,
            source_url=registry_mapping.market_rule_source_url,
        ),
        reliability=reliability,
    )


def _availability(capture: RawCapture, *, admissible_at: datetime | None = None) -> PrimarySourceAvailability:
    """Represent bounded exchange-clock adjustment without changing raw receipt time."""

    timestamp = _utc(admissible_at or capture.received_at, field="admissible_at")
    if timestamp < capture.received_at:
        raise ValueError("admissible_at cannot precede the immutable raw receipt")
    reliability = SourceReliability(
        source_uid=BINANCE_SOURCE_UID,
        source_class=SourceClass.PRIMARY_SOURCE,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.95"),
        assessed_at=timestamp,
        rationale="fixed Binance Spot BTCUSDT 1-minute kline WebSocket stream",
    )
    return PrimarySourceAvailability(
        availability_uid=f"binance:availability/{capture.sha256}",
        primary_source_uid=BINANCE_SOURCE_UID,
        status=PrimarySourceStatus.AVAILABLE,
        checked_at=timestamp,
        first_seen_at=timestamp,
        retrieved_at=timestamp,
        ingested_at=timestamp,
        provenance=Provenance(
            source_uid=BINANCE_SOURCE_UID,
            raw_artifact_uid=f"binance:raw/{capture.sha256}",
            parser_version=PARSER_VERSION,
            content_hash=capture.sha256,
            retrieved_at=timestamp,
            source_url="https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams",
        ),
        reliability=reliability,
    )


@dataclass(frozen=True, slots=True)
class _CandleCandidate:
    observation: ReferencePriceObservation | None
    clock_adjustment: Mapping[str, object] | None = None
    rejection_reason: str | None = None


def _final_candle_observation(
    *,
    frame: object,
    capture: RawCapture,
    mapping: DocumentedReferenceSource,
) -> _CandleCandidate:
    try:
        decoded = json.loads(_raw_bytes(frame).decode("utf-8"), parse_float=Decimal)
        if not isinstance(decoded, Mapping) or decoded.get("e") != "kline" or decoded.get("s") != "BTCUSDT":
            return _CandleCandidate(None)
        candle = decoded.get("k")
        if not isinstance(candle, Mapping) or candle.get("s") != "BTCUSDT" or candle.get("i") != "1m" or candle.get("x") is not True:
            return _CandleCandidate(None)
        high = Decimal(str(candle.get("h")))
        if not high.is_finite() or high < 0:
            return _CandleCandidate(None)
        start = _timestamp_millis(candle.get("t"), field="kline start")
        end = _timestamp_millis(candle.get("T"), field="kline end")
        exchange_event = _timestamp_millis(decoded.get("E"), field="exchange event time")
        if end <= start or exchange_event < end:
            return _CandleCandidate(None, rejection_reason="final candle timestamps are malformed or exchange event time predates candle end")
        event_ahead = exchange_event - capture.received_at
        candle_ahead = end - capture.received_at
        if event_ahead > MAX_CLOCK_SKEW or candle_ahead > MAX_CLOCK_SKEW:
            return _CandleCandidate(
                None,
                rejection_reason="exchange event or final candle end exceeds the fixed local clock-skew allowance",
            )
    except (UnicodeDecodeError, json.JSONDecodeError, InvalidOperation, ValueError, TypeError):
        return _CandleCandidate(None, rejection_reason="final candle frame is malformed")
    admissible_at = max(capture.received_at, exchange_event, end)
    availability = _availability(capture, admissible_at=admissible_at)
    observation = ReferencePriceObservation(
        reference_price_uid=f"binance:reference-price/{mapping.market_uid.rsplit('/', 1)[1]}/{end.strftime('%Y%m%dT%H%M%S%fZ')}/{capture.sha256}",
        market_uid=mapping.market_uid,
        price=high,
        observation_kind="candle_high",
        source_symbol="BTCUSDT",
        candle_interval="1m",
        candle_start=start,
        candle_end=end,
        is_final=True,
        event_time=end,
        first_seen_at=admissible_at,
        retrieved_at=admissible_at,
        ingested_at=admissible_at,
        source_mapping=mapping,
        primary_source_availability=availability,
        provenance=availability.provenance,
        reliability=availability.reliability,
    )
    return _CandleCandidate(
        observation,
        clock_adjustment={
            "policy": "bounded_exchange_clock_skew_v1",
            "max_clock_skew_seconds": str(Decimal(str(MAX_CLOCK_SKEW.total_seconds()))),
            "local_receipt_at": _utc_iso(capture.received_at),
            "exchange_event_at": _utc_iso(exchange_event),
            "candle_end_at": _utc_iso(end),
            "exchange_event_ahead_seconds": str(Decimal(str(max(event_ahead.total_seconds(), 0.0)))),
            "candle_end_ahead_seconds": str(Decimal(str(max(candle_ahead.total_seconds(), 0.0)))),
            "admissible_at": _utc_iso(admissible_at),
            "reason": "exchange event and final candle end were within the fixed bounded clock-skew allowance",
        },
    )


def _coverage(*, captures: tuple[RawCapture, ...], complete: bool, at: datetime, target: ApprovedMarketTarget, mapping: DocumentedBtcReferenceMapping) -> CoverageRecord:
    return CoverageRecord(
        platform=BINANCE_PLATFORM,
        dataset="btc_usdt_1m_final_candle_high",
        interval_start=at,
        interval_end=at,
        fetched_at=at,
        record_count=1 if complete else 0,
        complete=complete,
        raw_sha256=tuple(capture.sha256 for capture in captures),
        filters={
            "target_uid": target.target_uid,
            "market_uid": target.canonical_market_uid,
            "mapping_uid": mapping.mapping_uid,
            "source_uid": BINANCE_SOURCE_UID,
            "stream_url": BINANCE_STREAM_URL,
            "observation_kind": "candle_high",
            "candle_interval": "1m",
            "requires_closed_candle": True,
        },
    )


def _raw_capture_payload(capture: RawCapture) -> dict[str, object]:
    return {
        "sha256": capture.sha256,
        "byte_length": capture.byte_length,
        "object_path": str(capture.object_path),
        "receipt_path": str(capture.receipt_path),
        "received_at": capture.received_at,
        "platform": capture.platform,
        "source": capture.source,
    }


def _write_manifest(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=".pending-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(document, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


async def collect_approved_binance_btcusdt_reference(
    *,
    registry: Phase15SourceRegistry,
    target_uid: str,
    output_dir: str | Path,
    connect: WebSocketConnect | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    target = registry.target(target_uid)
    if target.reference_mapping_uid is None:
        raise ValueError("approved target has no documented reference mapping")
    mapping_config = registry.reference_mapping(target.reference_mapping_uid)
    validate_binance_target(target, mapping_config)
    root = Path(output_dir)
    store = RawArtifactStore(root / "raw")
    manifest_path = root / MANIFEST_FILENAME
    captures: list[RawCapture] = []
    mapping: DocumentedReferenceSource | None = None
    observation: ReferencePriceObservation | None = None
    clock_adjustment: Mapping[str, object] | None = None
    reason = ""
    status = "unavailable_mapping"
    try:
        mapping = _context_mapping(root=root, target=target, registry_mapping=mapping_config)
    except ValueError as exc:
        reason = str(exc)
    else:
        connection: BinanceConnection | None = None
        try:
            connection = await open_binance_btcusdt_kline_socket(connect=connect)
            deadline = asyncio.get_running_loop().time() + MAX_DURATION_SECONDS
            for _ in range(MAX_FRAMES):
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    frame = await asyncio.wait_for(connection.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                captured_at = _utc(now(), field="now")
                capture = store.capture(
                    _raw_bytes(frame),
                    platform=BINANCE_PLATFORM,
                    source=BINANCE_SOURCE_UID,
                    request={"transport": "websocket", "url": BINANCE_STREAM_URL, "configured_endpoint_ref": BINANCE_ENDPOINT_REF},
                    received_at=captured_at,
                    response_metadata={"frame_kind": type(frame).__name__},
                )
                captures.append(capture)
                candidate = _final_candle_observation(frame=frame, capture=capture, mapping=mapping)
                if candidate.observation is not None:
                    observation = candidate.observation
                    clock_adjustment = candidate.clock_adjustment
                    status = "collected"
                    reason = "fixed Binance BTCUSDT final 1-minute candle High was raw-lineaged"
                    break
                if candidate.rejection_reason is not None:
                    reason = candidate.rejection_reason
            if observation is None and not reason:
                reason = "bounded Binance stream ended without a final BTCUSDT 1-minute candle High"
                status = "unavailable_no_final_candle"
        except Exception as exc:
            if not reason:
                reason = f"fixed Binance stream was unavailable: {type(exc).__name__}"
                status = "unavailable_transport"
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except Exception:
                    pass
    local_cutoff = _utc(now(), field="now")
    cutoff = max(local_cutoff, observation.available_at) if observation is not None else local_cutoff
    availability = _availability(captures[-1], admissible_at=cutoff) if captures else None
    admission = admit_reference_price(
        market_uid=target.canonical_market_uid,
        as_of=cutoff,
        documented_sources=() if mapping is None else (mapping,),
        primary_source_availability=availability,
        observation=observation,
    )
    if admission.status != ReferenceAdmissionStatus.ADMITTED:
        status = "unavailable_reference"
        if not reason:
            reason = admission.reason
    complete = status == "collected" and admission.status == ReferenceAdmissionStatus.ADMITTED
    coverage = _coverage(captures=tuple(captures), complete=complete, at=cutoff, target=target, mapping=mapping_config)
    CoverageLedger(root / "coverage.jsonl").append(coverage)
    document = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "command": "collect-binance-btcusdt-reference",
        "status": "collected" if complete else status,
        "reason": reason,
        "target_uid": target.target_uid,
        "market_uid": target.canonical_market_uid,
        "as_of": cutoff,
        "created_at": cutoff,
        "admission": admission.model_dump(mode="json"),
        "mapping": None if mapping is None else mapping.model_dump(mode="json"),
        "primary_source_availability": None if availability is None else availability.model_dump(mode="json"),
        "observation": None if observation is None else observation.model_dump(mode="json"),
        "clock_adjustment": clock_adjustment,
        "raw_captures": [_raw_capture_payload(capture) for capture in captures],
        "coverage": [{
            "platform": coverage.platform, "dataset": coverage.dataset, "interval_start": coverage.interval_start,
            "interval_end": coverage.interval_end, "fetched_at": coverage.fetched_at, "record_count": coverage.record_count,
            "complete": coverage.complete, "raw_sha256": coverage.raw_sha256, "filters": dict(coverage.filters),
        }],
        "paths": {"raw": str(store.root), "coverage_ledger": str(root / "coverage.jsonl"), "manifest": str(manifest_path)},
    }
    _write_manifest(manifest_path, document)
    return {
        "command": "collect-binance-btcusdt-reference",
        "status": document["status"],
        "raw_receipt_count": len(captures),
        "observation_admitted": admission.status == ReferenceAdmissionStatus.ADMITTED,
        "paths": document["paths"],
    }


def run_approved_binance_btcusdt_reference_collection(
    *, registry_path: str | Path, target_uid: str, output_dir: str | Path
) -> dict[str, Any]:
    return asyncio.run(
        collect_approved_binance_btcusdt_reference(
            registry=load_phase15_source_registry(registry_path), target_uid=target_uid, output_dir=output_dir
        )
    )


__all__ = [
    "BINANCE_STREAM_URL", "MANIFEST_FILENAME", "collect_approved_binance_btcusdt_reference",
    "run_approved_binance_btcusdt_reference_collection", "validate_binance_target",
]
