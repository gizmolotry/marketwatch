"""Official Polymarket Data API trades and CLOB order-book connector.

Contracts:
* https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
* https://docs.polymarket.com/api-reference/market-data/get-order-book
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator, Mapping

from marketleak.domain import (
    ActorVisibility,
    OrderBookLevel,
    OrderBookSnapshot,
    RawArtifact,
    TradeFill,
    TradeSide,
)

from ..coverage import CapabilityMetadata
from ..normalize import decimal_from, qualified_id, require_text, stable_uid, utc_datetime
from ..quality import DataQualityReport
from ..raw_store import RawArtifactStore, RawCapture
from .http import EvidenceHttpClient
from .models import ConnectorPage, IngestionBatch


class PolymarketConnector:
    """Consume only documented, unauthenticated Polymarket endpoints."""

    DATA_API = "https://data-api.polymarket.com"
    CLOB_API = "https://clob.polymarket.com"
    PARSER_VERSION = "polymarket-public-v2.0.0"
    TRADE_SOURCE_UID = "polymarket:source/data-api-trades"
    BOOK_SOURCE_UID = "polymarket:source/clob-book"

    trade_capability = CapabilityMetadata(
        platform="polymarket",
        dataset="public_trades",
        actor_visibility="available",
        direction_visibility="available",
        depth_visibility="unavailable",
        pagination="offset (limit <= 10000, offset <= 10000)",
        official_documentation=(
            "https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets"
        ),
        notes=(
            "proxyWallet is the only actor attached to the public trade record",
            "maker/taker identity is not inferred",
            "the endpoint exposes no standalone trade id; fill UID is a stable composite",
        ),
    )
    orderbook_capability = CapabilityMetadata(
        platform="polymarket",
        dataset="orderbook_snapshots",
        actor_visibility="unavailable",
        direction_visibility="unavailable",
        depth_visibility="available",
        pagination="single token snapshot",
        official_documentation=(
            "https://docs.polymarket.com/api-reference/market-data/get-order-book"
        ),
        notes=("resting-order actors are not exposed",),
    )

    def __init__(self, http: EvidenceHttpClient):
        self.http = http

    @staticmethod
    def _raw_uid(capture: RawCapture) -> str:
        return f"polymarket:raw/{capture.sha256}"

    @classmethod
    def _raw_artifact(cls, capture: RawCapture, source_uid: str) -> RawArtifact:
        return RawArtifactStore.to_domain(
            capture,
            source_uid=source_uid,
            parser_version=cls.PARSER_VERSION,
        )

    @classmethod
    def normalize_trade(cls, item: Mapping[str, Any], capture: RawCapture) -> TradeFill:
        condition_id = require_text(item.get("conditionId"), "conditionId")
        asset = require_text(item.get("asset"), "asset")
        wallet = require_text(item.get("proxyWallet"), "proxyWallet").lower()
        side_text = require_text(item.get("side"), "side").upper()
        if side_text not in {"BUY", "SELL"}:
            raise ValueError("side must be explicitly BUY or SELL")
        event_time = utc_datetime(item.get("timestamp"), "timestamp")
        price = decimal_from(item.get("price"), "price", minimum=Decimal(0), maximum=Decimal(1))
        size = decimal_from(item.get("size"), "size", minimum=Decimal(0))
        transaction_hash = require_text(item.get("transactionHash"), "transactionHash").lower()
        # The public endpoint does not expose a trade id. Every documented field
        # that distinguishes executions participates in this deterministic UID.
        fill_uid = stable_uid(
            "polymarket",
            "fill",
            (
                transaction_hash,
                condition_id,
                asset,
                wallet,
                side_text,
                str(size),
                str(price),
                int(event_time.timestamp()),
                item.get("outcomeIndex"),
            ),
        )
        return TradeFill(
            event_time=event_time,
            ingested_at=capture.received_at,
            source_uid=cls.TRADE_SOURCE_UID,
            raw_artifact_uid=cls._raw_uid(capture),
            parser_version=cls.PARSER_VERSION,
            fill_uid=fill_uid,
            market_uid=qualified_id("polymarket", f"market/{condition_id}"),
            outcome_uid=qualified_id("polymarket", f"outcome/{asset}"),
            platform="polymarket",
            price=price,
            size=size,
            side=TradeSide(side_text.lower()),
            actor_visibility=ActorVisibility.PUBLIC_WALLET,
            actor_uid=qualified_id("polymarket", f"wallet/{wallet}"),
            maker=None,
            taker=None,
            transaction_uid=qualified_id("polymarket", f"tx/{transaction_hash}"),
        )

    def iter_trade_pages(
        self,
        *,
        market: str | None = None,
        event_id: int | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        page_size: int = 1000,
        max_pages: int = 11,
    ) -> Iterator[ConnectorPage[TradeFill]]:
        if page_size < 1 or page_size > 10_000:
            raise ValueError("page_size must be in [1, 10000]")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        normalized_start = utc_datetime(start, "start") if start else None
        normalized_end = utc_datetime(end, "end") if end else None
        offset = 0
        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_size, "offset": offset}
            if market is not None:
                params["market"] = market
            if event_id is not None:
                params["eventId"] = event_id
            parsed = self.http.get_json(
                platform="polymarket",
                source="data-api/trades",
                url=f"{self.DATA_API}/trades",
                params=params,
            )
            if not isinstance(parsed.payload, list):
                raise ValueError("Polymarket /trades response must be a list")
            records: list[TradeFill] = []
            for item in parsed.payload:
                if not isinstance(item, Mapping):
                    continue
                record = self.normalize_trade(item, parsed.raw)
                if normalized_start and record.event_time < normalized_start:
                    continue
                if normalized_end and record.event_time > normalized_end:
                    continue
                records.append(record)
            next_offset = offset + len(parsed.payload)
            exhausted = len(parsed.payload) < page_size
            api_ceiling = next_offset > 10_000
            complete = exhausted
            continuation = None if complete else str(next_offset)
            yield ConnectorPage(
                tuple(records),
                self._raw_artifact(parsed.raw, self.TRADE_SOURCE_UID),
                continuation,
                complete,
            )
            if complete or api_ceiling:
                return
            offset = next_offset

    def fetch_trades(self, **kwargs: Any) -> IngestionBatch:
        quality = DataQualityReport(source="polymarket:data-api/trades")
        batch = IngestionBatch(capabilities=[self.trade_capability], quality=quality)
        for page in self.iter_trade_pages(**kwargs):
            quality.received += len(page.records)
            quality.normalized += len(page.records)
            batch.fills.extend(page.records)
            batch.raw_artifacts.append(page.raw_artifact)
            batch.continuation = page.continuation
            batch.complete = page.complete
        return batch

    @classmethod
    def normalize_orderbook(cls, payload: Mapping[str, Any], capture: RawCapture) -> OrderBookSnapshot:
        market = require_text(payload.get("market"), "market")
        asset = require_text(payload.get("asset_id"), "asset_id")
        source_hash = require_text(payload.get("hash"), "hash")
        event_time = utc_datetime(payload.get("timestamp"), "timestamp")

        def levels(name: str, reverse: bool) -> tuple[OrderBookLevel, ...]:
            raw_levels = payload.get(name)
            if not isinstance(raw_levels, list):
                raise ValueError(f"{name} must be a list")
            normalized = [
                OrderBookLevel(
                    price=decimal_from(level.get("price"), f"{name}.price", minimum=Decimal(0), maximum=Decimal(1)),
                    size=decimal_from(level.get("size"), f"{name}.size", minimum=Decimal(0)),
                )
                for level in raw_levels
                if isinstance(level, Mapping)
            ]
            return tuple(sorted(normalized, key=lambda level: level.price, reverse=reverse))

        return OrderBookSnapshot(
            event_time=event_time,
            ingested_at=capture.received_at,
            source_uid=cls.BOOK_SOURCE_UID,
            raw_artifact_uid=cls._raw_uid(capture),
            parser_version=cls.PARSER_VERSION,
            snapshot_uid=qualified_id("polymarket", f"book/{asset}/{source_hash}"),
            market_uid=qualified_id("polymarket", f"market/{market}"),
            outcome_uid=qualified_id("polymarket", f"outcome/{asset}"),
            platform="polymarket",
            bids=levels("bids", True),
            asks=levels("asks", False),
        )

    def fetch_orderbook(self, token_id: str) -> IngestionBatch:
        parsed = self.http.get_json(
            platform="polymarket",
            source="clob/book",
            url=f"{self.CLOB_API}/book",
            params={"token_id": require_text(token_id, "token_id")},
        )
        if not isinstance(parsed.payload, Mapping):
            raise ValueError("Polymarket /book response must be an object")
        snapshot = self.normalize_orderbook(parsed.payload, parsed.raw)
        return IngestionBatch(
            snapshots=[snapshot],
            raw_artifacts=[self._raw_artifact(parsed.raw, self.BOOK_SOURCE_UID)],
            capabilities=[self.orderbook_capability],
            quality=DataQualityReport(
                source="polymarket:clob/book", received=1, normalized=1
            ),
        )
