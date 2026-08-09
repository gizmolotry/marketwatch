"""Official Kalshi public trades and order-book connector.

Contracts:
* https://docs.kalshi.com/api-reference/market/get-trades
* https://docs.kalshi.com/api-reference/market/get-market-orderbook
* https://docs.kalshi.com/getting_started/orderbook_responses
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator, Mapping
from urllib.parse import quote

from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
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


class KalshiConnector:
    """Consume documented public endpoints without implying account identity."""

    BASE_API = "https://external-api.kalshi.com/trade-api/v2"
    PARSER_VERSION = "kalshi-public-v2.0.0"
    TRADE_SOURCE_UID = "kalshi:source/public-trades"
    BOOK_SOURCE_UID = "kalshi:source/public-orderbook"

    trade_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="public_trades",
        actor_visibility="unavailable",
        direction_visibility="partial",
        depth_visibility="unavailable",
        pagination="opaque cursor; min_ts/max_ts Unix-second filters",
        official_documentation="https://docs.kalshi.com/api-reference/market/get-trades",
        notes=(
            "the public response does not expose account identifiers",
            "a fill side is emitted only when an explicit taker outcome side is present",
            "otherwise the trade is retained solely as a last-trade price observation",
        ),
    )
    orderbook_capability = CapabilityMetadata(
        platform="kalshi",
        dataset="orderbook_snapshots",
        actor_visibility="unavailable",
        direction_visibility="unavailable",
        depth_visibility="available",
        pagination="single market snapshot with optional depth",
        official_documentation="https://docs.kalshi.com/getting_started/orderbook_responses",
        notes=(
            "the endpoint returns YES and NO bids, not actor-level orders",
            "opposite-outcome bids are not converted into synthetic asks",
        ),
    )

    def __init__(self, http: EvidenceHttpClient):
        self.http = http

    @staticmethod
    def _raw_uid(capture: RawCapture) -> str:
        return f"kalshi:raw/{capture.sha256}"

    @classmethod
    def _raw_artifact(cls, capture: RawCapture, source_uid: str) -> RawArtifact:
        return RawArtifactStore.to_domain(
            capture,
            source_uid=source_uid,
            parser_version=cls.PARSER_VERSION,
        )

    @classmethod
    def normalize_trade(
        cls,
        item: Mapping[str, Any],
        capture: RawCapture,
    ) -> tuple[PriceObservation, TradeFill | None]:
        trade_id = require_text(item.get("trade_id"), "trade_id")
        ticker = require_text(item.get("ticker"), "ticker")
        event_time = utc_datetime(item.get("created_time"), "created_time")
        yes_price = decimal_from(
            item.get("yes_price_dollars"),
            "yes_price_dollars",
            minimum=Decimal(0),
            maximum=Decimal(1),
        )
        market_uid = qualified_id("kalshi", f"market/{ticker}")
        yes_outcome_uid = qualified_id("kalshi", f"outcome/{ticker}/yes")
        observation = PriceObservation(
            event_time=event_time,
            ingested_at=capture.received_at,
            source_uid=cls.TRADE_SOURCE_UID,
            raw_artifact_uid=cls._raw_uid(capture),
            parser_version=cls.PARSER_VERSION,
            observation_uid=qualified_id("kalshi", f"trade-price/{trade_id}/yes"),
            market_uid=market_uid,
            outcome_uid=yes_outcome_uid,
            platform="kalshi",
            price=yes_price,
            kind=ObservationKind.LAST_TRADE,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
        )

        # The current public schema does not guarantee direction. Historical
        # response versions sometimes include an explicit taker outcome side;
        # use it when present, and never infer direction from prices.
        taker_outcome = item.get("taker_outcome_side", item.get("taker_side"))
        if taker_outcome is None:
            return observation, None
        outcome = str(taker_outcome).strip().lower()
        if outcome not in {"yes", "no"}:
            return observation, None
        size = decimal_from(item.get("count_fp"), "count_fp", minimum=Decimal(0))
        if outcome == "yes":
            outcome_uid = yes_outcome_uid
            price = yes_price
        else:
            outcome_uid = qualified_id("kalshi", f"outcome/{ticker}/no")
            price = decimal_from(
                item.get("no_price_dollars"),
                "no_price_dollars",
                minimum=Decimal(0),
                maximum=Decimal(1),
            )
        fill = TradeFill(
            event_time=event_time,
            ingested_at=capture.received_at,
            source_uid=cls.TRADE_SOURCE_UID,
            raw_artifact_uid=cls._raw_uid(capture),
            parser_version=cls.PARSER_VERSION,
            fill_uid=qualified_id("kalshi", f"fill/{trade_id}/{outcome}"),
            market_uid=market_uid,
            outcome_uid=outcome_uid,
            platform="kalshi",
            price=price,
            size=size,
            side=TradeSide.BUY,
            actor_visibility=ActorVisibility.NOT_AVAILABLE,
            actor_uid=None,
            maker=None,
            taker=None,
            transaction_uid=None,
        )
        return observation, fill

    def iter_trade_pages(
        self,
        *,
        ticker: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        page_size: int = 1000,
        max_pages: int = 100,
    ) -> Iterator[ConnectorPage[PriceObservation | TradeFill]]:
        if page_size < 1 or page_size > 1000:
            raise ValueError("page_size must be in [1, 1000]")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_size}
            if cursor:
                params["cursor"] = cursor
            if ticker:
                params["ticker"] = ticker
            if start:
                params["min_ts"] = int(utc_datetime(start, "start").timestamp())
            if end:
                params["max_ts"] = int(utc_datetime(end, "end").timestamp())
            parsed = self.http.get_json(
                platform="kalshi",
                source="markets/trades",
                url=f"{self.BASE_API}/markets/trades",
                params=params,
            )
            if not isinstance(parsed.payload, Mapping) or not isinstance(parsed.payload.get("trades"), list):
                raise ValueError("Kalshi trades response must contain a trades list")
            records: list[PriceObservation | TradeFill] = []
            for item in parsed.payload["trades"]:
                if not isinstance(item, Mapping):
                    continue
                observation, fill = self.normalize_trade(item, parsed.raw)
                records.append(observation)
                if fill is not None:
                    records.append(fill)
            next_cursor_value = parsed.payload.get("cursor")
            next_cursor = str(next_cursor_value).strip() if next_cursor_value is not None else ""
            if next_cursor and next_cursor in seen_cursors:
                raise ValueError("Kalshi returned a repeated pagination cursor")
            complete = not next_cursor
            yield ConnectorPage(
                tuple(records),
                self._raw_artifact(parsed.raw, self.TRADE_SOURCE_UID),
                next_cursor or None,
                complete,
                parsed.raw,
                parsed.attempt_captures,
            )
            if complete:
                return
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def fetch_trades(self, **kwargs: Any) -> IngestionBatch:
        quality = DataQualityReport(source="kalshi:markets/trades")
        batch = IngestionBatch(capabilities=[self.trade_capability], quality=quality)
        for page in self.iter_trade_pages(**kwargs):
            for record in page.records:
                if isinstance(record, TradeFill):
                    batch.fills.append(record)
                else:
                    batch.observations.append(record)
            quality.received += len(batch.observations) - quality.received
            quality.normalized = len(batch.observations)
            if page.raw_capture is not None:
                batch.raw_artifacts.extend(
                    self._raw_artifact(capture, self.TRADE_SOURCE_UID)
                    for capture in page.raw_attempt_captures
                )
                batch.raw_captures.extend(page.raw_attempt_captures)
            else:
                batch.raw_artifacts.append(page.raw_artifact)
            batch.continuation = page.continuation
            batch.complete = page.complete
        return batch

    @classmethod
    def normalize_orderbook(
        cls,
        ticker: str,
        payload: Mapping[str, Any],
        capture: RawCapture,
    ) -> tuple[OrderBookSnapshot, OrderBookSnapshot]:
        book = payload.get("orderbook_fp")
        if not isinstance(book, Mapping):
            raise ValueError("Kalshi orderbook response must contain orderbook_fp")
        market_uid = qualified_id("kalshi", f"market/{ticker}")

        def bid_levels(name: str) -> tuple[OrderBookLevel, ...]:
            raw_levels = book.get(name)
            if not isinstance(raw_levels, list):
                raise ValueError(f"orderbook_fp.{name} must be a list")
            levels: list[OrderBookLevel] = []
            for raw_level in raw_levels:
                if not isinstance(raw_level, (list, tuple)) or len(raw_level) != 2:
                    raise ValueError(f"orderbook_fp.{name} levels must be [price, count] pairs")
                levels.append(
                    OrderBookLevel(
                        price=decimal_from(
                            raw_level[0], f"{name}.price", minimum=Decimal(0), maximum=Decimal(1)
                        ),
                        size=decimal_from(raw_level[1], f"{name}.count", minimum=Decimal(0)),
                    )
                )
            return tuple(sorted(levels, key=lambda level: level.price, reverse=True))

        snapshots: list[OrderBookSnapshot] = []
        for outcome, levels in (("yes", bid_levels("yes_dollars")), ("no", bid_levels("no_dollars"))):
            snapshots.append(
                OrderBookSnapshot(
                    event_time=capture.received_at,
                    ingested_at=capture.received_at,
                    source_uid=cls.BOOK_SOURCE_UID,
                    raw_artifact_uid=cls._raw_uid(capture),
                    parser_version=cls.PARSER_VERSION,
                    snapshot_uid=stable_uid(
                        "kalshi", "book", (ticker, outcome, capture.sha256)
                    ),
                    market_uid=market_uid,
                    outcome_uid=qualified_id("kalshi", f"outcome/{ticker}/{outcome}"),
                    platform="kalshi",
                    bids=levels,
                    asks=(),
                )
            )
        return snapshots[0], snapshots[1]

    def fetch_orderbook(self, ticker: str, *, depth: int | None = None) -> IngestionBatch:
        normalized_ticker = require_text(ticker, "ticker")
        params: dict[str, Any] = {}
        if depth is not None:
            if isinstance(depth, bool) or depth < 0:
                raise ValueError("depth must be non-negative")
            params["depth"] = depth
        parsed = self.http.get_json(
            platform="kalshi",
            source="market/orderbook",
            url=f"{self.BASE_API}/markets/{quote(normalized_ticker, safe='')}/orderbook",
            params=params,
        )
        if not isinstance(parsed.payload, Mapping):
            raise ValueError("Kalshi orderbook response must be an object")
        snapshots = list(self.normalize_orderbook(normalized_ticker, parsed.payload, parsed.raw))
        return IngestionBatch(
            snapshots=snapshots,
            raw_artifacts=[
                self._raw_artifact(capture, self.BOOK_SOURCE_UID)
                for capture in parsed.attempt_captures
            ],
            raw_captures=list(parsed.attempt_captures),
            capabilities=[self.orderbook_capability],
            quality=DataQualityReport(
                source="kalshi:market/orderbook", received=1, normalized=2
            ),
        )
