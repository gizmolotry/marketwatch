"""Official Polymarket Data API trades and CLOB order-book connector.

Contracts:
* https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
* https://docs.polymarket.com/api-reference/market-data/get-order-book
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterator, Mapping

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
    MAX_TRADE_LIMIT = 10_000
    MAX_TRADE_OFFSET = 10_000
    _WALLET_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
    _PUBLIC_TRADE_PARAMETERS = frozenset(
        {
            "limit",
            "offset",
            "takerOnly",
            "market",
            "eventId",
            "user",
            "side",
            "start",
            "end",
            "filterType",
            "filterAmount",
        }
    )

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
            "generic collection preserves the API default by omitting takerOnly",
            "wallet-history collection explicitly requests takerOnly=false",
            "user-scoped collection sends start=1 when no later lower bound is supplied",
            "every offset crawl is frozen to the last completed whole-second end watermark",
            "a full page at the offset ceiling is partial and requires a narrower time window",
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

    def __init__(
        self,
        http: EvidenceHttpClient,
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        self.http = http
        self._clock = clock or (lambda: datetime.now(UTC))

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
    def _normalize_wallet(cls, user: str | None) -> str | None:
        if user is None:
            return None
        wallet = require_text(user, "user")
        if not cls._WALLET_PATTERN.fullmatch(wallet):
            raise ValueError("user must be a 0x-prefixed 40-hex-character wallet address")
        return wallet.lower()

    @classmethod
    def trade_query_filters(
        cls,
        *,
        market: str | None = None,
        event_id: int | None = None,
        user: str | None = None,
        side: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        taker_only: bool | None = None,
    ) -> dict[str, Any]:
        """Return the exact non-pagination filters sent to ``/trades``.

        This mapping is suitable for ``CoverageRecord.filters``. Generic calls
        preserve legacy API behavior by omitting ``takerOnly``; callers that
        require both wallet trade roles must explicitly pass ``False``.
        """

        if market is not None and event_id is not None:
            raise ValueError("market and event_id are mutually exclusive")
        if event_id is not None and (isinstance(event_id, bool) or event_id < 1):
            raise ValueError("event_id must be a positive integer")
        if taker_only is not None and not isinstance(taker_only, bool):
            raise ValueError("taker_only must be a boolean")
        # The Data API's query enum is uppercase.  Do not normalize a caller's
        # value here: a lower-case value, an empty value, or a domain enum is
        # ambiguous at this external boundary and must not silently change the
        # exact query/provenance record.
        if side is not None and (type(side) is not str or side not in {"BUY", "SELL"}):
            raise ValueError("side must be exactly BUY or SELL")
        normalized_user = cls._normalize_wallet(user)
        normalized_start = utc_datetime(start, "start") if start is not None else None
        normalized_end = utc_datetime(end, "end") if end is not None else None
        if normalized_start is not None and normalized_start.microsecond:
            raise ValueError("start must use whole-second precision")
        if normalized_end is not None and normalized_end.microsecond:
            raise ValueError("end must use whole-second precision")
        if normalized_start is not None and normalized_end is not None and normalized_end < normalized_start:
            raise ValueError("end must be at or after start")

        filters: dict[str, Any] = {}
        if taker_only is not None:
            filters["takerOnly"] = taker_only
        if market is not None:
            filters["market"] = require_text(market, "market")
        if event_id is not None:
            filters["eventId"] = event_id
        if normalized_user is not None:
            filters["user"] = normalized_user
        if side is not None:
            filters["side"] = side
        if normalized_start is not None:
            filters["start"] = int(normalized_start.timestamp())
        elif normalized_user is not None and market is None and event_id is None:
            # The documented default is only the most recent approximately
            # three years. A positive epoch opts a user-only query into full
            # history. Market/event-scoped queries retain their source floor;
            # start can narrow that window but cannot extend it.
            filters["start"] = 1
        if normalized_end is not None:
            filters["end"] = int(normalized_end.timestamp())
        return filters

    @classmethod
    def _continuation_filter_snapshot(
        cls,
        continuation: str | None,
    ) -> Mapping[str, Any] | None:
        if continuation is None:
            return None
        token = require_text(continuation, "continuation")
        if token.isdigit():
            return None
        try:
            payload = json.loads(token)
        except json.JSONDecodeError as exc:
            raise ValueError("continuation is not a valid Polymarket trade cursor") from exc
        if not isinstance(payload, Mapping) or payload.get("version") != 1:
            raise ValueError("continuation has an unsupported Polymarket trade cursor version")
        filters = payload.get("filters")
        if not isinstance(filters, Mapping):
            raise ValueError("continuation is missing its Polymarket trade filter snapshot")
        return filters

    def resolve_trade_query_filters(
        self,
        *,
        market: str | None = None,
        event_id: int | None = None,
        user: str | None = None,
        side: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        taker_only: bool | None = None,
        continuation: str | None = None,
    ) -> dict[str, Any]:
        """Resolve the immutable filter snapshot for one offset crawl."""

        resolved_end = end
        if resolved_end is None:
            cursor_filters = self._continuation_filter_snapshot(continuation)
            cursor_end = cursor_filters.get("end") if cursor_filters is not None else None
            if isinstance(cursor_end, int) and not isinstance(cursor_end, bool):
                resolved_end = datetime.fromtimestamp(cursor_end, tz=UTC)
            elif continuation is not None:
                raise ValueError(
                    "legacy continuation requires an explicit whole-second end watermark"
                )
            else:
                # The current second is still open: a later delivery can carry
                # the same integer timestamp and shift offset pagination. Freeze
                # at the last completed second instead.
                resolved_end = (
                    utc_datetime(self._clock(), "clock").replace(microsecond=0)
                    - timedelta(seconds=1)
                )
        return self.trade_query_filters(
            market=market,
            event_id=event_id,
            user=user,
            side=side,
            start=start,
            end=resolved_end,
            taker_only=taker_only,
        )

    @classmethod
    def _encode_trade_continuation(
        cls,
        *,
        offset: int,
        filters: Mapping[str, Any],
        state: str = "page",
    ) -> str:
        return json.dumps(
            {
                "filters": dict(filters),
                "offset": offset,
                "state": state,
                "version": 1,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def _decode_trade_continuation(
        cls,
        continuation: str | None,
        *,
        filters: Mapping[str, Any],
    ) -> int:
        if continuation is None:
            return 0
        token = require_text(continuation, "continuation")
        # Preserve compatibility with legacy offset-only tokens while ensuring
        # new tokens bind a resume to the exact wallet/window/filter query.
        if token.isdigit():
            offset = int(token)
            if offset > cls.MAX_TRADE_OFFSET:
                raise ValueError("continuation offset exceeds the Polymarket API ceiling")
            return offset
        try:
            payload = json.loads(token)
        except json.JSONDecodeError as exc:
            raise ValueError("continuation is not a valid Polymarket trade cursor") from exc
        if not isinstance(payload, Mapping) or payload.get("version") != 1:
            raise ValueError("continuation has an unsupported Polymarket trade cursor version")
        if payload.get("state") == "split_required":
            raise ValueError(
                "continuation reached the Polymarket offset ceiling; subdivide the start/end window"
            )
        if payload.get("state") != "page" or payload.get("filters") != dict(filters):
            raise ValueError("continuation does not match the requested Polymarket trade filters")
        offset_value = payload.get("offset")
        if isinstance(offset_value, bool) or not isinstance(offset_value, int):
            raise ValueError("continuation offset must be an integer")
        if offset_value < 0 or offset_value > cls.MAX_TRADE_OFFSET:
            raise ValueError("continuation offset is outside the Polymarket API range")
        return offset_value

    @classmethod
    def normalize_trade(cls, item: Mapping[str, Any], capture: RawCapture) -> TradeFill:
        condition_id = require_text(item.get("conditionId"), "conditionId")
        asset = require_text(item.get("asset"), "asset")
        source_wallet = require_text(item.get("proxyWallet"), "proxyWallet")
        if not cls._WALLET_PATTERN.fullmatch(source_wallet):
            raise ValueError(
                "proxyWallet must be a 0x-prefixed 40-hex-character wallet address"
            )
        wallet = source_wallet.lower()
        # Do not repair casing at the source boundary: the documented source
        # enum is exactly uppercase BUY/SELL and provenance must retain that
        # contract instead of accepting an ambiguous variant.
        side_text = require_text(item.get("side"), "side")
        if side_text not in {"BUY", "SELL"}:
            raise ValueError("source side must be exactly BUY or SELL")
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
        user: str | None = None,
        side: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        taker_only: bool | None = None,
        page_size: int = 1000,
        max_pages: int = 11,
        continuation: str | None = None,
        http_attempt_limit: int | None = None,
    ) -> Iterator[ConnectorPage[TradeFill]]:
        if page_size < 1 or page_size > self.MAX_TRADE_LIMIT:
            raise ValueError("page_size must be in [1, 10000]")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        filters = self.resolve_trade_query_filters(
            market=market,
            event_id=event_id,
            user=user,
            side=side,
            start=start,
            end=end,
            taker_only=taker_only,
            continuation=continuation,
        )
        normalized_start = (
            datetime.fromtimestamp(filters["start"], tz=UTC)
            if "start" in filters
            else None
        )
        normalized_end = datetime.fromtimestamp(filters["end"], tz=UTC)
        normalized_user = filters.get("user")
        offset = self._decode_trade_continuation(continuation, filters=filters)
        for _ in range(max_pages):
            params: dict[str, Any] = {"limit": page_size, "offset": offset, **filters}
            parsed = self.http.get_json(
                platform="polymarket",
                source="data-api/trades",
                url=f"{self.DATA_API}/trades",
                params=params,
                public_parameter_allowlist=self._PUBLIC_TRADE_PARAMETERS,
                max_attempts=http_attempt_limit,
            )
            if not isinstance(parsed.payload, list):
                raise ValueError("Polymarket /trades response must be a list")
            records: list[TradeFill] = []
            for item in parsed.payload:
                if not isinstance(item, Mapping):
                    raise ValueError("Polymarket /trades response rows must all be objects")
                record = self.normalize_trade(item, parsed.raw)
                if normalized_start and record.event_time < normalized_start:
                    raise ValueError("Polymarket /trades returned a row before the exact start filter")
                if normalized_end and record.event_time > normalized_end:
                    raise ValueError("Polymarket /trades returned a row after the exact end filter")
                if market is not None and record.market_uid != qualified_id(
                    "polymarket", f"market/{market}"
                ):
                    raise ValueError("Polymarket /trades returned a row outside the exact market filter")
                if normalized_user is not None and record.actor_uid != qualified_id(
                    "polymarket", f"wallet/{normalized_user}"
                ):
                    raise ValueError("Polymarket /trades returned a row outside the exact user filter")
                if side is not None and record.side is not TradeSide(side.lower()):
                    raise ValueError("Polymarket /trades returned a row outside the exact side filter")
                records.append(record)
            next_offset = offset + len(parsed.payload)
            exhausted = len(parsed.payload) < page_size
            complete = exhausted
            api_ceiling = not exhausted and next_offset > self.MAX_TRADE_OFFSET
            next_continuation = None
            if not complete:
                next_continuation = self._encode_trade_continuation(
                    offset=next_offset,
                    filters=filters,
                    state="split_required" if api_ceiling else "page",
                )
            yield ConnectorPage(
                tuple(records),
                self._raw_artifact(parsed.raw, self.TRADE_SOURCE_UID),
                next_continuation,
                complete,
                parsed.raw,
                parsed.attempt_captures,
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
            if page.raw_capture is not None:
                # The final artifact is already present above.  Preceding
                # retry attempts are response evidence too, even though they
                # did not produce normalized records.
                batch.raw_artifacts[-1:] = [
                    self._raw_artifact(capture, self.TRADE_SOURCE_UID)
                    for capture in page.raw_attempt_captures
                ]
                batch.raw_captures.extend(page.raw_attempt_captures)
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
            raw_artifacts=[
                self._raw_artifact(capture, self.BOOK_SOURCE_UID)
                for capture in parsed.attempt_captures
            ],
            raw_captures=list(parsed.attempt_captures),
            capabilities=[self.orderbook_capability],
            quality=DataQualityReport(
                source="polymarket:clob/book", received=1, normalized=1
            ),
        )
