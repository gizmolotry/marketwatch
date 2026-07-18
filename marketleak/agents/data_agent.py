from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Any

import aiohttp
import pandas as pd
from pydantic import ValidationError

from marketleak.schemas import Market, MarketTick


class IngestionEngine:
    """Multi-platform async scraper with raw evidence capture and parquet output."""

    REQUEST_CONCURRENCY_LIMIT = 20
    MAX_REQUEST_RETRIES = 5

    def __init__(
        self,
        output_dir: str = "demo_data",
        request_timeout: int = 10,
        target_markets: int = 50_000,
    ):
        self.output_dir = output_dir
        self.raw_evidence_dir = os.path.join(self.output_dir, "raw_evidence")
        self.request_timeout = request_timeout
        self.target_markets = target_markets
        self.validation_errors: list[str] = []
        self._request_semaphore: asyncio.Semaphore | None = None
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.raw_evidence_dir, exist_ok=True)

    def _get_request_semaphore(self) -> asyncio.Semaphore:
        if self._request_semaphore is None:
            self._request_semaphore = asyncio.Semaphore(self.REQUEST_CONCURRENCY_LIMIT)
        return self._request_semaphore

    def _reset_request_semaphore(self) -> None:
        self._request_semaphore = asyncio.Semaphore(self.REQUEST_CONCURRENCY_LIMIT)

    def _save_raw_response(self, platform: str, raw_text: str) -> str:
        raw_path = os.path.join(
            self.raw_evidence_dir,
            f"{platform}_raw_{time.time_ns()}.json",
        )
        with open(raw_path, "w", encoding="utf-8") as raw_file:
            raw_file.write(raw_text)
        return raw_path

    @staticmethod
    def _retry_delay(attempt_index: int, retry_after: str | None = None) -> float:
        delay = min(60.0, float(2**attempt_index))
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                pass
        return delay

    async def _request_json_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        platform: str,
    ) -> tuple[Any | None, int]:
        semaphore = self._get_request_semaphore()

        for attempt in range(self.MAX_REQUEST_RETRIES):
            try:
                async with semaphore:
                    async with session.get(url) as response:
                        raw_text = await response.text()
                        raw_path = self._save_raw_response(platform, raw_text)
                        status_code = response.status
                        retry_after = response.headers.get("Retry-After")

                if status_code == 429 and attempt < self.MAX_REQUEST_RETRIES - 1:
                    delay = self._retry_delay(attempt, retry_after)
                    print(
                        f"[{platform}] HTTP 429 for {url}. "
                        f"Retrying in {delay:.1f}s. Raw saved to {raw_path}"
                    )
                    await asyncio.sleep(delay)
                    continue

                if status_code != 200:
                    print(f"[{platform}] HTTP {status_code} for {url}. Raw saved to {raw_path}")
                    return None, status_code

                try:
                    return json.loads(raw_text), status_code
                except json.JSONDecodeError as exc:
                    print(f"[{platform}] Invalid JSON for {url}. Raw saved to {raw_path}: {exc}")
                    return None, status_code

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < self.MAX_REQUEST_RETRIES - 1:
                    delay = self._retry_delay(attempt)
                    print(f"[{platform}] Request failed for {url}: {exc}. Retrying in {delay:.1f}s.")
                    await asyncio.sleep(delay)
                    continue
                print(f"[{platform}] Request failed for {url} after retries: {exc}")
                return None, 0

        return None, 0

    @staticmethod
    def _required_text(value: Any, field_name: str) -> str:
        if value is None:
            raise ValueError(f"missing required field: {field_name}")
        text = str(value).strip()
        if not text:
            raise ValueError(f"empty required field: {field_name}")
        return text

    @staticmethod
    def _unix_seconds(value: Any) -> int:
        if value is None or isinstance(value, bool):
            raise ValueError("timestamp is missing or invalid")

        if isinstance(value, (int, float)):
            if pd.isna(value):
                raise ValueError("timestamp is NaN")
            seconds = int(value)
            return seconds // 1000 if seconds > 10_000_000_000 else seconds

        text = str(value).strip()
        if not text:
            raise ValueError("timestamp is empty")
        if text.lstrip("-").isdigit():
            seconds = int(text)
            return seconds // 1000 if seconds > 10_000_000_000 else seconds

        parsed = pd.to_datetime(text, utc=True, errors="raise")
        if pd.isna(parsed):
            raise ValueError("timestamp could not be parsed")
        return int(parsed.timestamp())

    @staticmethod
    def _tick_uid(platform: str, market_uid: str, timestamp: int, price: float, source_key: str) -> str:
        raw_uid = f"{platform}|{market_uid}|{timestamp}|{price:.10f}|{source_key}"
        digest = hashlib.sha256(raw_uid.encode("utf-8")).hexdigest()
        return f"{platform}:{digest}"

    @staticmethod
    def _first_clob_token(raw_tokens: Any) -> str | None:
        if raw_tokens is None:
            return None
        if isinstance(raw_tokens, str):
            try:
                tokens = json.loads(raw_tokens)
            except json.JSONDecodeError:
                return None
        elif isinstance(raw_tokens, list):
            tokens = raw_tokens
        else:
            return None

        if not tokens:
            return None
        return str(tokens[0])

    @staticmethod
    def _chunks(items: list[Any], size: int) -> list[list[Any]]:
        return [items[index : index + size] for index in range(0, len(items), size)]

    def _record_validation_error(self, source: str, exc: Exception) -> None:
        self.validation_errors.append(f"{source}: {exc}")

    def _make_market(
        self,
        *,
        platform: str,
        source_market_id: Any,
        market_slug: Any,
        question: Any,
        close_time: Any,
        source: str,
    ) -> Market | None:
        try:
            source_id = self._required_text(source_market_id, "source_market_id")
            return Market(
                market_uid=f"{platform}:{source_id}",
                platform=platform,
                market_slug=self._required_text(market_slug, "market_slug"),
                question=self._required_text(question, "question"),
                close_time=self._required_text(close_time, "close_time"),
            )
        except (ValidationError, ValueError) as exc:
            self._record_validation_error(source, exc)
            return None

    def _make_tick(
        self,
        *,
        platform: str,
        market_uid: str,
        timestamp_value: Any,
        price_value: Any,
        source_key: str,
        source: str,
    ) -> MarketTick | None:
        try:
            timestamp = self._unix_seconds(timestamp_value)
            price = float(price_value)
            return MarketTick(
                tick_uid=self._tick_uid(platform, market_uid, timestamp, price, source_key),
                market_uid=market_uid,
                timestamp=timestamp,
                price=price,
                platform=platform,
            )
        except (ValidationError, ValueError, TypeError) as exc:
            self._record_validation_error(source, exc)
            return None

    @staticmethod
    def _kalshi_snapshot_price(market: dict[str, Any]) -> float:
        if market.get("last_price_dollars") is not None:
            return float(market.get("last_price_dollars"))
        if market.get("last_price") is not None:
            return float(market.get("last_price")) / 100.0
        return 0.0

    @staticmethod
    def _kalshi_trade_price(trade: dict[str, Any]) -> float:
        price = trade.get("price")
        if price is None:
            price = trade.get("yes_price")
        if price is None:
            raise ValueError("missing Kalshi trade price")
        return float(price) / 100.0

    async def _fetch_polymarket_page_async(
        self,
        session: aiohttp.ClientSession,
        offset: int,
        limit: int,
    ) -> tuple[int, list[dict[str, Any]]]:
        url = (
            "https://gamma-api.polymarket.com/markets"
            f"?active=true&limit={limit}&offset={offset}&order=createdAt&ascending=false"
        )
        payload, status_code = await self._request_json_async(session, url, "polymarket")
        if status_code != 200 or not payload:
            return offset, []
        if not isinstance(payload, list):
            print("Polymarket markets response was not a list.")
            return offset, []
        return offset, [market for market in payload if isinstance(market, dict)]

    async def _process_polymarket_market_async(
        self,
        session: aiohttp.ClientSession,
        market_json: dict[str, Any],
    ) -> tuple[Market | None, list[MarketTick]]:
        token_id = self._first_clob_token(market_json.get("clobTokenIds"))
        if not token_id:
            return None, []

        history_url = f"https://clob.polymarket.com/prices-history?market={token_id}&interval=all"
        history_payload, history_status = await self._request_json_async(session, history_url, "polymarket")
        if history_status != 200 or not isinstance(history_payload, dict):
            return None, []

        history = history_payload.get("history", [])
        if not history:
            return None, []

        market = self._make_market(
            platform="polymarket",
            source_market_id=market_json.get("id") or market_json.get("conditionId") or token_id,
            market_slug=market_json.get("slug") or market_json.get("id") or token_id,
            question=market_json.get("question"),
            close_time=market_json.get("endDate") or market_json.get("end_date") or market_json.get("close_time"),
            source=f"polymarket market {market_json.get('id') or token_id}",
        )
        if market is None:
            return None, []

        ticks: list[MarketTick] = []
        for point_index, point in enumerate(history):
            if not isinstance(point, dict):
                self._record_validation_error(
                    f"polymarket tick {market.market_uid} index {point_index}",
                    ValueError("history point was not an object"),
                )
                continue

            tick = self._make_tick(
                platform="polymarket",
                market_uid=market.market_uid,
                timestamp_value=point.get("t"),
                price_value=point.get("p"),
                source_key=f"{token_id}:{point_index}:{point.get('t')}:{point.get('p')}",
                source=f"polymarket tick {market.market_uid} index {point_index}",
            )
            if tick is not None:
                ticks.append(tick)

        if not ticks:
            return None, []
        return market, ticks

    async def fetch_polymarket_async(
        self,
        target_markets: int | None = None,
    ) -> tuple[list[Market], list[MarketTick]]:
        target_markets = self.target_markets if target_markets is None else target_markets
        print(f"Scraping Polymarket (Target: {target_markets} markets)...")
        markets_by_uid: dict[str, Market] = {}
        ticks: list[MarketTick] = []
        offset = 0
        limit = 100
        page_batch_size = self.REQUEST_CONCURRENCY_LIMIT
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0.0.0"}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            while len(markets_by_uid) < target_markets:
                offsets = [offset + (index * limit) for index in range(page_batch_size)]
                page_results = await asyncio.gather(
                    *(self._fetch_polymarket_page_async(session, page_offset, limit) for page_offset in offsets)
                )
                ordered_pages = [markets for _, markets in sorted(page_results, key=lambda item: item[0])]
                candidate_markets = [market for markets in ordered_pages for market in markets]
                if not candidate_markets:
                    break

                remaining = target_markets - len(markets_by_uid)
                candidate_markets = candidate_markets[: max(remaining * 2, remaining)]
                market_results = await asyncio.gather(
                    *(self._process_polymarket_market_async(session, market_json) for market_json in candidate_markets)
                )

                for market, market_ticks in market_results:
                    if market is None or not market_ticks:
                        continue
                    if market.market_uid in markets_by_uid:
                        continue
                    markets_by_uid[market.market_uid] = market
                    ticks.extend(market_ticks)
                    collected = len(markets_by_uid)
                    if collected % 50 == 0:
                        print(f"[Polymarket] Collected {collected}/{target_markets} markets")
                    if collected >= target_markets:
                        break

                offset += limit * page_batch_size
                if any(not page for page in ordered_pages):
                    break

        return list(markets_by_uid.values()), ticks

    async def _fetch_kalshi_market_pages_async(
        self,
        session: aiohttp.ClientSession,
        target_markets: int,
    ) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor = None

        while len(markets) < target_markets:
            url = "https://external-api.kalshi.com/trade-api/v2/markets?limit=100"
            if cursor:
                url += f"&cursor={cursor}"

            payload, status_code = await self._request_json_async(session, url, "kalshi")
            if status_code != 200:
                print("Failed to fetch Kalshi markets.")
                break
            if not isinstance(payload, dict):
                print("Kalshi markets response was not an object.")
                break

            markets_payload = payload.get("markets", [])
            if not markets_payload:
                break

            for market_json in markets_payload:
                if isinstance(market_json, dict):
                    markets.append(market_json)
                if len(markets) >= target_markets:
                    break

            if len(markets) % 5_000 == 0:
                print(f"[Kalshi] Fetched {len(markets)}/{target_markets} market definitions")

            cursor = payload.get("cursor")
            if not cursor:
                break

        return markets[:target_markets]

    async def _fetch_kalshi_trades_async(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        trades: list[dict[str, Any]] = []
        trade_cursor = None

        for _ in range(max_pages):
            trade_url = f"https://external-api.kalshi.com/trade-api/v2/markets/{ticker}/trades?limit=100"
            if trade_cursor:
                trade_url += f"&cursor={trade_cursor}"

            trade_payload, trade_status = await self._request_json_async(session, trade_url, "kalshi")
            if trade_status != 200 or not isinstance(trade_payload, dict):
                break

            batch_trades = trade_payload.get("trades", [])
            if not batch_trades:
                break

            trades.extend(trade for trade in batch_trades if isinstance(trade, dict))
            trade_cursor = trade_payload.get("cursor")
            if not trade_cursor:
                break

        return trades

    async def _process_kalshi_market_async(
        self,
        session: aiohttp.ClientSession,
        market_json: dict[str, Any],
    ) -> tuple[Market | None, list[MarketTick]]:
        ticker = market_json.get("ticker")
        if not ticker:
            return None, []
        ticker = str(ticker)

        market = self._make_market(
            platform="kalshi",
            source_market_id=ticker,
            market_slug=ticker,
            question=market_json.get("title") or ticker,
            close_time=(
                market_json.get("close_time")
                or market_json.get("expiration_time")
                or market_json.get("expected_expiration_time")
            ),
            source=f"kalshi market {ticker}",
        )
        if market is None:
            return None, []

        ticks: list[MarketTick] = []
        trades = await self._fetch_kalshi_trades_async(session, ticker)

        if trades:
            for trade_index, trade in enumerate(trades):
                try:
                    price = self._kalshi_trade_price(trade)
                except (TypeError, ValueError) as exc:
                    self._record_validation_error(f"kalshi trade {ticker} index {trade_index}", exc)
                    continue

                tick = self._make_tick(
                    platform="kalshi",
                    market_uid=market.market_uid,
                    timestamp_value=trade.get("created_time") or trade.get("created_ts") or trade.get("time"),
                    price_value=price,
                    source_key=str(trade.get("trade_id") or trade.get("id") or f"{ticker}:trade:{trade_index}"),
                    source=f"kalshi trade {ticker} index {trade_index}",
                )
                if tick is not None:
                    ticks.append(tick)
        else:
            try:
                snapshot_price = self._kalshi_snapshot_price(market_json)
            except (TypeError, ValueError) as exc:
                self._record_validation_error(f"kalshi snapshot {ticker}", exc)
                snapshot_price = None

            if snapshot_price is not None:
                tick = self._make_tick(
                    platform="kalshi",
                    market_uid=market.market_uid,
                    timestamp_value=market_json.get("updated_time") or market_json.get("close_time"),
                    price_value=snapshot_price,
                    source_key=f"{ticker}:snapshot",
                    source=f"kalshi snapshot {ticker}",
                )
                if tick is not None:
                    ticks.append(tick)

        if not ticks:
            return None, []
        return market, ticks

    async def fetch_kalshi_async(
        self,
        target_markets: int | None = None,
    ) -> tuple[list[Market], list[MarketTick]]:
        target_markets = self.target_markets if target_markets is None else target_markets
        print(f"Scraping Kalshi (Target: {target_markets} markets)...")
        markets_by_uid: dict[str, Market] = {}
        ticks: list[MarketTick] = []
        headers = {"User-Agent": "Mozilla/5.0"}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            market_payloads = await self._fetch_kalshi_market_pages_async(session, target_markets)
            market_results = await asyncio.gather(
                *(self._process_kalshi_market_async(session, market_json) for market_json in market_payloads)
            )

        for market, market_ticks in market_results:
            if market is None or not market_ticks:
                continue
            if market.market_uid in markets_by_uid:
                continue
            markets_by_uid[market.market_uid] = market
            ticks.extend(market_ticks)
            collected = len(markets_by_uid)
            if collected % 50 == 0:
                print(f"[Kalshi] Collected {collected}/{target_markets} markets")
            if collected >= target_markets:
                break

        return list(markets_by_uid.values()), ticks

    async def run_all_async(self) -> None:
        print("--- Starting Multi-Platform Ingestion Engine ---")
        self.validation_errors = []
        self._reset_request_semaphore()

        polymarket_task = self.fetch_polymarket_async()
        kalshi_task = self.fetch_kalshi_async()
        (polymarket_markets, polymarket_ticks), (kalshi_markets, kalshi_ticks) = await asyncio.gather(
            polymarket_task,
            kalshi_task,
        )

        markets_by_uid = {
            market.market_uid: market
            for market in [*polymarket_markets, *kalshi_markets]
        }
        ticks = [*polymarket_ticks, *kalshi_ticks]

        market_columns = list(Market.model_fields.keys())
        tick_columns = list(MarketTick.model_fields.keys())
        markets_df = pd.DataFrame(
            [market.model_dump() for market in markets_by_uid.values()],
            columns=market_columns,
        )
        ticks_df = pd.DataFrame(
            [tick.model_dump() for tick in ticks],
            columns=tick_columns,
        )

        if not markets_df.empty:
            markets_df = markets_df.sort_values(["platform", "market_uid"]).reset_index(drop=True)
        if not ticks_df.empty:
            ticks_df = ticks_df.sort_values(["platform", "market_uid", "timestamp", "tick_uid"]).reset_index(drop=True)

        markets_path = os.path.join(self.output_dir, "markets.parquet")
        ticks_path = os.path.join(self.output_dir, "ticks.parquet")
        markets_df.to_parquet(markets_path, index=False, engine="pyarrow", compression="snappy")
        ticks_df.to_parquet(ticks_path, index=False, engine="pyarrow", compression="snappy")

        print(f"\nSuccessfully wrote {len(markets_df)} markets to {markets_path}.")
        print(f"Successfully wrote {len(ticks_df)} ticks to {ticks_path}.")
        print(f"Raw evidence saved under {self.raw_evidence_dir}.")
        print(f"Breakdown: Polymarket ticks: {len(polymarket_ticks)}, Kalshi ticks: {len(kalshi_ticks)}")

        if self.validation_errors:
            print(f"Skipped {len(self.validation_errors)} invalid records during strict validation.")
            for error in self.validation_errors[:10]:
                print(f" - {error}")
            if len(self.validation_errors) > 10:
                print(f" - ... {len(self.validation_errors) - 10} more")

    def run_all(self) -> None:
        asyncio.run(self.run_all_async())


if __name__ == "__main__":
    engine = IngestionEngine()
    asyncio.run(engine.run_all_async())
