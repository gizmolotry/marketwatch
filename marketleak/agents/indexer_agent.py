from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import web3

from marketleak.schemas import MarketTick


POLYMARKET_CTF_TOKEN = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
TRANSFER_SINGLE_TOPIC0 = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
OUTPUT_DIR = Path("demo_data/onchain_ticks")

CHUNK_SIZE = 10_000
MAX_TICKS = 6_000_000
MAX_BLOCKS = 5_000_000
WRITE_BATCH_SIZE = 100_000
POLYGON_BLOCK_SECONDS = 2.1
VALUE_SCALE = 1_000_000.0
PLATFORM = "polymarket_onchain"

MARKET_TICK_COLUMNS = list(MarketTick.model_fields.keys())
LOGGER = logging.getLogger(__name__)


def _get_log_value(log: Any, key: str) -> Any:
    if hasattr(log, key):
        return getattr(log, key)
    return log[key]


def _hex_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value if value.startswith("0x") else f"0x{value}"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + bytes(value).hex()
    if hasattr(value, "hex"):
        text = value.hex()
        return text if str(text).startswith("0x") else f"0x{text}"
    return str(value)


def _hex_data_to_bytes(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    text = _hex_string(value)
    if text.startswith("0x"):
        text = text[2:]
    if len(text) % 2:
        text = f"0{text}"
    return bytes.fromhex(text)


def _decode_transfer_single_data(log: Any) -> tuple[int, float, float]:
    payload = _hex_data_to_bytes(_get_log_value(log, "data"))
    if len(payload) < 64:
        raise ValueError(f"TransferSingle data must be at least 64 bytes, got {len(payload)}")
    asset_id = int.from_bytes(payload[0:32], "big")
    value = int.from_bytes(payload[32:64], "big")
    
    size = float(value) / VALUE_SCALE
    price = 0.5
        
    return asset_id, price, size


def _price_from_value(value: int) -> float:
    price = float(value) / VALUE_SCALE
    if price < 0.0:
        return 0.0
    if price > 1.0:
        return 1.0
    return price


def _tick_from_log(log: Any, latest_block_num: int, latest_block_time: int) -> dict[str, Any]:
    block_number = int(_get_log_value(log, "blockNumber"))
    log_index = int(_get_log_value(log, "logIndex"))
    tx_hash = _hex_string(_get_log_value(log, "transactionHash"))
    
    topics = _get_log_value(log, "topics")
    if len(topics) < 4:
        raise ValueError("Missing indexed topics for TransferSingle")
        
    maker = "0x" + _hex_string(topics[2])[-40:]
    taker = "0x" + _hex_string(topics[3])[-40:]
    
    asset_id, price, size = _decode_transfer_single_data(log)

    timestamp = latest_block_time - int((latest_block_num - block_number) * POLYGON_BLOCK_SECONDS)
    if timestamp < 0:
        timestamp = 0

    return {
        "tick_uid": f"{tx_hash}:{log_index}",
        "market_uid": str(asset_id),
        "timestamp": int(timestamp),
        "price": price,
        "size": size,
        "maker": web3.Web3.to_checksum_address(maker),
        "taker": web3.Web3.to_checksum_address(taker),
        "platform": PLATFORM,
    }


def _dataframe_from_ticks(ticks: Iterable[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(ticks, columns=MARKET_TICK_COLUMNS)
    if df.empty:
        return df.astype(
            {
                "tick_uid": "string",
                "market_uid": "string",
                "timestamp": "int64",
                "price": "float64",
                "size": "float64",
                "maker": "string",
                "taker": "string",
                "platform": "string",
            }
        )

    df = df.dropna(subset=["tick_uid", "market_uid", "timestamp", "price", "size", "maker", "taker", "platform"])
    df["tick_uid"] = df["tick_uid"].astype(str)
    df["market_uid"] = df["market_uid"].astype(str)
    df["timestamp"] = pd.to_numeric(df["timestamp"], downcast="integer").astype("int64")
    df["price"] = pd.to_numeric(df["price"]).clip(lower=0.0, upper=1.0).astype("float64")
    df["size"] = pd.to_numeric(df["size"]).astype("float64")
    df["maker"] = df["maker"].astype(str)
    df["taker"] = df["taker"].astype(str)
    df["platform"] = df["platform"].astype(str)
    return df[MARKET_TICK_COLUMNS]


def _market_tick_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            ("tick_uid", pa.string()),
            ("market_uid", pa.string()),
            ("timestamp", pa.int64()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("maker", pa.string()),
            ("taker", pa.string()),
            ("platform", pa.string()),
        ]
    )


async def _fetch_logs_with_retries(
    w3: web3.AsyncWeb3,
    *,
    from_block: int,
    to_block: int,
    retries: int = 5,
) -> list[Any]:
    params = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": POLYMARKET_CTF_TOKEN,
        "topics": [TRANSFER_SINGLE_TOPIC0],
    }
    delay = 1.0
    for attempt in range(retries):
        try:
            return list(await w3.eth.get_logs(params))
        except Exception as exc:
            if attempt == retries - 1:
                if from_block < to_block:
                    mid_block = (from_block + to_block) // 2
                    LOGGER.warning(
                        "eth_getLogs failed for %s-%s; splitting range after error: %s",
                        from_block,
                        to_block,
                        exc,
                    )
                    lower, upper = await asyncio.gather(
                        _fetch_logs_with_retries(w3, from_block=from_block, to_block=mid_block, retries=retries),
                        _fetch_logs_with_retries(w3, from_block=mid_block + 1, to_block=to_block, retries=retries),
                    )
                    return [*lower, *upper]
                raise
            LOGGER.warning(
                "eth_getLogs attempt %s/%s failed for blocks %s-%s: %s",
                attempt + 1,
                retries,
                from_block,
                to_block,
                exc,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
    return []


async def sync_onchain_ticks(
    *,
    rpc_url: str | None = None,
    output_dir: Path | str = OUTPUT_DIR,
    chunk_size: int = CHUNK_SIZE,
    max_ticks: int = MAX_TICKS,
    max_blocks: int = MAX_BLOCKS,
    write_batch_size: int = WRITE_BATCH_SIZE,
) -> int:
    rpc_url = rpc_url or os.getenv("POLYGON_RPC_URL")
    if not rpc_url:
        raise ValueError("POLYGON_RPC_URL environment variable is required")
    
    output_dir = Path(output_dir)
    staging_dir = output_dir / "staging"
    published_dir = output_dir / "published"
    staging_dir.mkdir(parents=True, exist_ok=True)
    published_dir.mkdir(parents=True, exist_ok=True)

    w3 = web3.AsyncWeb3(web3.AsyncHTTPProvider(rpc_url))
    from web3.middleware import ExtraDataToPOAMiddleware
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    
    latest_block = await w3.eth.get_block("latest")
    latest_block_num = int(_get_log_value(latest_block, "number"))
    latest_block_time = int(_get_log_value(latest_block, "timestamp"))

    LOGGER.info(
        "Starting Polygon CTF TransferSingle sync from block %s to %s blocks back",
        latest_block_num,
        max_blocks,
    )

    writer: pq.ParquetWriter | None = None
    schema = _market_tick_arrow_schema()
    pending_ticks: list[dict[str, Any]] = []
    collected = 0
    chunks_processed = 0
    blocks_processed = 0
    current_to_block = latest_block_num

    try:
        while collected + len(pending_ticks) < max_ticks and blocks_processed < max_blocks and current_to_block >= 0:
            current_from_block = max(0, current_to_block - chunk_size + 1)
            logs = await _fetch_logs_with_retries(
                w3,
                from_block=current_from_block,
                to_block=current_to_block,
            )
            chunks_processed += 1
            blocks_processed += current_to_block - current_from_block + 1

            for log in logs:
                if collected + len(pending_ticks) >= max_ticks:
                    break
                try:
                    pending_ticks.append(_tick_from_log(log, latest_block_num, latest_block_time))
                except (KeyError, TypeError, ValueError) as exc:
                    LOGGER.debug("Skipping malformed OrderFilled log: %s", exc)

            if len(pending_ticks) >= write_batch_size:
                batch_df = _dataframe_from_ticks(pending_ticks)
                if not batch_df.empty:
                    table = pa.Table.from_pandas(batch_df, schema=schema, preserve_index=False)
                    filename = f"ticks_{current_from_block}_{current_to_block}.parquet"
                    staging_path = staging_dir / filename
                    published_path = published_dir / filename
                    
                    # Atomic write and rename
                    with pq.ParquetWriter(staging_path, schema=schema, compression="zstd") as writer:
                        writer.write_table(table)
                    staging_path.rename(published_path)
                    
                    collected += len(batch_df)
                pending_ticks.clear()

            if chunks_processed % 10 == 0:
                LOGGER.info(
                    "Processed %s chunks / %s blocks; collected %s ticks; next block %s",
                    chunks_processed,
                    blocks_processed,
                    collected + len(pending_ticks),
                    current_from_block - 1,
                )

            current_to_block = current_from_block - 1

        if pending_ticks:
            batch_df = _dataframe_from_ticks(pending_ticks)
            if not batch_df.empty:
                table = pa.Table.from_pandas(batch_df, schema=schema, preserve_index=False)
                filename = f"ticks_{current_from_block}_{current_to_block}_tail.parquet"
                staging_path = staging_dir / filename
                published_path = published_dir / filename
                
                with pq.ParquetWriter(staging_path, schema=schema, compression="zstd") as writer:
                    writer.write_table(table)
                staging_path.rename(published_path)
                collected += len(batch_df)

        LOGGER.info("Finished sync with %s ticks written to %s", collected, published_dir)
        return collected
    finally:
        if writer is not None:
            writer.close()
        disconnect = getattr(w3.provider, "disconnect", None)
        if disconnect is not None:
            result = disconnect()
            if asyncio.iscoroutine(result):
                await result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index Polymarket CTF TransferSingle logs from Polygon.")
    parser.add_argument("--rpc-url", default=None, help="Polygon RPC URL. Defaults to POLYGON_RPC_URL.")
    parser.add_argument("--output", default=str(OUTPUT_DIR), help="Parquet output directory.")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Block range per eth_getLogs request.")
    parser.add_argument("--max-ticks", type=int, default=MAX_TICKS, help="Maximum ticks to collect.")
    parser.add_argument("--max-blocks", type=int, default=MAX_BLOCKS, help="Maximum blocks to process.")
    parser.add_argument("--write-batch-size", type=int, default=WRITE_BATCH_SIZE, help="Rows per parquet write batch.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


async def _main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await sync_onchain_ticks(
        rpc_url=args.rpc_url,
        output_dir=args.output,
        chunk_size=args.chunk_size,
        max_ticks=args.max_ticks,
        max_blocks=args.max_blocks,
        write_batch_size=args.write_batch_size,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
