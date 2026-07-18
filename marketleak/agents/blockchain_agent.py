from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential


EVM_ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")


class PolygonscanRateLimitError(RuntimeError):
    """Raised when Polygonscan/Etherscan asks the client to retry later."""


@dataclass
class BlockchainEnrichmentResult:
    """Summary of graph enrichment performed for an anomaly."""

    market_uid: str
    event_node_id: str
    wallet_addresses: list[str] = field(default_factory=list)
    wallet_node_ids: list[str] = field(default_factory=list)
    normal_transaction_count: int = 0
    token_transfer_count: int = 0
    rpc_observation_count: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def added_blockchain_records(self) -> int:
        return self.normal_transaction_count + self.token_transfer_count + self.rpc_observation_count


class BlockchainAgent:
    """Fetch Polygon wallet activity and project it into GraphRepository."""

    ADDRESS_FIELDS = {
        "address",
        "wallet",
        "wallet_address",
        "trader_wallet",
        "trader_address",
        "proxy_wallet",
        "proxyWallet",
        "marketMakerAddress",
        "submitted_by",
        "resolvedBy",
        "maker",
        "taker",
        "from",
        "to",
    }

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_url: str = "https://api.etherscan.io/v2/api",
        chain_id: str = "137",
        rpc_url: str | None = None,
        request_timeout: int = 10,
        tx_limit: int = 25,
        raw_evidence_dir: str | Path = "demo_data/raw_evidence",
        raw_evidence_scan_limit: int = 50,
        session: requests.Session | None = None,
        polygonscan_retry_attempts: int = 5,
        polygonscan_retry_wait: Any | None = None,
    ):
        self.api_key = api_key or os.getenv("POLYGONSCAN_API_KEY") or os.getenv("ETHERSCAN_API_KEY")
        self.api_url = api_url
        self.chain_id = str(chain_id)
        self.rpc_url = rpc_url or os.getenv("POLYGON_RPC_URL") or "https://polygon-rpc.com"
        self.request_timeout = request_timeout
        self.tx_limit = tx_limit
        self.raw_evidence_dir = Path(raw_evidence_dir)
        self.raw_evidence_scan_limit = raw_evidence_scan_limit
        self.session = session or requests.Session()
        self.polygonscan_retry_attempts = polygonscan_retry_attempts
        self.polygonscan_retry_wait = polygonscan_retry_wait or wait_exponential(multiplier=1, min=2, max=10)

    @staticmethod
    def normalize_address(address: str) -> str:
        return address.lower()

    @classmethod
    def is_evm_address(cls, value: Any) -> bool:
        return isinstance(value, str) and bool(EVM_ADDRESS_RE.fullmatch(value.strip()))

    @classmethod
    def _extract_addresses(cls, value: Any) -> list[str]:
        addresses: list[str] = []
        if value is None:
            return addresses
        if isinstance(value, str):
            return [cls.normalize_address(match.group(0)) for match in EVM_ADDRESS_RE.finditer(value)]
        if isinstance(value, dict):
            for item_value in value.values():
                addresses.extend(cls._extract_addresses(item_value))
            return addresses
        if isinstance(value, (list, tuple, set)):
            for item in value:
                addresses.extend(cls._extract_addresses(item))
        return addresses

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                result.append(value)
        return result

    @staticmethod
    def _row_get(row: Any, key: str, default: Any = None) -> Any:
        if hasattr(row, "get"):
            return row.get(key, default)
        return getattr(row, key, default)

    def wallets_from_anomaly(self, anomaly_row: Any) -> list[str]:
        addresses: list[str] = []
        for field_name in self.ADDRESS_FIELDS:
            addresses.extend(self._extract_addresses(self._row_get(anomaly_row, field_name)))
        return self._dedupe_preserve_order(addresses)

    def wallets_from_environment(self) -> list[str]:
        raw = os.getenv("MARKETLEAK_SUSPICIOUS_WALLETS", "")
        return self._dedupe_preserve_order(self._extract_addresses(raw))

    def wallets_from_raw_evidence(self, market_slug: str | None, market_uid: str | None) -> list[str]:
        if not self.raw_evidence_dir.exists():
            return []

        identifiers = {value for value in [market_slug, market_uid] if value}
        if not identifiers:
            return []

        addresses: list[str] = []
        files = sorted(
            self.raw_evidence_dir.glob("polymarket_raw_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in files[: self.raw_evidence_scan_limit]:
            try:
                import json

                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue

            for market_json in self._iter_dicts(payload):
                if not self._dict_matches_market(market_json, identifiers):
                    continue
                addresses.extend(self._extract_addresses_from_address_fields(market_json))

        return self._dedupe_preserve_order(addresses)

    def collect_wallets_for_anomaly(self, anomaly_row: Any) -> list[str]:
        market_slug = str(self._row_get(anomaly_row, "market_slug", "") or "")
        market_uid = str(self._row_get(anomaly_row, "market_uid", "") or "")

        addresses: list[str] = []
        addresses.extend(self.wallets_from_anomaly(anomaly_row))
        addresses.extend(self.wallets_from_environment())
        addresses.extend(self.wallets_from_raw_evidence(market_slug, market_uid))
        return self._dedupe_preserve_order(addresses)

    def enrich_graph_for_anomaly(
        self,
        graph_repo: Any,
        anomaly_candidate: Any,
        anomaly_row: Any,
        *,
        event_uid: str | None = None,
    ) -> BlockchainEnrichmentResult:
        market_uid = str(getattr(anomaly_candidate, "market_uid", None) or self._row_get(anomaly_row, "market_uid", "unknown"))
        market_slug = str(getattr(anomaly_candidate, "market_slug", None) or self._row_get(anomaly_row, "market_slug", market_uid))
        question = str(getattr(anomaly_candidate, "question", None) or self._row_get(anomaly_row, "question", ""))
        shock_timestamp = float(getattr(anomaly_candidate, "shock_timestamp", 0.0) or self._row_get(anomaly_row, "timestamp", 0.0) or 0.0)
        event_node_id = event_uid or f"event:{market_uid}"

        graph_repo.upsert_node(
            event_node_id,
            "Event",
            {
                "market_uid": market_uid,
                "market_slug": market_slug,
                "question": question,
                "source": "marketleak.anomaly",
            },
        )

        result = BlockchainEnrichmentResult(market_uid=market_uid, event_node_id=event_node_id)
        wallets = self.collect_wallets_for_anomaly(anomaly_row)
        if not wallets:
            result.errors.append("No Polygon wallet addresses were available on the anomaly row, env, or raw evidence.")
            return result

        graph_repo.upsert_node(
            "chain:polygon",
            "Blockchain",
            {"chain_id": self.chain_id, "name": "Polygon PoS", "source": "polygon_rpc"},
        )

        for address in wallets:
            wallet_node_id = self._wallet_node_id(address)
            result.wallet_addresses.append(address)
            result.wallet_node_ids.append(wallet_node_id)

            graph_repo.upsert_node(
                wallet_node_id,
                "Wallet",
                {
                    "address": address,
                    "chain": "polygon",
                    "chain_id": self.chain_id,
                    "market_uid": market_uid,
                    "market_slug": market_slug,
                    "source": "polygon",
                    "last_enriched_at": time.time(),
                },
            )
            graph_repo.upsert_edge(
                wallet_node_id,
                result.event_node_id,
                "ASSOCIATED_WITH_ANOMALY",
                {
                    "market_uid": market_uid,
                    "market_slug": market_slug,
                    "shock_timestamp": shock_timestamp,
                    "source": "marketleak.anomaly",
                },
            )
            graph_repo.upsert_edge(
                wallet_node_id,
                "chain:polygon",
                "OBSERVED_ON_CHAIN",
                {"source": "polygon_rpc", "chain_id": self.chain_id},
            )

            rpc_observed = self._enrich_wallet_rpc(graph_repo, address, wallet_node_id, result)
            if rpc_observed:
                result.rpc_observation_count += 1

            normal_transactions = self._fetch_account_records("txlist", address, result)
            for tx in normal_transactions:
                if self._add_normal_transaction(graph_repo, tx):
                    result.normal_transaction_count += 1

            token_transfers = self._fetch_account_records("tokentx", address, result)
            for tx in token_transfers:
                if self._add_token_transfer(graph_repo, tx):
                    result.token_transfer_count += 1

        result.wallet_node_ids = self._dedupe_preserve_order(result.wallet_node_ids)
        result.wallet_addresses = self._dedupe_preserve_order(result.wallet_addresses)
        return result

    def _fetch_account_records(self, action: str, address: str, result: BlockchainEnrichmentResult) -> list[dict[str, Any]]:
        retryer = Retrying(
            stop=stop_after_attempt(self.polygonscan_retry_attempts),
            wait=self.polygonscan_retry_wait,
            retry=retry_if_exception_type(PolygonscanRateLimitError),
            reraise=True,
        )
        return retryer(self._fetch_account_records_once, action, address, result)

    def _fetch_account_records_once(self, action: str, address: str, result: BlockchainEnrichmentResult) -> list[dict[str, Any]]:
        if not self.api_key:
            return []

        params = {
            "chainid": self.chain_id,
            "module": "account",
            "action": action,
            "address": address,
            "startblock": 0,
            "endblock": 999999999,
            "page": 1,
            "offset": self.tx_limit,
            "sort": "desc",
            "apikey": self.api_key,
        }
        try:
            response = self.session.get(self.api_url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            result.errors.append(f"{action} request failed for {address}: {exc}")
            return []
        except ValueError as exc:
            result.errors.append(f"{action} returned invalid JSON for {address}: {exc}")
            return []

        if not isinstance(payload, dict):
            result.errors.append(
                f"{action} returned malformed JSON payload for {address}: expected object, got {type(payload).__name__}"
            )
            return []

        records = payload.get("result")
        if payload.get("status") == "1" and isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]

        message = str(payload.get("message", "")).lower()
        record_message = str(records or "").lower()
        if self._is_api_limit_message(message) or self._is_api_limit_message(record_message):
            raise PolygonscanRateLimitError(
                f"{action} rate limited for {address}: {payload.get('message') or records}"
            )

        if "no transactions" in message:
            return []

        if records:
            result.errors.append(f"{action} returned non-success response for {address}: {payload.get('message') or records}")
        return []

    @staticmethod
    def _is_api_limit_message(message: str) -> bool:
        return any(
            marker in message
            for marker in (
                "rate limit",
                "rate-limit",
                "too many requests",
                "limit reached",
                "throttle",
                "usage cap",
            )
        )

    def _enrich_wallet_rpc(
        self,
        graph_repo: Any,
        address: str,
        wallet_node_id: str,
        result: BlockchainEnrichmentResult,
    ) -> bool:
        observed = False
        balance = self._rpc("eth_getBalance", [address, "latest"], result)
        tx_count = self._rpc("eth_getTransactionCount", [address, "latest"], result)

        properties: dict[str, Any] = {}
        if isinstance(balance, str):
            properties["balance_wei"] = self._hex_to_int(balance)
            observed = True
        if isinstance(tx_count, str):
            properties["transaction_count"] = self._hex_to_int(tx_count)
            observed = True

        if properties:
            properties.update({"address": address, "chain": "polygon", "source": "polygon_rpc"})
            state_node_id = f"polygon_rpc_state:{address}"
            graph_repo.upsert_node(wallet_node_id, "Wallet", properties)
            graph_repo.upsert_node(
                state_node_id,
                "RPCState",
                {
                    "address": address,
                    "chain": "polygon",
                    "chain_id": self.chain_id,
                    "balance_wei": properties.get("balance_wei"),
                    "transaction_count": properties.get("transaction_count"),
                    "observed_at": time.time(),
                    "source": "polygon_rpc",
                },
            )
            graph_repo.upsert_edge(
                wallet_node_id,
                state_node_id,
                "HAS_RPC_STATE",
                {
                    "balance_wei": properties.get("balance_wei"),
                    "transaction_count": properties.get("transaction_count"),
                    "source": "polygon_rpc",
                },
            )
            graph_repo.upsert_edge(
                state_node_id,
                "chain:polygon",
                "OBSERVED_ON_CHAIN",
                {"source": "polygon_rpc", "chain_id": self.chain_id},
            )
        return observed

    def _rpc(self, method: str, params: list[Any], result: BlockchainEnrichmentResult) -> Any:
        if not self.rpc_url:
            return None
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": method}
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            result.errors.append(f"RPC {method} failed: {exc}")
            return None
        except ValueError as exc:
            result.errors.append(f"RPC {method} returned invalid JSON: {exc}")
            return None
        if not isinstance(data, dict):
            result.errors.append(f"RPC {method} returned malformed JSON payload: expected object, got {type(data).__name__}")
            return None
        if "error" in data:
            result.errors.append(f"RPC {method} error: {data['error']}")
            return None
        return data.get("result")

    def _add_normal_transaction(self, graph_repo: Any, tx: dict[str, Any]) -> bool:
        from_addr = self._normalized_tx_address(tx.get("from"))
        to_addr = self._normalized_tx_address(tx.get("to"))
        tx_hash = str(tx.get("hash") or "").strip().lower()
        if not from_addr or not to_addr or not tx_hash:
            return False

        tx_node_id = f"polygon_tx:{tx_hash}"
        self._upsert_wallet_pair(graph_repo, from_addr, to_addr)
        graph_repo.upsert_node(
            tx_node_id,
            "Transaction",
            {
                "hash": tx_hash,
                "chain": "polygon",
                "chain_id": self.chain_id,
                "block_number": self._safe_int(tx.get("blockNumber")),
                "timestamp": self._safe_int(tx.get("timeStamp")),
                "value_wei": self._safe_int(tx.get("value")),
                "method_id": tx.get("methodId"),
                "function_name": tx.get("functionName"),
                "is_error": tx.get("isError"),
                "source": "polygonscan.txlist",
            },
        )
        graph_repo.upsert_edge(self._wallet_node_id(from_addr), tx_node_id, "SENT_TRANSACTION", {"source": "polygonscan.txlist"})
        graph_repo.upsert_edge(tx_node_id, self._wallet_node_id(to_addr), "RECEIVED_BY", {"source": "polygonscan.txlist"})
        graph_repo.upsert_edge(
            self._wallet_node_id(from_addr),
            self._wallet_node_id(to_addr),
            "TRANSFERRED_TO",
            {
                "tx_hash": tx_hash,
                "value_wei": self._safe_int(tx.get("value")),
                "timestamp": self._safe_int(tx.get("timeStamp")),
                "source": "polygonscan.txlist",
            },
        )
        return True

    def _add_token_transfer(self, graph_repo: Any, tx: dict[str, Any]) -> bool:
        from_addr = self._normalized_tx_address(tx.get("from"))
        to_addr = self._normalized_tx_address(tx.get("to"))
        tx_hash = str(tx.get("hash") or "").strip().lower()
        contract = self._normalized_tx_address(tx.get("contractAddress"))
        if not from_addr or not to_addr or not tx_hash:
            return False

        tx_node_id = f"polygon_tx:{tx_hash}"
        token_node_id = f"polygon_token:{contract}" if contract else f"polygon_token:unknown:{tx_hash}"
        self._upsert_wallet_pair(graph_repo, from_addr, to_addr)
        graph_repo.upsert_node(
            tx_node_id,
            "Transaction",
            {
                "hash": tx_hash,
                "chain": "polygon",
                "chain_id": self.chain_id,
                "block_number": self._safe_int(tx.get("blockNumber")),
                "timestamp": self._safe_int(tx.get("timeStamp")),
                "source": "polygonscan.tokentx",
            },
        )
        graph_repo.upsert_node(
            token_node_id,
            "Token",
            {
                "contract_address": contract,
                "token_name": tx.get("tokenName"),
                "token_symbol": tx.get("tokenSymbol"),
                "token_decimal": self._safe_int(tx.get("tokenDecimal")),
                "chain": "polygon",
                "source": "polygonscan.tokentx",
            },
        )
        edge_properties = {
            "tx_hash": tx_hash,
            "token_contract": contract,
            "token_symbol": tx.get("tokenSymbol"),
            "value": tx.get("value"),
            "timestamp": self._safe_int(tx.get("timeStamp")),
            "source": "polygonscan.tokentx",
        }
        graph_repo.upsert_edge(self._wallet_node_id(from_addr), tx_node_id, "SENT_TOKEN_TRANSFER", edge_properties)
        graph_repo.upsert_edge(tx_node_id, token_node_id, "TRANSFERS_TOKEN", edge_properties)
        graph_repo.upsert_edge(tx_node_id, self._wallet_node_id(to_addr), "TOKEN_RECEIVED_BY", edge_properties)
        graph_repo.upsert_edge(self._wallet_node_id(from_addr), self._wallet_node_id(to_addr), "TOKEN_TRANSFERRED_TO", edge_properties)
        return True

    def _upsert_wallet_pair(self, graph_repo: Any, from_addr: str, to_addr: str) -> None:
        for address in [from_addr, to_addr]:
            graph_repo.upsert_node(
                self._wallet_node_id(address),
                "Wallet",
                {"address": address, "chain": "polygon", "chain_id": self.chain_id, "source": "polygonscan"},
            )

    @classmethod
    def _extract_addresses_from_address_fields(cls, data: dict[str, Any]) -> list[str]:
        addresses: list[str] = []
        for key, value in data.items():
            if key in cls.ADDRESS_FIELDS or "address" in key.lower() or "wallet" in key.lower():
                addresses.extend(cls._extract_addresses(value))
        return addresses

    @classmethod
    def _dict_matches_market(cls, data: dict[str, Any], identifiers: set[str]) -> bool:
        fields = ["slug", "market_slug", "id", "conditionId", "questionID"]
        for field_name in fields:
            value = data.get(field_name)
            if value is not None and str(value) in identifiers:
                return True
        return False

    @classmethod
    def _iter_dicts(cls, value: Any):
        if isinstance(value, dict):
            yield value
            for item in value.values():
                yield from cls._iter_dicts(item)
        elif isinstance(value, list):
            for item in value:
                yield from cls._iter_dicts(item)

    @classmethod
    def _normalized_tx_address(cls, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        match = EVM_ADDRESS_RE.fullmatch(value.strip())
        if not match:
            return None
        return cls.normalize_address(value)

    @staticmethod
    def _wallet_node_id(address: str) -> str:
        return f"wallet:{address.lower()}"

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _hex_to_int(value: str) -> int | None:
        try:
            return int(value, 16)
        except (TypeError, ValueError):
            return None
