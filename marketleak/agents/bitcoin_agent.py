from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential


class MempoolRateLimitError(RuntimeError):
    """Raised when mempool.space asks the client to slow down."""


@dataclass
class BitcoinWalletEnrichmentResult:
    """Summary of Bitcoin wallet graph enrichment."""

    btc_address: str
    event_node_id: str
    wallet_node_id: str
    cluster_node_id: str
    balance: int | None = None
    funded_txo_sum: int | None = None
    spent_txo_sum: int | None = None
    utxo_count: int = 0
    cluster_type: str | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def enriched(self) -> bool:
        return not self.errors and self.cluster_type is not None


class BitcoinAgent:
    """Fetch Bitcoin wallet data from mempool.space and add it to GraphRepository."""

    WHALE_FUNDED_TXO_SUM = 100_000_000_000
    EXCHANGE_UTXO_COUNT = 10_000

    def __init__(
        self,
        *,
        api_base_url: str = "https://mempool.space/api",
        request_timeout: int = 10,
        session: requests.Session | None = None,
        retry_attempts: int = 5,
        retry_wait: Any | None = None,
    ):
        self.api_base_url = api_base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.session = session or requests.Session()
        self.retry_attempts = retry_attempts
        self.retry_wait = retry_wait or wait_exponential(multiplier=1, min=2, max=30)

    def enrich_bitcoin_wallet(
        self,
        graph_repo: Any,
        btc_address: str,
        event_node_id: str,
    ) -> BitcoinWalletEnrichmentResult:
        address = self._normalize_address(btc_address)
        wallet_node_id = f"btc_wallet:{address}"
        cluster_node_id = f"btc_cluster:{address}"
        result = BitcoinWalletEnrichmentResult(
            btc_address=address,
            event_node_id=event_node_id,
            wallet_node_id=wallet_node_id,
            cluster_node_id=cluster_node_id,
        )

        if not address:
            result.errors.append("Bitcoin address is required.")
            return result
        if not event_node_id:
            result.errors.append("event_node_id is required.")
            return result

        wallet_payload = self._request_json(f"/address/{address}", result)
        utxos_payload = self._request_json(f"/address/{address}/utxo", result)
        if wallet_payload is None or utxos_payload is None:
            return result
        if not isinstance(wallet_payload, dict):
            result.errors.append(
                f"Address endpoint returned malformed JSON for {address}: "
                f"expected object, got {type(wallet_payload).__name__}"
            )
            return result
        if not isinstance(utxos_payload, list):
            result.errors.append(
                f"UTXO endpoint returned malformed JSON for {address}: "
                f"expected list, got {type(utxos_payload).__name__}"
            )
            return result

        chain_stats = wallet_payload.get("chain_stats") or {}
        if not isinstance(chain_stats, dict):
            result.errors.append(
                f"Address endpoint returned malformed chain_stats for {address}: "
                f"expected object, got {type(chain_stats).__name__}"
            )
            return result

        funded_txo_sum = self._safe_int(chain_stats.get("funded_txo_sum"), default=0)
        spent_txo_sum = self._safe_int(chain_stats.get("spent_txo_sum"), default=0)
        utxo_count = len(utxos_payload)
        balance = funded_txo_sum - spent_txo_sum
        cluster_type = self._cluster_type(funded_txo_sum, utxo_count)

        graph_repo.upsert_node(
            wallet_node_id,
            "BitcoinWallet",
            {
                "address": address,
                "balance": balance,
                "funded_txo_sum": funded_txo_sum,
                "spent_txo_sum": spent_txo_sum,
                "utxo_count": utxo_count,
                "source": "mempool.space",
                "last_enriched_at": time.time(),
            },
        )
        graph_repo.upsert_node(
            cluster_node_id,
            "UTXOCluster",
            {
                "cluster_type": cluster_type,
                "heuristic_label": f"{cluster_type}Cluster",
                "address": address,
                "funded_txo_sum": funded_txo_sum,
                "utxo_count": utxo_count,
                "source": "mempool.space",
            },
        )
        graph_repo.upsert_edge(
            wallet_node_id,
            cluster_node_id,
            "BELONGS_TO_CLUSTER",
            {
                "cluster_type": cluster_type,
                "source": "mempool.space",
            },
        )
        graph_repo.upsert_edge(
            event_node_id,
            wallet_node_id,
            "FUNDED_BY_BTC",
            {
                "address": address,
                "balance": balance,
                "source": "mempool.space",
            },
        )

        result.balance = balance
        result.funded_txo_sum = funded_txo_sum
        result.spent_txo_sum = spent_txo_sum
        result.utxo_count = utxo_count
        result.cluster_type = cluster_type
        return result

    def _request_json(self, path: str, result: BitcoinWalletEnrichmentResult) -> Any | None:
        url = f"{self.api_base_url}{path}"
        retryer = Retrying(
            stop=stop_after_attempt(self.retry_attempts),
            wait=self.retry_wait,
            retry=retry_if_exception_type(MempoolRateLimitError),
            reraise=True,
        )

        try:
            return retryer(self._request_json_once, url)
        except MempoolRateLimitError as exc:
            result.errors.append(f"Rate limited while requesting {url}: {exc}")
        except requests.RequestException as exc:
            result.errors.append(f"Request failed for {url}: {exc}")
        except ValueError as exc:
            result.errors.append(f"Invalid JSON returned for {url}: {exc}")
        return None

    def _request_json_once(self, url: str) -> Any:
        response = self.session.get(url, timeout=self.request_timeout)
        if getattr(response, "status_code", None) == 429:
            retry_after = self._retry_after_seconds(response)
            if retry_after:
                time.sleep(retry_after)
            raise MempoolRateLimitError("HTTP 429")

        response.raise_for_status()
        return response.json()

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> float:
        retry_after = getattr(response, "headers", {}).get("Retry-After")
        if retry_after is None:
            return 0.0
        try:
            return max(0.0, min(60.0, float(retry_after)))
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _cluster_type(cls, funded_txo_sum: int, utxo_count: int) -> str:
        if funded_txo_sum > cls.WHALE_FUNDED_TXO_SUM:
            return "Whale"
        if utxo_count > cls.EXCHANGE_UTXO_COUNT:
            return "Exchange"
        return "Retail"

    @staticmethod
    def _normalize_address(address: str) -> str:
        return str(address or "").strip()

    @staticmethod
    def _safe_int(value: Any, *, default: int = 0) -> int:
        if value in (None, ""):
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
