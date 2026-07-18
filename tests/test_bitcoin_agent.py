import requests

from marketleak.agents.bitcoin_agent import BitcoinAgent
from marketleak.graph.repository import GraphRepository


BTC_ADDRESS = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class FakeMempoolSession:
    def __init__(self, *, funded_txo_sum, spent_txo_sum=10, utxo_count=2):
        self.funded_txo_sum = funded_txo_sum
        self.spent_txo_sum = spent_txo_sum
        self.utxo_count = utxo_count

    def get(self, url, timeout=None):
        if url.endswith(f"/address/{BTC_ADDRESS}"):
            return FakeResponse(
                {
                    "address": BTC_ADDRESS,
                    "chain_stats": {
                        "funded_txo_sum": self.funded_txo_sum,
                        "spent_txo_sum": self.spent_txo_sum,
                    },
                }
            )
        if url.endswith(f"/address/{BTC_ADDRESS}/utxo"):
            return FakeResponse([{"txid": str(index)} for index in range(self.utxo_count)])
        raise AssertionError(f"Unexpected URL: {url}")


def test_bitcoin_agent_adds_wallet_cluster_and_event_edges():
    repo = GraphRepository()
    agent = BitcoinAgent(session=FakeMempoolSession(funded_txo_sum=1_000, spent_txo_sum=400, utxo_count=2))

    result = agent.enrich_bitcoin_wallet(repo, BTC_ADDRESS, "event:market-1")

    assert result.enriched
    assert result.balance == 600
    assert result.cluster_type == "Retail"

    wallet_node_id = f"btc_wallet:{BTC_ADDRESS}"
    cluster_node_id = f"btc_cluster:{BTC_ADDRESS}"
    assert repo.graph.nodes[wallet_node_id]["label"] == "BitcoinWallet"
    assert repo.graph.nodes[wallet_node_id]["address"] == BTC_ADDRESS
    assert repo.graph.nodes[wallet_node_id]["balance"] == 600
    assert repo.graph.nodes[wallet_node_id]["funded_txo_sum"] == 1_000
    assert repo.graph.nodes[cluster_node_id]["label"] == "UTXOCluster"
    assert repo.graph.nodes[cluster_node_id]["cluster_type"] == "Retail"

    edge_types = [data["type"] for _, _, data in repo.graph.edges(data=True)]
    assert "BELONGS_TO_CLUSTER" in edge_types
    assert "FUNDED_BY_BTC" in edge_types


def test_bitcoin_agent_classifies_whale_before_exchange():
    repo = GraphRepository()
    agent = BitcoinAgent(
        session=FakeMempoolSession(
            funded_txo_sum=BitcoinAgent.WHALE_FUNDED_TXO_SUM + 1,
            utxo_count=BitcoinAgent.EXCHANGE_UTXO_COUNT + 1,
        )
    )

    result = agent.enrich_bitcoin_wallet(repo, BTC_ADDRESS, "event:market-1")

    assert result.cluster_type == "Whale"
    assert repo.graph.nodes[f"btc_cluster:{BTC_ADDRESS}"]["cluster_type"] == "Whale"


def test_bitcoin_agent_classifies_exchange_by_utxo_count():
    repo = GraphRepository()
    agent = BitcoinAgent(
        session=FakeMempoolSession(
            funded_txo_sum=BitcoinAgent.WHALE_FUNDED_TXO_SUM,
            utxo_count=BitcoinAgent.EXCHANGE_UTXO_COUNT + 1,
        )
    )

    result = agent.enrich_bitcoin_wallet(repo, BTC_ADDRESS, "event:market-1")

    assert result.cluster_type == "Exchange"
    assert repo.graph.nodes[f"btc_cluster:{BTC_ADDRESS}"]["cluster_type"] == "Exchange"
