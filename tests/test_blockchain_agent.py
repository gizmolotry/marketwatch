from types import SimpleNamespace

import requests

from marketleak.agents.blockchain_agent import BlockchainAgent, BlockchainEnrichmentResult
from marketleak.graph.repository import GraphRepository


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"
TOKEN = "0x3333333333333333333333333333333333333333"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakePolygonSession:
    def get(self, url, params=None, timeout=None):
        action = params["action"]
        if action == "txlist":
            return FakeResponse(
                {
                    "status": "1",
                    "message": "OK",
                    "result": [
                        {
                            "blockNumber": "700",
                            "timeStamp": "1783200000",
                            "hash": "0xaaa",
                            "from": WALLET_A,
                            "to": WALLET_B,
                            "value": "1000000000000000000",
                            "methodId": "0x",
                            "functionName": "",
                            "isError": "0",
                        }
                    ],
                }
            )
        if action == "tokentx":
            return FakeResponse(
                {
                    "status": "1",
                    "message": "OK",
                    "result": [
                        {
                            "blockNumber": "701",
                            "timeStamp": "1783200010",
                            "hash": "0xbbb",
                            "from": WALLET_B,
                            "to": WALLET_A,
                            "contractAddress": TOKEN,
                            "value": "5000000",
                            "tokenName": "USD Coin",
                            "tokenSymbol": "USDC",
                            "tokenDecimal": "6",
                        }
                    ],
                }
            )
        raise AssertionError(f"Unexpected action: {action}")

    def post(self, url, json=None, timeout=None):
        if json["method"] == "eth_getBalance":
            return FakeResponse({"jsonrpc": "2.0", "id": json["id"], "result": "0xde0b6b3a7640000"})
        if json["method"] == "eth_getTransactionCount":
            return FakeResponse({"jsonrpc": "2.0", "id": json["id"], "result": "0x2"})
        raise AssertionError(f"Unexpected RPC method: {json['method']}")


def test_blockchain_agent_adds_polygon_wallet_transactions_and_edges():
    repo = GraphRepository()
    agent = BlockchainAgent(api_key="test", session=FakePolygonSession(), tx_limit=2)
    candidate = SimpleNamespace(
        market_uid="market-1",
        market_slug="polygon-market",
        question="Will the test event happen?",
        shock_timestamp=1783200000.0,
    )
    row = {
        "market_uid": "market-1",
        "market_slug": "polygon-market",
        "wallet_address": WALLET_A,
        "timestamp": 1783200000.0,
    }

    result = agent.enrich_graph_for_anomaly(repo, candidate, row, event_uid="event:market-1")

    assert result.wallet_node_ids == [f"wallet:{WALLET_A.lower()}"]
    assert result.normal_transaction_count == 1
    assert result.token_transfer_count == 1
    assert result.rpc_observation_count == 1

    assert repo.graph.nodes[f"wallet:{WALLET_A.lower()}"]["label"] == "Wallet"
    assert repo.graph.nodes[f"wallet:{WALLET_A.lower()}"]["balance_wei"] == 10**18
    assert repo.graph.nodes["polygon_tx:0xaaa"]["label"] == "Transaction"
    assert repo.graph.nodes[f"polygon_token:{TOKEN.lower()}"]["token_symbol"] == "USDC"
    assert repo.graph.nodes[f"polygon_rpc_state:{WALLET_A.lower()}"]["label"] == "RPCState"
    assert repo.graph.nodes[f"polygon_rpc_state:{WALLET_A.lower()}"]["transaction_count"] == 2

    edge_types = [data["type"] for _, _, data in repo.graph.edges(data=True)]
    assert "ASSOCIATED_WITH_ANOMALY" in edge_types
    assert "TRANSFERRED_TO" in edge_types
    assert "TOKEN_TRANSFERRED_TO" in edge_types
    assert "HAS_RPC_STATE" in edge_types

    paths = repo.find_evidence_paths(f"wallet:{WALLET_A.lower()}", ["Event"])
    assert any(path == [f"wallet:{WALLET_A.lower()}", "event:market-1"] for path, _ in paths)


class SecretEchoingSession:
    def get(self, url, params=None, timeout=None):
        raise requests.RequestException(
            f"request rejected at {url}?apikey={params['apikey']}&signature=signed-value"
        )


def test_blockchain_agent_never_exposes_api_or_signed_query_secrets_in_errors():
    api_key = "etherscan-live-secret"
    agent = BlockchainAgent(api_key=api_key, session=SecretEchoingSession())
    result = BlockchainEnrichmentResult(market_uid="market-1", event_node_id="event:market-1")

    records = agent._fetch_account_records_once("txlist", WALLET_A, result)

    assert records == []
    rendered = " ".join(result.errors)
    assert api_key not in rendered
    assert "signed-value" not in rendered
    assert rendered.endswith("<RequestException>")
